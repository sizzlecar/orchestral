//! Session-scoped `/goal` autonomous objective state, command parser, and built-in tools.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use orchestral_core::agent_protocol::spi::AgentJournalStore;
use orchestral_core::agent_protocol::wire::{AgentSessionId, ToolActivityEvidence};
use orchestral_core::tool_protocol::{
    EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency,
    ToolInvocation, ToolOutcome, ToolOutput, ToolRestriction,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};

/// Default maximum autonomous turns for `/goal <objective>` when `--max-turns` is not specified.
pub const DEFAULT_GOAL_MAX_TURNS: u32 = 8;

/// Hard upper bound on `--max-turns` to prevent runaway autonomous loops.
pub const MAX_GOAL_MAX_TURNS: u32 = 50;

/// Lifecycle state of a Session's autonomous goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionGoalStatus {
    Active,
    Paused,
    Completed,
    Blocked,
}

impl SessionGoalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
        }
    }
}

/// Serializable snapshot of a Session's goal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionGoalView {
    pub session_id: String,
    pub objective: String,
    pub status: SessionGoalStatus,
    pub turns_used: u32,
    pub max_turns: u32,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_note: Option<String>,
}

impl SessionGoalView {
    /// Formats the initial prompt for Turn 1 of the goal.
    pub fn initial_prompt(&self) -> String {
        format!(
            "[Autonomous Goal · turn {}/{}]\n\
             Objective: {}\n\n\
             Work systematically toward this objective and verify your changes.\n\
             - When the objective is fully satisfied and verified, call `goal_update` with `status: \"completed\"` and a concise `summary`.\n\
             - If you are making multi-turn progress and need another turn, you may call `goal_update` with `status: \"in_progress\"` and a `summary` checkpoint.\n\
             - If you hit an unrecoverable blocker or require a user decision, call `goal_update` with `status: \"blocked\"` (or ask the user via input request) so autonomous continuation pauses.",
            self.turns_used, self.max_turns, self.objective
        )
    }

    /// Formats the continuation prompt for subsequent autonomous turns.
    pub fn continuation_prompt(&self) -> String {
        let note = self
            .progress_note
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| format!("\nLatest checkpoint note: {s}"))
            .unwrap_or_default();
        format!(
            "[Autonomous Goal Continuation · turn {}/{}]\n\
             Objective: {}{}\n\n\
             Continue working toward the objective from the current workspace and session state.\n\
             - Inspect remaining work and run verification before claiming completion.\n\
             - Once the objective is completely met and verified, call `goal_update` with `status: \"completed\"` and a concise `summary`.\n\
             - If blocked on user input or external failure, call `goal_update` with `status: \"blocked\"` and explain why.",
            self.turns_used, self.max_turns, self.objective, note
        )
    }
}

#[derive(Debug, Clone)]
struct StoredGoal {
    objective: String,
    status: SessionGoalStatus,
    turns_used: u32,
    max_turns: u32,
    created_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    progress_note: Option<String>,
    awaiting_turn_completion: bool,
}

impl StoredGoal {
    fn to_view(&self, session_id: &AgentSessionId) -> SessionGoalView {
        SessionGoalView {
            session_id: session_id.as_str().to_owned(),
            objective: self.objective.clone(),
            status: self.status,
            turns_used: self.turns_used,
            max_turns: self.max_turns,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
            progress_note: self.progress_note.clone(),
        }
    }
}

/// Shared, thread-safe registry of per-Session goals.
#[derive(Clone, Default)]
pub struct SessionGoalRegistry {
    inner: Arc<Mutex<BTreeMap<String, StoredGoal>>>,
}

impl SessionGoalRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets or replaces the active goal for `session_id` and claims Turn 1 for immediate execution.
    pub fn start(
        &self,
        session_id: &AgentSessionId,
        objective: String,
        max_turns: Option<u32>,
    ) -> Result<SessionGoalView, String> {
        let objective = objective.trim().to_owned();
        if objective.is_empty() {
            return Err("goal objective must not be empty".to_owned());
        }
        let max_turns = max_turns.unwrap_or(DEFAULT_GOAL_MAX_TURNS);
        if !(1..=MAX_GOAL_MAX_TURNS).contains(&max_turns) {
            return Err(format!(
                "max_turns must be between 1 and {MAX_GOAL_MAX_TURNS} (got {max_turns})"
            ));
        }
        let now_ms = unix_now_ms();
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        let stored = StoredGoal {
            objective,
            status: SessionGoalStatus::Active,
            turns_used: 1,
            max_turns,
            created_at_unix_ms: now_ms,
            updated_at_unix_ms: now_ms,
            progress_note: None,
            awaiting_turn_completion: true,
        };
        let view = stored.to_view(session_id);
        guard.insert(session_id.as_str().to_owned(), stored);
        Ok(view)
    }

    /// Sets or updates a goal without immediately claiming a host-driven turn (e.g. when called by the Agent inside an already running turn).
    pub fn set_from_agent(
        &self,
        session_id: &AgentSessionId,
        objective: Option<String>,
        status: SessionGoalStatus,
        max_turns: Option<u32>,
        progress_note: Option<String>,
    ) -> Result<SessionGoalView, String> {
        let now_ms = unix_now_ms();
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        if let Some(existing) = guard.get_mut(session_id.as_str()) {
            if let Some(objective) = objective {
                let trimmed = objective.trim();
                if trimmed.is_empty() {
                    return Err("goal objective must not be empty".to_owned());
                }
                existing.objective = trimmed.to_owned();
            }
            if let Some(max_turns) = max_turns {
                if !(1..=MAX_GOAL_MAX_TURNS).contains(&max_turns) {
                    return Err(format!(
                        "max_turns must be between 1 and {MAX_GOAL_MAX_TURNS} (got {max_turns})"
                    ));
                }
                existing.max_turns = max_turns;
            }
            existing.status = status;
            if let Some(note) = progress_note {
                let trimmed = note.trim();
                existing.progress_note = (!trimmed.is_empty()).then(|| trimmed.to_owned());
            }
            existing.updated_at_unix_ms = now_ms;
            return Ok(existing.to_view(session_id));
        }

        let objective = objective
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "no active goal in this Session; provide `objective` to start one".to_owned()
            })?
            .to_owned();
        let max_turns = max_turns.unwrap_or(DEFAULT_GOAL_MAX_TURNS);
        if !(1..=MAX_GOAL_MAX_TURNS).contains(&max_turns) {
            return Err(format!(
                "max_turns must be between 1 and {MAX_GOAL_MAX_TURNS} (got {max_turns})"
            ));
        }
        let stored = StoredGoal {
            objective,
            status,
            turns_used: 1,
            max_turns,
            created_at_unix_ms: now_ms,
            updated_at_unix_ms: now_ms,
            progress_note: progress_note
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty()),
            awaiting_turn_completion: matches!(status, SessionGoalStatus::Active),
        };
        let view = stored.to_view(session_id);
        guard.insert(session_id.as_str().to_owned(), stored);
        Ok(view)
    }

    pub fn get(&self, session_id: &AgentSessionId) -> Option<SessionGoalView> {
        let guard = self.inner.lock().expect("goal registry lock poisoned");
        guard
            .get(session_id.as_str())
            .map(|stored| stored.to_view(session_id))
    }

    pub fn pause(&self, session_id: &AgentSessionId) -> Option<SessionGoalView> {
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        let stored = guard.get_mut(session_id.as_str())?;
        if stored.status == SessionGoalStatus::Active {
            stored.status = SessionGoalStatus::Paused;
        }
        stored.awaiting_turn_completion = false;
        stored.updated_at_unix_ms = unix_now_ms();
        Some(stored.to_view(session_id))
    }

    /// Resumes a paused or blocked goal, or triggers the next turn of an active goal.
    pub fn resume_next_turn(&self, session_id: &AgentSessionId) -> Result<SessionGoalView, String> {
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        let stored = guard
            .get_mut(session_id.as_str())
            .ok_or_else(|| "No goal is configured in this session".to_owned())?;
        if stored.status == SessionGoalStatus::Completed {
            return Err("Goal is already completed; start a new `/goal <objective>`".to_owned());
        }
        if stored.turns_used >= stored.max_turns {
            stored.max_turns = (stored.turns_used + DEFAULT_GOAL_MAX_TURNS).min(MAX_GOAL_MAX_TURNS);
        }
        stored.status = SessionGoalStatus::Active;
        stored.turns_used = stored.turns_used.saturating_add(1);
        stored.awaiting_turn_completion = true;
        stored.updated_at_unix_ms = unix_now_ms();
        Ok(stored.to_view(session_id))
    }

    pub fn clear(&self, session_id: &AgentSessionId) -> Option<SessionGoalView> {
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        guard
            .remove(session_id.as_str())
            .map(|stored| stored.to_view(session_id))
    }

    /// Notifies the goal registry that a Run in `session_id` has finished.
    /// If `completed_cleanly` is false (e.g., cancelled, failed, or incomplete), an active goal is paused so it does not spin in a failure loop.
    pub fn notify_run_finished(
        &self,
        session_id: &AgentSessionId,
        completed_cleanly: bool,
    ) -> Option<SessionGoalView> {
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        let stored = guard.get_mut(session_id.as_str())?;
        stored.awaiting_turn_completion = false;
        if stored.status == SessionGoalStatus::Active
            && (!completed_cleanly || stored.turns_used >= stored.max_turns)
        {
            stored.status = SessionGoalStatus::Paused;
            stored.updated_at_unix_ms = unix_now_ms();
        }
        Some(stored.to_view(session_id))
    }

    /// Claims the next autonomous continuation turn if the goal is `Active`, not currently awaiting a turn, and under `max_turns`.
    pub fn pop_continuation(&self, session_id: &AgentSessionId) -> Option<SessionGoalView> {
        let mut guard = self.inner.lock().expect("goal registry lock poisoned");
        let stored = guard.get_mut(session_id.as_str())?;
        if stored.status != SessionGoalStatus::Active || stored.awaiting_turn_completion {
            return None;
        }
        if stored.turns_used >= stored.max_turns {
            stored.status = SessionGoalStatus::Paused;
            stored.updated_at_unix_ms = unix_now_ms();
            return None;
        }
        stored.turns_used = stored.turns_used.saturating_add(1);
        stored.awaiting_turn_completion = true;
        stored.updated_at_unix_ms = unix_now_ms();
        Some(stored.to_view(session_id))
    }
}

/// Parsed representation of a `/goal` slash command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedGoalCommand {
    /// `/goal` or `/goal status`: open the goal status/management menu.
    Status,
    /// `/goal stop` or `/goal pause`: pause autonomous continuation.
    Pause,
    /// `/goal resume` or `/goal continue`: resume a paused/blocked goal.
    Resume,
    /// `/goal clear` or `/goal reset`: remove the current goal.
    Clear,
    /// `/goal [--max-turns N] <objective>`: start a new autonomous goal.
    Start {
        objective: String,
        max_turns: Option<u32>,
    },
}

/// Parses a `/goal` command string.
pub fn parse_goal_command(raw: &str) -> Result<ParsedGoalCommand, String> {
    let trimmed = raw.trim();
    let rest = trimmed.strip_prefix("/goal").unwrap_or(trimmed).trim();
    if rest.is_empty() || rest.eq_ignore_ascii_case("status") || rest.eq_ignore_ascii_case("list") {
        return Ok(ParsedGoalCommand::Status);
    }
    if matches!(
        rest.to_ascii_lowercase().as_str(),
        "stop" | "pause" | "off" | "halt"
    ) {
        return Ok(ParsedGoalCommand::Pause);
    }
    if matches!(
        rest.to_ascii_lowercase().as_str(),
        "resume" | "continue" | "next"
    ) {
        return Ok(ParsedGoalCommand::Resume);
    }
    if matches!(
        rest.to_ascii_lowercase().as_str(),
        "clear" | "reset" | "delete" | "remove"
    ) {
        return Ok(ParsedGoalCommand::Clear);
    }

    let mut max_turns = None;
    let mut tokens = rest.split_whitespace().peekable();
    let mut objective_parts = Vec::new();
    while let Some(token) = tokens.next() {
        if token == "--max-turns" || token == "--turns" || token == "-n" {
            let value = tokens
                .next()
                .ok_or_else(|| format!("{token} requires a positive integer"))?;
            let parsed = value.parse::<u32>().map_err(|_| {
                format!("invalid max turns '{value}'; expected 1..={MAX_GOAL_MAX_TURNS}")
            })?;
            if !(1..=MAX_GOAL_MAX_TURNS).contains(&parsed) {
                return Err(format!(
                    "max_turns must be between 1 and {MAX_GOAL_MAX_TURNS} (got {parsed})"
                ));
            }
            max_turns = Some(parsed);
        } else if let Some(value) = token
            .strip_prefix("--max-turns=")
            .or_else(|| token.strip_prefix("--turns="))
        {
            let parsed = value.parse::<u32>().map_err(|_| {
                format!("invalid max turns '{value}'; expected 1..={MAX_GOAL_MAX_TURNS}")
            })?;
            if !(1..=MAX_GOAL_MAX_TURNS).contains(&parsed) {
                return Err(format!(
                    "max_turns must be between 1 and {MAX_GOAL_MAX_TURNS} (got {parsed})"
                ));
            }
            max_turns = Some(parsed);
        } else {
            objective_parts.push(token);
        }
    }

    let objective = objective_parts.join(" ");
    if objective.trim().is_empty() {
        return Err("Usage: /goal [--max-turns N] <objective> | /goal status | /goal pause | /goal resume | /goal clear".to_owned());
    }
    Ok(ParsedGoalCommand::Start {
        objective,
        max_turns,
    })
}

async fn resolve_invoking_session_id(
    runs: &Arc<dyn AgentJournalStore>,
    execution: &GuardedToolExecution,
) -> Result<AgentSessionId, String> {
    let run = runs
        .load_run(&execution.invocation.run_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("invoking Run is not registered")?;
    run.validate_shape().map_err(|error| error.to_string())?;
    let spec = &run.registration.run().spec;
    if spec.run_id != execution.invocation.run_id {
        return Err("registered Run identity mismatch".to_owned());
    }
    Ok(spec.session_id.clone())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoalUpdateArgs {
    status: String,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    max_turns: Option<u32>,
}

/// Built-in `goal_update` Tool executor allowing the Agent to report goal completion, checkpoints, or blockers.
#[derive(Clone)]
pub struct GuardedGoalUpdateExecutor {
    runs: Arc<dyn AgentJournalStore>,
    goals: SessionGoalRegistry,
}

impl GuardedGoalUpdateExecutor {
    pub fn new(runs: Arc<dyn AgentJournalStore>, goals: SessionGoalRegistry) -> Self {
        Self { runs, goals }
    }

    async fn update(&self, execution: &GuardedToolExecution) -> Result<Value, String> {
        let args: GoalUpdateArgs = serde_json::from_value(execution.invocation.arguments.clone())
            .map_err(|error| error.to_string())?;
        let status = match args.status.trim().to_ascii_lowercase().as_str() {
            "in_progress" | "active" | "running" => SessionGoalStatus::Active,
            "completed" | "complete" | "done" => SessionGoalStatus::Completed,
            "blocked" => SessionGoalStatus::Blocked,
            "paused" | "pause" => SessionGoalStatus::Paused,
            other => {
                return Err(format!(
                    "unsupported goal status '{other}'; expected 'in_progress', 'completed', 'blocked', or 'paused'"
                ))
            }
        };
        let session_id = resolve_invoking_session_id(&self.runs, execution).await?;
        let view = self.goals.set_from_agent(
            &session_id,
            args.objective,
            status,
            args.max_turns,
            args.summary,
        )?;
        serde_json::to_value(view).map_err(|error| error.to_string())
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedGoalUpdateExecutor {
    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        if let Some(ToolOutcome::Completed {
            output: ToolOutput::Inline(value),
        }) = outcome
        {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("updated");
            let turns_used = value.get("turns_used").and_then(Value::as_u64).unwrap_or(1);
            let max_turns = value.get("max_turns").and_then(Value::as_u64).unwrap_or(1);
            let note = value
                .get("progress_note")
                .and_then(Value::as_str)
                .or_else(|| value.get("objective").and_then(Value::as_str))
                .unwrap_or_default();
            return vec![ToolActivityEvidence::Note {
                text: format!("goal {status} ({turns_used}/{max_turns}): {note}"),
            }];
        }
        let status = invocation
            .arguments
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("update");
        vec![ToolActivityEvidence::Note {
            text: format!("goal {status}"),
        }]
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        match self.update(&execution).await {
            Ok(output) => ToolOutcome::Completed {
                output: output.into(),
            },
            Err(message) => ToolOutcome::Rejected {
                code: "goal_update_invalid".to_owned(),
                message,
            },
        }
    }
}

/// Descriptor for the built-in `goal_update` Tool.
pub fn guarded_goal_update_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/goal_update/v1"),
        model_schema: ModelToolSchema {
            name: "goal_update".to_owned(),
            description: "Update the status or checkpoint note of the current Session's autonomous `/goal`. Call with status='completed' when the user's goal is fully satisfied and verified so autonomous continuation stops cleanly; call with status='in_progress' to record a checkpoint summary across turns; or call with status='blocked' if user intervention is required.".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["in_progress", "completed", "blocked", "paused"],
                        "description": "Goal status: 'completed' when verified done, 'in_progress' to continue across turns, 'blocked' or 'paused' to stop automatic continuation."
                    },
                    "summary": {
                        "type": "string",
                        "description": "Concise progress checkpoint, verification summary, or blocker reason."
                    },
                    "objective": {
                        "type": "string",
                        "description": "Optional objective text when starting or refining a Session goal."
                    },
                    "max_turns": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_GOAL_MAX_TURNS,
                        "description": "Optional maximum autonomous turns (1..=50)."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": [
                "session_id",
                "objective",
                "status",
                "turns_used",
                "max_turns",
                "created_at_unix_ms",
                "updated_at_unix_ms"
            ],
            "properties": {
                "session_id": { "type": "string" },
                "objective": { "type": "string" },
                "status": { "type": "string" },
                "turns_used": { "type": "integer" },
                "max_turns": { "type": "integer" },
                "created_at_unix_ms": { "type": "integer" },
                "updated_at_unix_ms": { "type": "integer" },
                "progress_note": { "type": "string" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::SessionRead]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_goal_commands() {
        assert_eq!(
            parse_goal_command("/goal").unwrap(),
            ParsedGoalCommand::Status
        );
        assert_eq!(
            parse_goal_command("/goal status").unwrap(),
            ParsedGoalCommand::Status
        );
        assert_eq!(
            parse_goal_command("/goal stop").unwrap(),
            ParsedGoalCommand::Pause
        );
        assert_eq!(
            parse_goal_command("/goal resume").unwrap(),
            ParsedGoalCommand::Resume
        );
        assert_eq!(
            parse_goal_command("/goal clear").unwrap(),
            ParsedGoalCommand::Clear
        );
        assert_eq!(
            parse_goal_command("/goal --max-turns 5 fix all clippy warnings").unwrap(),
            ParsedGoalCommand::Start {
                objective: "fix all clippy warnings".to_owned(),
                max_turns: Some(5),
            }
        );
        assert_eq!(
            parse_goal_command("/goal implement /goal command --max-turns=12").unwrap(),
            ParsedGoalCommand::Start {
                objective: "implement /goal command".to_owned(),
                max_turns: Some(12),
            }
        );
    }

    #[test]
    fn goal_registry_tracks_turns_and_stops_on_completion_or_failure() {
        let registry = SessionGoalRegistry::new();
        let session = AgentSessionId::new("s1");
        let started = registry
            .start(&session, "ship feature".to_owned(), Some(3))
            .unwrap();
        assert_eq!(started.turns_used, 1);
        assert_eq!(started.max_turns, 3);
        assert_eq!(started.status, SessionGoalStatus::Active);
        // While Turn 1 is still running, pop_continuation returns None.
        assert!(registry.pop_continuation(&session).is_none());

        // Turn 1 finishes cleanly -> Turn 2 can be popped.
        registry.notify_run_finished(&session, true);
        let turn2 = registry.pop_continuation(&session).unwrap();
        assert_eq!(turn2.turns_used, 2);

        // Agent marks completed during Turn 2 -> no Turn 3 after Turn 2 finishes.
        registry
            .set_from_agent(
                &session,
                None,
                SessionGoalStatus::Completed,
                None,
                Some("all tests pass".to_owned()),
            )
            .unwrap();
        registry.notify_run_finished(&session, true);
        assert!(registry.pop_continuation(&session).is_none());
        assert_eq!(
            registry.get(&session).unwrap().status,
            SessionGoalStatus::Completed
        );
    }
}
