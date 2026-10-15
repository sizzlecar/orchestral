//! Guarded, Session-scoped scheduled task (timer / `/loop`) registry and Tools.
//!
//! Scheduled tasks belong to the Session derived from the Host-registered
//! invoking Run (or the active TUI Session). Model arguments cannot target
//! another Session.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use orchestral_core::agent_protocol::spi::AgentJournalStore;
use orchestral_core::agent_protocol::wire::{AgentSessionId, ToolActivityEvidence};
use orchestral_core::tool_protocol::{
    EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency,
    ToolInvocation, ToolOutcome, ToolOutput, ToolRestriction,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};

/// Default recurring interval when `/loop <prompt>` omits an explicit duration.
pub const DEFAULT_LOOP_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Minimum allowed schedule interval or delay.
pub const MIN_SCHEDULE_INTERVAL: Duration = Duration::from_secs(1);
/// Maximum allowed schedule interval or delay (7 days).
pub const MAX_SCHEDULE_INTERVAL: Duration = Duration::from_secs(7 * 86_400);
/// Recurring session tasks automatically expire after 3 days.
pub const MAX_RECURRING_LIFETIME: Duration = Duration::from_secs(3 * 86_400);
/// Maximum active scheduled tasks retained per Session.
pub const MAX_SCHEDULES_PER_SESSION: usize = 50;

/// Serializable view of one active scheduled task in a Session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledTaskView {
    pub id: String,
    pub session_id: String,
    pub prompt: String,
    pub schedule_spec: String,
    pub interval_secs: u64,
    pub recurring: bool,
    pub created_at_unix_ms: u64,
    pub next_fire_unix_ms: u64,
    pub remaining_secs: u64,
    pub fire_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_runs: Option<u64>,
}

/// Due scheduled task claimed for execution by the Session host loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiredScheduledTask {
    pub id: String,
    pub session_id: AgentSessionId,
    pub prompt: String,
    pub schedule_spec: String,
    pub recurring: bool,
    pub fire_count: u64,
    pub completed: bool,
}

impl FiredScheduledTask {
    /// Formats the user-turn prompt delivered to the Session when a timer fires.
    pub fn formatted_prompt(&self) -> String {
        format!(
            "[Scheduled {} ({})] {}",
            self.id, self.schedule_spec, self.prompt
        )
    }
}

/// Parsed representation of a user `/loop` slash command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedLoopCommand {
    List,
    ClearAll,
    Delete {
        id: String,
    },
    RunNow {
        id: String,
    },
    Create {
        prompt: String,
        interval: Duration,
        recurring: bool,
    },
}

#[derive(Debug, Clone)]
struct ScheduledTaskEntry {
    id: String,
    numeric_id: u64,
    session_id: AgentSessionId,
    prompt: String,
    schedule_spec: String,
    interval: Duration,
    recurring: bool,
    created_at: Instant,
    created_at_unix_ms: u64,
    next_fire_at: Instant,
    fire_count: u64,
    max_runs: Option<u64>,
}

impl ScheduledTaskEntry {
    fn to_view(&self, now: Instant, now_unix_ms: u64) -> ScheduledTaskView {
        let remaining = self.next_fire_at.saturating_duration_since(now);
        let remaining_secs = if remaining.is_zero() {
            0
        } else {
            remaining.as_secs().max(1)
        };
        let next_fire_unix_ms =
            now_unix_ms.saturating_add(u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX));
        ScheduledTaskView {
            id: self.id.clone(),
            session_id: self.session_id.as_str().to_owned(),
            prompt: self.prompt.clone(),
            schedule_spec: self.schedule_spec.clone(),
            interval_secs: self.interval.as_secs(),
            recurring: self.recurring,
            created_at_unix_ms: self.created_at_unix_ms,
            next_fire_unix_ms,
            remaining_secs,
            fire_count: self.fire_count,
            max_runs: self.max_runs,
        }
    }

    fn matches_id(&self, query: &str) -> bool {
        let query = query.trim().trim_start_matches('#');
        self.id.eq_ignore_ascii_case(query)
            || query
                .parse::<u64>()
                .is_ok_and(|numeric| numeric == self.numeric_id)
    }
}

#[derive(Default)]
struct RegistryState {
    next_id: u64,
    sessions: BTreeMap<String, Vec<ScheduledTaskEntry>>,
}

/// Thread-safe, Session-scoped scheduled task registry shared by Host UI and Tools.
#[derive(Clone, Default)]
pub struct SessionScheduleRegistry {
    state: Arc<Mutex<RegistryState>>,
    notify: Arc<Notify>,
}

impl SessionScheduleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Future that resolves whenever any Session schedule is created, fired, or removed.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    pub fn create(
        &self,
        session_id: &AgentSessionId,
        prompt: impl Into<String>,
        interval: Duration,
        recurring: bool,
        max_runs: Option<u64>,
    ) -> Result<ScheduledTaskView, String> {
        let prompt = prompt.into().trim().to_owned();
        if prompt.is_empty() {
            return Err("prompt must not be empty".to_owned());
        }
        if prompt.len() > 16_384 {
            return Err("prompt must fit within 16384 UTF-8 bytes".to_owned());
        }
        let secs = interval.as_secs();
        if interval < MIN_SCHEDULE_INTERVAL {
            return Err("schedule interval must be at least 1s".to_owned());
        }
        if interval > MAX_SCHEDULE_INTERVAL {
            return Err("schedule interval cannot exceed 7d".to_owned());
        }
        if max_runs == Some(0) {
            return Err("max_runs must be at least 1 when provided".to_owned());
        }
        let normalized_interval = Duration::from_secs(secs);
        let schedule_spec = if recurring {
            format!("every {}", format_duration_compact(secs))
        } else {
            format!("once in {}", format_duration_compact(secs))
        };
        let now = Instant::now();
        let now_unix_ms = current_unix_ms();
        let view = {
            let mut guard = self.state.lock().expect("schedule registry lock poisoned");
            {
                let entries = guard
                    .sessions
                    .entry(session_id.as_str().to_owned())
                    .or_default();
                prune_expired_entries(entries, now);
                if entries.len() >= MAX_SCHEDULES_PER_SESSION {
                    return Err(format!(
                        "Session already has the maximum of {MAX_SCHEDULES_PER_SESSION} active scheduled tasks"
                    ));
                }
            }
            guard.next_id = guard.next_id.saturating_add(1);
            let numeric_id = guard.next_id;
            let entry = ScheduledTaskEntry {
                id: format!("loop-{numeric_id}"),
                numeric_id,
                session_id: session_id.clone(),
                prompt,
                schedule_spec,
                interval: normalized_interval,
                recurring,
                created_at: now,
                created_at_unix_ms: now_unix_ms,
                next_fire_at: now + normalized_interval,
                fire_count: 0,
                max_runs: if recurring { max_runs } else { Some(1) },
            };
            let view = entry.to_view(now, now_unix_ms);
            guard
                .sessions
                .entry(session_id.as_str().to_owned())
                .or_default()
                .push(entry);
            view
        };
        self.notify.notify_waiters();
        Ok(view)
    }

    pub fn list(&self, session_id: &AgentSessionId) -> Vec<ScheduledTaskView> {
        let now = Instant::now();
        let now_unix_ms = current_unix_ms();
        let mut guard = self.state.lock().expect("schedule registry lock poisoned");
        let Some(entries) = guard.sessions.get_mut(session_id.as_str()) else {
            return Vec::new();
        };
        prune_expired_entries(entries, now);
        entries
            .iter()
            .map(|entry| entry.to_view(now, now_unix_ms))
            .collect()
    }

    pub fn count(&self, session_id: &AgentSessionId) -> usize {
        let now = Instant::now();
        let mut guard = self.state.lock().expect("schedule registry lock poisoned");
        let Some(entries) = guard.sessions.get_mut(session_id.as_str()) else {
            return 0;
        };
        prune_expired_entries(entries, now);
        entries.len()
    }

    pub fn delete(&self, session_id: &AgentSessionId, id: &str) -> Option<ScheduledTaskView> {
        let now = Instant::now();
        let now_unix_ms = current_unix_ms();
        let removed = {
            let mut guard = self.state.lock().expect("schedule registry lock poisoned");
            let entries = guard.sessions.get_mut(session_id.as_str())?;
            prune_expired_entries(entries, now);
            let index = entries.iter().position(|entry| entry.matches_id(id))?;
            entries.remove(index).to_view(now, now_unix_ms)
        };
        self.notify.notify_waiters();
        Some(removed)
    }

    pub fn clear_session(&self, session_id: &AgentSessionId) -> Vec<ScheduledTaskView> {
        let now = Instant::now();
        let now_unix_ms = current_unix_ms();
        let removed = {
            let mut guard = self.state.lock().expect("schedule registry lock poisoned");
            let Some(mut entries) = guard.sessions.remove(session_id.as_str()) else {
                return Vec::new();
            };
            prune_expired_entries(&mut entries, now);
            entries
                .into_iter()
                .map(|entry| entry.to_view(now, now_unix_ms))
                .collect::<Vec<_>>()
        };
        if !removed.is_empty() {
            self.notify.notify_waiters();
        }
        removed
    }

    /// Returns the earliest `next_fire_at` instant for the given Session.
    pub fn next_deadline(&self, session_id: &AgentSessionId) -> Option<Instant> {
        let now = Instant::now();
        let mut guard = self.state.lock().expect("schedule registry lock poisoned");
        let entries = guard.sessions.get_mut(session_id.as_str())?;
        prune_expired_entries(entries, now);
        entries.iter().map(|entry| entry.next_fire_at).min()
    }

    /// Claims the earliest due task (`next_fire_at <= now`) for the given Session.
    ///
    /// Recurring tasks advance `next_fire_at` to `now + interval`, coalescing
    /// any missed intervals while the Session was busy with a previous Run.
    pub fn pop_due(&self, session_id: &AgentSessionId, now: Instant) -> Option<FiredScheduledTask> {
        let fired = {
            let mut guard = self.state.lock().expect("schedule registry lock poisoned");
            let entries = guard.sessions.get_mut(session_id.as_str())?;
            prune_expired_entries(entries, now);
            let (due_index, _) = entries
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.next_fire_at <= now)
                .min_by_key(|(_, entry)| (entry.next_fire_at, entry.numeric_id))?;
            Some(fire_entry_at(entries, due_index, now))
        };
        if fired.is_some() {
            self.notify.notify_waiters();
        }
        fired
    }

    /// Immediately claims and triggers a scheduled task by ID (e.g. from `/loop run <id>`).
    pub fn trigger_now(
        &self,
        session_id: &AgentSessionId,
        id: &str,
        now: Instant,
    ) -> Option<FiredScheduledTask> {
        let fired = {
            let mut guard = self.state.lock().expect("schedule registry lock poisoned");
            let entries = guard.sessions.get_mut(session_id.as_str())?;
            prune_expired_entries(entries, now);
            let index = entries.iter().position(|entry| entry.matches_id(id))?;
            Some(fire_entry_at(entries, index, now))
        };
        if fired.is_some() {
            self.notify.notify_waiters();
        }
        fired
    }

    /// Marks a scheduled task as immediately due (`next_fire_at = now`) so the Session loop fires it.
    pub fn mark_due_now(
        &self,
        session_id: &AgentSessionId,
        id: &str,
        now: Instant,
    ) -> Option<ScheduledTaskView> {
        let now_unix_ms = current_unix_ms();
        let view = {
            let mut guard = self.state.lock().expect("schedule registry lock poisoned");
            let entries = guard.sessions.get_mut(session_id.as_str())?;
            prune_expired_entries(entries, now);
            let entry = entries.iter_mut().find(|entry| entry.matches_id(id))?;
            entry.next_fire_at = now;
            entry.to_view(now, now_unix_ms)
        };
        self.notify.notify_waiters();
        Some(view)
    }
}

fn prune_expired_entries(entries: &mut Vec<ScheduledTaskEntry>, now: Instant) {
    entries.retain(|entry| {
        !entry.recurring || now.saturating_duration_since(entry.created_at) < MAX_RECURRING_LIFETIME
    });
}

fn fire_entry_at(
    entries: &mut Vec<ScheduledTaskEntry>,
    index: usize,
    now: Instant,
) -> FiredScheduledTask {
    let entry = &mut entries[index];
    entry.fire_count = entry.fire_count.saturating_add(1);
    let reached_max = entry.max_runs.is_some_and(|max| entry.fire_count >= max);
    let expired = entry.recurring
        && now.saturating_duration_since(entry.created_at) >= MAX_RECURRING_LIFETIME;
    let completed = !entry.recurring || reached_max || expired;
    let fired = FiredScheduledTask {
        id: entry.id.clone(),
        session_id: entry.session_id.clone(),
        prompt: entry.prompt.clone(),
        schedule_spec: entry.schedule_spec.clone(),
        recurring: entry.recurring,
        fire_count: entry.fire_count,
        completed,
    };
    if completed {
        entries.remove(index);
    } else {
        entry.next_fire_at = now + entry.interval;
    }
    fired
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Formats a duration in seconds as a compact human-readable string (`30s`, `5m`, `1h30m`).
pub fn format_duration_compact(mut secs: u64) -> String {
    if secs == 0 {
        return "0s".to_owned();
    }
    let days = secs / 86_400;
    secs %= 86_400;
    let hours = secs / 3_600;
    secs %= 3_600;
    let mins = secs / 60;
    secs %= 60;
    let mut out = String::new();
    if days > 0 {
        out.push_str(&format!("{days}d"));
    }
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if mins > 0 {
        out.push_str(&format!("{mins}m"));
    }
    if secs > 0 {
        out.push_str(&format!("{secs}s"));
    }
    out
}

/// Parses an interval or cron specification into `(Duration, Option<recurring_override>)`.
///
/// Accepts compact durations (`30s`, `5m`, `1h30m`, `1d`), spaced units
/// (`5 minutes`, `2 hours`, `10秒`, `5分钟`), prefixed forms (`every 5m`,
/// `in 10m`, `once 30s`), and common 5-field cron intervals (`*/5 * * * *`,
/// `0 * * * *`, `0 */2 * * *`).
pub fn parse_schedule_interval(spec: &str) -> Result<(Duration, Option<bool>), String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err("interval specification must not be empty".to_owned());
    }
    let lower = trimmed.to_lowercase();
    if let Some(rest) = lower.strip_prefix("every ") {
        let duration = parse_raw_duration_or_cron(rest.trim())?;
        return Ok((duration, Some(true)));
    }
    for prefix in ["once in ", "once ", "in "] {
        if let Some(rest) = lower.strip_prefix(prefix) {
            let duration = parse_raw_duration_or_cron(rest.trim())?;
            return Ok((duration, Some(false)));
        }
    }
    if trimmed.split_whitespace().count() == 5 {
        let duration = parse_cron_interval(trimmed)?;
        return Ok((duration, Some(true)));
    }
    let duration = parse_raw_duration_or_cron(trimmed)?;
    Ok((duration, None))
}

fn parse_raw_duration_or_cron(input: &str) -> Result<Duration, String> {
    if input.split_whitespace().count() == 5 {
        return parse_cron_interval(input);
    }
    parse_duration_tokens(input)
}

fn parse_cron_interval(cron: &str) -> Result<Duration, String> {
    let fields: Vec<&str> = cron.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(format!("expected 5-field cron expression, got '{cron}'"));
    }
    let (minute, hour, dom, month, dow) = (fields[0], fields[1], fields[2], fields[3], fields[4]);
    if dom != "*" || month != "*" || dow != "*" {
        return Err(format!(
            "unsupported day/month restriction in cron expression '{cron}'; use interval format like '5m', '1h', or '*/5 * * * *'"
        ));
    }
    if minute == "*" && hour == "*" {
        return Ok(Duration::from_secs(60));
    }
    if let Some(step) = minute.strip_prefix("*/") {
        if hour == "*" {
            let mins: u64 = step
                .parse()
                .map_err(|_| format!("invalid minute step in cron '{cron}'"))?;
            if mins == 0 {
                return Err("cron minute step must be at least 1".to_owned());
            }
            return Ok(Duration::from_secs(mins.saturating_mul(60)));
        }
    }
    if minute.parse::<u8>().is_ok_and(|m| m < 60) {
        if hour == "*" {
            return Ok(Duration::from_secs(3_600));
        }
        if let Some(step) = hour.strip_prefix("*/") {
            let hours: u64 = step
                .parse()
                .map_err(|_| format!("invalid hour step in cron '{cron}'"))?;
            if hours == 0 {
                return Err("cron hour step must be at least 1".to_owned());
            }
            return Ok(Duration::from_secs(hours.saturating_mul(3_600)));
        }
        if hour.parse::<u8>().is_ok_and(|h| h < 24) {
            return Ok(Duration::from_secs(86_400));
        }
    }
    Err(format!(
        "could not convert cron '{cron}' to a session timer interval; use e.g. '5m', '1h', or '*/5 * * * *'"
    ))
}

fn unit_multiplier_secs(unit: &str) -> Option<u64> {
    match unit.trim().to_lowercase().as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" | "秒" | "秒钟" => Some(1),
        "m" | "min" | "mins" | "minute" | "minutes" | "分" | "分钟" => Some(60),
        "h" | "hr" | "hrs" | "hour" | "hours" | "时" | "小时" => Some(3_600),
        "d" | "day" | "days" | "天" => Some(86_400),
        _ => None,
    }
}

fn parse_duration_tokens(input: &str) -> Result<Duration, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("empty duration".to_owned());
    }
    let mut total_secs: u64 = 0;
    let mut chars = trimmed.chars().peekable();
    let mut found_any = false;
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        let mut digits = String::new();
        while let Some(&d) = chars.peek() {
            if d.is_ascii_digit() {
                digits.push(d);
                chars.next();
            } else {
                break;
            }
        }
        if digits.is_empty() {
            return Err(format!(
                "invalid duration '{trimmed}' (expected e.g. 30s, 5m, 1h, 1d)"
            ));
        }
        let value: u64 = digits
            .parse()
            .map_err(|_| format!("invalid duration number '{digits}'"))?;
        while let Some(&ws) = chars.peek() {
            if ws.is_whitespace() {
                chars.next();
            } else {
                break;
            }
        }
        let mut unit = String::new();
        while let Some(&u) = chars.peek() {
            if !u.is_ascii_digit() && !u.is_whitespace() {
                unit.push(u);
                chars.next();
            } else {
                break;
            }
        }
        if unit.is_empty() {
            return Err(format!(
                "missing time unit after '{digits}' in '{trimmed}' (use s, m, h, or d)"
            ));
        }
        let mult = unit_multiplier_secs(&unit).ok_or_else(|| {
            format!("unknown time unit '{unit}' in '{trimmed}' (use s, m, h, or d)")
        })?;
        total_secs = total_secs.saturating_add(value.saturating_mul(mult));
        found_any = true;
    }
    if !found_any || total_secs == 0 {
        return Err("duration must be at least 1s".to_owned());
    }
    let duration = Duration::from_secs(total_secs);
    if duration > MAX_SCHEDULE_INTERVAL {
        return Err("schedule interval cannot exceed 7d".to_owned());
    }
    Ok(duration)
}

/// Parses a `/loop` slash command into a [`ParsedLoopCommand`].
///
/// Supported forms:
/// - `/loop` or `/loop list` -> [`ParsedLoopCommand::List`]
/// - `/loop clear` / `/loop stop all` -> [`ParsedLoopCommand::ClearAll`]
/// - `/loop delete <id>` / `/loop cancel <id>` / `/loop stop <id>` / `/loop rm <id>` -> [`ParsedLoopCommand::Delete`]
/// - `/loop run <id>` / `/loop trigger <id>` -> [`ParsedLoopCommand::RunNow`]
/// - `/loop 5m <prompt>` / `/loop every 5m <prompt>` -> recurring [`ParsedLoopCommand::Create`]
/// - `/loop in 10m <prompt>` / `/loop once 10m <prompt>` -> one-shot [`ParsedLoopCommand::Create`]
/// - `/loop <prompt> every 5m` -> recurring [`ParsedLoopCommand::Create`]
/// - `/loop <prompt>` -> recurring [`ParsedLoopCommand::Create`] with default 10m interval
pub fn parse_loop_command(command: &str) -> Result<ParsedLoopCommand, String> {
    let trimmed = command.trim();
    let rest = if let Some(after) = trimmed.strip_prefix("/loop") {
        after.trim()
    } else {
        trimmed
    };
    if rest.is_empty()
        || rest.eq_ignore_ascii_case("list")
        || rest.eq_ignore_ascii_case("ls")
        || rest.eq_ignore_ascii_case("status")
    {
        return Ok(ParsedLoopCommand::List);
    }
    if rest.eq_ignore_ascii_case("clear")
        || rest.eq_ignore_ascii_case("stop all")
        || rest.eq_ignore_ascii_case("cancel all")
        || rest.eq_ignore_ascii_case("delete all")
    {
        return Ok(ParsedLoopCommand::ClearAll);
    }
    for prefix in ["delete ", "cancel ", "stop ", "rm ", "remove "] {
        if rest.len() > prefix.len() && rest[..prefix.len()].eq_ignore_ascii_case(prefix) {
            let target = rest[prefix.len()..].trim();
            if target.eq_ignore_ascii_case("all") {
                return Ok(ParsedLoopCommand::ClearAll);
            }
            if target.is_empty() {
                return Err("missing scheduled task ID to cancel".to_owned());
            }
            return Ok(ParsedLoopCommand::Delete {
                id: target.to_owned(),
            });
        }
    }
    for prefix in ["run ", "trigger "] {
        if rest.len() > prefix.len() && rest[..prefix.len()].eq_ignore_ascii_case(prefix) {
            let target = rest[prefix.len()..].trim();
            if target.is_empty() {
                return Err("missing scheduled task ID to run".to_owned());
            }
            return Ok(ParsedLoopCommand::RunNow {
                id: target.to_owned(),
            });
        }
    }

    // Check explicit leading keyword: `once in ...`, `once ...`, `in ...`, `every ...`
    for (prefix, recurring) in [
        ("once in ", false),
        ("once ", false),
        ("in ", false),
        ("every ", true),
    ] {
        if rest.len() >= prefix.len() && rest[..prefix.len()].eq_ignore_ascii_case(prefix) {
            let after_prefix = rest[prefix.len()..].trim();
            if let Some((interval, prompt)) = split_leading_duration(after_prefix)? {
                let prompt = prompt.trim();
                if prompt.is_empty() {
                    return Err(
                        "missing prompt for /loop (usage: /loop [interval] <prompt>)".to_owned(),
                    );
                }
                return Ok(ParsedLoopCommand::Create {
                    prompt: prompt.to_owned(),
                    interval,
                    recurring,
                });
            }
        }
    }

    // Check leading duration without keyword: `/loop 5m check build` or `/loop 5 minutes check build`
    if let Some((interval, prompt)) = split_leading_duration(rest)? {
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return Err("missing prompt for /loop (usage: /loop [interval] <prompt>)".to_owned());
        }
        return Ok(ParsedLoopCommand::Create {
            prompt: prompt.to_owned(),
            interval,
            recurring: true,
        });
    }

    // Check trailing `every <duration>` or `in <duration>`:
    if let Some((prompt, interval, recurring)) = split_trailing_duration(rest)? {
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return Err("missing prompt for /loop (usage: /loop [interval] <prompt>)".to_owned());
        }
        return Ok(ParsedLoopCommand::Create {
            prompt: prompt.to_owned(),
            interval,
            recurring,
        });
    }

    // Default: recurring every 10m (Claude Code default `/loop <prompt>` behavior)
    Ok(ParsedLoopCommand::Create {
        prompt: rest.to_owned(),
        interval: DEFAULT_LOOP_INTERVAL,
        recurring: true,
    })
}

fn split_leading_duration(input: &str) -> Result<Option<(Duration, &str)>, String> {
    let trimmed = input.trim_start();
    if !trimmed.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Ok(None);
    }
    // Try consuming 1 token (e.g. `5m`, `1h30m`, `10分钟`) or 2 tokens (e.g. `5 minutes`).
    let mut token_ends = Vec::new();
    let mut in_ws = false;
    for (idx, ch) in trimmed.char_indices() {
        if ch.is_whitespace() {
            if !in_ws {
                token_ends.push(idx);
                if token_ends.len() >= 2 {
                    break;
                }
                in_ws = true;
            }
        } else {
            in_ws = false;
        }
    }
    token_ends.push(trimmed.len());

    // Prefer 2-token match if the first token is purely digits and the second is a valid unit.
    if token_ends.len() >= 2 {
        let first = &trimmed[..token_ends[0]];
        let two_tokens = &trimmed[..token_ends[1]];
        if first.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(duration) = parse_duration_tokens(two_tokens) {
                return Ok(Some((duration, &trimmed[token_ends[1]..])));
            }
        }
    }
    let first = &trimmed[..token_ends[0]];
    if let Ok(duration) = parse_duration_tokens(first) {
        return Ok(Some((duration, &trimmed[token_ends[0]..])));
    }
    // If the user started with a number + invalid unit or zero duration, surface the error
    // when the first token starts with digits and has letters attached.
    if first.chars().any(|c| !c.is_ascii_digit()) {
        parse_duration_tokens(first)?;
    }
    Ok(None)
}

fn split_trailing_duration(input: &str) -> Result<Option<(&str, Duration, bool)>, String> {
    let lower = input.to_lowercase();
    for (marker, recurring) in [(" every ", true), (" once in ", false), (" in ", false)] {
        if let Some(pos) = lower.rfind(marker) {
            let prompt = &input[..pos];
            let candidate = input[pos + marker.len()..].trim();
            if !prompt.trim().is_empty() && !candidate.is_empty() {
                if let Ok(duration) = parse_duration_tokens(candidate) {
                    return Ok(Some((prompt, duration, recurring)));
                }
            }
        }
    }
    Ok(None)
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
struct ScheduleCreateArgs {
    prompt: String,
    interval: String,
    #[serde(default = "default_recurring")]
    recurring: bool,
    #[serde(default)]
    max_runs: Option<u64>,
}

fn default_recurring() -> bool {
    true
}

/// Creates a recurring or one-shot scheduled task in the invoking Run's Session.
#[derive(Clone)]
pub struct GuardedScheduleCreateExecutor {
    runs: Arc<dyn AgentJournalStore>,
    schedules: SessionScheduleRegistry,
}

impl GuardedScheduleCreateExecutor {
    pub fn new(runs: Arc<dyn AgentJournalStore>, schedules: SessionScheduleRegistry) -> Self {
        Self { runs, schedules }
    }

    async fn create(&self, execution: &GuardedToolExecution) -> Result<Value, String> {
        let args: ScheduleCreateArgs =
            serde_json::from_value(execution.invocation.arguments.clone())
                .map_err(|error| error.to_string())?;
        let (interval, recurring_override) = parse_schedule_interval(&args.interval)?;
        let recurring = recurring_override.unwrap_or(args.recurring);
        let session_id = resolve_invoking_session_id(&self.runs, execution).await?;
        let created =
            self.schedules
                .create(&session_id, args.prompt, interval, recurring, args.max_runs)?;
        serde_json::to_value(created).map_err(|error| error.to_string())
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedScheduleCreateExecutor {
    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        if let Some(ToolOutcome::Completed {
            output: ToolOutput::Inline(value),
        }) = outcome
        {
            if let (Some(id), Some(spec), Some(prompt)) = (
                value.get("id").and_then(Value::as_str),
                value.get("schedule_spec").and_then(Value::as_str),
                value.get("prompt").and_then(Value::as_str),
            ) {
                return vec![ToolActivityEvidence::Note {
                    text: format!("{id} ({spec}): {prompt}"),
                }];
            }
        }
        let interval = invocation
            .arguments
            .get("interval")
            .and_then(Value::as_str)
            .unwrap_or("timer");
        let prompt = invocation
            .arguments
            .get("prompt")
            .and_then(Value::as_str)
            .unwrap_or_default();
        vec![ToolActivityEvidence::Note {
            text: format!("{interval}: {prompt}"),
        }]
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        match self.create(&execution).await {
            Ok(output) => ToolOutcome::Completed {
                output: output.into(),
            },
            Err(message) => ToolOutcome::Rejected {
                code: "schedule_create_invalid".to_owned(),
                message,
            },
        }
    }
}

/// Descriptor for the built-in `schedule_create` Tool.
pub fn guarded_schedule_create_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/schedule_create/v1"),
        model_schema: ModelToolSchema {
            name: "schedule_create".to_owned(),
            description: "Schedule a recurring loop or one-shot delayed prompt in the current Session. When the timer expires and the Session is idle, the prompt is automatically submitted as a new turn. Use this when the user asks to poll, monitor, repeat on an interval (like /loop), or set a reminder/timer.".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["prompt", "interval"],
                "properties": {
                    "prompt": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Task prompt to execute when the timer fires."
                    },
                    "interval": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Interval or delay specification, e.g. '30s', '5m', '1h', '1h30m', '*/5 * * * *', or 'in 10m'."
                    },
                    "recurring": {
                        "type": "boolean",
                        "default": true,
                        "description": "True to repeat every interval (/loop); false to run once after the delay."
                    },
                    "max_runs": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Optional maximum number of executions before automatically stopping."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": [
                "id",
                "session_id",
                "prompt",
                "schedule_spec",
                "interval_secs",
                "recurring",
                "created_at_unix_ms",
                "next_fire_unix_ms",
                "remaining_secs",
                "fire_count"
            ],
            "properties": {
                "id": { "type": "string" },
                "session_id": { "type": "string" },
                "prompt": { "type": "string" },
                "schedule_spec": { "type": "string" },
                "interval_secs": { "type": "integer" },
                "recurring": { "type": "boolean" },
                "created_at_unix_ms": { "type": "integer" },
                "next_fire_unix_ms": { "type": "integer" },
                "remaining_secs": { "type": "integer" },
                "fire_count": { "type": "integer" },
                "max_runs": { "type": "integer" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::SessionRead]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

/// Lists active scheduled tasks in the invoking Run's Session.
#[derive(Clone)]
pub struct GuardedScheduleListExecutor {
    runs: Arc<dyn AgentJournalStore>,
    schedules: SessionScheduleRegistry,
}

impl GuardedScheduleListExecutor {
    pub fn new(runs: Arc<dyn AgentJournalStore>, schedules: SessionScheduleRegistry) -> Self {
        Self { runs, schedules }
    }

    async fn list(&self, execution: &GuardedToolExecution) -> Result<Value, String> {
        if execution
            .invocation
            .arguments
            .as_object()
            .is_none_or(|map| !map.is_empty())
        {
            return Err("schedule_list does not accept arguments".to_owned());
        }
        let session_id = resolve_invoking_session_id(&self.runs, execution).await?;
        let tasks = self.schedules.list(&session_id);
        Ok(json!({
            "count": tasks.len(),
            "tasks": tasks,
        }))
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedScheduleListExecutor {
    fn activity_evidence(
        &self,
        _invocation: &ToolInvocation,
        outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        if let Some(ToolOutcome::Completed {
            output: ToolOutput::Inline(value),
        }) = outcome
        {
            if let Some(count) = value.get("count").and_then(Value::as_u64) {
                return vec![ToolActivityEvidence::Note {
                    text: format!(
                        "{count} active scheduled task{}",
                        if count == 1 { "" } else { "s" }
                    ),
                }];
            }
        }
        Vec::new()
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        match self.list(&execution).await {
            Ok(output) => ToolOutcome::Completed {
                output: output.into(),
            },
            Err(message) => ToolOutcome::Rejected {
                code: "schedule_list_invalid".to_owned(),
                message,
            },
        }
    }
}

/// Descriptor for the built-in `schedule_list` Tool.
pub fn guarded_schedule_list_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/schedule_list/v1"),
        model_schema: ModelToolSchema {
            name: "schedule_list".to_owned(),
            description: "List active scheduled tasks (loops and timers) in the current Session."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["count", "tasks"],
            "properties": {
                "count": { "type": "integer" },
                "tasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": [
                            "id",
                            "session_id",
                            "prompt",
                            "schedule_spec",
                            "interval_secs",
                            "recurring",
                            "created_at_unix_ms",
                            "next_fire_unix_ms",
                            "remaining_secs",
                            "fire_count"
                        ],
                        "properties": {
                            "id": { "type": "string" },
                            "session_id": { "type": "string" },
                            "prompt": { "type": "string" },
                            "schedule_spec": { "type": "string" },
                            "interval_secs": { "type": "integer" },
                            "recurring": { "type": "boolean" },
                            "created_at_unix_ms": { "type": "integer" },
                            "next_fire_unix_ms": { "type": "integer" },
                            "remaining_secs": { "type": "integer" },
                            "fire_count": { "type": "integer" },
                            "max_runs": { "type": "integer" }
                        },
                        "additionalProperties": false
                    }
                }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::SessionRead]),
        restriction,
        idempotency: ToolIdempotency::Pure,
        concurrency: ToolConcurrency::ParallelSafe,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleDeleteArgs {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    all: bool,
}

/// Cancels and removes a scheduled task (or all tasks) in the invoking Run's Session.
#[derive(Clone)]
pub struct GuardedScheduleDeleteExecutor {
    runs: Arc<dyn AgentJournalStore>,
    schedules: SessionScheduleRegistry,
}

impl GuardedScheduleDeleteExecutor {
    pub fn new(runs: Arc<dyn AgentJournalStore>, schedules: SessionScheduleRegistry) -> Self {
        Self { runs, schedules }
    }

    async fn delete(&self, execution: &GuardedToolExecution) -> Result<Value, String> {
        let args: ScheduleDeleteArgs =
            serde_json::from_value(execution.invocation.arguments.clone())
                .map_err(|error| error.to_string())?;
        let session_id = resolve_invoking_session_id(&self.runs, execution).await?;
        let deleted = if args.all {
            self.schedules.clear_session(&session_id)
        } else {
            let id = args
                .id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .ok_or("provide either 'id' or 'all: true' to schedule_delete")?;
            let removed = self
                .schedules
                .delete(&session_id, id)
                .ok_or_else(|| format!("scheduled task '{id}' was not found in this Session"))?;
            vec![removed]
        };
        let remaining_count = self.schedules.count(&session_id);
        Ok(json!({
            "deleted_count": deleted.len(),
            "deleted": deleted,
            "remaining_count": remaining_count,
        }))
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedScheduleDeleteExecutor {
    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        if let Some(ToolOutcome::Completed {
            output: ToolOutput::Inline(inline),
        }) = outcome
        {
            if let Some(deleted) = inline.get("deleted").and_then(Value::as_array) {
                let ids = deleted
                    .iter()
                    .filter_map(|item| item.get("id").and_then(Value::as_str))
                    .collect::<Vec<_>>();
                if !ids.is_empty() {
                    return vec![ToolActivityEvidence::Note {
                        text: format!("cancelled {}", ids.join(", ")),
                    }];
                }
            }
        }
        if let Some(id) = invocation.arguments.get("id").and_then(Value::as_str) {
            return vec![ToolActivityEvidence::Note {
                text: format!("cancel {id}"),
            }];
        }
        Vec::new()
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        match self.delete(&execution).await {
            Ok(output) => ToolOutcome::Completed {
                output: output.into(),
            },
            Err(message) => ToolOutcome::Rejected {
                code: "schedule_delete_invalid".to_owned(),
                message,
            },
        }
    }
}

/// Descriptor for the built-in `schedule_delete` Tool.
pub fn guarded_schedule_delete_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/schedule_delete/v1"),
        model_schema: ModelToolSchema {
            name: "schedule_delete".to_owned(),
            description: "Cancel and remove a scheduled task in the current Session by its ID (e.g. 'loop-1' or '1'), or pass all=true to clear all scheduled tasks in the Session.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Scheduled task ID to cancel (e.g. 'loop-1' or '1')."
                    },
                    "all": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, cancel all scheduled tasks in the current Session."
                    }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["deleted_count", "deleted", "remaining_count"],
            "properties": {
                "deleted_count": { "type": "integer" },
                "remaining_count": { "type": "integer" },
                "deleted": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": [
                            "id",
                            "session_id",
                            "prompt",
                            "schedule_spec",
                            "interval_secs",
                            "recurring",
                            "created_at_unix_ms",
                            "next_fire_unix_ms",
                            "remaining_secs",
                            "fire_count"
                        ],
                        "properties": {
                            "id": { "type": "string" },
                            "session_id": { "type": "string" },
                            "prompt": { "type": "string" },
                            "schedule_spec": { "type": "string" },
                            "interval_secs": { "type": "integer" },
                            "recurring": { "type": "boolean" },
                            "created_at_unix_ms": { "type": "integer" },
                            "next_fire_unix_ms": { "type": "integer" },
                            "remaining_secs": { "type": "integer" },
                            "fire_count": { "type": "integer" },
                            "max_runs": { "type": "integer" }
                        },
                        "additionalProperties": false
                    }
                }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::SessionRead]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_durations_and_cron_intervals() {
        assert_eq!(
            parse_schedule_interval("30s").unwrap(),
            (Duration::from_secs(30), None)
        );
        assert_eq!(
            parse_schedule_interval("5m").unwrap(),
            (Duration::from_secs(300), None)
        );
        assert_eq!(
            parse_schedule_interval("1h30m").unwrap(),
            (Duration::from_secs(5_400), None)
        );
        assert_eq!(
            parse_schedule_interval("every 10 minutes").unwrap(),
            (Duration::from_secs(600), Some(true))
        );
        assert_eq!(
            parse_schedule_interval("in 2 hours").unwrap(),
            (Duration::from_secs(7_200), Some(false))
        );
        assert_eq!(
            parse_schedule_interval("once 5分钟").unwrap(),
            (Duration::from_secs(300), Some(false))
        );
        assert_eq!(
            parse_schedule_interval("*/5 * * * *").unwrap(),
            (Duration::from_secs(300), Some(true))
        );
        assert_eq!(
            parse_schedule_interval("0 */2 * * *").unwrap(),
            (Duration::from_secs(7_200), Some(true))
        );
    }

    #[test]
    fn parses_claude_style_loop_commands() {
        assert_eq!(
            parse_loop_command("/loop").unwrap(),
            ParsedLoopCommand::List
        );
        assert_eq!(
            parse_loop_command("/loop list").unwrap(),
            ParsedLoopCommand::List
        );
        assert_eq!(
            parse_loop_command("/loop clear").unwrap(),
            ParsedLoopCommand::ClearAll
        );
        assert_eq!(
            parse_loop_command("/loop delete 2").unwrap(),
            ParsedLoopCommand::Delete { id: "2".to_owned() }
        );
        assert_eq!(
            parse_loop_command("/loop run loop-1").unwrap(),
            ParsedLoopCommand::RunNow {
                id: "loop-1".to_owned()
            }
        );
        assert_eq!(
            parse_loop_command("/loop 5m check the build").unwrap(),
            ParsedLoopCommand::Create {
                prompt: "check the build".to_owned(),
                interval: Duration::from_secs(300),
                recurring: true,
            }
        );
        assert_eq!(
            parse_loop_command("/loop every 2 minutes run cargo test").unwrap(),
            ParsedLoopCommand::Create {
                prompt: "run cargo test".to_owned(),
                interval: Duration::from_secs(120),
                recurring: true,
            }
        );
        assert_eq!(
            parse_loop_command("/loop in 10m check deployment").unwrap(),
            ParsedLoopCommand::Create {
                prompt: "check deployment".to_owned(),
                interval: Duration::from_secs(600),
                recurring: false,
            }
        );
        assert_eq!(
            parse_loop_command("/loop check the CI status every 15m").unwrap(),
            ParsedLoopCommand::Create {
                prompt: "check the CI status".to_owned(),
                interval: Duration::from_secs(900),
                recurring: true,
            }
        );
        assert_eq!(
            parse_loop_command("/loop check the deploy").unwrap(),
            ParsedLoopCommand::Create {
                prompt: "check the deploy".to_owned(),
                interval: DEFAULT_LOOP_INTERVAL,
                recurring: true,
            }
        );
    }

    #[test]
    fn registry_manages_recurring_and_one_shot_tasks_per_session() {
        let registry = SessionScheduleRegistry::new();
        let session_a = AgentSessionId::new("session-a");
        let session_b = AgentSessionId::new("session-b");

        let task1 = registry
            .create(
                &session_a,
                "check build",
                Duration::from_secs(60),
                true,
                Some(2),
            )
            .unwrap();
        assert_eq!(task1.id, "loop-1");
        assert_eq!(task1.schedule_spec, "every 1m");

        let task2 = registry
            .create(
                &session_a,
                "remind me",
                Duration::from_secs(10),
                false,
                None,
            )
            .unwrap();
        assert_eq!(task2.id, "loop-2");
        assert_eq!(task2.schedule_spec, "once in 10s");

        // Session B is isolated from Session A.
        assert!(registry.list(&session_b).is_empty());
        assert_eq!(registry.list(&session_a).len(), 2);

        // Fire one-shot task2 at +15s: it should complete and be removed.
        let now = Instant::now() + Duration::from_secs(15);
        let fired2 = registry.pop_due(&session_a, now).unwrap();
        assert_eq!(fired2.id, "loop-2");
        assert!(fired2.completed);
        assert_eq!(registry.list(&session_a).len(), 1);

        // Fire recurring task1 twice (max_runs = 2): first fire keeps it, second removes it.
        let t1 = Instant::now() + Duration::from_secs(61);
        let fired1_first = registry.pop_due(&session_a, t1).unwrap();
        assert_eq!(fired1_first.id, "loop-1");
        assert_eq!(fired1_first.fire_count, 1);
        assert!(!fired1_first.completed);
        assert_eq!(registry.list(&session_a).len(), 1);

        let t2 = t1 + Duration::from_secs(61);
        let fired1_second = registry.pop_due(&session_a, t2).unwrap();
        assert_eq!(fired1_second.id, "loop-1");
        assert_eq!(fired1_second.fire_count, 2);
        assert!(fired1_second.completed);
        assert!(registry.list(&session_a).is_empty());
    }
}
