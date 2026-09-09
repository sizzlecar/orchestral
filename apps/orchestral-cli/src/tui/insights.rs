//! Readable, explicitly incomplete views of journaled usage and context facts.
use orchestral_core::agent_protocol::wire::{AgentEvent, UsageReport};
use orchestral_core::agent_session::AgentSessionEvent;
use orchestral_core::session_history::SessionHistory;

pub(crate) fn usage(history: Option<&SessionHistory>) -> String {
    let Some(history) = history else {
        return "No requests recorded.".to_owned();
    };
    let reports = history
        .runs
        .iter()
        .map(|run| {
            let report = run
                .records
                .iter()
                .rev()
                .find_map(|r| match &r.event.payload {
                    AgentEvent::DeliveryCommitted { delivery } => delivery.usage.as_ref(),
                    _ => None,
                });
            (run.registration.run_id(), report)
        })
        .collect::<Vec<_>>();
    let total = |field: fn(&UsageReport) -> Option<u64>| {
        let values = reports
            .iter()
            .filter_map(|(_, r)| r.and_then(field))
            .collect::<Vec<_>>();
        if values.is_empty() {
            return "— (not reported)".to_owned();
        }
        let sum = values
            .iter()
            .fold(0_u64, |total, n| total.saturating_add(*n));
        if values.len() == reports.len() {
            sum.to_string()
        } else {
            format!(
                "{sum} reported · {}/{} requests",
                values.len(),
                reports.len()
            )
        }
    };
    let mut lines = vec![format!(
        "Session totals · {} requests\nInput tokens: {}\nOutput tokens: {}\nTool calls: {}",
        reports.len(),
        total(|r| r.input_tokens),
        total(|r| r.output_tokens),
        total(|r| r.tool_calls)
    )];
    lines.push("\nRequests (newest first):".to_owned());
    for (id, report) in reports.into_iter().rev() {
        let number = |value: Option<u64>| value.map_or_else(|| "—".to_owned(), |v| v.to_string());
        let Some(report) = report else {
            lines.push(format!("{id}\n  Usage: — (not reported)"));
            continue;
        };
        let cost = report.cost.as_ref().map_or_else(
            || "— (not reported)".to_owned(),
            |cost| {
                format!(
                    "{} {}.{:06} (reported)",
                    cost.currency,
                    cost.microunits / 1_000_000,
                    cost.microunits % 1_000_000
                )
            },
        );
        lines.push(format!(
            "{id}\n  Input: {} · Output: {} · Tools: {}\n  Cost: {cost}",
            number(report.input_tokens),
            number(report.output_tokens),
            number(report.tool_calls)
        ));
    }
    lines.join("\n")
}

pub(crate) fn context(history: Option<&SessionHistory>, current_run: Option<&str>) -> String {
    let records = history.map_or(&[][..], |h| h.records.as_slice());
    let selected_run = current_run.or_else(|| {
        history?
            .runs
            .last()
            .map(|run| run.registration.run_id().as_str())
    });
    let scope = match (selected_run, current_run) {
        (Some(id), Some(_)) => format!("Current request: {id}"),
        (Some(id), None) => format!("Latest request: {id}"),
        (None, _) => "No requests recorded.".to_owned(),
    };
    let request_records = records
        .iter()
        .filter(|record| Some(record.run_id.as_str()) == selected_run)
        .collect::<Vec<_>>();
    let latest = request_records
        .iter()
        .rev()
        .find_map(|r| match &r.payload {
            AgentSessionEvent::RunOutputCommitted { usage, .. }
            | AgentSessionEvent::ToolExchangeCommitted { usage, .. } => Some(usage.as_ref()),
            _ => None,
        })
        .flatten()
        .and_then(|usage| usage.input_tokens);
    let mut lines = vec![format!(
        "{scope}\nLast reported model input for this request: {}\nCurrent context occupancy: — (no live estimate)\n",
        latest.map_or_else(
            || "— (not reported)".to_owned(),
            |v| format!("{v} tokens; last recorded usage")
        )
    )];
    lines.push("Skills loaded for this request:".to_owned());
    let mut skills = std::collections::BTreeSet::new();
    for record in request_records {
        if let AgentSessionEvent::SkillLoaded { load } = &record.payload {
            let descriptor = &load.package.descriptor;
            skills.insert(format!(
                "{} · {}",
                descriptor.name, descriptor.source.locator
            ));
        }
    }
    if skills.is_empty() {
        lines.push("None recorded for this request.".to_owned());
    } else {
        lines.extend(skills);
    }
    lines.push("\nLatest recorded compaction in this session:".to_owned());
    let compacted = records.iter().rev().find_map(|record| match &record.payload {
        AgentSessionEvent::CompactionCommitted { source, strategy, .. }
        | AgentSessionEvent::ActiveRunCompactionCommitted { source, strategy, .. } => Some(format!(
            "Session records {}–{} · {strategy}\nCommitted at session record {}\nWall-clock time: — (not recorded)",
            source.first_session_seq, source.last_session_seq, record.session_seq)),
        _ => None,
    });
    lines.push(compacted.unwrap_or_else(|| "None recorded".to_owned()));
    lines.join("\n")
}
