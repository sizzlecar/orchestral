//! Opt-in measurement, separate from deterministic correctness tests.
use super::{render_cached, update, RenderCache, TranscriptEntry, UiEffect, UiMsg, UiState};
use ratatui::{backend::TestBackend, Terminal};
use std::time::{Duration, Instant};

#[test]
#[ignore = "local latency measurement; run with --ignored --nocapture"]
fn large_history_stream_and_file_completion_latency() {
    let mut state = UiState::new("measurement", "configured-model");
    state.transcript = (0..10_000).map(|i| TranscriptEntry::assistant(
        format!("entry-{i}"), format!("Recorded answer {i}: 中文 and emoji 👩🏽‍💻, with enough text to wrap across a narrow terminal."),
    )).collect();
    state.files = std::sync::Arc::new(
        (0..10_000)
            .map(|i| {
                super::menu::Choice::new(
                    format!("src/module_{i}/main.rs"),
                    format!("/workspace/src/module_{i}/main.rs"),
                    "/workspace",
                )
            })
            .collect(),
    );
    state.files_loaded = true;
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".to_owned(),
        },
    );
    let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
    let mut cache = RenderCache::default();
    let started = Instant::now();
    terminal
        .draw(|frame| {
            render_cached(frame, &state, &mut cache);
        })
        .unwrap();
    let initial = started.elapsed();
    state.transcript_dirty_from = state.transcript.len();
    let mut samples = Vec::new();
    for i in 0..100 {
        let due = Instant::now() + Duration::from_millis(50);
        state.composer.clear();
        state.composer_cursor = 0;
        let started = Instant::now();
        update(
            &mut state,
            UiMsg::StreamDelta {
                delta_id: format!("delta-{i}"),
                output_id: "stream".to_owned(),
                order: i,
                text: "New output 中文. ".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "active-tool".to_owned(),
                tool_name: "search".to_owned(),
                state: orchestral_core::agent_protocol::wire::ToolActivityState::Running,
                evidence: vec![
                    orchestral_core::agent_protocol::wire::ToolActivityEvidence::Note {
                        text: format!("Progress {i}"),
                    },
                ],
            },
        );
        update(&mut state, UiMsg::InsertText("@src/main".to_owned()));
        terminal
            .draw(|frame| {
                render_cached(frame, &state, &mut cache);
            })
            .unwrap();
        state.transcript_dirty_from = state.transcript.len();
        samples.push(started.elapsed());
        std::thread::sleep(due.saturating_duration_since(Instant::now()));
    }
    samples.sort();
    let started = Instant::now();
    let effects = update(&mut state, UiMsg::Cancel);
    assert!(matches!(effects.as_slice(), [UiEffect::CancelRun { .. }]));
    eprintln!("10,000 history entries + 10,000 files, 20 text/tool updates/s, 100 samples, TestBackend 100x30: initial={initial:?}, edit+render p95={:?}, max={:?}, cancel reduction={:?}", samples[94], samples[99], started.elapsed());
    assert!(
        samples[94] <= Duration::from_millis(100),
        "local edit/render latency exceeds target"
    );
}
