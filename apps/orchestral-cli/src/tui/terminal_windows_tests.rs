use super::*;
use crossterm::cursor::{position, MoveTo};
use crossterm::terminal::is_raw_mode_enabled;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::fs;
use std::io::Read;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const RESULT_ENV: &str = "ORCHESTRAL_NATIVE_TERMINAL_PROBE_RESULT";

#[test]
fn native_console_restores_modes_and_original_buffer_for_each_exit_path() {
    let path = std::env::temp_dir().join(format!(
        "orchestral-terminal-probe-{}-{}.json",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open native test console");
    let mut writer = pair.master.take_writer().unwrap();
    // portable-pty requests cursor inheritance before the test child starts.
    writer.write_all(b"\x1b[1;1R").unwrap();
    writer.flush().unwrap();
    let mut command = CommandBuilder::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "tui::terminal::tests::native_restore_child",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]);
    command.env(RESULT_ENV, &path);
    let mut child = pair.slave.spawn_command(command).unwrap();
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader().unwrap();
    let drain = std::thread::spawn(move || {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        output
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break (status, false);
        }
        if Instant::now() >= deadline {
            child.kill().expect("terminate stalled console probe");
            break (child.wait().expect("reap console probe"), true);
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    drop(writer);
    drop(pair.master);
    let output = drain.join().unwrap();
    let result = fs::read(&path);
    let _ = fs::remove_file(&path);
    assert!(
        !timed_out && status.success(),
        "{status:?}: {}",
        String::from_utf8_lossy(&output)
    );
    // Requiring the typed observations also rejects an accidentally empty test filter.
    let results: serde_json::Value = serde_json::from_slice(&result.unwrap()).unwrap();
    for mode in ["explicit", "drop", "unwind"] {
        let observation = &results[mode];
        assert_eq!(observation["raw_during"], true, "{observation}");
        assert_eq!(observation["raw_after"], false, "{observation}");
        assert_ne!(
            observation["cursor_during"], observation["cursor_before"],
            "{observation}"
        );
        assert_eq!(
            observation["cursor_after"], observation["cursor_before"],
            "{observation}"
        );
    }
}

#[test]
#[ignore = "executed by the parent test inside a real Windows pseudoconsole"]
fn native_restore_child() {
    let path = std::env::var_os(RESULT_ENV).expect("parent supplies observation path");
    let mut observations = serde_json::Map::new();
    for mode in ["explicit", "drop", "unwind"] {
        assert!(!is_raw_mode_enabled().unwrap());
        execute!(io::stdout(), MoveTo(3, 4)).unwrap();
        let before = position().unwrap();
        let mut during = None;
        let mut raw_during = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut session = TerminalSession::enter().unwrap();
            let raw = is_raw_mode_enabled().unwrap();
            assert!(raw);
            raw_during = Some(raw);
            session
                .draw(|frame| {
                    frame.render_widget(
                        ratatui::widgets::Paragraph::new("alternate buffer probe"),
                        frame.area(),
                    );
                    frame.set_cursor_position((17, 11));
                })
                .unwrap();
            let cursor = position().unwrap();
            assert_ne!(cursor, before, "probe must change the active buffer cursor");
            during = Some(cursor);
            if mode == "unwind" {
                panic!("exercise TerminalSession Drop during unwinding");
            }
            if mode == "explicit" {
                session.restore().unwrap();
            }
        }));
        assert_eq!(result.is_err(), mode == "unwind");
        let after = position().unwrap();
        let raw_after = is_raw_mode_enabled().unwrap();
        assert!(!raw_after, "raw input mode survived {mode}");
        assert_eq!(
            after, before,
            "original screen buffer was not restored by {mode}"
        );
        observations.insert(
            mode.into(),
            serde_json::json!({
                "cursor_before": before, "cursor_during": during.unwrap(),
                "cursor_after": after, "raw_during": raw_during.unwrap(), "raw_after": raw_after,
            }),
        );
    }
    fs::write(path, serde_json::to_vec(&observations).unwrap()).unwrap();
}
