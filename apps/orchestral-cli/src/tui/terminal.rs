use std::cell::Cell;
use std::io::{self, Stdout, Write};
use std::rc::Rc;

use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use super::render::{draw_with_scrollback, DrawOutcome, RenderCache};
use super::state::UiState;

type CrosstermTerminal = Terminal<CrosstermBackend<FrameWriter>>;

/// Buffers an entire scrollback + viewport frame in memory so `Stdout`'s
/// `LineWriter` cannot flush an intermediate cleared screen when `append_lines`
/// emits `\n`.
pub(crate) struct FrameWriter {
    stdout: Stdout,
    buffer: Vec<u8>,
    defer_flush: Rc<Cell<bool>>,
}

impl FrameWriter {
    fn new(stdout: Stdout, defer_flush: Rc<Cell<bool>>) -> Self {
        Self {
            stdout,
            buffer: Vec::with_capacity(16 * 1024),
            defer_flush,
        }
    }

    fn flush_now(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let output = replace_fullscreen_clear(&self.buffer);
            let mut lock = self.stdout.lock();
            lock.write_all(&output)?;
            lock.flush()?;
            self.buffer.clear();
        } else {
            self.stdout.flush()?;
        }
        Ok(())
    }
}

/// Replaces `CSI 2 J` (`ClearType::All`) with per-row `CSI 2 K` (`ClearType::CurrentLine`)
/// so terminal emulators on the main screen never copy the erased UI frame into scrollback.
fn replace_fullscreen_clear(buffer: &[u8]) -> Vec<u8> {
    const CLEAR_ALL: &[u8] = b"\x1b[2J";
    if !buffer
        .windows(CLEAR_ALL.len())
        .any(|window| window == CLEAR_ALL)
    {
        return buffer.to_vec();
    }
    let (_, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let mut replacement = Vec::with_capacity(usize::from(rows) * 12 + 8);
    for row in 1..=rows.max(1) {
        let _ = write!(replacement, "\x1b[{row};1H\x1b[2K");
    }
    replacement.extend_from_slice(b"\x1b[1;1H");

    let mut out = Vec::with_capacity(buffer.len() + replacement.len());
    let mut index = 0;
    while index < buffer.len() {
        if buffer[index..].starts_with(CLEAR_ALL) {
            out.extend_from_slice(&replacement);
            index += CLEAR_ALL.len();
        } else {
            out.push(buffer[index]);
            index += 1;
        }
    }
    out
}

impl Write for FrameWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if !self.defer_flush.get() && self.buffer.len() >= 64 * 1024 {
            self.flush_now()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.defer_flush.get() {
            return Ok(());
        }
        self.flush_now()
    }
}

/// Owns every terminal mode enabled by the TUI on the main screen buffer.
/// Staying on the main screen preserves the terminal's native scrollback and
/// mouse text selection; Drop is the last-resort restoration path for ordinary
/// errors and unwinding panics.
pub(crate) struct TerminalSession {
    terminal: CrosstermTerminal,
    defer_flush: Rc<Cell<bool>>,
    exit_cursor_row: u16,
    active: bool,
}

impl TerminalSession {
    pub(crate) fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        let (_, rows) = crossterm::terminal::size().unwrap_or((80, 24));
        let init_result = (|| -> io::Result<()> {
            execute!(stdout, MoveTo(0, rows.saturating_sub(1)))?;
            for _ in 0..rows.max(1) {
                stdout.write_all(b"\n")?;
            }
            for y in 0..rows.max(1) {
                execute!(stdout, MoveTo(0, y), Clear(ClearType::CurrentLine))?;
            }
            execute!(stdout, MoveTo(0, 0), EnableBracketedPaste, Hide)
        })();
        if let Err(error) = init_result {
            let _ = disable_raw_mode();
            return Err(error);
        }
        let defer_flush = Rc::new(Cell::new(false));
        let writer = FrameWriter::new(stdout, Rc::clone(&defer_flush));
        let terminal = match Terminal::new(CrosstermBackend::new(writer)) {
            Ok(terminal) => terminal,
            Err(error) => {
                restore_process_terminal();
                return Err(error);
            }
        };
        Ok(Self {
            terminal,
            defer_flush,
            exit_cursor_row: 0,
            active: true,
        })
    }

    #[cfg(all(test, windows))]
    pub(crate) fn draw(&mut self, render: impl FnOnce(&mut ratatui::Frame<'_>)) -> io::Result<()> {
        self.terminal.draw(render).map(|_| ())
    }

    pub(crate) fn draw_cached(
        &mut self,
        state: &UiState,
        cache: &mut RenderCache,
    ) -> io::Result<DrawOutcome> {
        self.defer_flush.set(true);
        let result = draw_with_scrollback(&mut self.terminal, state, cache);
        self.exit_cursor_row = cache.exit_cursor_row();
        self.defer_flush.set(false);
        let flush_result = self.terminal.backend_mut().flush();
        let outcome = result?;
        flush_result?;
        Ok(outcome)
    }

    pub(crate) fn resize(&mut self) -> io::Result<()> {
        self.terminal.autoresize()
    }

    pub(crate) fn restore(&mut self) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        self.active = false;

        let (_, rows) = self
            .terminal
            .size()
            .map(|size| (size.width, size.height))
            .or_else(|_| crossterm::terminal::size())
            .unwrap_or((80, 24));
        let height = rows.max(1);
        let clean_from = self.exit_cursor_row.min(height);
        let mut first_error = None;
        for y in clean_from..height {
            if let Err(error) = execute!(
                self.terminal.backend_mut(),
                MoveTo(0, y),
                Clear(ClearType::CurrentLine)
            ) {
                first_error.get_or_insert(error);
            }
        }
        let target_row = if clean_from >= height {
            if let Err(error) = execute!(self.terminal.backend_mut(), MoveTo(0, height - 1)) {
                first_error.get_or_insert(error);
            }
            if let Err(error) = self.terminal.backend_mut().write_all(b"\r\n") {
                first_error.get_or_insert(error);
            }
            if let Err(error) = execute!(
                self.terminal.backend_mut(),
                Clear(ClearType::CurrentLine),
                Clear(ClearType::FromCursorDown)
            ) {
                first_error.get_or_insert(error);
            }
            height - 1
        } else {
            if let Err(error) = execute!(
                self.terminal.backend_mut(),
                MoveTo(0, clean_from),
                Clear(ClearType::FromCursorDown)
            ) {
                first_error.get_or_insert(error);
            }
            clean_from
        };
        if let Err(error) = execute!(
            self.terminal.backend_mut(),
            MoveTo(0, target_row),
            DisableBracketedPaste,
            Show
        ) {
            first_error.get_or_insert(error);
        }
        if let Err(error) = self.terminal.show_cursor() {
            first_error.get_or_insert(error);
        }
        if let Err(error) = self.terminal.backend_mut().flush() {
            first_error.get_or_insert(error);
        }
        if let Err(error) = disable_raw_mode() {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

fn restore_process_terminal() {
    let mut stdout = io::stdout();
    let _ = execute!(stdout, DisableBracketedPaste, Show);
    let _ = stdout.flush();
    let _ = disable_raw_mode();
}

#[cfg(all(test, windows))]
#[path = "terminal_windows_tests.rs"]
mod tests;
