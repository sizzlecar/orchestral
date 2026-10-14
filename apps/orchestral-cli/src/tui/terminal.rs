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
            let mut lock = self.stdout.lock();
            lock.write_all(&self.buffer)?;
            lock.flush()?;
            self.buffer.clear();
        } else {
            self.stdout.flush()?;
        }
        Ok(())
    }
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
    active: bool,
}

impl TerminalSession {
    pub(crate) fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(
            stdout,
            Clear(ClearType::All),
            MoveTo(0, 0),
            EnableBracketedPaste,
            Hide
        ) {
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

        let mut first_error = self.terminal.show_cursor().err();
        if let Err(error) = execute!(self.terminal.backend_mut(), DisableBracketedPaste, Show) {
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
