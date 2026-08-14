//! Terminal plumbing for interactive sessions.
//!
//! # Why raw bytes, not key events
//!
//! A remote shell is not a UI toolkit. Whatever the local terminal emits —
//! UTF-8, control codes, mouse reports, bracketed-paste markers, the kitty
//! keyboard protocol, an unfinished escape sequence split across two reads —
//! belongs on the wire untouched, because the *remote* program is what decides
//! how to interpret it.
//!
//! Decoding stdin into key events and re-encoding them is the tempting design
//! and it is wrong. It silently drops everything the decoder does not model, and
//! it cannot round-trip mode-dependent sequences: when a full-screen program
//! turns on application cursor mode (DECCKM), the arrow keys must become
//! `ESC O A` rather than `ESC [ A`, and only the remote knows which mode it is
//! in. Re-encoding from a decoded `Up` key sends the wrong bytes and breaks
//! arrow keys inside `vim`.
//!
//! So this module puts the terminal in raw mode, forwards stdin verbatim, writes
//! agent output to stdout verbatim, and separately watches for resize.

use bytes::Bytes;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use crate::errors::{Error, Result};

/// Read buffer size for stdin.
///
/// Comfortably larger than any paste chunk a terminal delivers in one read, so
/// pastes are not split across messages more than necessary.
const STDIN_BUFFER: usize = 16 * 1024;

/// How often to check for a resize where no signal is available.
#[cfg(not(unix))]
const RESIZE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Terminal dimensions, in the shape the SSM agent expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TerminalSize {
    /// Width in columns.
    pub cols: u16,
    /// Height in rows.
    pub rows: u16,
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self { cols: 80, rows: 24 }
    }
}

impl TerminalSize {
    /// The controlling terminal's current size, or an 80×24 fallback when there
    /// is no terminal (a pipe, a CI job, a daemon).
    pub fn current() -> Self {
        crossterm::terminal::size()
            .map(|(cols, rows)| Self { cols, rows })
            .unwrap_or_default()
    }
}

/// Puts the terminal in raw mode and restores it on drop.
///
/// Restoration runs on every exit path including panics, so a crash cannot leave
/// the user's shell without echo. Nesting is handled: if raw mode was already on
/// when the guard was created, dropping it leaves raw mode on.
#[must_use = "raw mode is restored as soon as the guard is dropped"]
#[derive(Debug)]
pub struct RawModeGuard {
    was_raw: bool,
}

impl RawModeGuard {
    /// Enter raw mode.
    pub fn enter() -> Result<Self> {
        let was_raw = crossterm::terminal::is_raw_mode_enabled()?;
        if !was_raw {
            crossterm::terminal::enable_raw_mode()?;
        }
        Ok(Self { was_raw })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if !self.was_raw {
            if let Err(e) = crossterm::terminal::disable_raw_mode() {
                // Never panic in a destructor, but do not hide this either: the
                // user's terminal is now in a bad state and they need to know
                // that `reset` is the fix.
                eprintln!("warning: could not restore the terminal ({e}); run `reset`");
            }
        }
    }
}

/// What the local terminal produced.
#[derive(Debug, Clone)]
pub enum TerminalEvent {
    /// Bytes read from stdin, to forward verbatim.
    Input(Bytes),
    /// The terminal was resized.
    Resize(TerminalSize),
    /// Stdin reached end of file.
    Eof,
}

/// Reads stdin and watches for resize, emitting [`TerminalEvent`]s.
///
/// Stdin is read on a dedicated OS thread because there is no portable way to
/// poll a terminal file descriptor asynchronously, and because a blocked read
/// must not occupy a runtime worker.
#[derive(Debug)]
pub struct TerminalReader {
    events: mpsc::Receiver<TerminalEvent>,
    running: Arc<AtomicBool>,
}

impl TerminalReader {
    /// Start reading. The terminal should already be in raw mode.
    pub fn start() -> Self {
        let (tx, events) = mpsc::channel(64);
        let running = Arc::new(AtomicBool::new(true));

        spawn_stdin_thread(tx.clone(), Arc::clone(&running));
        spawn_resize_watcher(tx, Arc::clone(&running));

        Self { events, running }
    }

    /// Wait for the next event, or `None` once reading has stopped.
    pub async fn next(&mut self) -> Option<TerminalEvent> {
        self.events.recv().await
    }

    /// Stop reading.
    ///
    /// The stdin thread may stay blocked in `read` until the next keypress; it
    /// is detached and exits on its own, so this does not block.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }
}

impl Drop for TerminalReader {
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_stdin_thread(tx: mpsc::Sender<TerminalEvent>, running: Arc<AtomicBool>) {
    std::thread::Builder::new()
        .name("ssm-stdin".into())
        .spawn(move || {
            let mut stdin = io::stdin().lock();
            let mut buf = vec![0u8; STDIN_BUFFER];

            while running.load(Ordering::Acquire) {
                match stdin.read(&mut buf) {
                    Ok(0) => {
                        debug!("stdin reached EOF");
                        let _ = tx.blocking_send(TerminalEvent::Eof);
                        break;
                    }
                    Ok(n) => {
                        trace!(bytes = n, "read from stdin");
                        let chunk = Bytes::copy_from_slice(&buf[..n]);
                        if tx.blocking_send(TerminalEvent::Input(chunk)).is_err() {
                            break; // consumer is gone
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        warn!(error = %e, "stdin read failed");
                        let _ = tx.blocking_send(TerminalEvent::Eof);
                        break;
                    }
                }
            }
            debug!("stdin thread finished");
        })
        .expect("spawning the stdin thread must succeed");
}

#[cfg(unix)]
fn spawn_resize_watcher(tx: mpsc::Sender<TerminalEvent>, running: Arc<AtomicBool>) {
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};

        let mut winch = match signal(SignalKind::window_change()) {
            Ok(stream) => stream,
            Err(e) => {
                warn!(error = %e, "could not watch SIGWINCH; resizes will not propagate");
                return;
            }
        };

        let mut last = TerminalSize::current();
        while running.load(Ordering::Acquire) {
            if winch.recv().await.is_none() {
                break;
            }
            let size = TerminalSize::current();
            // The kernel can deliver SIGWINCH without an actual size change;
            // forwarding those would make the remote pty redraw for nothing.
            if size != last {
                last = size;
                debug!(cols = size.cols, rows = size.rows, "terminal resized");
                if tx.send(TerminalEvent::Resize(size)).await.is_err() {
                    break;
                }
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_resize_watcher(tx: mpsc::Sender<TerminalEvent>, running: Arc<AtomicBool>) {
    tokio::spawn(async move {
        // Windows has no SIGWINCH, so polling is the only option.
        let mut last = TerminalSize::current();
        let mut ticker = tokio::time::interval(RESIZE_POLL_INTERVAL);
        while running.load(Ordering::Acquire) {
            ticker.tick().await;
            let size = TerminalSize::current();
            if size != last {
                last = size;
                if tx.send(TerminalEvent::Resize(size)).await.is_err() {
                    break;
                }
            }
        }
    });
}

/// Write agent output to stdout verbatim and flush.
///
/// No translation: escape sequences, carriage returns and partial UTF-8 all go
/// through untouched, because the local terminal is what renders them.
pub fn write_output(data: &[u8]) -> Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(data)?;
    stdout.flush()?;
    Ok(())
}

/// Whether stdin and stdout are both connected to a terminal.
///
/// Interactive features are pointless — and raw mode fails — when either is a
/// pipe or a file.
pub fn is_terminal() -> bool {
    use std::io::IsTerminal;
    io::stdin().is_terminal() && io::stdout().is_terminal()
}

/// Error helper for callers that need a terminal but do not have one.
pub(crate) fn require_terminal() -> Result<()> {
    if is_terminal() {
        Ok(())
    } else {
        Err(Error::Config(
            "an interactive shell needs stdin and stdout attached to a terminal; \
             use Session::send and Session::output for piped or headless use"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_size_is_the_classic_terminal() {
        assert_eq!(TerminalSize::default(), TerminalSize { cols: 80, rows: 24 });
    }

    /// The agent expects lower-case `cols` and `rows`; anything else is ignored
    /// and the remote pty keeps its default geometry.
    #[test]
    fn size_serializes_to_the_agent_wire_format() {
        let json = serde_json::to_string(&TerminalSize {
            cols: 120,
            rows: 40,
        })
        .unwrap();
        assert_eq!(json, r#"{"cols":120,"rows":40}"#);
    }

    #[test]
    fn size_round_trips() {
        let size = TerminalSize {
            cols: 200,
            rows: 60,
        };
        let json = serde_json::to_vec(&size).unwrap();
        assert_eq!(serde_json::from_slice::<TerminalSize>(&json).unwrap(), size);
    }

    /// Under `cargo test` stdout is captured, so this must report false rather
    /// than attempting raw mode on a pipe.
    #[test]
    fn require_terminal_refuses_a_non_tty() {
        if !is_terminal() {
            let err = require_terminal().unwrap_err();
            assert!(err.to_string().contains("terminal"), "{err}");
        }
    }

    #[test]
    fn current_size_falls_back_without_a_terminal() {
        // Must not panic whether or not a terminal is attached.
        let size = TerminalSize::current();
        assert!(size.cols > 0 && size.rows > 0);
    }
}
