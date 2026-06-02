//! Cross-platform terminal handling for interactive shell sessions.
//!
//! This module provides production-grade terminal support that EXCEEDS the AWS
//! session-manager-plugin capabilities:
//!
//! ## Improvements Over AWS Reference Implementation
//!
//! | Feature | AWS Plugin | aws-ssm-bridge |
//! |---------|-----------|----------------|
//! | Raw mode | `stty` exec (Unix only) | crossterm (cross-platform) |
//! | Resize detection | 500ms polling | SIGWINCH instant |
//! | Signal handling | Basic | Full (INT, TSTP, QUIT, WINCH) |
//! | Terminal restore | Manual | Panic-safe RAII guard |
//! | Special keys | Hardcoded maps | crossterm native |
//! | Input handling | Blocking read | Async with batching |
//!
//! ## Usage
//!
//! ```rust,no_run
//! use aws_ssm_bridge::terminal::{Terminal, TerminalConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let terminal = Terminal::new(TerminalConfig::default())?;
//! let _guard = terminal.enable_raw_mode()?;  // RAII - restores on drop
//!
//! // Terminal is now in raw mode with:
//! // - No echo (prevents double echo)
//! // - No line buffering (immediate input)
//! // - Signal handling (Ctrl+C sends bytes, not signal)
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use crossterm::{
    cursor,
    event::{self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{self, ClearType},
};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::error;

use crate::errors::{Error, Result};

/// Terminal size in columns and rows (matches AWS SizeData format)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TerminalSize {
    /// Number of columns (width)
    #[serde(rename = "cols")]
    pub cols: u16,
    /// Number of rows (height)
    #[serde(rename = "rows")]
    pub rows: u16,
}

impl Default for TerminalSize {
    fn default() -> Self {
        // Reasonable defaults if terminal size cannot be determined
        Self { cols: 80, rows: 24 }
    }
}

impl TerminalSize {
    /// Get current terminal size
    pub fn current() -> Self {
        terminal::size()
            .map(|(cols, rows)| Self { cols, rows })
            .unwrap_or_default()
    }

    /// Serialize to JSON for SSM protocol (PayloadType::Size)
    pub fn to_json(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| Error::Config(format!("Failed to serialize size: {}", e)))
    }
}

/// Terminal configuration
#[derive(Debug, Clone)]
pub struct TerminalConfig {
    /// Input buffer size (bytes to batch before sending)
    pub input_buffer_size: usize,
    /// Maximum time to wait for more input before flushing (microseconds)
    pub input_batch_timeout_us: u64,
    /// Enable local echo (usually false for SSM sessions)
    pub local_echo: bool,
    /// Poll interval for resize when SIGWINCH not available
    pub resize_poll_interval: Duration,
}

impl Default for TerminalConfig {
    fn default() -> Self {
        Self {
            input_buffer_size: 1024,      // Match AWS StreamDataPayloadSize
            input_batch_timeout_us: 1000, // 1ms - fast response
            local_echo: false,
            resize_poll_interval: Duration::from_millis(100), // 5x faster than AWS's 500ms
        }
    }
}

/// Control signals that should be forwarded to remote
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSignal {
    /// Ctrl+C (SIGINT) - interrupt
    Interrupt,
    /// Ctrl+Z (SIGTSTP) - suspend
    Suspend,
    /// Ctrl+\ (SIGQUIT) - quit with core dump
    Quit,
    /// Ctrl+D (EOF)
    EndOfFile,
}

impl ControlSignal {
    /// Convert to byte representation for SSM protocol
    pub fn as_byte(&self) -> u8 {
        match self {
            ControlSignal::Interrupt => 0x03, // Ctrl+C
            ControlSignal::Suspend => 0x1A,   // Ctrl+Z
            ControlSignal::Quit => 0x1C,      // Ctrl+\
            ControlSignal::EndOfFile => 0x04, // Ctrl+D
        }
    }
}

/// Terminal input event
#[derive(Debug, Clone)]
pub enum TerminalInput {
    /// Raw bytes to send to remote
    Data(Bytes),
    /// Control signal
    Signal(ControlSignal),
    /// Terminal resize event
    Resize(TerminalSize),
    /// Terminal closed/EOF
    Eof,
}

/// RAII guard for raw mode - restores terminal on drop
pub struct RawModeGuard {
    was_raw: bool,
}

impl RawModeGuard {
    fn new() -> io::Result<Self> {
        let was_raw = terminal::is_raw_mode_enabled()?;
        if !was_raw {
            terminal::enable_raw_mode()?;
        }
        // Bracketed paste wraps clipboard pastes in escape sequences so the
        // application receives them as a single Event::Paste instead of a
        // rapid burst of KeyCode::Char events.  Without this, many terminal
        // emulators suppress paste entirely in raw mode.
        execute!(io::stdout(), EnableBracketedPaste)?;
        Ok(Self { was_raw })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        // Always disable bracketed paste, regardless of whether we enabled raw
        // mode, so the terminal is left in a clean state.
        let _ = execute!(io::stdout(), DisableBracketedPaste);
        if !self.was_raw {
            if let Err(e) = terminal::disable_raw_mode() {
                // Can't panic in drop, just log
                eprintln!("Warning: Failed to restore terminal: {}", e);
            }
        }
    }
}

/// High-performance cross-platform terminal handler
pub struct Terminal {
    config: TerminalConfig,
    /// Current terminal size (atomic for signal handler access)
    size: Arc<(AtomicU16, AtomicU16)>,
    /// Flag to stop input loop
    running: Arc<AtomicBool>,
}

impl Terminal {
    /// Create a new terminal handler
    pub fn new(config: TerminalConfig) -> Result<Self> {
        let size = TerminalSize::current();
        Ok(Self {
            config,
            size: Arc::new((AtomicU16::new(size.cols), AtomicU16::new(size.rows))),
            running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Enable raw mode with RAII guard (restores on drop/panic)
    ///
    /// # Safety
    /// The guard MUST be kept alive for the duration of raw mode usage.
    /// When dropped, terminal is automatically restored.
    pub fn enable_raw_mode(&self) -> Result<RawModeGuard> {
        RawModeGuard::new().map_err(|e| Error::Config(format!("Failed to enable raw mode: {}", e)))
    }

    /// Get current terminal size
    pub fn size(&self) -> TerminalSize {
        TerminalSize {
            cols: self.size.0.load(Ordering::Relaxed),
            rows: self.size.1.load(Ordering::Relaxed),
        }
    }

    /// Update cached terminal size and return if changed
    pub fn update_size(&self) -> Option<TerminalSize> {
        let current = TerminalSize::current();
        let old_cols = self.size.0.swap(current.cols, Ordering::Relaxed);
        let old_rows = self.size.1.swap(current.rows, Ordering::Relaxed);

        if old_cols != current.cols || old_rows != current.rows {
            Some(current)
        } else {
            None
        }
    }

    /// Start async input reader
    ///
    /// Returns a channel receiver for terminal input events.
    /// The input loop runs until `stop()` is called or EOF is received.
    pub fn start_input_reader(&self) -> mpsc::Receiver<TerminalInput> {
        let (tx, rx) = mpsc::channel(256);
        let running = self.running.clone();
        let size = self.size.clone();
        let config = self.config.clone();

        running.store(true, Ordering::Relaxed);

        std::thread::spawn(move || {
            Self::input_loop(tx, running, size, config);
        });

        rx
    }

    /// Stop the input reader
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Input reading loop (runs in dedicated thread)
    fn input_loop(
        tx: mpsc::Sender<TerminalInput>,
        running: Arc<AtomicBool>,
        size: Arc<(AtomicU16, AtomicU16)>,
        config: TerminalConfig,
    ) {
        let mut buffer = Vec::with_capacity(config.input_buffer_size);
        let batch_timeout = Duration::from_micros(config.input_batch_timeout_us);

        while running.load(Ordering::Relaxed) {
            // Poll for events with timeout
            match event::poll(batch_timeout) {
                Ok(true) => {
                    match event::read() {
                        Ok(Event::Key(key)) => {
                            if let Some(input) = Self::process_key_event(key) {
                                match input {
                                    TerminalInput::Data(data) => {
                                        buffer.extend_from_slice(&data);
                                        // Batch small inputs
                                        if buffer.len() >= config.input_buffer_size {
                                            let _ = tx.blocking_send(TerminalInput::Data(
                                                Bytes::from(std::mem::take(&mut buffer)),
                                            ));
                                        }
                                    }
                                    other => {
                                        // Flush buffer before signal
                                        if !buffer.is_empty() {
                                            let _ = tx.blocking_send(TerminalInput::Data(
                                                Bytes::from(std::mem::take(&mut buffer)),
                                            ));
                                        }
                                        let _ = tx.blocking_send(other);
                                    }
                                }
                            }
                        }
                        Ok(Event::Resize(cols, rows)) => {
                            // Update cached size
                            size.0.store(cols, Ordering::Relaxed);
                            size.1.store(rows, Ordering::Relaxed);

                            // Flush buffer first
                            if !buffer.is_empty() {
                                let _ = tx.blocking_send(TerminalInput::Data(Bytes::from(
                                    std::mem::take(&mut buffer),
                                )));
                            }
                            let _ = tx
                                .blocking_send(TerminalInput::Resize(TerminalSize { cols, rows }));
                        }
                        Ok(Event::Paste(text)) => {
                            // Bracketed paste: the terminal emulator wrapped a
                            // clipboard paste in ESC[?2004h/l markers and
                            // crossterm decoded it into a single Event::Paste.
                            // Flush any pending key-event buffer first, then
                            // send the entire paste as one Data chunk.
                            if !buffer.is_empty() {
                                let _ = tx.blocking_send(TerminalInput::Data(Bytes::from(
                                    std::mem::take(&mut buffer),
                                )));
                            }
                            let _ = tx.blocking_send(TerminalInput::Data(
                                Bytes::from(text.into_bytes()),
                            ));
                        }
                        Ok(_) => {} // Ignore mouse, focus events
                        Err(e) => {
                            error!("Terminal read error: {}", e);
                            break;
                        }
                    }
                }
                Ok(false) => {
                    // Timeout - flush any buffered input
                    if !buffer.is_empty() {
                        let _ = tx.blocking_send(TerminalInput::Data(Bytes::from(std::mem::take(
                            &mut buffer,
                        ))));
                    }
                }
                Err(e) => {
                    error!("Terminal poll error: {}", e);
                    break;
                }
            }
        }

        // Send EOF on exit
        let _ = tx.blocking_send(TerminalInput::Eof);
    }

    /// Convert key event to terminal input
    fn process_key_event(key: KeyEvent) -> Option<TerminalInput> {
        // Handle control characters
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('c') => Some(TerminalInput::Signal(ControlSignal::Interrupt)),
                KeyCode::Char('z') => Some(TerminalInput::Signal(ControlSignal::Suspend)),
                KeyCode::Char('\\') => Some(TerminalInput::Signal(ControlSignal::Quit)),
                KeyCode::Char('d') => Some(TerminalInput::Signal(ControlSignal::EndOfFile)),
                // Other Ctrl+key combinations - send as control codes
                KeyCode::Char(c) => {
                    let ctrl_code = (c as u8) & 0x1F;
                    Some(TerminalInput::Data(Bytes::from(vec![ctrl_code])))
                }
                _ => None,
            };
        }

        // Handle regular keys and special keys
        let bytes: Vec<u8> = match key.code {
            KeyCode::Char(c) => c.to_string().into_bytes(),
            KeyCode::Enter => vec![13], // CR (AWS expects CR, not LF)
            KeyCode::Backspace => vec![127], // DEL
            KeyCode::Tab => vec![9],
            KeyCode::Esc => vec![27],

            // Arrow keys - ANSI escape sequences
            KeyCode::Up => vec![27, 91, 65],    // ESC [ A
            KeyCode::Down => vec![27, 91, 66],  // ESC [ B
            KeyCode::Right => vec![27, 91, 67], // ESC [ C
            KeyCode::Left => vec![27, 91, 68],  // ESC [ D

            // Navigation keys
            KeyCode::Home => vec![27, 91, 72],          // ESC [ H
            KeyCode::End => vec![27, 91, 70],           // ESC [ F
            KeyCode::PageUp => vec![27, 91, 53, 126],   // ESC [ 5 ~
            KeyCode::PageDown => vec![27, 91, 54, 126], // ESC [ 6 ~
            KeyCode::Insert => vec![27, 91, 50, 126],   // ESC [ 2 ~
            KeyCode::Delete => vec![27, 91, 51, 126],   // ESC [ 3 ~

            // Function keys
            KeyCode::F(1) => vec![27, 79, 80],           // ESC O P
            KeyCode::F(2) => vec![27, 79, 81],           // ESC O Q
            KeyCode::F(3) => vec![27, 79, 82],           // ESC O R
            KeyCode::F(4) => vec![27, 79, 83],           // ESC O S
            KeyCode::F(5) => vec![27, 91, 49, 53, 126],  // ESC [ 15 ~
            KeyCode::F(6) => vec![27, 91, 49, 55, 126],  // ESC [ 17 ~
            KeyCode::F(7) => vec![27, 91, 49, 56, 126],  // ESC [ 18 ~
            KeyCode::F(8) => vec![27, 91, 49, 57, 126],  // ESC [ 19 ~
            KeyCode::F(9) => vec![27, 91, 50, 48, 126],  // ESC [ 20 ~
            KeyCode::F(10) => vec![27, 91, 50, 49, 126], // ESC [ 21 ~
            KeyCode::F(11) => vec![27, 91, 50, 51, 126], // ESC [ 23 ~
            KeyCode::F(12) => vec![27, 91, 50, 52, 126], // ESC [ 24 ~

            _ => return None,
        };

        Some(TerminalInput::Data(Bytes::from(bytes)))
    }

    /// Write output to terminal (handles ANSI escape sequences)
    pub fn write_output(data: &[u8]) -> io::Result<()> {
        let mut stdout = io::stdout().lock();
        stdout.write_all(data)?;
        stdout.flush()
    }

    /// Clear terminal screen
    pub fn clear_screen() -> io::Result<()> {
        execute!(
            io::stdout(),
            terminal::Clear(ClearType::All),
            cursor::MoveTo(0, 0)
        )
    }

    /// Set terminal title
    pub fn set_title(title: &str) -> io::Result<()> {
        execute!(io::stdout(), terminal::SetTitle(title))
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Interactive shell session with full terminal support
pub struct ShellSession {
    terminal: Terminal,
    /// Current terminal size for tracking changes
    last_size: TerminalSize,
}

impl ShellSession {
    /// Create a new shell session
    pub fn new(config: TerminalConfig) -> Result<Self> {
        let terminal = Terminal::new(config)?;
        let last_size = terminal.size();
        Ok(Self {
            terminal,
            last_size,
        })
    }

    /// Get the terminal handler
    pub fn terminal(&self) -> &Terminal {
        &self.terminal
    }

    /// Check if terminal size changed and return new size if so
    pub fn check_resize(&mut self) -> Option<TerminalSize> {
        let current = self.terminal.size();
        if current != self.last_size {
            self.last_size = current;
            Some(current)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_size_default() {
        let size = TerminalSize::default();
        assert_eq!(size.cols, 80);
        assert_eq!(size.rows, 24);
    }

    #[test]
    fn test_terminal_size_json() {
        let size = TerminalSize {
            cols: 120,
            rows: 40,
        };
        let json = size.to_json().unwrap();
        let parsed: TerminalSize = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed, size);
    }

    #[test]
    fn test_control_signal_bytes() {
        assert_eq!(ControlSignal::Interrupt.as_byte(), 0x03);
        assert_eq!(ControlSignal::Suspend.as_byte(), 0x1A);
        assert_eq!(ControlSignal::Quit.as_byte(), 0x1C);
        assert_eq!(ControlSignal::EndOfFile.as_byte(), 0x04);
    }

    #[test]
    fn test_terminal_config_default() {
        let config = TerminalConfig::default();
        assert_eq!(config.input_buffer_size, 1024);
        assert!(!config.local_echo);
    }

    #[test]
    fn test_key_event_arrow_keys() {
        // Test that arrow keys produce correct ANSI sequences
        let up = Terminal::process_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert!(matches!(up, Some(TerminalInput::Data(d)) if d.as_ref() == [27, 91, 65]));

        let down = Terminal::process_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert!(matches!(down, Some(TerminalInput::Data(d)) if d.as_ref() == [27, 91, 66]));
    }

    #[test]
    fn test_key_event_enter() {
        let enter = Terminal::process_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(matches!(enter, Some(TerminalInput::Data(d)) if d.as_ref() == [13]));
    }

    #[test]
    fn test_key_event_ctrl_c() {
        let ctrl_c =
            Terminal::process_key_event(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(matches!(
            ctrl_c,
            Some(TerminalInput::Signal(ControlSignal::Interrupt))
        ));
    }
}
