use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use ssh2::{KeyboardInteractivePrompt, Prompt, Session};

use crate::logger;
use crate::model::ConnectionSettings;
use vt100::Parser;

use std::path::Path;

const READ_BUF_SIZE: usize = 4096;
pub const TERM_SCROLLBACK_LEN: usize = 5000;

#[derive(Debug)]
pub enum UiMessage {
    Status(String),
    Screen(vt100::Screen),
    ScrollbackMax(usize),
    Connected(bool),
    AuthPrompt(AuthPrompt),
}

#[derive(Debug, Clone)]
pub struct AuthPrompt {
    pub instructions: String,
    pub prompts: Vec<AuthPromptItem>,
}

#[derive(Debug, Clone)]
pub struct AuthPromptItem {
    pub text: String,
    pub echo: bool,
}

#[derive(Debug)]
pub enum WorkerMessage {
    Input(Vec<u8>),
    Resize {
        rows: u16,
        cols: u16,
        width_px: u32,
        height_px: u32,
    },
    SetScrollback(usize),
    AuthResponse(Vec<String>),
    Disconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalQuery {
    Status, // CSI 5 n
    CursorPosition { private: bool }, // CSI 6 n / CSI ? 6 n
    DeviceAttributes, // CSI c / CSI 0 c
}

#[derive(Debug, Clone)]
struct CsiQueryScanner {
    state: ScanState,
    params: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanState {
    Ground,
    Esc,
    Csi,
}

impl Default for CsiQueryScanner {
    fn default() -> Self {
        Self {
            state: ScanState::Ground,
            params: Vec::new(),
        }
    }
}

impl CsiQueryScanner {
    fn feed(&mut self, b: u8) -> Option<TerminalQuery> {
        match self.state {
            ScanState::Ground => {
                if b == 0x1b {
                    self.state = ScanState::Esc;
                }
                None
            }
            ScanState::Esc => {
                if b == b'[' {
                    self.state = ScanState::Csi;
                    self.params.clear();
                } else {
                    self.state = ScanState::Ground;
                }
                None
            }
            ScanState::Csi => {
                if (0x40..=0x7e).contains(&b) {
                    let params = std::mem::take(&mut self.params);
                    self.state = ScanState::Ground;
                    Self::interpret_csi(b, &params)
                } else {
                    // Parameter + intermediate bytes. We only need a tiny amount.
                    if self.params.len() < 32 {
                        self.params.push(b);
                    } else {
                        self.state = ScanState::Ground;
                        self.params.clear();
                    }
                    None
                }
            }
        }
    }

    fn interpret_csi(final_byte: u8, params: &[u8]) -> Option<TerminalQuery> {
        match final_byte {
            b'n' => match params {
                b"5" => Some(TerminalQuery::Status),
                b"6" => Some(TerminalQuery::CursorPosition { private: false }),
                b"?6" => Some(TerminalQuery::CursorPosition { private: true }),
                _ => None,
            },
            b'c' => {
                if params.is_empty() || params == b"0" {
                    Some(TerminalQuery::DeviceAttributes)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn respond_to_query(
    channel: &mut ssh2::Channel,
    parser: &Parser,
    query: TerminalQuery,
) {
    match query {
        TerminalQuery::Status => {
            let _ = channel.write_all(b"\x1b[0n");
            let _ = channel.flush();
        }
        TerminalQuery::CursorPosition { private } => {
            let (rows, cols) = parser.screen().size();
            let (mut row, mut col) = parser.screen().cursor_position();
            if rows > 0 {
                row = row.min(rows.saturating_sub(1));
            } else {
                row = 0;
            }
            if cols > 0 {
                col = col.min(cols.saturating_sub(1));
            } else {
                col = 0;
            }

            let row_1 = row.saturating_add(1);
            let col_1 = col.saturating_add(1);
            let s = if private {
                format!("\x1b[?{row_1};{col_1}R")
            } else {
                format!("\x1b[{row_1};{col_1}R")
            };
            let _ = channel.write_all(s.as_bytes());
            let _ = channel.flush();
        }
        TerminalQuery::DeviceAttributes => {
            // Minimal "VT100 with advanced video option". Good enough for most apps.
            let _ = channel.write_all(b"\x1b[?1;0c");
            let _ = channel.flush();
        }
    }
}

fn process_with_query_responses(
    parser: &mut Parser,
    scanner: &mut CsiQueryScanner,
    channel: &mut ssh2::Channel,
    bytes: &[u8],
) {
    let mut last = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        if let Some(q) = scanner.feed(b) {
            parser.process(&bytes[last..=i]);
            respond_to_query(channel, parser, q);
            last = i + 1;
        }
    }
    if last < bytes.len() {
        parser.process(&bytes[last..]);
    }
}

fn compute_scrollback_max(parser: &mut Parser) -> usize {
    let cur = parser.screen().scrollback();
    parser.set_scrollback(usize::MAX);
    let max = parser.screen().scrollback();
    parser.set_scrollback(cur);
    max
}

fn request_auth_responses(
    ui_tx: &Sender<UiMessage>,
    worker_rx: &Receiver<WorkerMessage>,
    username: &str,
    instructions: &str,
    prompts: Vec<AuthPromptItem>,
    log_path: &str,
) -> Result<Vec<String>> {
    logger::log_line(
        log_path,
        &format!(
            "Requesting auth responses (user={username:?}, prompts={}).",
            prompts.len()
        ),
    );
    let _ = ui_tx.send(UiMessage::Status("Authentication required".to_string()));
    let _ = ui_tx.send(UiMessage::AuthPrompt(AuthPrompt {
        instructions: instructions.to_string(),
        prompts: prompts.clone(),
    }));

    let timeout = Duration::from_secs(600);
    loop {
        match worker_rx.recv_timeout(timeout) {
            Ok(WorkerMessage::AuthResponse(mut responses)) => {
                responses.resize(prompts.len(), String::new());
                responses.truncate(prompts.len());
                return Ok(responses);
            }
            Ok(WorkerMessage::Disconnect) => return Err(anyhow!("Disconnected during authentication")),
            Ok(_) => {
                // Ignore other messages while authenticating.
            }
            Err(RecvTimeoutError::Timeout) => return Err(anyhow!("Timed out waiting for authentication input")),
            Err(RecvTimeoutError::Disconnected) => return Err(anyhow!("UI channel disconnected")),
        }
    }
}

struct UiPrompter<'a> {
    ui_tx: &'a Sender<UiMessage>,
    worker_rx: &'a Receiver<WorkerMessage>,
    log_path: &'a str,
    cached_password: Option<&'a str>,
}

impl<'a> KeyboardInteractivePrompt for UiPrompter<'a> {
    fn prompt<'p>(
        &mut self,
        username: &str,
        instructions: &str,
        prompts: &[Prompt<'p>],
    ) -> Vec<String> {
        if let Some(pw) = self.cached_password {
            if !pw.is_empty() && !prompts.is_empty() && prompts.iter().all(|p| !p.echo) {
                return prompts.iter().map(|_| pw.to_string()).collect();
            }
        }

        let items: Vec<AuthPromptItem> = prompts
            .iter()
            .map(|p| AuthPromptItem {
                text: p.text.to_string(),
                echo: p.echo,
            })
            .collect();

        match request_auth_responses(self.ui_tx, self.worker_rx, username, instructions, items, self.log_path) {
            Ok(responses) => responses,
            Err(err) => {
                logger::log_line(self.log_path, &format!("Auth prompt failed: {err}"));
                vec![String::new(); prompts.len()]
            }
        }
    }
}

pub fn start_shell(
    settings: ConnectionSettings,
    scrollback_len: usize,
    ui_tx: Sender<UiMessage>,
    worker_rx: Receiver<WorkerMessage>,
    log_path: String,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        logger::log_line(&log_path, "Starting SSH worker.");
        let result = run_shell(settings, scrollback_len, &ui_tx, worker_rx, &log_path);
        if let Err(err) = result {
            logger::log_line(&log_path, &format!("Worker error: {err}"));
            let _ = ui_tx.send(UiMessage::Status(format!("Connection failed: {err}")));
            let _ = ui_tx.send(UiMessage::Connected(false));
        }
    })
}

fn run_shell(
    settings: ConnectionSettings,
    scrollback_len: usize,
    ui_tx: &Sender<UiMessage>,
    worker_rx: Receiver<WorkerMessage>,
    log_path: &str,
) -> Result<()> {
    logger::log_line(log_path, "Validating settings.");
    if settings.host.trim().is_empty() {
        return Err(anyhow!("Host is required"));
    }
    if settings.username.trim().is_empty() {
        return Err(anyhow!("Username is required"));
    }

    let addr = format!("{}:{}", settings.host.trim(), settings.port);
    logger::log_line(log_path, &format!("Connecting TCP to {addr}."));
    let tcp = TcpStream::connect(&addr).with_context(|| format!("Failed to connect to {}", addr))?;
    tcp.set_read_timeout(Some(Duration::from_secs(10)))
        .context("Failed to set read timeout")?;
    tcp.set_write_timeout(Some(Duration::from_secs(10)))
        .context("Failed to set write timeout")?;

    logger::log_line(log_path, "Creating SSH session.");
    let mut session = Session::new().context("Failed to create SSH session")?;
    session.set_tcp_stream(tcp);
    logger::log_line(log_path, "Performing SSH handshake.");
    session.handshake().context("SSH handshake failed")?;

    let username = settings.username.trim();
    let methods = session.auth_methods(username).unwrap_or("");
    logger::log_line(log_path, &format!("Server auth methods: {methods}"));
    let supports_kbd = methods
        .split(',')
        .map(|s| s.trim())
        .any(|m| m.eq_ignore_ascii_case("keyboard-interactive"));
    let supports_pass = methods
        .split(',')
        .map(|s| s.trim())
        .any(|m| m.eq_ignore_ascii_case("password"));
    let supports_pubkey = methods
        .split(',')
        .map(|s| s.trim())
        .any(|m| m.eq_ignore_ascii_case("publickey"));

    // Try private key authentication if configured.
    if !settings.private_key_path.trim().is_empty() && supports_pubkey {
        let key_path = settings.private_key_path.trim();
        logger::log_line(log_path, &format!("Authenticating via private key: {key_path}"));
        let pk = Path::new(key_path);
        if !pk.exists() {
            logger::log_line(log_path, "Private key path does not exist.");
        } else {
            let passphrase = if settings.key_passphrase.trim().is_empty() {
                None
            } else {
                Some(settings.key_passphrase.as_str())
            };
            if let Err(err) = session.userauth_pubkey_file(username, None, pk, passphrase) {
                logger::log_line(log_path, &format!("Private key auth failed: {err}"));

                // We might be missing (or have an incorrect) key passphrase. Prompt once and retry.
                let err_s = err.to_string().to_ascii_lowercase();
                let might_need_passphrase = settings.key_passphrase.trim().is_empty()
                    || err_s.contains("passphrase")
                    || err_s.contains("decrypt")
                    || err_s.contains("parse");
                if might_need_passphrase && !session.authenticated() {
                    logger::log_line(log_path, "Private key may need a passphrase; prompting user.");
                    if let Ok(responses) = request_auth_responses(
                        ui_tx,
                        &worker_rx,
                        username,
                        "Private key authentication may require a passphrase.",
                        vec![AuthPromptItem {
                            text: "Key passphrase (optional):".to_string(),
                            echo: false,
                        }],
                        log_path,
                    ) {
                        let pw = responses.get(0).cloned().unwrap_or_default();
                        // Empty passphrase is allowed (e.g. unencrypted key); in that case, retrying is pointless.
                        if !pw.is_empty() {
                            if let Err(err) = session.userauth_pubkey_file(username, None, pk, Some(&pw)) {
                                logger::log_line(log_path, &format!("Private key auth (with passphrase) failed: {err}"));
                            }
                        }
                    }
                }
            }

            // Some servers require multiple auth methods (e.g. publickey + password/OTP).
            // In that case, pubkey can succeed but the session is not fully authenticated yet.
            if !session.authenticated() {
                logger::log_line(
                    log_path,
                    "Private key accepted, but server requires additional authentication.",
                );
                let _ = ui_tx.send(UiMessage::Status(
                    "Additional authentication required (server policy)".to_string(),
                ));
            }
        }
    }

    // Prefer explicit password auth when a password is provided, but fall back to
    // keyboard-interactive (common with PAM) and prompt the user as needed.
    if !session.authenticated() && !settings.password.is_empty() && supports_pass {
        logger::log_line(log_path, "Authenticating via password.");
        if let Err(err) = session.userauth_password(username, &settings.password) {
            logger::log_line(log_path, &format!("Password auth failed: {err}"));
        }
    }

    if !session.authenticated() && supports_kbd {
        logger::log_line(log_path, "Authenticating via keyboard-interactive.");
        let mut prompter = UiPrompter {
            ui_tx,
            worker_rx: &worker_rx,
            log_path,
            cached_password: if settings.password.is_empty() {
                None
            } else {
                Some(settings.password.as_str())
            },
        };
        if let Err(err) = session.userauth_keyboard_interactive(username, &mut prompter) {
            logger::log_line(log_path, &format!("Keyboard-interactive auth failed: {err}"));
        }
    }

    // Last resort: if the server supports password but not keyboard-interactive, we must
    // ask the user explicitly for a password (the server cannot "prompt" in the shell).
    if !session.authenticated() && supports_pass && !supports_kbd {
        // If the user didn't provide a password up front, ask for it now and try password auth.
        logger::log_line(log_path, "Password needed; prompting user.");
        let pw = loop {
            let responses = request_auth_responses(
                ui_tx,
                &worker_rx,
                username,
                "Server requested password authentication.",
                vec![AuthPromptItem {
                    text: "Password:".to_string(),
                    echo: false,
                }],
                log_path,
            )?;
            let pw = responses.get(0).cloned().unwrap_or_default();
            if !pw.is_empty() {
                break pw;
            }
            logger::log_line(log_path, "Empty password submitted; reprompting.");
            let _ = ui_tx.send(UiMessage::Status("Password required".to_string()));
        };
        session
            .userauth_password(username, &pw)
            .context("SSH password authentication failed")?;
    }

    if !session.authenticated() {
        return Err(anyhow!("SSH authentication failed"));
    }

    logger::log_line(log_path, "Opening SSH channel.");
    let mut channel = session
        .channel_session()
        .context("Failed to open SSH channel")?;
    // Merge stderr into stdout so apps that emit terminal control sequences on stderr
    // (or split output streams) still behave correctly in our single-stream renderer.
    let _ = channel.handle_extended_data(ssh2::ExtendedData::Merge);
    logger::log_line(log_path, "Requesting PTY.");
    channel
        .request_pty("xterm-256color", None, None)
        .context("Failed to request PTY")?;
    // Some servers ignore the pty type for TERM; set it explicitly as well.
    let _ = channel.setenv("TERM", "xterm-256color");
    logger::log_line(log_path, "Starting shell.");
    channel.shell().context("Failed to start shell")?;
    // Keep the session in blocking mode, but with a short timeout so reads don't
    // block forever and starve input handling.
    session.set_timeout(100);

    let _ = ui_tx.send(UiMessage::Status("Connected successfully".to_string()));
    let _ = ui_tx.send(UiMessage::Connected(true));
    logger::log_line(log_path, "Shell connected.");

    let mut read_buf = [0u8; READ_BUF_SIZE];
    let len = scrollback_len.clamp(0, 200_000);
    let len = if len == 0 { TERM_SCROLLBACK_LEN } else { len };
    let mut parser = Parser::new(24, 80, len);
    let mut scanner = CsiQueryScanner::default();
    loop {
        let mut disconnected = false;

        loop {
            match worker_rx.try_recv() {
                Ok(WorkerMessage::Input(data)) => {
                    let _ = channel.write_all(&data);
                    let _ = channel.flush();
                }
                Ok(WorkerMessage::Resize {
                    rows,
                    cols,
                    width_px,
                    height_px,
                }) => {
                    // Keep the remote PTY and our local parser in sync.
                    let _ = channel.request_pty_size(
                        cols.into(),
                        rows.into(),
                        Some(width_px),
                        Some(height_px),
                    );
                    parser.set_size(rows, cols);
                    let _ = ui_tx.send(UiMessage::Screen(parser.screen().clone()));
                    let _ = ui_tx.send(UiMessage::ScrollbackMax(compute_scrollback_max(&mut parser)));
                }
                Ok(WorkerMessage::SetScrollback(rows)) => {
                    parser.set_scrollback(rows);
                    let _ = ui_tx.send(UiMessage::Screen(parser.screen().clone()));
                    let _ = ui_tx.send(UiMessage::ScrollbackMax(compute_scrollback_max(&mut parser)));
                }
                Ok(WorkerMessage::Disconnect) => {
                    disconnected = true;
                    break;
                }
                Ok(WorkerMessage::AuthResponse(_)) => {
                    // Auth responses are only expected during authentication.
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        if disconnected {
            let _ = channel.close();
            logger::log_line(log_path, "Disconnected on request.");
            let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
            let _ = ui_tx.send(UiMessage::Connected(false));
            return Ok(());
        }

        match channel.read(&mut read_buf) {
            Ok(0) => {
                if channel.eof() {
                    logger::log_line(log_path, "Channel EOF.");
                    let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
                    let _ = ui_tx.send(UiMessage::Connected(false));
                    return Ok(());
                }
            }
            Ok(n) => {
                process_with_query_responses(
                    &mut parser,
                    &mut scanner,
                    &mut channel,
                    &read_buf[..n],
                );
                let _ = ui_tx.send(UiMessage::Screen(parser.screen().clone()));
                let _ = ui_tx.send(UiMessage::ScrollbackMax(compute_scrollback_max(&mut parser)));
            }
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(err) => {
                logger::log_line(log_path, &format!("Channel read failed: {err}"));
                return Err(anyhow!("Channel read failed: {err}"));
            }
        }

        thread::sleep(Duration::from_millis(10));
    }
}
