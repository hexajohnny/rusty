use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use russh::client::{self, AuthResult, KeyboardInteractiveAuthResponse};
use russh::keys::{self, PrivateKeyWithHashAlg, load_secret_key};
use russh::{ChannelMsg, Disconnect, MethodKind, MethodSet};
use tokio::io::AsyncWriteExt;
use vt100::Parser;

use crate::logger;
use crate::model::ConnectionSettings;

const READ_POLL_INTERVAL: Duration = Duration::from_millis(30);
pub const TERM_SCROLLBACK_LEN: usize = 5000;

#[derive(Debug)]
pub enum UiMessage {
    Status(String),
    Screen(Box<vt100::Screen>),
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

async fn respond_to_query<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    parser: &Parser,
    query: TerminalQuery,
) -> Result<()> {
    let response = match query {
        TerminalQuery::Status => b"\x1b[0n".to_vec(),
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
            s.into_bytes()
        }
        TerminalQuery::DeviceAttributes => {
            // Minimal "VT100 with advanced video option". Good enough for most apps.
            b"\x1b[?1;0c".to_vec()
        }
    };

    writer
        .write_all(&response)
        .await
        .context("Failed to write terminal query response")?;
    writer
        .flush()
        .await
        .context("Failed to flush terminal query response")?;
    Ok(())
}

async fn process_with_query_responses<W: tokio::io::AsyncWrite + Unpin>(
    parser: &mut Parser,
    scanner: &mut CsiQueryScanner,
    writer: &mut W,
    bytes: &[u8],
) -> Result<()> {
    let mut last = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        if let Some(q) = scanner.feed(b) {
            parser.process(&bytes[last..=i]);
            respond_to_query(writer, parser, q).await?;
            last = i + 1;
        }
    }
    if last < bytes.len() {
        parser.process(&bytes[last..]);
    }
    Ok(())
}

fn detect_private_key_format(path: &Path) -> Option<&'static str> {
    let bytes = std::fs::read(path).ok()?;
    let first_line = bytes
        .split(|b| *b == b'\n')
        .next()
        .map(|line| String::from_utf8_lossy(line).trim().to_string())?;

    if first_line.contains("OPENSSH PRIVATE KEY") {
        Some("OPENSSH")
    } else if first_line.contains("RSA PRIVATE KEY") {
        Some("RSA-PEM")
    } else if first_line.contains("EC PRIVATE KEY") {
        Some("EC-PEM")
    } else if first_line.contains("PRIVATE KEY") {
        Some("PKCS8-PEM")
    } else {
        None
    }
}

fn method_kind_name(method: MethodKind) -> &'static str {
    match method {
        MethodKind::None => "none",
        MethodKind::Password => "password",
        MethodKind::PublicKey => "publickey",
        MethodKind::HostBased => "hostbased",
        MethodKind::KeyboardInteractive => "keyboard-interactive",
    }
}

fn method_set_to_csv(methods: &MethodSet) -> String {
    if methods.is_empty() {
        "none".to_string()
    } else {
        methods
            .iter()
            .map(|m| method_kind_name(*m))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn supports_method(methods: &MethodSet, target: MethodKind) -> bool {
    methods.iter().any(|m| *m == target)
}

fn compute_scrollback_max(parser: &mut Parser) -> usize {
    let cur = parser.screen().scrollback();
    parser.set_scrollback(usize::MAX);
    let max = parser.screen().scrollback();
    parser.set_scrollback(cur);
    max
}

fn send_screen(ui_tx: &Sender<UiMessage>, parser: &mut Parser) {
    let _ = ui_tx.send(UiMessage::Screen(Box::new(parser.screen().clone())));
}

fn send_scrollback_max(ui_tx: &Sender<UiMessage>, parser: &mut Parser) {
    let _ = ui_tx.send(UiMessage::ScrollbackMax(compute_scrollback_max(parser)));
}

fn request_auth_responses(
    ui_tx: &Sender<UiMessage>,
    worker_rx: &Receiver<WorkerMessage>,
    username: &str,
    instructions: &str,
    prompts: Vec<AuthPromptItem>,
    log_path: &str,
) -> Result<Vec<String>> {
    if prompts.is_empty() || prompts.iter().all(|p| p.text.trim().is_empty()) {
        logger::log_line(
            log_path,
            &format!(
                "Auth prompt requires no user input (user={username:?}); auto-continuing."
            ),
        );
        return Ok(vec![String::new(); prompts.len()]);
    }

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
            Ok(WorkerMessage::Disconnect) => {
                return Err(anyhow!("Disconnected during authentication"));
            }
            Ok(_) => {
                // Ignore other messages while authenticating.
            }
            Err(RecvTimeoutError::Timeout) => {
                return Err(anyhow!("Timed out waiting for authentication input"));
            }
            Err(RecvTimeoutError::Disconnected) => return Err(anyhow!("UI channel disconnected")),
        }
    }
}

fn apply_auth_result(
    result: AuthResult,
    authenticated: &mut bool,
    remaining_methods: &mut MethodSet,
) -> bool {
    match result {
        AuthResult::Success => {
            *authenticated = true;
            *remaining_methods = MethodSet::empty();
            false
        }
        AuthResult::Failure {
            remaining_methods: methods,
            partial_success,
        } => {
            *authenticated = false;
            *remaining_methods = methods;
            partial_success
        }
    }
}

fn error_mentions_passphrase(err: &anyhow::Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("encrypted")
        || s.contains("passphrase")
        || s.contains("decrypt")
        || s.contains("private key")
}

async fn authenticate_with_private_key(
    session: &mut client::Handle<AcceptAllClient>,
    username: &str,
    private_key: &Path,
    passphrase: Option<&str>,
    best_rsa_hash: Option<keys::HashAlg>,
) -> Result<AuthResult> {
    let key_pair = load_secret_key(private_key, passphrase)
        .with_context(|| format!("Unable to load private key {}", private_key.display()))?;
    let key = PrivateKeyWithHashAlg::new(Arc::new(key_pair), best_rsa_hash);

    session
        .authenticate_publickey(username, key)
        .await
        .context("SSH public key authentication request failed")
}

async fn authenticate_keyboard_interactive(
    session: &mut client::Handle<AcceptAllClient>,
    username: &str,
    ui_tx: &Sender<UiMessage>,
    worker_rx: &Receiver<WorkerMessage>,
    cached_password: Option<&str>,
    log_path: &str,
) -> Result<AuthResult> {
    let mut reply = session
        .authenticate_keyboard_interactive_start(username, None::<String>)
        .await
        .context("SSH keyboard-interactive request failed")?;

    loop {
        match reply {
            KeyboardInteractiveAuthResponse::Success => return Ok(AuthResult::Success),
            KeyboardInteractiveAuthResponse::Failure {
                remaining_methods,
                partial_success,
            } => {
                return Ok(AuthResult::Failure {
                    remaining_methods,
                    partial_success,
                });
            }
            KeyboardInteractiveAuthResponse::InfoRequest {
                instructions,
                prompts,
                ..
            } => {
                let responses = if let Some(pw) = cached_password {
                    if !pw.is_empty() && !prompts.is_empty() && prompts.iter().all(|p| !p.echo) {
                        prompts.iter().map(|_| pw.to_string()).collect()
                    } else {
                        let items: Vec<AuthPromptItem> = prompts
                            .iter()
                            .map(|p| AuthPromptItem {
                                text: p.prompt.clone(),
                                echo: p.echo,
                            })
                            .collect();
                        request_auth_responses(
                            ui_tx, worker_rx, username, &instructions, items, log_path,
                        )?
                    }
                } else {
                    let items: Vec<AuthPromptItem> = prompts
                        .iter()
                        .map(|p| AuthPromptItem {
                            text: p.prompt.clone(),
                            echo: p.echo,
                        })
                        .collect();
                    request_auth_responses(ui_tx, worker_rx, username, &instructions, items, log_path)?
                };

                reply = session
                    .authenticate_keyboard_interactive_respond(responses)
                    .await
                    .context("SSH keyboard-interactive response failed")?;
            }
        }
    }
}

async fn authenticate_via_agent(
    session: &mut client::Handle<AcceptAllClient>,
    username: &str,
    best_rsa_hash: Option<keys::HashAlg>,
) -> Result<AuthResult> {
    #[cfg(unix)]
    let mut agent = keys::agent::client::AgentClient::connect_env().await?;

    #[cfg(windows)]
    let mut agent = {
        if let Ok(sock) = std::env::var("SSH_AUTH_SOCK") {
            keys::agent::client::AgentClient::connect_named_pipe(sock).await?
        } else {
            keys::agent::client::AgentClient::connect_named_pipe(r"\\.\pipe\openssh-ssh-agent")
                .await?
        }
    };

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (session, username, best_rsa_hash);
        return Err(anyhow!(
            "ssh-agent authentication is unsupported on this platform"
        ));
    }

    let identities = agent
        .request_identities()
        .await
        .context("Failed to list identities from ssh-agent")?;
    if identities.is_empty() {
        return Err(anyhow!("ssh-agent has no identities"));
    }

    let mut last_failure = AuthResult::Failure {
        remaining_methods: MethodSet::empty(),
        partial_success: false,
    };

    for key in identities {
        let hash_alg = match key.algorithm() {
            keys::Algorithm::Rsa { .. } => best_rsa_hash,
            _ => None,
        };

        let auth_result = session
            .authenticate_publickey_with(username, key, hash_alg, &mut agent)
            .await
            .context("ssh-agent signing/authentication failed")?;

        match auth_result {
            AuthResult::Success => return Ok(AuthResult::Success),
            AuthResult::Failure {
                partial_success: true,
                remaining_methods,
            } => {
                return Ok(AuthResult::Failure {
                    remaining_methods,
                    partial_success: true,
                });
            }
            failure @ AuthResult::Failure { .. } => {
                last_failure = failure;
            }
        }
    }

    Ok(last_failure)
}

struct AcceptAllClient;

impl client::Handler for AcceptAllClient {
    type Error = russh::Error;

    fn check_server_key(
        &mut self,
        _server_public_key: &keys::PublicKey,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(true) }
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

        let result = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt.block_on(run_shell_async(
                settings,
                scrollback_len,
                &ui_tx,
                worker_rx,
                &log_path,
            )),
            Err(err) => Err(anyhow!("Failed to create async runtime: {err}")),
        };

        if let Err(err) = result {
            logger::log_line(&log_path, &format!("Worker error: {err}"));
            let _ = ui_tx.send(UiMessage::Status(format!("Connection failed: {err}")));
            let _ = ui_tx.send(UiMessage::Connected(false));
        }
    })
}

async fn run_shell_async(
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
    let tcp = tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("Failed to connect to {addr}"))?;
    let _ = tcp.set_nodelay(true);

    let mut config = client::Config::default();
    // Keep sessions alive indefinitely while idle.
    config.inactivity_timeout = None;
    config.keepalive_interval = Some(Duration::from_secs(20));
    // 0 means "do not auto-close after missed keepalive replies".
    config.keepalive_max = 0;
    let config = Arc::new(config);

    logger::log_line(log_path, "Creating SSH session.");
    logger::log_line(log_path, "Performing SSH handshake.");
    let mut session = client::connect_stream(config, tcp, AcceptAllClient)
        .await
        .context("SSH handshake failed")?;

    let username = settings.username.trim();
    let mut remaining_methods = MethodSet::empty();
    let mut authenticated = false;

    let probe = session
        .authenticate_none(username)
        .await
        .context("Failed to query server auth methods")?;
    match probe {
        AuthResult::Success => {
            authenticated = true;
        }
        AuthResult::Failure {
            remaining_methods: methods,
            ..
        } => {
            remaining_methods = methods;
        }
    }

    logger::log_line(
        log_path,
        &format!("Server auth methods: {}", method_set_to_csv(&remaining_methods)),
    );

    let mut supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
    let mut supports_pass = supports_method(&remaining_methods, MethodKind::Password);
    let mut supports_pubkey = supports_method(&remaining_methods, MethodKind::PublicKey);
    let mut pubkey_partially_accepted = false;

    let best_rsa_hash = if supports_pubkey {
        match session.best_supported_rsa_hash().await {
            Ok(v) => v.flatten(),
            Err(err) => {
                logger::log_line(
                    log_path,
                    &format!("Unable to query best RSA hash from server: {err}"),
                );
                None
            }
        }
    } else {
        None
    };

    if !authenticated && supports_pubkey && !settings.private_key_path.trim().is_empty() {
        let key_path = settings.private_key_path.trim();
        logger::log_line(log_path, &format!("Authenticating via private key: {key_path}"));

        let private_key = Path::new(key_path);
        if !private_key.exists() {
            logger::log_line(log_path, "Private key path does not exist.");
        } else {
            let sidecar_pub = Path::new(&format!("{key_path}.pub")).exists();
            if sidecar_pub {
                logger::log_line(
                    log_path,
                    "Sidecar .pub file detected; russh derives public key from the private key.",
                );
            }

            if let Some(fmt) = detect_private_key_format(private_key) {
                logger::log_line(log_path, &format!("Detected private key format: {fmt}"));
            } else {
                logger::log_line(log_path, "Could not determine private key format from file header.");
            }

            let saved_passphrase = if settings.key_passphrase.trim().is_empty() {
                None
            } else {
                Some(settings.key_passphrase.as_str())
            };

            let mut key_auth_result = match authenticate_with_private_key(
                &mut session,
                username,
                private_key,
                saved_passphrase,
                best_rsa_hash,
            )
            .await
            {
                Ok(result) => Some(result),
                Err(err) => {
                    logger::log_line(log_path, &format!("Private key auth failed: {err}"));
                    if error_mentions_passphrase(&err) {
                        logger::log_line(log_path, "Private key may need a passphrase; prompting user.");
                        match request_auth_responses(
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
                            Ok(responses) => {
                                let pw = responses.first().cloned().unwrap_or_default();
                                if !pw.is_empty() {
                                    match authenticate_with_private_key(
                                        &mut session,
                                        username,
                                        private_key,
                                        Some(&pw),
                                        best_rsa_hash,
                                    )
                                    .await
                                    {
                                        Ok(result) => Some(result),
                                        Err(err) => {
                                            logger::log_line(
                                                log_path,
                                                &format!(
                                                    "Private key auth (with passphrase) failed: {err}"
                                                ),
                                            );
                                            None
                                        }
                                    }
                                } else {
                                    None
                                }
                            }
                            Err(err) => {
                                logger::log_line(log_path, &format!("Auth prompt failed: {err}"));
                                None
                            }
                        }
                    } else {
                        if saved_passphrase.is_some() {
                            logger::log_line(
                                log_path,
                                "Skipping passphrase re-prompt because a saved passphrase already exists and the error was not passphrase-related.",
                            );
                        }
                        None
                    }
                }
            };

            if let Some(auth_result) = key_auth_result.take() {
                let partial =
                    apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);

                if authenticated {
                    logger::log_line(log_path, "Private key authentication succeeded.");
                } else if partial {
                    pubkey_partially_accepted = true;
                    logger::log_line(
                        log_path,
                        "Private key accepted, but server requires additional authentication.",
                    );
                    let _ = ui_tx.send(UiMessage::Status(
                        "Additional authentication required (server policy)".to_string(),
                    ));
                } else {
                    logger::log_line(
                        log_path,
                        "Private key auth failed: Username/PublicKey combination invalid",
                    );
                }

                supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
                supports_pass = supports_method(&remaining_methods, MethodKind::Password);
                supports_pubkey = supports_method(&remaining_methods, MethodKind::PublicKey);
            }
        }
    }

    if !authenticated && supports_pubkey {
        logger::log_line(log_path, "Authenticating via ssh-agent.");
        match authenticate_via_agent(&mut session, username, best_rsa_hash).await {
            Ok(auth_result) => {
                let partial =
                    apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);

                if authenticated {
                    logger::log_line(log_path, "ssh-agent authentication succeeded.");
                } else if partial {
                    pubkey_partially_accepted = true;
                    logger::log_line(
                        log_path,
                        "ssh-agent key accepted, but server requires additional authentication.",
                    );
                    let _ = ui_tx.send(UiMessage::Status(
                        "Additional authentication required (server policy)".to_string(),
                    ));
                } else {
                    logger::log_line(log_path, "ssh-agent auth failed.");
                }

                supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
                supports_pass = supports_method(&remaining_methods, MethodKind::Password);
            }
            Err(err) => {
                logger::log_line(log_path, &format!("ssh-agent auth failed: {err}"));
            }
        }
    }

    if !authenticated && supports_kbd {
        logger::log_line(log_path, "Authenticating via keyboard-interactive.");
        match authenticate_keyboard_interactive(
            &mut session,
            username,
            ui_tx,
            &worker_rx,
            if settings.password.is_empty() {
                None
            } else {
                Some(settings.password.as_str())
            },
            log_path,
        )
        .await
        {
            Ok(auth_result) => {
                let partial =
                    apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);

                if partial && !authenticated {
                    logger::log_line(
                        log_path,
                        "Keyboard-interactive accepted, but additional auth is required.",
                    );
                } else if !authenticated {
                    logger::log_line(log_path, "Keyboard-interactive auth failed.");
                }

                supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
                supports_pass = supports_method(&remaining_methods, MethodKind::Password);
            }
            Err(err) => {
                logger::log_line(log_path, &format!("Keyboard-interactive auth failed: {err}"));
            }
        }
    }

    if !authenticated && !settings.password.is_empty() && supports_pass {
        if pubkey_partially_accepted {
            logger::log_line(
                log_path,
                "Falling back to explicit password auth after pubkey partial authentication.",
            );
        } else {
            logger::log_line(log_path, "Authenticating via password.");
        }

        match session
            .authenticate_password(username, settings.password.clone())
            .await
        {
            Ok(auth_result) => {
                let partial =
                    apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);

                if partial && !authenticated {
                    logger::log_line(log_path, "Password accepted, but additional auth is required.");
                } else if !authenticated {
                    logger::log_line(log_path, "Password auth failed.");
                }

                supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
                supports_pass = supports_method(&remaining_methods, MethodKind::Password);
            }
            Err(err) => {
                logger::log_line(log_path, &format!("Password auth failed: {err}"));
            }
        }
    }

    // Last resort: if the server supports password but not keyboard-interactive, we must
    // ask the user explicitly for a password (the server cannot "prompt" in the shell).
    if !authenticated && supports_pass && !supports_kbd {
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
            let pw = responses.first().cloned().unwrap_or_default();
            if !pw.is_empty() {
                break pw;
            }
            logger::log_line(log_path, "Empty password submitted; reprompting.");
            let _ = ui_tx.send(UiMessage::Status("Password required".to_string()));
        };

        let auth_result = session
            .authenticate_password(username, pw)
            .await
            .context("SSH password authentication failed")?;
        let _ = apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);
    }

    if !authenticated {
        return Err(anyhow!("SSH authentication failed"));
    }

    logger::log_line(log_path, "Opening SSH channel.");
    let mut channel = session
        .channel_open_session()
        .await
        .context("Failed to open SSH channel")?;

    logger::log_line(log_path, "Requesting PTY.");
    channel
        .request_pty(false, "xterm-256color", 80, 24, 0, 0, &[])
        .await
        .context("Failed to request PTY")?;

    // Some servers ignore the pty type for TERM; set it explicitly as well.
    let _ = channel.set_env(false, "TERM", "xterm-256color").await;

    logger::log_line(log_path, "Starting shell.");
    channel
        .request_shell(true)
        .await
        .context("Failed to start shell")?;

    let _ = ui_tx.send(UiMessage::Status("Connected successfully".to_string()));
    let _ = ui_tx.send(UiMessage::Connected(true));
    logger::log_line(log_path, "Shell connected.");

    let len = scrollback_len.clamp(0, 200_000);
    let len = if len == 0 { TERM_SCROLLBACK_LEN } else { len };
    let mut parser = Parser::new(24, 80, len);
    let mut scanner = CsiQueryScanner::default();
    let mut screen_dirty = true;
    let mut scrollback_dirty = true;
    let screen_emit_interval = Duration::from_millis(16);
    let mut last_screen_emit = Instant::now()
        .checked_sub(screen_emit_interval)
        .unwrap_or_else(Instant::now);

    let mut writer = channel.make_writer();

    loop {
        let mut disconnected = false;

        loop {
            match worker_rx.try_recv() {
                Ok(WorkerMessage::Input(data)) => {
                    writer
                        .write_all(&data)
                        .await
                        .context("Channel write failed")?;
                    writer.flush().await.context("Channel flush failed")?;
                }
                Ok(WorkerMessage::Resize {
                    rows,
                    cols,
                    width_px,
                    height_px,
                }) => {
                    // Keep the remote PTY and our local parser in sync.
                    channel
                        .window_change(cols.into(), rows.into(), width_px, height_px)
                        .await
                        .map_err(|err| anyhow!("Failed to resize PTY: {err}"))?;
                    parser.set_size(rows, cols);
                    screen_dirty = true;
                    scrollback_dirty = true;
                }
                Ok(WorkerMessage::SetScrollback(rows)) => {
                    parser.set_scrollback(rows);
                    screen_dirty = true;
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
            let _ = channel.eof().await;
            let _ = channel.close().await;
            let _ = session
                .disconnect(Disconnect::ByApplication, "", "English")
                .await;
            logger::log_line(log_path, "Disconnected on request.");
            let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
            let _ = ui_tx.send(UiMessage::Connected(false));
            return Ok(());
        }

        match tokio::time::timeout(READ_POLL_INTERVAL, channel.wait()).await {
            Ok(Some(ChannelMsg::Data { data })) => {
                process_with_query_responses(&mut parser, &mut scanner, &mut writer, data.as_ref())
                    .await?;
                screen_dirty = true;
                scrollback_dirty = true;
            }
            Ok(Some(ChannelMsg::ExtendedData { data, .. })) => {
                process_with_query_responses(&mut parser, &mut scanner, &mut writer, data.as_ref())
                    .await?;
                screen_dirty = true;
                scrollback_dirty = true;
            }
            Ok(Some(ChannelMsg::Eof)) | Ok(Some(ChannelMsg::Close)) => {
                logger::log_line(log_path, "Channel EOF.");
                let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
                let _ = ui_tx.send(UiMessage::Connected(false));
                return Ok(());
            }
            Ok(Some(_)) => {
                // Ignore other channel events for interactive shell mode.
            }
            Ok(None) => {
                logger::log_line(log_path, "Channel closed.");
                let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
                let _ = ui_tx.send(UiMessage::Connected(false));
                return Ok(());
            }
            Err(_) => {
                // Poll timeout to keep worker input processing responsive.
            }
        }

        if scrollback_dirty {
            send_scrollback_max(ui_tx, &mut parser);
            scrollback_dirty = false;
        }

        if screen_dirty && last_screen_emit.elapsed() >= screen_emit_interval {
            send_screen(ui_tx, &mut parser);
            screen_dirty = false;
            last_screen_emit = Instant::now();
        }
    }
}
