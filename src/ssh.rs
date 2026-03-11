use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crate::terminal_emulator::Parser;
use anyhow::{anyhow, Context, Result};
use russh::client::{self, AuthResult, KeyboardInteractiveAuthResponse};
use russh::keys::{self, load_secret_key, PrivateKeyWithHashAlg};
use russh::{ChannelMsg, Disconnect, MethodKind, MethodSet};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::FileType as SftpFileType;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::logger;
use crate::model::ConnectionSettings;

const READ_POLL_INTERVAL: Duration = Duration::from_millis(30);
pub const TERM_SCROLLBACK_LEN: usize = 5000;
const TERM_SCREEN_EMIT_INTERVAL_BASE: Duration = Duration::from_millis(16);
const TERM_SCREEN_EMIT_INTERVAL_MEDIUM: Duration = Duration::from_millis(24);
const TERM_SCREEN_EMIT_INTERVAL_HIGH: Duration = Duration::from_millis(33);
const TERM_SCREEN_EMIT_INTERVAL_EXTREME: Duration = Duration::from_millis(40);
const TERM_SCREEN_RATE_WINDOW: Duration = Duration::from_millis(250);

fn app_data_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|p| p.to_path_buf()))
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."))
        .join("data")
}

fn user_home_dir() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            let mut home = PathBuf::from(drive);
            home.push(path);
            Some(home)
        })
}

fn ensure_known_hosts_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
}

fn app_known_hosts_path() -> PathBuf {
    let path = user_home_dir()
        .map(|home| home.join(".ssh").join("known_hosts"))
        .unwrap_or_else(|| app_data_dir().join("known_hosts"));
    ensure_known_hosts_parent(&path);
    path
}

fn remove_known_hosts_line(path: &Path, line_to_remove: usize) -> std::io::Result<()> {
    let contents = std::fs::read_to_string(path)?;
    let mut found = false;
    let mut kept: Vec<&str> = Vec::new();

    for (idx, line) in contents.lines().enumerate() {
        let current_line = idx + 1;
        if current_line == line_to_remove {
            found = true;
            continue;
        }
        kept.push(line);
    }

    if !found {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "known_hosts entry line {line_to_remove} was not found in {}",
                path.display()
            ),
        ));
    }

    let mut rewritten = kept.join("\n");
    if !rewritten.is_empty() {
        rewritten.push('\n');
    }
    std::fs::write(path, rewritten)
}

#[derive(Debug)]
pub enum UiMessage {
    Status(String),
    Screen(Box<crate::terminal_emulator::Screen>),
    ScrollbackMax(usize),
    Connected(bool),
    AuthPrompt(AuthPrompt),
    HostKeyPrompt(HostKeyPrompt),
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

fn auth_prompt_text(text: &str, echo: bool, index: usize) -> String {
    let trimmed = text.trim();
    if !trimmed.is_empty() {
        return text.to_string();
    }

    if echo {
        format!("Response {}:", index + 1)
    } else if index == 0 {
        "Secret response:".to_string()
    } else {
        format!("Secret response {}:", index + 1)
    }
}

fn auth_prompt_items_from_keyboard<T>(prompts: &[T]) -> Vec<AuthPromptItem>
where
    T: std::borrow::Borrow<russh::client::Prompt>,
{
    prompts
        .iter()
        .enumerate()
        .map(|(idx, prompt)| {
            let prompt = prompt.borrow();
            AuthPromptItem {
                text: auth_prompt_text(&prompt.prompt, prompt.echo, idx),
                echo: prompt.echo,
            }
        })
        .collect()
}

fn looks_like_password_prompt_text(text: &str) -> bool {
    let text = text.trim().to_ascii_lowercase();
    text.is_empty() || text.contains("password")
}

fn can_auto_fill_cached_password<T>(prompts: &[T], password: &str) -> bool
where
    T: std::borrow::Borrow<russh::client::Prompt>,
{
    if password.is_empty() || prompts.len() != 1 {
        return false;
    }

    let prompt = prompts[0].borrow();
    !prompt.echo && looks_like_password_prompt_text(&prompt.prompt)
}

#[derive(Debug, Clone)]
pub struct HostKeyPrompt {
    pub host: String,
    pub port: u16,
    pub algorithm: String,
    pub fingerprint: String,
    pub known_hosts_path: String,
    pub changed_line: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKeyDecision {
    TrustAndSave,
    ReplaceAndSave,
    Reject,
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
    AttachSftpClient {
        client_id: u64,
        ui_tx: Sender<SftpUiMessage>,
        worker_rx: Receiver<SftpWorkerMessage>,
    },
    TransferCommand(TransferCommand),
    AuthResponse(Vec<String>),
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct SftpEntry {
    pub file_name: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified_unix: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum SftpCommand {
    ListDir {
        request_id: u64,
        path: String,
    },
    MakeDir {
        request_id: u64,
        path: String,
    },
    Rename {
        request_id: u64,
        old_path: String,
        new_path: String,
    },
    Delete {
        request_id: u64,
        path: String,
        is_dir: bool,
    },
}

#[derive(Debug, Clone)]
pub enum SftpEvent {
    ListDir {
        request_id: u64,
        path: String,
        entries: Vec<SftpEntry>,
    },
    OperationOk {
        request_id: u64,
        message: String,
    },
    OperationErr {
        request_id: u64,
        message: String,
    },
}

#[derive(Debug)]
pub enum SftpWorkerMessage {
    Command(SftpCommand),
    Disconnect,
}

#[derive(Debug)]
pub enum SftpUiMessage {
    Status(String),
    Connected(bool),
    Event(SftpEvent),
}

#[derive(Debug)]
pub enum TransferCommand {
    Download {
        request_id: u64,
        remote_path: String,
        local_path: String,
        resume_from_local: bool,
        event_tx: Sender<DownloadManagerEvent>,
        cancel_rx: Receiver<()>,
    },
    Upload {
        request_id: u64,
        remote_path: String,
        local_path: String,
        event_tx: Sender<DownloadManagerEvent>,
        cancel_rx: Receiver<()>,
    },
}

#[derive(Debug, Clone)]
pub enum DownloadManagerEvent {
    Started {
        request_id: u64,
        remote_path: String,
        local_path: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    Progress {
        request_id: u64,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
        speed_bps: f64,
    },
    Paused {
        request_id: u64,
        message: String,
    },
    Finished {
        request_id: u64,
        local_path: String,
    },
    Failed {
        request_id: u64,
        message: String,
    },
    Canceled {
        request_id: u64,
        local_path: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalQuery {
    Status,                           // CSI 5 n
    CursorPosition { private: bool }, // CSI 6 n / CSI ? 6 n
    DeviceAttributes,                 // CSI c / CSI 0 c
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
    methods.contains(&target)
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

fn adaptive_screen_emit_interval(window_bytes: u64, window_elapsed: Duration) -> Duration {
    let secs = window_elapsed.as_secs_f64();
    if secs <= 0.0 {
        return TERM_SCREEN_EMIT_INTERVAL_BASE;
    }
    let bytes_per_sec = window_bytes as f64 / secs;
    if bytes_per_sec >= 350_000.0 {
        TERM_SCREEN_EMIT_INTERVAL_EXTREME
    } else if bytes_per_sec >= 180_000.0 {
        TERM_SCREEN_EMIT_INTERVAL_HIGH
    } else if bytes_per_sec >= 90_000.0 {
        TERM_SCREEN_EMIT_INTERVAL_MEDIUM
    } else {
        TERM_SCREEN_EMIT_INTERVAL_BASE
    }
}

async fn open_sftp_channel(session: &mut SshHandle, log_path: &str) -> Result<SftpSession> {
    logger::log_line(log_path, "Opening SFTP channel.");
    let channel = session
        .channel_open_session()
        .await
        .context("Failed to open SFTP channel")?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .context("Failed to request SFTP subsystem")?;
    SftpSession::new(channel.into_stream())
        .await
        .context("Failed to initialize SFTP session")
}

fn sort_sftp_entries(entries: &mut [SftpEntry]) {
    entries.sort_by(|a, b| {
        b.is_dir.cmp(&a.is_dir).then_with(|| {
            a.file_name
                .to_ascii_lowercase()
                .cmp(&b.file_name.to_ascii_lowercase())
        })
    });
}

fn sftp_command_request_id(cmd: &SftpCommand) -> u64 {
    match cmd {
        SftpCommand::ListDir { request_id, .. }
        | SftpCommand::MakeDir { request_id, .. }
        | SftpCommand::Rename { request_id, .. }
        | SftpCommand::Delete { request_id, .. } => *request_id,
    }
}

async fn execute_sftp_command(sftp: &SftpSession, cmd: SftpCommand) -> Result<SftpEvent> {
    match cmd {
        SftpCommand::ListDir { request_id, path } => {
            let canonical = sftp.canonicalize(path.clone()).await.unwrap_or(path);
            let read_dir = sftp
                .read_dir(canonical.clone())
                .await
                .with_context(|| format!("Failed to read remote directory: {canonical}"))?;

            let mut entries: Vec<SftpEntry> = read_dir
                .map(|entry| {
                    let metadata = entry.metadata();
                    let modified_unix = metadata
                        .modified()
                        .ok()
                        .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_secs());

                    SftpEntry {
                        file_name: entry.file_name(),
                        is_dir: matches!(entry.file_type(), SftpFileType::Dir),
                        size: metadata.len(),
                        modified_unix,
                    }
                })
                .collect();
            sort_sftp_entries(&mut entries);

            Ok(SftpEvent::ListDir {
                request_id,
                path: canonical,
                entries,
            })
        }
        SftpCommand::MakeDir { request_id, path } => {
            sftp.create_dir(path.clone())
                .await
                .with_context(|| format!("Failed to create directory: {path}"))?;
            Ok(SftpEvent::OperationOk {
                request_id,
                message: format!("Created folder: {path}"),
            })
        }
        SftpCommand::Rename {
            request_id,
            old_path,
            new_path,
        } => {
            sftp.rename(old_path.clone(), new_path.clone())
                .await
                .with_context(|| format!("Failed to rename: {old_path} -> {new_path}"))?;
            Ok(SftpEvent::OperationOk {
                request_id,
                message: format!("Renamed to: {new_path}"),
            })
        }
        SftpCommand::Delete {
            request_id,
            path,
            is_dir,
        } => {
            if is_dir {
                sftp.remove_dir(path.clone())
                    .await
                    .with_context(|| format!("Failed to delete directory: {path}"))?;
            } else {
                sftp.remove_file(path.clone())
                    .await
                    .with_context(|| format!("Failed to delete file: {path}"))?;
            }
            Ok(SftpEvent::OperationOk {
                request_id,
                message: format!("Deleted: {path}"),
            })
        }
    }
}

fn transfer_cancel_requested(cancel_rx: &Receiver<()>) -> bool {
    matches!(
        cancel_rx.try_recv(),
        Ok(_) | Err(TryRecvError::Disconnected)
    )
}

async fn run_sftp_download_with_session(
    sftp: SftpSession,
    request_id: u64,
    remote_path: String,
    local_path: String,
    resume_from_local: bool,
    event_tx: &Sender<DownloadManagerEvent>,
    cancel_rx: &Receiver<()>,
) -> Result<()> {
    let mut remote = sftp
        .open(remote_path.clone())
        .await
        .with_context(|| format!("Failed to open remote file: {remote_path}"))?;
    let total_bytes = remote.metadata().await.ok().map(|m| m.len());

    let local_path_obj = std::path::Path::new(&local_path);
    if let Some(parent) = local_path_obj.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create local directory: {}", parent.display())
            })?;
        }
    }

    let mut downloaded_bytes: u64 = 0;
    let mut out = if resume_from_local {
        let existing_size = std::fs::metadata(local_path_obj)
            .map(|m| m.len())
            .unwrap_or(0);
        if existing_size > 0 {
            match total_bytes {
                Some(total) if existing_size == total => {
                    let _ = event_tx.send(DownloadManagerEvent::Started {
                        request_id,
                        remote_path,
                        local_path: local_path.clone(),
                        downloaded_bytes: existing_size,
                        total_bytes,
                    });
                    let _ = event_tx.send(DownloadManagerEvent::Finished {
                        request_id,
                        local_path,
                    });
                    return Ok(());
                }
                Some(total) if existing_size > total => std::fs::File::create(local_path_obj)
                    .with_context(|| format!("Failed to reset local file: {local_path}"))?,
                _ => {
                    remote
                        .seek(std::io::SeekFrom::Start(existing_size))
                        .await
                        .with_context(|| {
                            format!("Failed to seek remote file for resume: {remote_path}")
                        })?;
                    downloaded_bytes = existing_size;
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(local_path_obj)
                        .with_context(|| {
                            format!("Failed to open local file for resume: {local_path}")
                        })?
                }
            }
        } else {
            std::fs::File::create(local_path_obj)
                .with_context(|| format!("Failed to create local file: {local_path}"))?
        }
    } else {
        std::fs::File::create(local_path_obj)
            .with_context(|| format!("Failed to create local file: {local_path}"))?
    };

    let _ = event_tx.send(DownloadManagerEvent::Started {
        request_id,
        remote_path,
        local_path: local_path.clone(),
        downloaded_bytes,
        total_bytes,
    });

    let started_at = Instant::now();
    let mut downloaded_this_attempt: u64 = 0;
    let mut buf = [0u8; 128 * 1024];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            let _ = event_tx.send(DownloadManagerEvent::Canceled {
                request_id,
                local_path: local_path.clone(),
            });
            return Ok(());
        }

        let n = remote
            .read(&mut buf)
            .await
            .context("Failed while reading remote file")?;
        if n == 0 {
            break;
        }
        std::io::Write::write_all(&mut out, &buf[..n])
            .context("Failed while writing local file")?;
        downloaded_bytes = downloaded_bytes.saturating_add(n as u64);
        downloaded_this_attempt = downloaded_this_attempt.saturating_add(n as u64);
        let elapsed = started_at.elapsed().as_secs_f64();
        let speed_bps = if elapsed > 0.0 {
            downloaded_this_attempt as f64 / elapsed
        } else {
            0.0
        };
        let _ = event_tx.send(DownloadManagerEvent::Progress {
            request_id,
            downloaded_bytes,
            total_bytes,
            speed_bps,
        });
    }

    std::io::Write::flush(&mut out).context("Failed to flush local file")?;
    let _ = event_tx.send(DownloadManagerEvent::Finished {
        request_id,
        local_path,
    });
    Ok(())
}

async fn run_sftp_upload_with_session(
    sftp: SftpSession,
    request_id: u64,
    remote_path: String,
    local_path: String,
    event_tx: &Sender<DownloadManagerEvent>,
    cancel_rx: &Receiver<()>,
) -> Result<()> {
    let mut local = tokio::fs::File::open(local_path.clone())
        .await
        .with_context(|| format!("Failed to open local file: {local_path}"))?;
    let total_bytes = local.metadata().await.ok().map(|m| m.len());
    let mut remote = sftp
        .create(remote_path.clone())
        .await
        .with_context(|| format!("Failed to create remote file: {remote_path}"))?;

    let _ = event_tx.send(DownloadManagerEvent::Started {
        request_id,
        remote_path,
        local_path: local_path.clone(),
        downloaded_bytes: 0,
        total_bytes,
    });

    let started_at = Instant::now();
    let mut uploaded_bytes: u64 = 0;
    let mut buf = [0u8; 128 * 1024];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            let _ = event_tx.send(DownloadManagerEvent::Canceled {
                request_id,
                local_path: local_path.clone(),
            });
            return Ok(());
        }

        let n = local
            .read(&mut buf)
            .await
            .context("Failed while reading local file")?;
        if n == 0 {
            break;
        }
        remote
            .write_all(&buf[..n])
            .await
            .context("Failed while writing remote file")?;
        uploaded_bytes = uploaded_bytes.saturating_add(n as u64);
        let elapsed = started_at.elapsed().as_secs_f64();
        let speed_bps = if elapsed > 0.0 {
            uploaded_bytes as f64 / elapsed
        } else {
            0.0
        };
        let _ = event_tx.send(DownloadManagerEvent::Progress {
            request_id,
            downloaded_bytes: uploaded_bytes,
            total_bytes,
            speed_bps,
        });
    }

    remote
        .flush()
        .await
        .context("Failed to flush remote file")?;
    remote
        .shutdown()
        .await
        .context("Failed to finalize remote file")?;
    let _ = event_tx.send(DownloadManagerEvent::Finished {
        request_id,
        local_path,
    });
    Ok(())
}

async fn run_sftp_client_async(
    client_id: u64,
    sftp: SftpSession,
    ui_tx: Sender<SftpUiMessage>,
    worker_rx: Receiver<SftpWorkerMessage>,
    log_path: String,
) -> Result<()> {
    logger::log_line(&log_path, &format!("SFTP client {client_id} connected."));
    let _ = ui_tx.send(SftpUiMessage::Status("Connected".to_string()));
    let _ = ui_tx.send(SftpUiMessage::Connected(true));

    loop {
        let mut disconnected = false;

        loop {
            match worker_rx.try_recv() {
                Ok(SftpWorkerMessage::Command(cmd)) => {
                    let request_id = sftp_command_request_id(&cmd);
                    match execute_sftp_command(&sftp, cmd).await {
                        Ok(event) => {
                            let _ = ui_tx.send(SftpUiMessage::Event(event));
                        }
                        Err(err) => {
                            logger::log_line(
                                &log_path,
                                &format!("SFTP client {client_id} command failed: {err}"),
                            );
                            let _ = ui_tx.send(SftpUiMessage::Event(SftpEvent::OperationErr {
                                request_id,
                                message: err.to_string(),
                            }));
                        }
                    }
                }
                Ok(SftpWorkerMessage::Disconnect) => {
                    disconnected = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        if disconnected {
            logger::log_line(&log_path, &format!("SFTP client {client_id} disconnected."));
            let _ = ui_tx.send(SftpUiMessage::Status("Disconnected".to_string()));
            let _ = ui_tx.send(SftpUiMessage::Connected(false));
            return Ok(());
        }

        tokio::time::sleep(READ_POLL_INTERVAL).await;
    }
}

fn request_auth_responses(
    ui_tx: &Sender<UiMessage>,
    worker_rx: &Receiver<WorkerMessage>,
    username: &str,
    instructions: &str,
    prompts: Vec<AuthPromptItem>,
    log_path: &str,
) -> Result<Vec<String>> {
    if prompts.is_empty() {
        logger::log_line(
            log_path,
            &format!("Auth prompt requires no user input (user={username:?}); auto-continuing."),
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
    session: &mut SshHandle,
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
    session: &mut SshHandle,
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
                let items = auth_prompt_items_from_keyboard(&prompts);
                let responses = if items.is_empty() {
                    Vec::new()
                } else if let Some(pw) = cached_password {
                    if can_auto_fill_cached_password(&prompts, pw) {
                        vec![pw.to_string()]
                    } else {
                        request_auth_responses(
                            ui_tx,
                            worker_rx,
                            username,
                            &instructions,
                            items,
                            log_path,
                        )?
                    }
                } else {
                    request_auth_responses(
                        ui_tx,
                        worker_rx,
                        username,
                        &instructions,
                        items,
                        log_path,
                    )?
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
    session: &mut SshHandle,
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

type SshHandle = client::Handle<KnownHostsClient>;

struct ActiveSftpClient {
    client_id: u64,
    ui_tx: Sender<SftpUiMessage>,
    abort_handle: tokio::task::AbortHandle,
}

fn stop_sftp_client(client: ActiveSftpClient, message: &str) {
    if client.abort_handle.is_finished() {
        return;
    }
    client.abort_handle.abort();
    let _ = client
        .ui_tx
        .send(SftpUiMessage::Status(message.to_string()));
    let _ = client.ui_tx.send(SftpUiMessage::Connected(false));
}

fn stop_active_sftp_clients(active_clients: &mut Vec<ActiveSftpClient>, message: &str) {
    for client in active_clients.drain(..) {
        stop_sftp_client(client, message);
    }
}

struct ActiveTransfer {
    request_id: u64,
    event_tx: Sender<DownloadManagerEvent>,
    abort_handle: tokio::task::AbortHandle,
}

fn stop_active_transfers(active_transfers: &mut Vec<ActiveTransfer>, message: &str) {
    for transfer in active_transfers.drain(..) {
        if transfer.abort_handle.is_finished() {
            continue;
        }
        transfer.abort_handle.abort();
        let _ = transfer.event_tx.send(DownloadManagerEvent::Paused {
            request_id: transfer.request_id,
            message: message.to_string(),
        });
    }
}

enum HostKeyVerificationMode {
    Interactive {
        ui_tx: Sender<UiMessage>,
        decision_rx: Receiver<HostKeyDecision>,
        log_path: String,
    },
}

struct KnownHostsClient {
    host: String,
    port: u16,
    known_hosts_path: PathBuf,
    mode: HostKeyVerificationMode,
}

impl KnownHostsClient {
    fn interactive(
        host: String,
        port: u16,
        ui_tx: Sender<UiMessage>,
        decision_rx: Receiver<HostKeyDecision>,
        log_path: String,
    ) -> Self {
        Self {
            host,
            port,
            known_hosts_path: app_known_hosts_path(),
            mode: HostKeyVerificationMode::Interactive {
                ui_tx,
                decision_rx,
                log_path,
            },
        }
    }

    fn verify_server_key(
        &mut self,
        server_public_key: &keys::PublicKey,
    ) -> Result<bool, russh::Error> {
        match keys::known_hosts::check_known_hosts_path(
            &self.host,
            self.port,
            server_public_key,
            &self.known_hosts_path,
        ) {
            Ok(true) => Ok(true),
            Ok(false) => self.handle_host_key_prompt(server_public_key, None),
            Err(keys::Error::KeyChanged { line }) => {
                self.handle_host_key_prompt(server_public_key, Some(line))
            }
            Err(err) => Err(err.into()),
        }
    }

    fn handle_host_key_prompt(
        &mut self,
        server_public_key: &keys::PublicKey,
        changed_line: Option<usize>,
    ) -> Result<bool, russh::Error> {
        match &mut self.mode {
            HostKeyVerificationMode::Interactive {
                ui_tx,
                decision_rx,
                log_path,
            } => {
                let fingerprint =
                    format!("{}", server_public_key.fingerprint(keys::HashAlg::Sha256));
                let prompt = HostKeyPrompt {
                    host: self.host.clone(),
                    port: self.port,
                    algorithm: server_public_key.algorithm().to_string(),
                    fingerprint,
                    known_hosts_path: self.known_hosts_path.to_string_lossy().into_owned(),
                    changed_line,
                };
                let prompt_message = if let Some(line) = changed_line {
                    format!(
                        "Host key changed for {}:{} (known_hosts line {}); waiting for user decision.",
                        self.host, self.port, line
                    )
                } else {
                    format!(
                        "Unknown host key for {}:{}; waiting for user trust decision.",
                        self.host, self.port
                    )
                };
                logger::log_line(log_path.as_str(), &prompt_message);
                let _ = ui_tx.send(UiMessage::HostKeyPrompt(prompt));

                match decision_rx.recv_timeout(Duration::from_secs(600)) {
                    Ok(HostKeyDecision::TrustAndSave) => {
                        keys::known_hosts::learn_known_hosts_path(
                            &self.host,
                            self.port,
                            server_public_key,
                            &self.known_hosts_path,
                        )?;
                        logger::log_line(
                            log_path.as_str(),
                            &format!(
                                "Pinned host key for {}:{} to {}.",
                                self.host,
                                self.port,
                                self.known_hosts_path.display()
                            ),
                        );
                        Ok(true)
                    }
                    Ok(HostKeyDecision::ReplaceAndSave) => {
                        let Some(line) = changed_line else {
                            logger::log_line(
                                log_path.as_str(),
                                &format!(
                                    "Ignoring replace-host-key decision for {}:{} because no existing entry was provided.",
                                    self.host, self.port
                                ),
                            );
                            return Ok(false);
                        };
                        remove_known_hosts_line(&self.known_hosts_path, line)?;
                        keys::known_hosts::learn_known_hosts_path(
                            &self.host,
                            self.port,
                            server_public_key,
                            &self.known_hosts_path,
                        )?;
                        logger::log_line(
                            log_path.as_str(),
                            &format!(
                                "Replaced pinned host key for {}:{} in {} (old line {}).",
                                self.host,
                                self.port,
                                self.known_hosts_path.display(),
                                line
                            ),
                        );
                        Ok(true)
                    }
                    Ok(HostKeyDecision::Reject) => {
                        logger::log_line(
                            log_path.as_str(),
                            &format!("User rejected host key for {}:{}.", self.host, self.port),
                        );
                        Ok(false)
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        logger::log_line(
                            log_path.as_str(),
                            &format!(
                                "Timed out waiting for host key decision for {}:{}.",
                                self.host, self.port
                            ),
                        );
                        Ok(false)
                    }
                    Err(RecvTimeoutError::Disconnected) => Ok(false),
                }
            }
        }
    }
}

impl client::Handler for KnownHostsClient {
    type Error = russh::Error;

    fn check_server_key(
        &mut self,
        server_public_key: &keys::PublicKey,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        let result = self.verify_server_key(server_public_key);
        async move { result }
    }
}

pub fn start_shell(
    settings: ConnectionSettings,
    scrollback_len: usize,
    ui_tx: Sender<UiMessage>,
    worker_rx: Receiver<WorkerMessage>,
    host_key_rx: Receiver<HostKeyDecision>,
    log_path: String,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        logger::log_line(&log_path, "Starting SSH worker.");

        let result = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => {
                let local = tokio::task::LocalSet::new();
                local.block_on(
                    &rt,
                    run_shell_async(
                        settings,
                        scrollback_len,
                        &ui_tx,
                        worker_rx,
                        host_key_rx,
                        &log_path,
                    ),
                )
            }
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
    host_key_rx: Receiver<HostKeyDecision>,
    log_path: &str,
) -> Result<()> {
    logger::log_line(log_path, "Validating settings.");
    if settings.host.trim().is_empty() {
        return Err(anyhow!("Host is required"));
    }
    if settings.username.trim().is_empty() {
        return Err(anyhow!("Username is required"));
    }

    let host = settings.host.trim().to_string();
    let port = settings.port;
    let addr = format!("{host}:{port}");
    logger::log_line(log_path, &format!("Connecting TCP to {addr}."));
    let tcp = tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("Failed to connect to {addr}"))?;
    let _ = tcp.set_nodelay(true);

    let config = client::Config {
        // Keep sessions alive indefinitely while idle.
        inactivity_timeout: None,
        keepalive_interval: Some(Duration::from_secs(20)),
        // 0 means "do not auto-close after missed keepalive replies".
        keepalive_max: 0,
        ..Default::default()
    };
    let config = Arc::new(config);

    logger::log_line(log_path, "Creating SSH session.");
    logger::log_line(log_path, "Performing SSH handshake.");
    let mut session = client::connect_stream(
        config,
        tcp,
        KnownHostsClient::interactive(host, port, ui_tx.clone(), host_key_rx, log_path.to_string()),
    )
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
        &format!(
            "Server auth methods: {}",
            method_set_to_csv(&remaining_methods)
        ),
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
        logger::log_line(
            log_path,
            &format!("Authenticating via private key: {key_path}"),
        );

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
                logger::log_line(
                    log_path,
                    "Could not determine private key format from file header.",
                );
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
                        logger::log_line(
                            log_path,
                            "Private key may need a passphrase; prompting user.",
                        );
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
                logger::log_line(
                    log_path,
                    &format!("Keyboard-interactive auth failed: {err}"),
                );
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
                    logger::log_line(
                        log_path,
                        "Password accepted, but additional auth is required.",
                    );
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
    let mut screen_emit_interval = TERM_SCREEN_EMIT_INTERVAL_BASE;
    let mut last_screen_emit = Instant::now()
        .checked_sub(screen_emit_interval)
        .unwrap_or_else(Instant::now);
    let mut screen_rate_window_started = Instant::now();
    let mut screen_rate_window_bytes: u64 = 0;
    let mut active_sftp_clients: Vec<ActiveSftpClient> = Vec::new();
    let mut active_transfers: Vec<ActiveTransfer> = Vec::new();

    let mut writer = channel.make_writer();

    loop {
        let mut disconnected = false;
        active_sftp_clients.retain(|client| !client.abort_handle.is_finished());
        active_transfers.retain(|transfer| !transfer.abort_handle.is_finished());

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
                Ok(WorkerMessage::AttachSftpClient {
                    client_id,
                    ui_tx,
                    worker_rx,
                }) => {
                    if let Some(index) = active_sftp_clients
                        .iter()
                        .position(|client| client.client_id == client_id)
                    {
                        let client = active_sftp_clients.swap_remove(index);
                        stop_sftp_client(client, "SFTP session restarted.");
                    }

                    let _ = ui_tx.send(SftpUiMessage::Status(
                        "Connecting SFTP session...".to_string(),
                    ));

                    match open_sftp_channel(&mut session, log_path).await {
                        Ok(sftp) => {
                            let log_path = log_path.to_string();
                            let tracked_ui_tx = ui_tx.clone();
                            let task = tokio::task::spawn_local(async move {
                                if let Err(err) = run_sftp_client_async(
                                    client_id,
                                    sftp,
                                    ui_tx.clone(),
                                    worker_rx,
                                    log_path.clone(),
                                )
                                .await
                                {
                                    logger::log_line(
                                        &log_path,
                                        &format!("SFTP client {client_id} failed: {err}"),
                                    );
                                    let _ = ui_tx.send(SftpUiMessage::Status(err.to_string()));
                                    let _ = ui_tx.send(SftpUiMessage::Connected(false));
                                }
                            });
                            active_sftp_clients.push(ActiveSftpClient {
                                client_id,
                                ui_tx: tracked_ui_tx,
                                abort_handle: task.abort_handle(),
                            });
                            drop(task);
                        }
                        Err(err) => {
                            logger::log_line(
                                log_path,
                                &format!("Failed to open SFTP client {client_id}: {err}"),
                            );
                            let _ = ui_tx.send(SftpUiMessage::Status(format!(
                                "Failed to open SFTP session: {err}"
                            )));
                            let _ = ui_tx.send(SftpUiMessage::Connected(false));
                        }
                    }
                }
                Ok(WorkerMessage::TransferCommand(cmd)) => match cmd {
                    TransferCommand::Download {
                        request_id,
                        remote_path,
                        local_path,
                        resume_from_local,
                        event_tx,
                        cancel_rx,
                    } => {
                        if transfer_cancel_requested(&cancel_rx) {
                            let _ = event_tx.send(DownloadManagerEvent::Canceled {
                                request_id,
                                local_path,
                            });
                            continue;
                        }

                        match open_sftp_channel(&mut session, log_path).await {
                            Ok(sftp) => {
                                let log_path = log_path.to_string();
                                let tracked_event_tx = event_tx.clone();
                                let task = tokio::task::spawn_local(async move {
                                    if let Err(err) = run_sftp_download_with_session(
                                        sftp,
                                        request_id,
                                        remote_path,
                                        local_path,
                                        resume_from_local,
                                        &event_tx,
                                        &cancel_rx,
                                    )
                                    .await
                                    {
                                        logger::log_line(
                                            &log_path,
                                            &format!(
                                                "Live download transfer {request_id} failed: {err}"
                                            ),
                                        );
                                        let _ = event_tx.send(DownloadManagerEvent::Failed {
                                            request_id,
                                            message: err.to_string(),
                                        });
                                    }
                                });
                                active_transfers.push(ActiveTransfer {
                                    request_id,
                                    event_tx: tracked_event_tx,
                                    abort_handle: task.abort_handle(),
                                });
                                drop(task);
                            }
                            Err(err) => {
                                logger::log_line(
                                    log_path,
                                    &format!(
                                        "Failed to open live SFTP channel for download {request_id}: {err}"
                                    ),
                                );
                                let _ = event_tx.send(DownloadManagerEvent::Failed {
                                    request_id,
                                    message: err.to_string(),
                                });
                            }
                        }
                    }
                    TransferCommand::Upload {
                        request_id,
                        remote_path,
                        local_path,
                        event_tx,
                        cancel_rx,
                    } => {
                        if transfer_cancel_requested(&cancel_rx) {
                            let _ = event_tx.send(DownloadManagerEvent::Canceled {
                                request_id,
                                local_path,
                            });
                            continue;
                        }

                        match open_sftp_channel(&mut session, log_path).await {
                            Ok(sftp) => {
                                let log_path = log_path.to_string();
                                let tracked_event_tx = event_tx.clone();
                                let task = tokio::task::spawn_local(async move {
                                    if let Err(err) = run_sftp_upload_with_session(
                                        sftp,
                                        request_id,
                                        remote_path,
                                        local_path,
                                        &event_tx,
                                        &cancel_rx,
                                    )
                                    .await
                                    {
                                        logger::log_line(
                                            &log_path,
                                            &format!(
                                                "Live upload transfer {request_id} failed: {err}"
                                            ),
                                        );
                                        let _ = event_tx.send(DownloadManagerEvent::Failed {
                                            request_id,
                                            message: err.to_string(),
                                        });
                                    }
                                });
                                active_transfers.push(ActiveTransfer {
                                    request_id,
                                    event_tx: tracked_event_tx,
                                    abort_handle: task.abort_handle(),
                                });
                                drop(task);
                            }
                            Err(err) => {
                                logger::log_line(
                                    log_path,
                                    &format!(
                                        "Failed to open live SFTP channel for upload {request_id}: {err}"
                                    ),
                                );
                                let _ = event_tx.send(DownloadManagerEvent::Failed {
                                    request_id,
                                    message: err.to_string(),
                                });
                            }
                        }
                    }
                },
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
            stop_active_sftp_clients(
                &mut active_sftp_clients,
                "SFTP session disconnected. Reconnect the source terminal to continue.",
            );
            stop_active_transfers(
                &mut active_transfers,
                "Transfer paused because the SSH session disconnected. Reconnect the tab and click retry.",
            );
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
                screen_rate_window_bytes =
                    screen_rate_window_bytes.saturating_add(data.len() as u64);
                screen_dirty = true;
                scrollback_dirty = true;
            }
            Ok(Some(ChannelMsg::ExtendedData { data, .. })) => {
                process_with_query_responses(&mut parser, &mut scanner, &mut writer, data.as_ref())
                    .await?;
                screen_rate_window_bytes =
                    screen_rate_window_bytes.saturating_add(data.len() as u64);
                screen_dirty = true;
                scrollback_dirty = true;
            }
            Ok(Some(ChannelMsg::Eof)) | Ok(Some(ChannelMsg::Close)) => {
                stop_active_sftp_clients(
                    &mut active_sftp_clients,
                    "SFTP session disconnected. Reconnect the source terminal to continue.",
                );
                stop_active_transfers(
                    &mut active_transfers,
                    "Transfer paused because the SSH session disconnected. Reconnect the tab and click retry.",
                );
                logger::log_line(log_path, "Channel EOF.");
                let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
                let _ = ui_tx.send(UiMessage::Connected(false));
                return Ok(());
            }
            Ok(Some(_)) => {
                // Ignore other channel events for interactive shell mode.
            }
            Ok(None) => {
                stop_active_sftp_clients(
                    &mut active_sftp_clients,
                    "SFTP session disconnected. Reconnect the source terminal to continue.",
                );
                stop_active_transfers(
                    &mut active_transfers,
                    "Transfer paused because the SSH session disconnected. Reconnect the tab and click retry.",
                );
                logger::log_line(log_path, "Channel closed.");
                let _ = ui_tx.send(UiMessage::Status("Disconnected".to_string()));
                let _ = ui_tx.send(UiMessage::Connected(false));
                return Ok(());
            }
            Err(_) => {
                // Poll timeout to keep worker input processing responsive.
            }
        }

        let rate_window_elapsed = screen_rate_window_started.elapsed();
        if rate_window_elapsed >= TERM_SCREEN_RATE_WINDOW {
            screen_emit_interval =
                adaptive_screen_emit_interval(screen_rate_window_bytes, rate_window_elapsed);
            screen_rate_window_started = Instant::now();
            screen_rate_window_bytes = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn blank_auth_prompt_gets_safe_label() {
        assert_eq!(auth_prompt_text("", false, 0), "Secret response:");
        assert_eq!(auth_prompt_text("   ", false, 1), "Secret response 2:");
        assert_eq!(auth_prompt_text("", true, 0), "Response 1:");
    }

    #[test]
    fn password_prompt_detection_is_narrow() {
        assert!(looks_like_password_prompt_text(""));
        assert!(looks_like_password_prompt_text("Password:"));
        assert!(looks_like_password_prompt_text("Login password for alice:"));
        assert!(!looks_like_password_prompt_text("Verification code:"));
        assert!(!looks_like_password_prompt_text("Duo passcode or option:"));
    }

    #[test]
    fn remove_known_hosts_line_rewrites_target_entry() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rusty-known-hosts-test-{}-{unique}.tmp",
            std::process::id()
        ));
        fs::write(
            &path,
            "alpha ssh-ed25519 AAAA\nbeta ssh-ed25519 BBBB\ngamma ssh-ed25519 CCCC\n",
        )
        .unwrap();

        remove_known_hosts_line(&path, 2).unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "alpha ssh-ed25519 AAAA\ngamma ssh-ed25519 CCCC\n");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn remove_known_hosts_line_errors_for_missing_line() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rusty-known-hosts-missing-line-{}-{unique}.tmp",
            std::process::id()
        ));
        fs::write(&path, "alpha ssh-ed25519 AAAA\n").unwrap();

        let err = remove_known_hosts_line(&path, 3).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);

        let _ = fs::remove_file(path);
    }
}
