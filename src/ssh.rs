use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::rc::Rc;
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
use russh_sftp::protocol::{FileAttributes, FileType as SftpFileType, OpenFlags};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::logger;
use crate::model::ConnectionSettings;

const SESSION_HOUSEKEEPING_INTERVAL: Duration = Duration::from_millis(250);
pub const TERM_SCROLLBACK_LEN: usize = 5000;
const TERM_SCREEN_EMIT_INTERVAL_BASE: Duration = Duration::from_millis(24);
const TERM_SCREEN_EMIT_INTERVAL_MEDIUM: Duration = Duration::from_millis(33);
const TERM_SCREEN_EMIT_INTERVAL_HIGH: Duration = Duration::from_millis(50);
const TERM_SCREEN_EMIT_INTERVAL_EXTREME: Duration = Duration::from_millis(66);
const TERM_SCREEN_RATE_WINDOW: Duration = Duration::from_millis(250);
const FAST_REMOTE_COMPARE_BATCH_LIMIT: usize = 64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssueKind {
    Info,
    Authentication,
    HostKey,
    Permission,
    Path,
    Transport,
    Configuration,
    Unknown,
}

impl IssueKind {
    pub fn is_error(self) -> bool {
        !matches!(self, Self::Info)
    }
}

#[derive(Debug, Clone)]
pub struct StatusUpdate {
    pub kind: IssueKind,
    pub message: String,
}

impl StatusUpdate {
    fn info(message: impl Into<String>) -> Self {
        Self {
            kind: IssueKind::Info,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransferIssue {
    pub kind: IssueKind,
    pub message: String,
}

#[derive(Debug)]
pub enum UiMessage {
    Status(StatusUpdate),
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
    Input {
        client_id: u64,
        data: Vec<u8>,
    },
    Resize {
        client_id: u64,
        rows: u16,
        cols: u16,
        width_px: u32,
        height_px: u32,
    },
    SetScrollback {
        client_id: u64,
        rows: usize,
    },
    AttachTerminalClient {
        client_id: u64,
        ui_tx: Sender<UiMessage>,
        scrollback_len: usize,
    },
    DetachTerminalClient {
        client_id: u64,
    },
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
    pub uid: Option<u32>,
    pub user: Option<String>,
    pub gid: Option<u32>,
    pub group: Option<String>,
    pub permissions: Option<u32>,
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
        paths: Vec<String>,
    },
    Copy {
        request_id: u64,
        source_paths: Vec<String>,
        destination_dir: String,
    },
    Move {
        request_id: u64,
        source_paths: Vec<String>,
        destination_dir: String,
    },
    SetPermissions {
        request_id: u64,
        paths: Vec<String>,
        mode: u32,
    },
    SetOwnership {
        request_id: u64,
        paths: Vec<String>,
        owner: Option<String>,
        group: Option<String>,
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
        issue: TransferIssue,
    },
}

#[derive(Debug)]
pub enum SftpWorkerMessage {
    Command(SftpCommand),
    Disconnect,
}

#[derive(Debug)]
pub enum SftpUiMessage {
    Status(StatusUpdate),
    Connected(bool),
    Event(SftpEvent),
}

#[derive(Debug)]
enum TerminalClientCommand {
    Input(Vec<u8>),
    Resize {
        rows: u16,
        cols: u16,
        width_px: u32,
        height_px: u32,
    },
    SetScrollback(usize),
    Disconnect,
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
        resume_from_remote_temp: bool,
        event_tx: Sender<DownloadManagerEvent>,
        cancel_rx: Receiver<()>,
        conflict_response_rx: Receiver<UploadConflictResponse>,
    },
}

#[derive(Debug, Clone)]
pub struct UploadConflictPrompt {
    pub request_id: u64,
    pub local_path: String,
    pub remote_path: String,
    pub conflict_index: usize,
    pub conflict_total: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadConflictChoice {
    OverwriteIfDifferent,
    Skip,
    CancelTransfer,
}

#[derive(Debug, Clone, Copy)]
pub struct UploadConflictResponse {
    pub choice: UploadConflictChoice,
    pub apply_to_all: bool,
}

#[derive(Debug, Clone)]
pub enum DownloadManagerEvent {
    Preparing {
        request_id: u64,
        total_bytes: Option<u64>,
        message: String,
    },
    UploadConflictPrompt {
        prompt: UploadConflictPrompt,
    },
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
        issue: TransferIssue,
    },
    Finished {
        request_id: u64,
        local_path: String,
        message: Option<String>,
    },
    Failed {
        request_id: u64,
        issue: TransferIssue,
    },
    Canceled {
        request_id: u64,
        local_path: String,
    },
}

#[derive(Debug)]
pub struct DetachedTransferRequest {
    pub settings: ConnectionSettings,
    pub request_id: u64,
    pub remote_path: String,
    pub local_path: String,
    pub event_tx: Sender<DownloadManagerEvent>,
    pub cancel_rx: Receiver<()>,
    pub conflict_response_rx: Receiver<UploadConflictResponse>,
    pub log_path: String,
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
    parser: &mut Parser,
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

fn send_message<T>(tx: &Sender<T>, message: T) {
    let _ = tx.send(message);
    crate::tray::request_app_repaint();
}

fn send_screen(ui_tx: &Sender<UiMessage>, parser: &mut Parser) {
    send_message(ui_tx, UiMessage::Screen(Box::new(parser.screen().clone())));
}

fn send_scrollback_max(ui_tx: &Sender<UiMessage>, parser: &mut Parser) {
    send_message(
        ui_tx,
        UiMessage::ScrollbackMax(parser.screen().scrollback_max()),
    );
}

fn bridge_receiver_to_async<T: Send + 'static>(rx: Receiver<T>) -> UnboundedReceiver<T> {
    let (async_tx, async_rx) = unbounded_channel();
    thread::spawn(move || {
        while let Ok(message) = rx.recv() {
            if async_tx.send(message).is_err() {
                break;
            }
        }
    });
    async_rx
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

async fn open_sftp_channel(session: &SshHandle, log_path: &str) -> Result<SftpSession> {
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

struct RemoteExecResult {
    exit_status: Option<u32>,
    exit_signal: Option<String>,
    stdout: String,
    stderr: String,
}

enum FastDeleteError {
    Unsupported(anyhow::Error),
    Failed(anyhow::Error),
}

fn shell_quote_posix(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn shell_join_posix_args(values: &[String]) -> String {
    values
        .iter()
        .map(|value| shell_quote_posix(value))
        .collect::<Vec<_>>()
        .join(" ")
}

fn summarize_remote_exec_output(stdout: &str, stderr: &str) -> String {
    let stdout = stdout.trim();
    let stderr = stderr.trim();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => format!(" stdout: {stdout}"),
        (true, false) => format!(" stderr: {stderr}"),
        (false, false) => format!(" stdout: {stdout} stderr: {stderr}"),
    }
}

async fn run_remote_exec_command(
    session: &SshHandle,
    command: &str,
    log_path: &str,
) -> Result<RemoteExecResult> {
    logger::log_line(log_path, &format!("Running remote exec command: {command}"));
    let mut channel = session
        .channel_open_session()
        .await
        .context("Failed to open SSH exec channel")?;
    channel
        .exec(true, command)
        .await
        .context("Failed to start remote exec command")?;

    let mut exit_status = None;
    let mut exit_signal = None;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    loop {
        let Some(msg) = channel.wait().await else {
            break;
        };
        match msg {
            ChannelMsg::Data { data } => stdout.extend_from_slice(data.as_ref()),
            ChannelMsg::ExtendedData { data, .. } => stderr.extend_from_slice(data.as_ref()),
            ChannelMsg::ExitStatus {
                exit_status: status,
            } => {
                exit_status = Some(status);
            }
            ChannelMsg::ExitSignal { signal_name, .. } => {
                exit_signal = Some(format!("{signal_name:?}"));
            }
            ChannelMsg::Eof | ChannelMsg::Close => {}
            _ => {}
        }
    }

    Ok(RemoteExecResult {
        exit_status,
        exit_signal,
        stdout: String::from_utf8_lossy(&stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&stderr).trim().to_string(),
    })
}

async fn try_fast_delete_remote_paths(
    session: &SshHandle,
    paths: &[String],
    log_path: &str,
) -> std::result::Result<(), FastDeleteError> {
    if paths.is_empty() {
        return Ok(());
    }

    let quoted_paths = paths
        .iter()
        .map(|path| shell_quote_posix(path))
        .collect::<Vec<_>>()
        .join(" ");
    let command = format!("rm -rf -- {quoted_paths}");
    let result = run_remote_exec_command(session, &command, log_path)
        .await
        .map_err(FastDeleteError::Unsupported)?;

    let output = summarize_remote_exec_output(&result.stdout, &result.stderr);
    if let Some(signal) = result.exit_signal.as_deref() {
        return Err(FastDeleteError::Failed(anyhow!(
            "Remote fast delete was terminated by signal {signal}.{}",
            output
        )));
    }

    match result.exit_status.unwrap_or(127) {
        0 => Ok(()),
        126 | 127 => Err(FastDeleteError::Unsupported(anyhow!(
            "Remote shell does not support fast delete.{}",
            output
        ))),
        status => {
            let stderr_lower = result.stderr.to_ascii_lowercase();
            if stderr_lower.contains("not found")
                || stderr_lower.contains("is not recognized")
                || stderr_lower.contains("unknown command")
            {
                Err(FastDeleteError::Unsupported(anyhow!(
                    "Remote shell does not support fast delete.{}",
                    output
                )))
            } else {
                Err(FastDeleteError::Failed(anyhow!(
                    "Remote fast delete failed with exit status {status}.{}",
                    output
                )))
            }
        }
    }
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

fn join_remote_path(base: &str, name: &str) -> String {
    let base = base.trim();
    let name = name.trim();
    if name.is_empty() {
        return base.to_string();
    }
    if name.starts_with('/') {
        return name.to_string();
    }
    if base.is_empty() || base == "." {
        return name.to_string();
    }
    if base == "/" {
        return format!("/{name}");
    }
    format!("{}/{}", base.trim_end_matches('/'), name)
}

fn normalize_remote_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return ".".to_string();
    }

    let absolute = trimmed.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in trimmed.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if let Some(last) = parts.last().copied() {
                    if last != ".." {
                        parts.pop();
                    } else if !absolute {
                        parts.push("..");
                    }
                } else if !absolute {
                    parts.push("..");
                }
            }
            other => parts.push(other),
        }
    }

    if absolute {
        if parts.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", parts.join("/"))
        }
    } else if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn remote_basename(path: &str) -> Option<String> {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() || trimmed == "." || trimmed == "/" {
        return None;
    }
    trimmed
        .rsplit('/')
        .next()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| name.to_string())
}

async fn ensure_remote_dir_all(sftp: &SftpSession, path: &str) -> Result<()> {
    let normalized = normalize_remote_path(path);
    if normalized.is_empty() || normalized == "." || normalized == "/" {
        return Ok(());
    }

    let absolute = normalized.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in normalized.split('/') {
        match part {
            "" => {}
            ".." => parts.push(part),
            other => {
                parts.push(other);
                if other == "." {
                    continue;
                }
                let current = if absolute {
                    format!("/{}", parts.join("/").trim_start_matches('/'))
                } else {
                    parts.join("/")
                };
                if current == "." || current == ".." || current.ends_with("/..") {
                    continue;
                }
                if sftp
                    .try_exists(current.clone())
                    .await
                    .with_context(|| format!("Failed to inspect remote directory: {current}"))?
                {
                    let metadata = sftp.metadata(current.clone()).await.with_context(|| {
                        format!("Failed to inspect remote directory: {current}")
                    })?;
                    if !metadata.file_type().is_dir() {
                        return Err(anyhow!("Remote path exists as a file: {current}"));
                    }
                } else {
                    sftp.create_dir(current.clone())
                        .await
                        .with_context(|| format!("Failed to create remote directory: {current}"))?;
                }
            }
        }
    }
    Ok(())
}

async fn ensure_remote_dir_exists(sftp: &SftpSession, path: &str) -> Result<()> {
    let normalized = normalize_remote_path(path);
    if normalized.is_empty() || normalized == "." || normalized == "/" {
        return Ok(());
    }

    if sftp
        .try_exists(normalized.clone())
        .await
        .with_context(|| format!("Failed to inspect remote directory: {normalized}"))?
    {
        let metadata = sftp
            .metadata(normalized.clone())
            .await
            .with_context(|| format!("Failed to inspect remote directory: {normalized}"))?;
        if !metadata.file_type().is_dir() {
            return Err(anyhow!("Remote path exists as a file: {normalized}"));
        }
    } else {
        sftp.create_dir(normalized.clone())
            .await
            .with_context(|| format!("Failed to create remote directory: {normalized}"))?;
    }

    Ok(())
}

async fn delete_remote_path_recursive(sftp: &SftpSession, root_path: &str) -> Result<()> {
    let mut stack: Vec<(String, bool)> = vec![(root_path.to_string(), false)];
    while let Some((path, visited)) = stack.pop() {
        if visited {
            sftp.remove_dir(path.clone())
                .await
                .with_context(|| format!("Failed to delete directory: {path}"))?;
            continue;
        }

        match sftp.read_dir(path.clone()).await {
            Ok(read_dir) => {
                stack.push((path.clone(), true));
                let mut children: Vec<String> = Vec::new();
                for entry in read_dir {
                    let name = entry.file_name();
                    if name.is_empty() || name == "." || name == ".." {
                        continue;
                    }
                    children.push(join_remote_path(&path, &name));
                }
                for child in children.into_iter().rev() {
                    stack.push((child, false));
                }
            }
            Err(_) => {
                sftp.remove_file(path.clone())
                    .await
                    .with_context(|| format!("Failed to delete file: {path}"))?;
            }
        }
    }
    Ok(())
}

async fn copy_remote_file(
    sftp: &SftpSession,
    source_path: &str,
    destination_path: &str,
) -> Result<()> {
    let temp_remote_path = remote_transfer_temp_path(destination_path);
    if sftp
        .try_exists(temp_remote_path.clone())
        .await
        .with_context(|| format!("Failed to inspect remote temp file: {temp_remote_path}"))?
    {
        let temp_metadata = sftp
            .metadata(temp_remote_path.clone())
            .await
            .with_context(|| format!("Failed to inspect remote temp file: {temp_remote_path}"))?;
        if temp_metadata.file_type().is_dir() {
            delete_remote_path_recursive(sftp, &temp_remote_path).await?;
        } else {
            sftp.remove_file(temp_remote_path.clone())
                .await
                .with_context(|| format!("Failed to clear remote temp file: {temp_remote_path}"))?;
        }
    }

    if let Some(parent) = destination_path.rsplit_once('/').map(|(parent, _)| parent) {
        if !parent.trim().is_empty() {
            ensure_remote_dir_all(sftp, parent).await?;
        }
    }

    let mut source = sftp
        .open(source_path.to_string())
        .await
        .with_context(|| format!("Failed to open remote file: {source_path}"))?;
    let mut destination = sftp
        .create(temp_remote_path.clone())
        .await
        .with_context(|| format!("Failed to create remote temp file: {temp_remote_path}"))?;
    let mut buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        let n = source
            .read(&mut buf)
            .await
            .with_context(|| format!("Failed while reading remote file: {source_path}"))?;
        if n == 0 {
            break;
        }
        destination
            .write_all(&buf[..n])
            .await
            .with_context(|| format!("Failed while writing remote file: {destination_path}"))?;
    }

    destination
        .flush()
        .await
        .with_context(|| format!("Failed to flush remote file: {destination_path}"))?;
    destination
        .shutdown()
        .await
        .with_context(|| format!("Failed to close remote file: {destination_path}"))?;
    replace_remote_file(sftp, &temp_remote_path, destination_path).await
}

async fn copy_remote_path_recursive(
    sftp: &SftpSession,
    source_path: &str,
    destination_path: &str,
) -> Result<()> {
    let source_metadata = sftp
        .metadata(source_path.to_string())
        .await
        .with_context(|| format!("Failed to inspect remote path: {source_path}"))?;
    if !source_metadata.file_type().is_dir() {
        return copy_remote_file(sftp, source_path, destination_path).await;
    }

    ensure_remote_dir_all(sftp, destination_path).await?;
    let mut queue: VecDeque<(String, String)> =
        VecDeque::from([(source_path.to_string(), destination_path.to_string())]);
    while let Some((source_dir, destination_dir)) = queue.pop_front() {
        let read_dir = sftp
            .read_dir(source_dir.clone())
            .await
            .with_context(|| format!("Failed to read remote directory: {source_dir}"))?;
        for entry in read_dir {
            let name = entry.file_name();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }
            let child_source = join_remote_path(&source_dir, &name);
            let child_destination = join_remote_path(&destination_dir, &name);
            if entry.file_type().is_dir() {
                ensure_remote_dir_all(sftp, &child_destination).await?;
                queue.push_back((child_source, child_destination));
            } else {
                copy_remote_file(sftp, &child_source, &child_destination).await?;
            }
        }
    }
    Ok(())
}

struct RemoteDownloadPlanEntry {
    remote_path: String,
    local_path: PathBuf,
}

#[derive(Debug)]
struct LocalUploadPlan {
    directories_to_create: Vec<String>,
    files: Vec<LocalUploadPlanEntry>,
    root_total_bytes: u64,
}

#[derive(Debug, Clone)]
struct LocalUploadPlanEntry {
    local_path: PathBuf,
    remote_path: String,
    size: u64,
    remote_exists: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemotePathKind {
    File,
    Directory,
}

#[derive(Clone, Copy)]
struct RemotePathInfo {
    kind: RemotePathKind,
    size: u64,
}

#[derive(Debug)]
struct LocalUploadTree {
    directories: Vec<String>,
    files: Vec<LocalUploadTreeFile>,
    total_bytes: u64,
}

#[derive(Debug)]
struct LocalUploadTreeFile {
    local_path: PathBuf,
    relative_path: String,
    size: u64,
}

#[derive(Debug, Default)]
struct RemoteUploadTree {
    directories: HashSet<String>,
    files: HashSet<String>,
}

#[derive(Debug)]
struct UploadTreeDiff {
    directories_to_create: Vec<String>,
    files: Vec<LocalUploadPlanEntry>,
    initial_conflict_count: usize,
}

#[derive(Debug)]
struct FinalizedUploadPlan {
    files_to_upload: Vec<LocalUploadPlanEntry>,
    total_bytes: u64,
    skipped_identical_files: usize,
}

#[derive(Clone, Copy)]
enum UploadConflictPolicy {
    OverwriteIfDifferentAll,
    SkipAll,
}

struct UploadTransferControl<'a> {
    request_id: u64,
    session: &'a SshHandle,
    event_tx: &'a Sender<DownloadManagerEvent>,
    cancel_rx: &'a Receiver<()>,
    conflict_response_rx: &'a Receiver<UploadConflictResponse>,
    log_path: &'a str,
}

struct TransferProgressReporter<'a> {
    request_id: u64,
    total_bytes: Option<u64>,
    transferred_bytes: u64,
    started_at: Instant,
    event_tx: &'a Sender<DownloadManagerEvent>,
}

impl<'a> TransferProgressReporter<'a> {
    fn new(
        request_id: u64,
        total_bytes: Option<u64>,
        event_tx: &'a Sender<DownloadManagerEvent>,
    ) -> Self {
        Self {
            request_id,
            total_bytes,
            transferred_bytes: 0,
            started_at: Instant::now(),
            event_tx,
        }
    }

    fn send_started(&self, remote_path: String, local_path: String) {
        send_download_event(
            self.event_tx,
            DownloadManagerEvent::Started {
                request_id: self.request_id,
                remote_path,
                local_path,
                downloaded_bytes: self.transferred_bytes,
                total_bytes: self.total_bytes,
            },
        );
    }

    fn current_speed_bps(&self) -> f64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.transferred_bytes as f64 / elapsed
        } else {
            0.0
        }
    }

    fn emit_progress(&self, speed_bps: f64) {
        send_download_event(
            self.event_tx,
            DownloadManagerEvent::Progress {
                request_id: self.request_id,
                downloaded_bytes: self.transferred_bytes,
                total_bytes: self.total_bytes,
                speed_bps,
            },
        );
    }

    fn add_progress(&mut self, delta: u64) {
        self.transferred_bytes = self.transferred_bytes.saturating_add(delta);
        self.emit_progress(self.current_speed_bps());
    }
}

fn sftp_command_request_id(cmd: &SftpCommand) -> u64 {
    match cmd {
        SftpCommand::ListDir { request_id, .. }
        | SftpCommand::MakeDir { request_id, .. }
        | SftpCommand::Rename { request_id, .. }
        | SftpCommand::Delete { request_id, .. }
        | SftpCommand::Copy { request_id, .. }
        | SftpCommand::Move { request_id, .. }
        | SftpCommand::SetPermissions { request_id, .. }
        | SftpCommand::SetOwnership { request_id, .. } => *request_id,
    }
}

fn apply_remote_permission_mode(existing_permissions: Option<u32>, mode: u32) -> u32 {
    (existing_permissions.unwrap_or_default() & !0o7777) | (mode & 0o7777)
}

async fn set_remote_path_permissions(sftp: &SftpSession, path: &str, mode: u32) -> Result<()> {
    let metadata = sftp
        .metadata(path.to_string())
        .await
        .with_context(|| format!("Failed to inspect remote path: {path}"))?;
    let mut attrs = FileAttributes::empty();
    attrs.permissions = Some(apply_remote_permission_mode(metadata.permissions, mode));
    sftp.set_metadata(path.to_string(), attrs)
        .await
        .with_context(|| format!("Failed to update permissions: {path}"))?;
    Ok(())
}

fn build_remote_ownership_command(
    paths: &[String],
    owner: Option<&str>,
    group: Option<&str>,
) -> Result<String> {
    if paths.is_empty() {
        return Err(anyhow!("No remote paths provided for ownership update"));
    }
    let owner = owner.map(str::trim).filter(|value| !value.is_empty());
    let group = group.map(str::trim).filter(|value| !value.is_empty());
    let quoted_paths = paths
        .iter()
        .map(|path| shell_quote_posix(path))
        .collect::<Vec<_>>()
        .join(" ");

    match (owner, group) {
        (Some(owner), Some(group)) => Ok(format!(
            "chown -- {} {quoted_paths}",
            shell_quote_posix(&format!("{owner}:{group}"))
        )),
        (Some(owner), None) => Ok(format!(
            "chown -- {} {quoted_paths}",
            shell_quote_posix(owner)
        )),
        (None, Some(group)) => Ok(format!(
            "chgrp -- {} {quoted_paths}",
            shell_quote_posix(group)
        )),
        (None, None) => Err(anyhow!("No owner or group provided for ownership update")),
    }
}

async fn set_remote_paths_ownership(
    session: Option<&SshHandle>,
    paths: &[String],
    owner: Option<&str>,
    group: Option<&str>,
    log_path: &str,
) -> Result<()> {
    let Some(session) = session else {
        return Err(anyhow!(
            "Remote ownership changes require an active SSH exec channel"
        ));
    };
    let command = build_remote_ownership_command(paths, owner, group)?;
    let result = run_remote_exec_command(session, &command, log_path).await?;
    let output = summarize_remote_exec_output(&result.stdout, &result.stderr);
    if let Some(signal) = result.exit_signal.as_deref() {
        return Err(anyhow!(
            "Remote ownership command was terminated by signal {signal}.{}",
            output
        ));
    }
    match result.exit_status.unwrap_or(127) {
        0 => Ok(()),
        status => Err(anyhow!(
            "Remote ownership command failed with exit status {status}.{}",
            output
        )),
    }
}

async fn execute_sftp_command(
    sftp: &SftpSession,
    session: Option<&SshHandle>,
    cmd: SftpCommand,
    log_path: &str,
) -> Result<SftpEvent> {
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
                        uid: metadata.uid,
                        user: metadata.user.clone(),
                        gid: metadata.gid,
                        group: metadata.group.clone(),
                        permissions: metadata.permissions,
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
        SftpCommand::Delete { request_id, paths } => {
            let mut used_fast_delete = false;
            if let Some(session) = session {
                match try_fast_delete_remote_paths(session, &paths, log_path).await {
                    Ok(()) => {
                        used_fast_delete = true;
                    }
                    Err(FastDeleteError::Unsupported(err)) => {
                        logger::log_line(
                            log_path,
                            &format!("Fast remote delete unavailable, falling back to SFTP: {err}"),
                        );
                    }
                    Err(FastDeleteError::Failed(err)) => return Err(err),
                }
            }
            if !used_fast_delete {
                for path in &paths {
                    delete_remote_path_recursive(sftp, path).await?;
                }
            }
            Ok(SftpEvent::OperationOk {
                request_id,
                message: if paths.len() == 1 {
                    format!("Deleted: {}", paths[0])
                } else {
                    format!("Deleted {} items", paths.len())
                },
            })
        }
        SftpCommand::Copy {
            request_id,
            source_paths,
            destination_dir,
        } => {
            ensure_remote_dir_all(sftp, &destination_dir).await?;
            for source_path in &source_paths {
                let Some(name) = remote_basename(source_path) else {
                    return Err(anyhow!("Invalid remote source path: {source_path}"));
                };
                let destination_path = join_remote_path(&destination_dir, &name);
                if normalize_remote_path(source_path) == normalize_remote_path(&destination_path) {
                    continue;
                }
                copy_remote_path_recursive(sftp, source_path, &destination_path).await?;
            }
            Ok(SftpEvent::OperationOk {
                request_id,
                message: if source_paths.len() == 1 {
                    format!("Copied to: {destination_dir}")
                } else {
                    format!("Copied {} items to {}", source_paths.len(), destination_dir)
                },
            })
        }
        SftpCommand::Move {
            request_id,
            source_paths,
            destination_dir,
        } => {
            ensure_remote_dir_all(sftp, &destination_dir).await?;
            for source_path in &source_paths {
                let Some(name) = remote_basename(source_path) else {
                    return Err(anyhow!("Invalid remote source path: {source_path}"));
                };
                let destination_path = join_remote_path(&destination_dir, &name);
                if normalize_remote_path(source_path) == normalize_remote_path(&destination_path) {
                    continue;
                }
                sftp.rename(source_path.clone(), destination_path.clone())
                    .await
                    .with_context(|| {
                        format!("Failed to move: {source_path} -> {destination_path}")
                    })?;
            }
            Ok(SftpEvent::OperationOk {
                request_id,
                message: if source_paths.len() == 1 {
                    format!("Moved to: {destination_dir}")
                } else {
                    format!("Moved {} items to {}", source_paths.len(), destination_dir)
                },
            })
        }
        SftpCommand::SetPermissions {
            request_id,
            paths,
            mode,
        } => {
            for path in &paths {
                set_remote_path_permissions(sftp, path, mode).await?;
            }
            Ok(SftpEvent::OperationOk {
                request_id,
                message: if paths.len() == 1 {
                    format!("Permissions updated to {:04o}: {}", mode & 0o7777, paths[0])
                } else {
                    format!(
                        "Updated permissions to {:04o} for {} items",
                        mode & 0o7777,
                        paths.len()
                    )
                },
            })
        }
        SftpCommand::SetOwnership {
            request_id,
            paths,
            owner,
            group,
        } => {
            set_remote_paths_ownership(
                session,
                &paths,
                owner.as_deref(),
                group.as_deref(),
                log_path,
            )
            .await?;
            let ownership = match (owner.as_deref(), group.as_deref()) {
                (Some(owner), Some(group)) => format!("{owner}:{group}"),
                (Some(owner), None) => owner.to_string(),
                (None, Some(group)) => format!("group {group}"),
                (None, None) => "ownership".to_string(),
            };
            Ok(SftpEvent::OperationOk {
                request_id,
                message: if paths.len() == 1 {
                    format!("Updated {ownership}: {}", paths[0])
                } else {
                    format!("Updated {ownership} for {} items", paths.len())
                },
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

#[derive(Debug)]
enum ConnectionError {
    MissingHost,
    MissingUsername,
    AuthenticationFailed,
    DetachedTransferNeedsCredentials,
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHost => write!(f, "Host is required"),
            Self::MissingUsername => write!(f, "Username is required"),
            Self::AuthenticationFailed => write!(f, "SSH authentication failed"),
            Self::DetachedTransferNeedsCredentials => write!(
                f,
                "Detached transfer requires saved credentials or a live authenticated tab"
            ),
        }
    }
}

impl std::error::Error for ConnectionError {}

fn issue_kind_from_io_kind(kind: std::io::ErrorKind) -> IssueKind {
    use std::io::ErrorKind;

    match kind {
        ErrorKind::NotFound => IssueKind::Path,
        ErrorKind::PermissionDenied => IssueKind::Permission,
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::AddrInUse
        | ErrorKind::AddrNotAvailable
        | ErrorKind::BrokenPipe
        | ErrorKind::TimedOut
        | ErrorKind::UnexpectedEof => IssueKind::Transport,
        ErrorKind::InvalidInput | ErrorKind::InvalidData => IssueKind::Configuration,
        _ => IssueKind::Unknown,
    }
}

fn issue_kind_from_sftp_status(code: russh_sftp::protocol::StatusCode) -> IssueKind {
    use russh_sftp::protocol::StatusCode;

    match code {
        StatusCode::NoSuchFile => IssueKind::Path,
        StatusCode::PermissionDenied => IssueKind::Permission,
        StatusCode::NoConnection | StatusCode::ConnectionLost | StatusCode::Eof => {
            IssueKind::Transport
        }
        StatusCode::OpUnsupported | StatusCode::BadMessage => IssueKind::Configuration,
        StatusCode::Failure | StatusCode::Ok => IssueKind::Unknown,
    }
}

fn issue_kind_from_russh_error(err: &russh::Error) -> IssueKind {
    match err {
        russh::Error::UnknownKey | russh::Error::KeyChanged { .. } => IssueKind::HostKey,
        russh::Error::Disconnect
        | russh::Error::HUP
        | russh::Error::ConnectionTimeout
        | russh::Error::KeepaliveTimeout
        | russh::Error::InactivityTimeout
        | russh::Error::SendError
        | russh::Error::Pending
        | russh::Error::IO(_) => IssueKind::Transport,
        russh::Error::NoAuthMethod
        | russh::Error::NotAuthenticated
        | russh::Error::UnsupportedAuthMethod => IssueKind::Authentication,
        russh::Error::CouldNotReadKey | russh::Error::Keys(keys::Error::KeyIsEncrypted) => {
            IssueKind::Configuration
        }
        russh::Error::Keys(keys::Error::KeyChanged { .. }) => IssueKind::HostKey,
        russh::Error::Keys(keys::Error::IO(io_err)) => issue_kind_from_io_kind(io_err.kind()),
        _ => IssueKind::Unknown,
    }
}

fn issue_kind_from_anyhow(err: &anyhow::Error) -> IssueKind {
    for cause in err.chain() {
        if let Some(conn_err) = cause.downcast_ref::<ConnectionError>() {
            return match conn_err {
                ConnectionError::MissingHost | ConnectionError::MissingUsername => {
                    IssueKind::Configuration
                }
                ConnectionError::AuthenticationFailed
                | ConnectionError::DetachedTransferNeedsCredentials => IssueKind::Authentication,
            };
        }
        if let Some(russh_err) = cause.downcast_ref::<russh::Error>() {
            return issue_kind_from_russh_error(russh_err);
        }
        if let Some(keys_err) = cause.downcast_ref::<keys::Error>() {
            return match keys_err {
                keys::Error::KeyChanged { .. } => IssueKind::HostKey,
                keys::Error::IO(io_err) => issue_kind_from_io_kind(io_err.kind()),
                keys::Error::KeyIsEncrypted | keys::Error::CouldNotReadKey => {
                    IssueKind::Configuration
                }
                _ => IssueKind::Unknown,
            };
        }
        if let Some(sftp_err) = cause.downcast_ref::<russh_sftp::client::error::Error>() {
            return match sftp_err {
                russh_sftp::client::error::Error::Status(status) => {
                    issue_kind_from_sftp_status(status.status_code)
                }
                russh_sftp::client::error::Error::IO(_)
                | russh_sftp::client::error::Error::Timeout => IssueKind::Transport,
                russh_sftp::client::error::Error::Limited(_)
                | russh_sftp::client::error::Error::UnexpectedPacket
                | russh_sftp::client::error::Error::UnexpectedBehavior(_) => IssueKind::Unknown,
            };
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            return issue_kind_from_io_kind(io_err.kind());
        }
    }
    IssueKind::Unknown
}

fn connection_status_from_error(err: &anyhow::Error) -> StatusUpdate {
    let kind = issue_kind_from_anyhow(err);
    let message = match kind {
        IssueKind::Authentication => {
            "Authentication failed. Check your password, private key, or MFA response.".to_string()
        }
        IssueKind::HostKey => format!("Host key verification failed. {}", err),
        IssueKind::Transport => format!("Transport error. {}", err),
        IssueKind::Configuration => format!("Configuration error. {}", err),
        IssueKind::Permission => format!("Permission error. {}", err),
        IssueKind::Path => format!("Path error. {}", err),
        IssueKind::Info | IssueKind::Unknown => format!("Connection failed. {}", err),
    };
    StatusUpdate { kind, message }
}

fn transfer_issue_from_error(err: &anyhow::Error) -> TransferIssue {
    let kind = issue_kind_from_anyhow(err);
    let message = match kind {
        IssueKind::Permission => format!("Permission error. {}", err),
        IssueKind::Path => format!("Path error. {}", err),
        IssueKind::Transport => format!("Transport error. {}", err),
        IssueKind::Authentication => format!("Authentication error. {}", err),
        IssueKind::HostKey => format!("Host key error. {}", err),
        IssueKind::Configuration => format!("Configuration error. {}", err),
        IssueKind::Info | IssueKind::Unknown => format!("Transfer failed. {}", err),
    };
    TransferIssue { kind, message }
}

fn send_ui_status(ui_tx: &Sender<UiMessage>, kind: IssueKind, message: impl Into<String>) {
    send_message(
        ui_tx,
        UiMessage::Status(StatusUpdate {
            kind,
            message: message.into(),
        }),
    );
}

fn send_sftp_status(ui_tx: &Sender<SftpUiMessage>, kind: IssueKind, message: impl Into<String>) {
    send_message(
        ui_tx,
        SftpUiMessage::Status(StatusUpdate {
            kind,
            message: message.into(),
        }),
    );
}

fn send_download_event(event_tx: &Sender<DownloadManagerEvent>, event: DownloadManagerEvent) {
    send_message(event_tx, event);
}

fn local_transfer_temp_path(local_path: &str) -> PathBuf {
    PathBuf::from(format!("{local_path}.rusty-part"))
}

fn remote_transfer_temp_path(remote_path: &str) -> String {
    format!("{remote_path}.rusty-part")
}

// Keep transfer I/O buffers off async futures' stacks.
const TRANSFER_IO_BUFFER_SIZE: usize = 128 * 1024;

#[cfg(windows)]
fn replace_local_file(temp_path: &Path, final_path: &Path) -> std::io::Result<()> {
    use std::iter;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let temp_wide: Vec<u16> = temp_path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    let final_wide: Vec<u16> = final_path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();

    let moved = unsafe {
        MoveFileExW(
            temp_wide.as_ptr(),
            final_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_local_file(temp_path: &Path, final_path: &Path) -> std::io::Result<()> {
    std::fs::rename(temp_path, final_path)
}

async fn replace_remote_file(sftp: &SftpSession, temp_path: &str, final_path: &str) -> Result<()> {
    if sftp
        .try_exists(final_path.to_string())
        .await
        .with_context(|| format!("Failed to check remote destination: {final_path}"))?
    {
        sftp.remove_file(final_path.to_string())
            .await
            .with_context(|| format!("Failed to replace existing remote file: {final_path}"))?;
    }

    sftp.rename(temp_path.to_string(), final_path.to_string())
        .await
        .with_context(|| format!("Failed to finalize remote file: {final_path}"))?;
    Ok(())
}

async fn build_remote_download_plan(
    sftp: &SftpSession,
    remote_root: &str,
    local_root: &Path,
) -> Result<(Vec<PathBuf>, Vec<RemoteDownloadPlanEntry>, u64)> {
    let mut directories: Vec<PathBuf> = vec![local_root.to_path_buf()];
    let mut files: Vec<RemoteDownloadPlanEntry> = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut queue: VecDeque<(String, PathBuf)> =
        VecDeque::from([(remote_root.to_string(), local_root.to_path_buf())]);

    while let Some((remote_dir, local_dir)) = queue.pop_front() {
        let read_dir = sftp
            .read_dir(remote_dir.clone())
            .await
            .with_context(|| format!("Failed to read remote directory: {remote_dir}"))?;
        for entry in read_dir {
            let name = entry.file_name();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }
            let child_remote = join_remote_path(&remote_dir, &name);
            let child_local = local_dir.join(&name);
            if entry.file_type().is_dir() {
                directories.push(child_local.clone());
                queue.push_back((child_remote, child_local));
            } else {
                total_bytes = total_bytes.saturating_add(entry.metadata().len());
                files.push(RemoteDownloadPlanEntry {
                    remote_path: child_remote,
                    local_path: child_local,
                });
            }
        }
    }

    Ok((directories, files, total_bytes))
}

async fn download_remote_file_to_path(
    sftp: &SftpSession,
    remote_path: &str,
    local_path: &Path,
    progress: &mut TransferProgressReporter<'_>,
    cancel_rx: &Receiver<()>,
    cancel_label: &str,
) -> Result<bool> {
    let mut remote = sftp
        .open(remote_path.to_string())
        .await
        .with_context(|| format!("Failed to open remote file: {remote_path}"))?;
    let temp_path = local_transfer_temp_path(local_path.to_string_lossy().as_ref());
    if let Some(parent) = temp_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create local directory: {}", parent.display())
            })?;
        }
    }

    let mut out = std::fs::File::create(&temp_path).with_context(|| {
        format!(
            "Failed to create temporary download file: {}",
            temp_path.display()
        )
    })?;
    let mut buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            send_download_event(
                progress.event_tx,
                DownloadManagerEvent::Canceled {
                    request_id: progress.request_id,
                    local_path: cancel_label.to_string(),
                },
            );
            return Ok(false);
        }

        let n = remote
            .read(&mut buf)
            .await
            .with_context(|| format!("Failed while reading remote file: {remote_path}"))?;
        if n == 0 {
            break;
        }
        std::io::Write::write_all(&mut out, &buf[..n]).with_context(|| {
            format!("Failed while writing local file: {}", local_path.display())
        })?;
        progress.add_progress(n as u64);
    }

    std::io::Write::flush(&mut out)
        .with_context(|| format!("Failed to flush local file: {}", local_path.display()))?;
    drop(out);
    replace_local_file(&temp_path, local_path).with_context(|| {
        format!(
            "Failed to move completed download into place: {}",
            local_path.display()
        )
    })?;
    Ok(true)
}

async fn run_sftp_download_directory_with_session(
    sftp: &SftpSession,
    request_id: u64,
    remote_path: String,
    local_path: String,
    event_tx: &Sender<DownloadManagerEvent>,
    cancel_rx: &Receiver<()>,
) -> Result<()> {
    if transfer_cancel_requested(cancel_rx) {
        send_download_event(
            event_tx,
            DownloadManagerEvent::Canceled {
                request_id,
                local_path,
            },
        );
        return Ok(());
    }

    send_download_event(
        event_tx,
        DownloadManagerEvent::Preparing {
            request_id,
            total_bytes: None,
            message: "Preparing folder download...".to_string(),
        },
    );

    let local_root = PathBuf::from(local_path.clone());
    let (directories, files, total_bytes) =
        build_remote_download_plan(sftp, &remote_path, &local_root).await?;

    for dir in directories {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create local directory: {}", dir.display()))?;
    }

    send_download_event(
        event_tx,
        DownloadManagerEvent::Preparing {
            request_id,
            total_bytes: Some(total_bytes),
            message: if files.is_empty() {
                "Folder download is ready...".to_string()
            } else {
                format!("Prepared {} file(s) for download...", files.len())
            },
        },
    );

    let mut progress = TransferProgressReporter::new(request_id, Some(total_bytes), event_tx);
    progress.send_started(remote_path.clone(), local_path.clone());

    for file in files {
        if !download_remote_file_to_path(
            sftp,
            &file.remote_path,
            &file.local_path,
            &mut progress,
            cancel_rx,
            &local_path,
        )
        .await?
        {
            return Ok(());
        }
    }

    send_transfer_finished(event_tx, request_id, local_path, None);
    Ok(())
}

fn send_transfer_finished(
    event_tx: &Sender<DownloadManagerEvent>,
    request_id: u64,
    local_path: String,
    message: Option<String>,
) {
    send_download_event(
        event_tx,
        DownloadManagerEvent::Finished {
            request_id,
            local_path,
            message,
        },
    );
}

fn local_upload_scan_path(local_root: &Path, current_dir: &Path) -> String {
    let root_label = local_root
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| local_root.display().to_string());

    match current_dir.strip_prefix(local_root) {
        Ok(relative) if !relative.as_os_str().is_empty() => {
            format!(
                "{root_label}/{}",
                relative.to_string_lossy().replace('\\', "/")
            )
        }
        _ => root_label,
    }
}

fn format_transfer_size_compact(value: u64) -> String {
    let kilobytes = value as f64 / 1024.0;
    if kilobytes >= 1024.0 * 1024.0 {
        format!("{:.2} GB", kilobytes / (1024.0 * 1024.0))
    } else if kilobytes >= 1024.0 {
        format!("{:.1} MB", kilobytes / 1024.0)
    } else {
        format!("{:.1} KB", kilobytes)
    }
}

fn send_upload_preparing(
    event_tx: &Sender<DownloadManagerEvent>,
    request_id: u64,
    total_bytes: Option<u64>,
    message: impl Into<String>,
) {
    send_download_event(
        event_tx,
        DownloadManagerEvent::Preparing {
            request_id,
            total_bytes,
            message: message.into(),
        },
    );
}

fn send_local_upload_tree_scan_update(
    event_tx: &Sender<DownloadManagerEvent>,
    request_id: u64,
    local_root: &Path,
    current_dir: &Path,
    directories_scanned: usize,
    files_scanned: usize,
    total_bytes: u64,
) {
    let scan_path = local_upload_scan_path(local_root, current_dir);
    send_upload_preparing(
        event_tx,
        request_id,
        Some(total_bytes),
        format!(
            "Scanning local tree {scan_path}... {} folder(s), {} file(s), {} total.",
            directories_scanned,
            files_scanned,
            format_transfer_size_compact(total_bytes),
        ),
    );
}

fn send_remote_upload_tree_scan_update(
    event_tx: &Sender<DownloadManagerEvent>,
    request_id: u64,
    current_dir: &str,
    directories_scanned: usize,
    files_scanned: usize,
    total_bytes: u64,
) {
    send_upload_preparing(
        event_tx,
        request_id,
        Some(total_bytes),
        format!(
            "Scanning remote tree {current_dir}... {} folder(s), {} file(s).",
            directories_scanned, files_scanned,
        ),
    );
}

fn remote_upload_tree_entry_counts(tree: &RemoteUploadTree) -> (usize, usize) {
    (tree.directories.len().saturating_add(1), tree.files.len())
}

fn parse_remote_upload_tree_output(stdout: &str) -> Result<RemoteUploadTree> {
    let mut tree = RemoteUploadTree::default();

    for raw_line in stdout.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() {
            continue;
        }

        let (kind, relative_path) = line
            .split_once('\t')
            .ok_or_else(|| anyhow!("Invalid remote tree entry format: {line}"))?;
        let relative_path = normalize_remote_path(relative_path);
        if relative_path == "." {
            continue;
        }

        match kind {
            "d" => {
                if tree.files.contains(&relative_path) {
                    return Err(anyhow!(
                        "Remote tree entry changed type during fast scan: {relative_path}"
                    ));
                }
                tree.directories.insert(relative_path);
            }
            "f" => {
                if tree.directories.contains(&relative_path) {
                    return Err(anyhow!(
                        "Remote tree entry changed type during fast scan: {relative_path}"
                    ));
                }
                tree.files.insert(relative_path);
            }
            other => {
                return Err(anyhow!("Invalid remote tree entry kind {other:?}"));
            }
        }
    }

    Ok(tree)
}

async fn try_fast_build_remote_upload_tree(
    remote_root: &str,
    control: &UploadTransferControl<'_>,
) -> Result<RemoteUploadTree> {
    let command = format!(
        "LC_ALL=C find {} -mindepth 1 \\( -type d -printf 'd\\t%P\\n' -o ! -type d -printf 'f\\t%P\\n' \\)",
        shell_quote_posix(remote_root)
    );
    let result = run_remote_exec_command(control.session, &command, control.log_path).await?;
    let output = summarize_remote_exec_output(&result.stdout, &result.stderr);
    if let Some(signal) = result.exit_signal.as_deref() {
        return Err(anyhow!(
            "Remote tree scan was terminated by signal {signal}.{}",
            output
        ));
    }

    match result.exit_status.unwrap_or(127) {
        0 => parse_remote_upload_tree_output(&result.stdout),
        status => Err(anyhow!(
            "Remote tree scan command failed with exit status {status}.{}",
            output
        )),
    }
}

fn parse_expected_remote_output_lines(stdout: &str, expected: usize) -> Result<Vec<&str>> {
    let lines: Vec<&str> = stdout
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .filter(|line| !line.is_empty())
        .collect();
    if lines.len() != expected {
        return Err(anyhow!(
            "Expected {expected} output line(s), received {}.",
            lines.len()
        ));
    }
    Ok(lines)
}

fn parse_remote_file_sizes_output(stdout: &str, expected: usize) -> Result<Vec<u64>> {
    parse_expected_remote_output_lines(stdout, expected)?
        .into_iter()
        .map(|line| {
            line.parse::<u64>()
                .with_context(|| format!("Invalid remote file size output: {line}"))
        })
        .collect()
}

fn parse_remote_file_md5_output(stdout: &str, expected: usize) -> Result<Vec<String>> {
    parse_expected_remote_output_lines(stdout, expected)?
        .into_iter()
        .map(|line| {
            let token = if line.starts_with("MD5(") {
                line.rsplit_once('=')
                    .map(|(_, hash)| hash.trim())
                    .ok_or_else(|| anyhow!("Invalid remote MD5 output: {line}"))?
            } else {
                line.split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow!("Invalid remote MD5 output: {line}"))?
            };
            let hash = token.to_ascii_lowercase();
            if hash.len() != 32 || !hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
                return Err(anyhow!("Invalid remote MD5 digest: {line}"));
            }
            Ok(hash)
        })
        .collect()
}

async fn run_remote_exec_command_variants(
    control: &UploadTransferControl<'_>,
    operation: &str,
    commands: &[String],
) -> Result<String> {
    let mut failures: Vec<String> = Vec::with_capacity(commands.len());

    for command in commands {
        let result = run_remote_exec_command(control.session, command, control.log_path).await?;
        let output = summarize_remote_exec_output(&result.stdout, &result.stderr);
        if let Some(signal) = result.exit_signal.as_deref() {
            failures.push(format!("terminated by signal {signal}.{}", output));
            continue;
        }

        match result.exit_status.unwrap_or(127) {
            0 => return Ok(result.stdout),
            status => failures.push(format!("exit status {status}.{}", output)),
        }
    }

    Err(anyhow!(
        "Remote {operation} command failed across {} variant(s): {}",
        commands.len(),
        failures.join(" | ")
    ))
}

async fn query_remote_file_sizes_batch(
    control: &UploadTransferControl<'_>,
    remote_paths: &[String],
) -> Result<Vec<u64>> {
    if remote_paths.is_empty() {
        return Ok(Vec::new());
    }

    let quoted_paths = shell_join_posix_args(remote_paths);
    let commands = [
        format!("LC_ALL=C stat -c '%s' -- {quoted_paths}"),
        format!("LC_ALL=C stat -f '%z' {quoted_paths}"),
    ];
    let stdout = run_remote_exec_command_variants(control, "size", &commands).await?;
    parse_remote_file_sizes_output(&stdout, remote_paths.len())
}

async fn query_remote_file_sizes(
    sftp: &SftpSession,
    control: &UploadTransferControl<'_>,
    remote_paths: &[String],
) -> Result<Vec<u64>> {
    match query_remote_file_sizes_batch(control, remote_paths).await {
        Ok(sizes) => Ok(sizes),
        Err(err) => {
            logger::log_line(
                control.log_path,
                &format!(
                    "Fast remote size query unavailable, falling back to SFTP metadata: {err}"
                ),
            );

            let mut sizes = Vec::with_capacity(remote_paths.len());
            for remote_path in remote_paths {
                match remote_path_info(sftp, remote_path).await? {
                    Some(RemotePathInfo {
                        kind: RemotePathKind::File,
                        size,
                    }) => sizes.push(size),
                    Some(RemotePathInfo {
                        kind: RemotePathKind::Directory,
                        ..
                    }) => {
                        return Err(anyhow!(
                            "Remote path exists as a folder where a file is required: {remote_path}"
                        ));
                    }
                    None => {
                        return Err(anyhow!(
                            "Remote file disappeared before upload comparison: {remote_path}"
                        ));
                    }
                }
            }
            Ok(sizes)
        }
    }
}

async fn query_remote_file_md5s_batch(
    control: &UploadTransferControl<'_>,
    remote_paths: &[String],
) -> Result<Vec<String>> {
    if remote_paths.is_empty() {
        return Ok(Vec::new());
    }

    let quoted_paths = shell_join_posix_args(remote_paths);
    let commands = [
        format!("LC_ALL=C md5sum -- {quoted_paths}"),
        format!("LC_ALL=C md5 -r {quoted_paths}"),
        format!("LC_ALL=C openssl md5 -r {quoted_paths}"),
    ];
    let stdout = run_remote_exec_command_variants(control, "MD5", &commands).await?;
    parse_remote_file_md5_output(&stdout, remote_paths.len())
}

async fn compute_local_file_md5(path: &Path, cancel_rx: &Receiver<()>) -> Result<Option<String>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open local file: {}", path.display()))?;
    let mut digest = md5::Context::new();
    let mut buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            return Ok(None);
        }

        let n = file
            .read(&mut buf)
            .await
            .with_context(|| format!("Failed while reading local file: {}", path.display()))?;
        if n == 0 {
            break;
        }
        digest.consume(&buf[..n]);
    }

    Ok(Some(format!("{:x}", digest.compute())))
}

async fn remote_path_info(sftp: &SftpSession, path: &str) -> Result<Option<RemotePathInfo>> {
    if !sftp
        .try_exists(path.to_string())
        .await
        .with_context(|| format!("Failed to inspect remote path: {path}"))?
    {
        return Ok(None);
    }

    let metadata = sftp
        .metadata(path.to_string())
        .await
        .with_context(|| format!("Failed to inspect remote path: {path}"))?;
    Ok(Some(RemotePathInfo {
        kind: if metadata.file_type().is_dir() {
            RemotePathKind::Directory
        } else {
            RemotePathKind::File
        },
        size: metadata.len(),
    }))
}

async fn remote_path_kind(sftp: &SftpSession, path: &str) -> Result<Option<RemotePathKind>> {
    Ok(remote_path_info(sftp, path).await?.map(|info| info.kind))
}

async fn remote_file_matches_local(
    sftp: &SftpSession,
    local_path: &Path,
    remote_path: &str,
    local_size: u64,
    remote_size: u64,
    cancel_rx: &Receiver<()>,
) -> Result<Option<bool>> {
    if local_size != remote_size {
        return Ok(Some(false));
    }

    let mut local = tokio::fs::File::open(local_path)
        .await
        .with_context(|| format!("Failed to open local file: {}", local_path.display()))?;
    let mut remote = sftp
        .open(remote_path.to_string())
        .await
        .with_context(|| format!("Failed to open remote file: {remote_path}"))?;
    let mut local_buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];
    let mut remote_buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            return Ok(None);
        }

        let local_n = local.read(&mut local_buf).await.with_context(|| {
            format!("Failed while reading local file: {}", local_path.display())
        })?;
        let remote_n = remote
            .read(&mut remote_buf)
            .await
            .with_context(|| format!("Failed while reading remote file: {remote_path}"))?;
        if local_n != remote_n {
            return Ok(Some(false));
        }
        if local_n == 0 {
            return Ok(Some(true));
        }
        if local_buf[..local_n] != remote_buf[..remote_n] {
            return Ok(Some(false));
        }
    }
}

async fn remote_file_needs_upload(
    sftp: &SftpSession,
    local_path: &Path,
    remote_path: &str,
    local_size: u64,
    cancel_rx: &Receiver<()>,
) -> Result<Option<bool>> {
    match remote_path_info(sftp, remote_path).await? {
        Some(RemotePathInfo {
            kind: RemotePathKind::Directory,
            ..
        }) => Err(anyhow!(
            "Remote path exists as a folder where a file is required: {remote_path}"
        )),
        Some(RemotePathInfo {
            kind: RemotePathKind::File,
            size: remote_size,
        }) => remote_file_matches_local(
            sftp,
            local_path,
            remote_path,
            local_size,
            remote_size,
            cancel_rx,
        )
        .await
        .map(|matches| matches.map(|matches| !matches)),
        None => Ok(Some(true)),
    }
}

async fn remote_file_needs_upload_with_fast_compare(
    sftp: &SftpSession,
    local_path: &Path,
    remote_path: &str,
    local_size: u64,
    control: &UploadTransferControl<'_>,
) -> Result<Option<bool>> {
    match remote_path_info(sftp, remote_path).await? {
        Some(RemotePathInfo {
            kind: RemotePathKind::Directory,
            ..
        }) => Err(anyhow!(
            "Remote path exists as a folder where a file is required: {remote_path}"
        )),
        Some(RemotePathInfo {
            kind: RemotePathKind::File,
            size: remote_size,
        }) => {
            if remote_size != local_size {
                return Ok(Some(true));
            }

            match query_remote_file_md5s_batch(control, &[remote_path.to_string()]).await {
                Ok(remote_hashes) => {
                    let Some(local_hash) =
                        compute_local_file_md5(local_path, control.cancel_rx).await?
                    else {
                        return Ok(None);
                    };
                    Ok(Some(local_hash != remote_hashes[0]))
                }
                Err(err) => {
                    logger::log_line(
                        control.log_path,
                        &format!(
                            "Fast remote MD5 compare unavailable for {remote_path}, falling back to direct compare: {err}"
                        ),
                    );
                    remote_file_needs_upload(
                        sftp,
                        local_path,
                        remote_path,
                        local_size,
                        control.cancel_rx,
                    )
                    .await
                }
            }
        }
        None => Ok(Some(true)),
    }
}

async fn build_local_upload_tree(
    local_root: &Path,
    request_id: u64,
    event_tx: &Sender<DownloadManagerEvent>,
    cancel_rx: &Receiver<()>,
) -> Result<Option<LocalUploadTree>> {
    let mut directories: Vec<String> = Vec::new();
    let mut files: Vec<LocalUploadTreeFile> = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut directories_scanned: usize = 1;
    let mut files_scanned: usize = 0;
    let mut last_update = Instant::now();
    let mut scanned_entries: usize = 0;
    let mut queue: VecDeque<(PathBuf, String)> =
        VecDeque::from([(local_root.to_path_buf(), String::new())]);

    while let Some((local_dir, relative_dir)) = queue.pop_front() {
        if transfer_cancel_requested(cancel_rx) {
            send_download_event(
                event_tx,
                DownloadManagerEvent::Canceled {
                    request_id,
                    local_path: local_root.display().to_string(),
                },
            );
            return Ok(None);
        }

        for entry in std::fs::read_dir(&local_dir)
            .with_context(|| format!("Failed to read local directory: {}", local_dir.display()))?
        {
            if transfer_cancel_requested(cancel_rx) {
                send_download_event(
                    event_tx,
                    DownloadManagerEvent::Canceled {
                        request_id,
                        local_path: local_root.display().to_string(),
                    },
                );
                return Ok(None);
            }

            let entry = entry.with_context(|| {
                format!(
                    "Failed to read local directory entry: {}",
                    local_dir.display()
                )
            })?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.trim().is_empty() {
                continue;
            }

            scanned_entries = scanned_entries.saturating_add(1);
            let local_child = entry.path();
            let relative_child = join_remote_path(&relative_dir, &name);
            let metadata = entry.metadata().with_context(|| {
                format!("Failed to inspect local path: {}", local_child.display())
            })?;
            if metadata.is_dir() {
                directories_scanned = directories_scanned.saturating_add(1);
                directories.push(relative_child.clone());
                queue.push_back((local_child, relative_child));
            } else {
                let size = metadata.len();
                files_scanned = files_scanned.saturating_add(1);
                total_bytes = total_bytes.saturating_add(size);
                files.push(LocalUploadTreeFile {
                    local_path: local_child,
                    relative_path: relative_child,
                    size,
                });
            }

            if scanned_entries == 1
                || scanned_entries.is_multiple_of(64)
                || last_update.elapsed() >= Duration::from_millis(350)
            {
                send_local_upload_tree_scan_update(
                    event_tx,
                    request_id,
                    local_root,
                    &local_dir,
                    directories_scanned,
                    files_scanned,
                    total_bytes,
                );
                last_update = Instant::now();
                tokio::task::yield_now().await;
            }
        }
    }

    directories.sort_by(|a, b| {
        a.to_ascii_lowercase()
            .cmp(&b.to_ascii_lowercase())
            .then_with(|| a.cmp(b))
    });
    files.sort_by(|a, b| {
        a.relative_path
            .to_ascii_lowercase()
            .cmp(&b.relative_path.to_ascii_lowercase())
            .then_with(|| a.relative_path.cmp(&b.relative_path))
    });
    send_local_upload_tree_scan_update(
        event_tx,
        request_id,
        local_root,
        local_root,
        directories_scanned,
        files_scanned,
        total_bytes,
    );
    Ok(Some(LocalUploadTree {
        directories,
        files,
        total_bytes,
    }))
}

async fn build_remote_upload_tree(
    sftp: &SftpSession,
    remote_root: &str,
    cancel_label: &str,
    control: &UploadTransferControl<'_>,
    total_bytes: u64,
) -> Result<Option<RemoteUploadTree>> {
    send_upload_preparing(
        control.event_tx,
        control.request_id,
        Some(total_bytes),
        "Scanning remote tree with remote find...".to_string(),
    );
    match try_fast_build_remote_upload_tree(remote_root, control).await {
        Ok(tree) => {
            let (directories_scanned, files_scanned) = remote_upload_tree_entry_counts(&tree);
            send_remote_upload_tree_scan_update(
                control.event_tx,
                control.request_id,
                remote_root,
                directories_scanned,
                files_scanned,
                total_bytes,
            );
            return Ok(Some(tree));
        }
        Err(err) => {
            logger::log_line(
                control.log_path,
                &format!(
                    "Fast remote tree scan failed for {remote_root}; falling back to SFTP: {err}"
                ),
            );
            send_upload_preparing(
                control.event_tx,
                control.request_id,
                Some(total_bytes),
                "Remote find unavailable, falling back to SFTP scan...".to_string(),
            );
        }
    }

    let mut tree = RemoteUploadTree::default();
    let mut directories_scanned: usize = 1;
    let mut files_scanned: usize = 0;
    let mut last_update = Instant::now();
    let mut scanned_entries: usize = 0;
    let mut queue: VecDeque<(String, String)> =
        VecDeque::from([(remote_root.to_string(), String::new())]);

    while let Some((remote_dir, relative_dir)) = queue.pop_front() {
        if transfer_cancel_requested(control.cancel_rx) {
            send_download_event(
                control.event_tx,
                DownloadManagerEvent::Canceled {
                    request_id: control.request_id,
                    local_path: cancel_label.to_string(),
                },
            );
            return Ok(None);
        }

        let read_dir = sftp
            .read_dir(remote_dir.clone())
            .await
            .with_context(|| format!("Failed to read remote directory: {remote_dir}"))?;
        for entry in read_dir {
            if transfer_cancel_requested(control.cancel_rx) {
                send_download_event(
                    control.event_tx,
                    DownloadManagerEvent::Canceled {
                        request_id: control.request_id,
                        local_path: cancel_label.to_string(),
                    },
                );
                return Ok(None);
            }

            let name = entry.file_name();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }

            scanned_entries = scanned_entries.saturating_add(1);
            let relative_child = join_remote_path(&relative_dir, &name);
            if entry.file_type().is_dir() {
                let remote_child = join_remote_path(&remote_dir, &name);
                directories_scanned = directories_scanned.saturating_add(1);
                tree.directories.insert(relative_child.clone());
                queue.push_back((remote_child, relative_child));
            } else {
                files_scanned = files_scanned.saturating_add(1);
                tree.files.insert(relative_child);
            }

            if scanned_entries == 1
                || scanned_entries.is_multiple_of(64)
                || last_update.elapsed() >= Duration::from_millis(350)
            {
                send_remote_upload_tree_scan_update(
                    control.event_tx,
                    control.request_id,
                    &remote_dir,
                    directories_scanned,
                    files_scanned,
                    total_bytes,
                );
                last_update = Instant::now();
                tokio::task::yield_now().await;
            }
        }
    }

    send_remote_upload_tree_scan_update(
        control.event_tx,
        control.request_id,
        remote_root,
        directories_scanned,
        files_scanned,
        total_bytes,
    );
    Ok(Some(tree))
}

fn diff_upload_trees(
    local_tree: &LocalUploadTree,
    remote_root: &str,
    root_remote_exists: bool,
    remote_tree: &RemoteUploadTree,
) -> Result<UploadTreeDiff> {
    let mut directories_to_create: Vec<String> = Vec::new();
    if !root_remote_exists {
        directories_to_create.push(remote_root.to_string());
    }

    for relative_dir in &local_tree.directories {
        let remote_dir = join_remote_path(remote_root, relative_dir);
        if remote_tree.files.contains(relative_dir) {
            return Err(anyhow!(
                "Remote path exists as a file where a folder is required: {remote_dir}"
            ));
        }
        if !remote_tree.directories.contains(relative_dir) {
            directories_to_create.push(remote_dir);
        }
    }

    let mut files: Vec<LocalUploadPlanEntry> = Vec::new();
    let mut initial_conflict_count: usize = 0;

    for local_file in &local_tree.files {
        let remote_path = join_remote_path(remote_root, &local_file.relative_path);
        if remote_tree.directories.contains(&local_file.relative_path) {
            return Err(anyhow!(
                "Remote path exists as a folder where a file is required: {remote_path}"
            ));
        }

        match remote_tree.files.contains(&local_file.relative_path) {
            true => {
                initial_conflict_count = initial_conflict_count.saturating_add(1);
                files.push(LocalUploadPlanEntry {
                    local_path: local_file.local_path.clone(),
                    remote_path,
                    size: local_file.size,
                    remote_exists: true,
                });
            }
            false => {
                files.push(LocalUploadPlanEntry {
                    local_path: local_file.local_path.clone(),
                    remote_path,
                    size: local_file.size,
                    remote_exists: false,
                });
            }
        }
    }

    Ok(UploadTreeDiff {
        directories_to_create,
        files,
        initial_conflict_count,
    })
}

async fn build_local_upload_plan(
    sftp: &SftpSession,
    control: &UploadTransferControl<'_>,
    local_root: &Path,
    remote_root: &str,
) -> Result<Option<LocalUploadPlan>> {
    let root_remote_kind = remote_path_kind(sftp, remote_root).await?;
    let root_remote_exists = match root_remote_kind {
        Some(RemotePathKind::Directory) => true,
        Some(RemotePathKind::File) => {
            return Err(anyhow!(
                "Remote upload destination exists as a file: {remote_root}"
            ));
        }
        None => false,
    };

    let Some(local_tree) = build_local_upload_tree(
        local_root,
        control.request_id,
        control.event_tx,
        control.cancel_rx,
    )
    .await?
    else {
        return Ok(None);
    };
    let root_total_bytes = local_tree.total_bytes;
    let cancel_label = local_root.display().to_string();

    let remote_tree = if root_remote_exists {
        let Some(tree) =
            build_remote_upload_tree(sftp, remote_root, &cancel_label, control, root_total_bytes)
                .await?
        else {
            return Ok(None);
        };
        tree
    } else {
        send_upload_preparing(
            control.event_tx,
            control.request_id,
            Some(root_total_bytes),
            "Scanning remote tree... destination folder does not exist yet.".to_string(),
        );
        RemoteUploadTree::default()
    };

    let diff = diff_upload_trees(&local_tree, remote_root, root_remote_exists, &remote_tree)?;
    let new_file_count = diff.files.len().saturating_sub(diff.initial_conflict_count);
    send_upload_preparing(
        control.event_tx,
        control.request_id,
        Some(root_total_bytes),
        if diff.initial_conflict_count == 0 {
            format!("Compared folder trees... {new_file_count} new file(s) ready to upload.")
        } else {
            format!(
                "Compared folder trees... {new_file_count} new file(s), {} existing remote file(s) need a decision.",
                diff.initial_conflict_count
            )
        },
    );

    Ok(Some(LocalUploadPlan {
        directories_to_create: diff.directories_to_create,
        files: diff.files,
        root_total_bytes,
    }))
}

async fn wait_for_upload_conflict_response(
    prompt: UploadConflictPrompt,
    control: &UploadTransferControl<'_>,
) -> Result<UploadConflictResponse> {
    send_download_event(
        control.event_tx,
        DownloadManagerEvent::UploadConflictPrompt { prompt },
    );

    loop {
        if transfer_cancel_requested(control.cancel_rx) {
            return Ok(UploadConflictResponse {
                choice: UploadConflictChoice::CancelTransfer,
                apply_to_all: false,
            });
        }

        match control.conflict_response_rx.try_recv() {
            Ok(response) => return Ok(response),
            Err(TryRecvError::Empty) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(TryRecvError::Disconnected) => {
                return Ok(UploadConflictResponse {
                    choice: UploadConflictChoice::CancelTransfer,
                    apply_to_all: false,
                });
            }
        }
    }
}

async fn resolve_upload_plan_conflicts(
    files: Vec<LocalUploadPlanEntry>,
    control: &UploadTransferControl<'_>,
) -> Result<Option<(Vec<LocalUploadPlanEntry>, usize)>> {
    let conflict_total = files.iter().filter(|file| file.remote_exists).count();
    if conflict_total == 0 {
        return Ok(Some((files, 0)));
    }

    let mut resolved_files: Vec<LocalUploadPlanEntry> = Vec::with_capacity(files.len());
    let mut skipped_files: usize = 0;
    let mut conflict_index: usize = 0;
    let mut conflict_policy: Option<UploadConflictPolicy> = None;

    for file in files {
        if !file.remote_exists {
            resolved_files.push(file);
            continue;
        }

        conflict_index = conflict_index.saturating_add(1);
        let response = match conflict_policy {
            Some(UploadConflictPolicy::OverwriteIfDifferentAll) => UploadConflictResponse {
                choice: UploadConflictChoice::OverwriteIfDifferent,
                apply_to_all: true,
            },
            Some(UploadConflictPolicy::SkipAll) => UploadConflictResponse {
                choice: UploadConflictChoice::Skip,
                apply_to_all: true,
            },
            None => {
                wait_for_upload_conflict_response(
                    UploadConflictPrompt {
                        request_id: control.request_id,
                        local_path: file.local_path.display().to_string(),
                        remote_path: file.remote_path.clone(),
                        conflict_index,
                        conflict_total,
                    },
                    control,
                )
                .await?
            }
        };

        match response.choice {
            UploadConflictChoice::OverwriteIfDifferent => {
                resolved_files.push(file);
                if response.apply_to_all {
                    conflict_policy = Some(UploadConflictPolicy::OverwriteIfDifferentAll);
                }
            }
            UploadConflictChoice::Skip => {
                skipped_files = skipped_files.saturating_add(1);
                if response.apply_to_all {
                    conflict_policy = Some(UploadConflictPolicy::SkipAll);
                }
            }
            UploadConflictChoice::CancelTransfer => {
                return Ok(None);
            }
        }
    }

    Ok(Some((resolved_files, skipped_files)))
}

async fn finalize_upload_plan_after_prompt(
    sftp: &SftpSession,
    files: Vec<LocalUploadPlanEntry>,
    control: &UploadTransferControl<'_>,
    root_total_bytes: u64,
) -> Result<Option<FinalizedUploadPlan>> {
    let existing_total = files.iter().filter(|file| file.remote_exists).count();
    if existing_total == 0 {
        let total_bytes = files.iter().map(|file| file.size).sum();
        return Ok(Some(FinalizedUploadPlan {
            files_to_upload: files,
            total_bytes,
            skipped_identical_files: 0,
        }));
    }

    send_upload_preparing(
        control.event_tx,
        control.request_id,
        Some(root_total_bytes),
        format!("Comparing {existing_total} existing remote file(s)..."),
    );

    let mut files_to_upload: Vec<LocalUploadPlanEntry> = Vec::with_capacity(files.len());
    let mut total_bytes: u64 = 0;
    let mut skipped_identical_files: usize = 0;
    let mut compared_existing: usize = 0;
    let mut direct_compare_fallback_logged = false;

    for chunk in files.chunks(FAST_REMOTE_COMPARE_BATCH_LIMIT) {
        if transfer_cancel_requested(control.cancel_rx) {
            return Ok(None);
        }

        let existing_positions: Vec<usize> = chunk
            .iter()
            .enumerate()
            .filter_map(|(idx, file)| file.remote_exists.then_some(idx))
            .collect();

        let remote_paths: Vec<String> = existing_positions
            .iter()
            .map(|idx| chunk[*idx].remote_path.clone())
            .collect();
        let remote_sizes = query_remote_file_sizes(sftp, control, &remote_paths).await?;
        let mut remote_sizes_by_index: Vec<Option<u64>> = vec![None; chunk.len()];
        for (position, remote_size) in existing_positions.iter().zip(remote_sizes.into_iter()) {
            remote_sizes_by_index[*position] = Some(remote_size);
        }

        let equal_size_positions: Vec<usize> = existing_positions
            .iter()
            .copied()
            .filter(|idx| remote_sizes_by_index[*idx] == Some(chunk[*idx].size))
            .collect();
        let equal_size_paths: Vec<String> = equal_size_positions
            .iter()
            .map(|idx| chunk[*idx].remote_path.clone())
            .collect();
        let remote_md5s = if equal_size_paths.is_empty() {
            Some(Vec::new())
        } else {
            match query_remote_file_md5s_batch(control, &equal_size_paths).await {
                Ok(md5s) => Some(md5s),
                Err(err) => {
                    logger::log_line(
                        control.log_path,
                        &format!(
                            "Fast remote MD5 compare unavailable, falling back to direct compare: {err}"
                        ),
                    );
                    None
                }
            }
        };
        let mut remote_md5s_by_index: Vec<Option<String>> = vec![None; chunk.len()];
        if let Some(remote_md5s) = remote_md5s {
            for (position, remote_md5) in equal_size_positions.iter().zip(remote_md5s.into_iter()) {
                remote_md5s_by_index[*position] = Some(remote_md5);
            }
        }

        for (idx, file) in chunk.iter().enumerate() {
            if !file.remote_exists {
                total_bytes = total_bytes.saturating_add(file.size);
                files_to_upload.push(file.clone());
                continue;
            }

            let remote_size = remote_sizes_by_index[idx].ok_or_else(|| {
                anyhow!(
                    "Missing remote size while preparing upload comparison: {}",
                    file.remote_path
                )
            })?;

            if remote_size != file.size {
                total_bytes = total_bytes.saturating_add(file.size);
                files_to_upload.push(file.clone());
                continue;
            }

            let identical = if let Some(remote_md5) = remote_md5s_by_index[idx].as_deref() {
                let Some(local_md5) =
                    compute_local_file_md5(&file.local_path, control.cancel_rx).await?
                else {
                    return Ok(None);
                };
                local_md5 == remote_md5
            } else {
                if !direct_compare_fallback_logged {
                    send_upload_preparing(
                        control.event_tx,
                        control.request_id,
                        Some(root_total_bytes),
                        "Remote MD5 unsupported here. Falling back to direct file compare for matching-size files.",
                    );
                    direct_compare_fallback_logged = true;
                }
                match remote_file_matches_local(
                    sftp,
                    &file.local_path,
                    &file.remote_path,
                    file.size,
                    remote_size,
                    control.cancel_rx,
                )
                .await?
                {
                    Some(matches) => matches,
                    None => return Ok(None),
                }
            };

            if identical {
                skipped_identical_files = skipped_identical_files.saturating_add(1);
            } else {
                total_bytes = total_bytes.saturating_add(file.size);
                files_to_upload.push(file.clone());
            }
        }

        compared_existing = compared_existing.saturating_add(existing_positions.len());
        if compared_existing == existing_total
            || (compared_existing != 0
                && compared_existing.is_multiple_of(FAST_REMOTE_COMPARE_BATCH_LIMIT))
        {
            send_upload_preparing(
                control.event_tx,
                control.request_id,
                Some(root_total_bytes),
                format!(
                    "Compared existing remote files... {compared_existing}/{existing_total} ready."
                ),
            );
        }
    }

    Ok(Some(FinalizedUploadPlan {
        files_to_upload,
        total_bytes,
        skipped_identical_files,
    }))
}

async fn upload_local_file_to_remote(
    sftp: &SftpSession,
    local_path: &Path,
    remote_path: &str,
    progress: &mut TransferProgressReporter<'_>,
    cancel_rx: &Receiver<()>,
    cancel_label: &str,
) -> Result<bool> {
    let mut local = tokio::fs::File::open(local_path)
        .await
        .with_context(|| format!("Failed to open local file: {}", local_path.display()))?;
    let temp_remote_path = remote_transfer_temp_path(remote_path);
    if sftp
        .try_exists(temp_remote_path.clone())
        .await
        .with_context(|| format!("Failed to inspect remote temp file: {temp_remote_path}"))?
    {
        let temp_metadata = sftp
            .metadata(temp_remote_path.clone())
            .await
            .with_context(|| format!("Failed to inspect remote temp file: {temp_remote_path}"))?;
        if temp_metadata.file_type().is_dir() {
            delete_remote_path_recursive(sftp, &temp_remote_path).await?;
        } else {
            sftp.remove_file(temp_remote_path.clone())
                .await
                .with_context(|| format!("Failed to clear remote temp file: {temp_remote_path}"))?;
        }
    }

    let mut remote = sftp
        .create(temp_remote_path.clone())
        .await
        .with_context(|| format!("Failed to create remote temp file: {temp_remote_path}"))?;
    let mut buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            send_download_event(
                progress.event_tx,
                DownloadManagerEvent::Canceled {
                    request_id: progress.request_id,
                    local_path: cancel_label.to_string(),
                },
            );
            return Ok(false);
        }

        let n = local.read(&mut buf).await.with_context(|| {
            format!("Failed while reading local file: {}", local_path.display())
        })?;
        if n == 0 {
            break;
        }
        remote
            .write_all(&buf[..n])
            .await
            .with_context(|| format!("Failed while writing remote file: {remote_path}"))?;
        progress.add_progress(n as u64);
    }

    remote
        .flush()
        .await
        .with_context(|| format!("Failed to flush remote file: {remote_path}"))?;
    remote
        .shutdown()
        .await
        .with_context(|| format!("Failed to close remote file: {remote_path}"))?;
    replace_remote_file(sftp, &temp_remote_path, remote_path).await?;
    Ok(true)
}

async fn run_sftp_upload_directory_with_session(
    sftp: &SftpSession,
    remote_path: String,
    local_path: String,
    control: &UploadTransferControl<'_>,
) -> Result<()> {
    if transfer_cancel_requested(control.cancel_rx) {
        send_download_event(
            control.event_tx,
            DownloadManagerEvent::Canceled {
                request_id: control.request_id,
                local_path,
            },
        );
        return Ok(());
    }

    let local_root = PathBuf::from(local_path.clone());
    send_upload_preparing(
        control.event_tx,
        control.request_id,
        None,
        "Scanning folder tree and checking the remote server...",
    );
    let Some(plan) = build_local_upload_plan(sftp, control, &local_root, &remote_path).await?
    else {
        return Ok(());
    };
    let directories_to_create = plan.directories_to_create;
    let files = plan.files;
    let root_total_bytes = plan.root_total_bytes;

    let Some((files_after_prompt, skipped_files)) =
        resolve_upload_plan_conflicts(files, control).await?
    else {
        send_download_event(
            control.event_tx,
            DownloadManagerEvent::Canceled {
                request_id: control.request_id,
                local_path,
            },
        );
        return Ok(());
    };

    let Some(finalized_plan) =
        finalize_upload_plan_after_prompt(sftp, files_after_prompt, control, root_total_bytes)
            .await?
    else {
        send_download_event(
            control.event_tx,
            DownloadManagerEvent::Canceled {
                request_id: control.request_id,
                local_path,
            },
        );
        return Ok(());
    };
    let files_to_upload = finalized_plan.files_to_upload;
    let total_bytes = finalized_plan.total_bytes;
    let skipped_identical_files = finalized_plan.skipped_identical_files;

    if !directories_to_create.is_empty() {
        send_upload_preparing(
            control.event_tx,
            control.request_id,
            Some(root_total_bytes),
            format!(
                "Creating {} remote folder(s) before upload...",
                directories_to_create.len()
            ),
        );
        for (idx, dir) in directories_to_create.iter().enumerate() {
            if transfer_cancel_requested(control.cancel_rx) {
                send_download_event(
                    control.event_tx,
                    DownloadManagerEvent::Canceled {
                        request_id: control.request_id,
                        local_path,
                    },
                );
                return Ok(());
            }
            ensure_remote_dir_exists(sftp, dir).await?;
            if idx == 0 || idx + 1 == directories_to_create.len() || (idx + 1).is_multiple_of(64) {
                send_upload_preparing(
                    control.event_tx,
                    control.request_id,
                    Some(root_total_bytes),
                    format!(
                        "Creating remote folders... {}/{} ready.",
                        idx + 1,
                        directories_to_create.len()
                    ),
                );
            }
        }
    }

    if files_to_upload.is_empty() {
        let mut summary_parts: Vec<String> = Vec::new();
        if !directories_to_create.is_empty() {
            summary_parts.push(format!(
                "Created {} remote folder(s)",
                directories_to_create.len()
            ));
        }
        if skipped_files > 0 {
            summary_parts.push(format!("Skipped {} existing file(s)", skipped_files));
        }
        let message = if summary_parts.is_empty() {
            "Nothing to upload.".to_string()
        } else {
            format!("{}.", summary_parts.join(". "))
        };
        send_transfer_finished(
            control.event_tx,
            control.request_id,
            local_path,
            Some(message),
        );
        return Ok(());
    }

    send_upload_preparing(
        control.event_tx,
        control.request_id,
        Some(root_total_bytes),
        format!("Prepared {} file(s) for upload...", files_to_upload.len()),
    );

    let mut progress =
        TransferProgressReporter::new(control.request_id, Some(total_bytes), control.event_tx);
    let mut uploaded_files: usize = 0;

    for file in &files_to_upload {
        progress.send_started(
            file.remote_path.clone(),
            file.local_path.display().to_string(),
        );
        if !upload_local_file_to_remote(
            sftp,
            &file.local_path,
            &file.remote_path,
            &mut progress,
            control.cancel_rx,
            &local_path,
        )
        .await?
        {
            return Ok(());
        }
        uploaded_files = uploaded_files.saturating_add(1);
    }

    let mut summary_parts: Vec<String> = Vec::new();
    if uploaded_files > 0 {
        summary_parts.push(format!("Uploaded {} file(s)", uploaded_files));
    }
    if skipped_identical_files > 0 {
        summary_parts.push(format!(
            "Skipped {} identical file(s)",
            skipped_identical_files
        ));
    }
    if skipped_files > 0 {
        summary_parts.push(format!("Skipped {} existing file(s)", skipped_files));
    }
    if summary_parts.is_empty() {
        summary_parts.push("Nothing to upload".to_string());
    }
    let finish_message = Some(format!("{}.", summary_parts.join(". ")));
    send_transfer_finished(
        control.event_tx,
        control.request_id,
        local_path,
        finish_message,
    );
    Ok(())
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
    let remote_metadata = sftp
        .metadata(remote_path.clone())
        .await
        .with_context(|| format!("Failed to inspect remote path: {remote_path}"))?;
    if remote_metadata.file_type().is_dir() {
        return run_sftp_download_directory_with_session(
            &sftp,
            request_id,
            remote_path,
            local_path,
            event_tx,
            cancel_rx,
        )
        .await;
    }

    let mut remote = sftp
        .open(remote_path.clone())
        .await
        .with_context(|| format!("Failed to open remote file: {remote_path}"))?;
    let total_bytes = Some(remote_metadata.len());

    let local_path_obj = PathBuf::from(local_path.clone());
    let temp_path = local_transfer_temp_path(&local_path);
    if let Some(parent) = temp_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create local directory: {}", parent.display())
            })?;
        }
    }

    let mut downloaded_bytes: u64 = 0;
    let mut out = if resume_from_local {
        let existing_size = std::fs::metadata(&temp_path).map(|m| m.len()).unwrap_or(0);
        if existing_size > 0 {
            match total_bytes {
                Some(total) if existing_size == total => {
                    replace_local_file(&temp_path, &local_path_obj).with_context(|| {
                        format!(
                            "Failed to finalize completed partial download: {}",
                            local_path_obj.display()
                        )
                    })?;
                    send_download_event(
                        event_tx,
                        DownloadManagerEvent::Started {
                            request_id,
                            remote_path,
                            local_path: local_path.clone(),
                            downloaded_bytes: existing_size,
                            total_bytes,
                        },
                    );
                    send_transfer_finished(event_tx, request_id, local_path, None);
                    return Ok(());
                }
                Some(total) if existing_size > total => std::fs::File::create(&temp_path)
                    .with_context(|| {
                        format!(
                            "Failed to reset temporary download file: {}",
                            temp_path.display()
                        )
                    })?,
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
                        .open(&temp_path)
                        .with_context(|| {
                            format!(
                                "Failed to open temporary download file for resume: {}",
                                temp_path.display()
                            )
                        })?
                }
            }
        } else {
            std::fs::File::create(&temp_path).with_context(|| {
                format!(
                    "Failed to create temporary download file: {}",
                    temp_path.display()
                )
            })?
        }
    } else {
        std::fs::File::create(&temp_path).with_context(|| {
            format!(
                "Failed to create temporary download file: {}",
                temp_path.display()
            )
        })?
    };

    send_download_event(
        event_tx,
        DownloadManagerEvent::Started {
            request_id,
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            downloaded_bytes,
            total_bytes,
        },
    );

    let started_at = Instant::now();
    let mut downloaded_this_attempt: u64 = 0;
    let mut buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        if transfer_cancel_requested(cancel_rx) {
            send_download_event(
                event_tx,
                DownloadManagerEvent::Canceled {
                    request_id,
                    local_path: local_path.clone(),
                },
            );
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
        send_download_event(
            event_tx,
            DownloadManagerEvent::Progress {
                request_id,
                downloaded_bytes,
                total_bytes,
                speed_bps,
            },
        );
    }

    std::io::Write::flush(&mut out).context("Failed to flush local file")?;
    drop(out);
    replace_local_file(&temp_path, &local_path_obj)
        .with_context(|| format!("Failed to move completed download into place: {local_path}"))?;
    send_transfer_finished(event_tx, request_id, local_path, None);
    Ok(())
}

async fn run_sftp_upload_with_session(
    sftp: SftpSession,
    request_id: u64,
    remote_path: String,
    local_path: String,
    resume_from_remote_temp: bool,
    control: UploadTransferControl<'_>,
) -> Result<()> {
    let local_path_obj = PathBuf::from(local_path.clone());
    if local_path_obj.is_dir() {
        return run_sftp_upload_directory_with_session(&sftp, remote_path, local_path, &control)
            .await;
    }

    let mut local = tokio::fs::File::open(local_path.clone())
        .await
        .with_context(|| format!("Failed to open local file: {local_path}"))?;
    let total_bytes = local.metadata().await.ok().map(|m| m.len());
    let temp_remote_path = remote_transfer_temp_path(&remote_path);
    let mut uploaded_bytes: u64 = 0;
    let existing_temp_size = if resume_from_remote_temp {
        sftp.metadata(temp_remote_path.clone())
            .await
            .ok()
            .map(|m| m.len())
            .unwrap_or(0)
    } else {
        0
    };

    if existing_temp_size == 0 {
        match remote_path_info(&sftp, &remote_path).await? {
            Some(RemotePathInfo {
                kind: RemotePathKind::File,
                ..
            }) => {
                let response = wait_for_upload_conflict_response(
                    UploadConflictPrompt {
                        request_id,
                        local_path: local_path_obj.display().to_string(),
                        remote_path: remote_path.clone(),
                        conflict_index: 1,
                        conflict_total: 1,
                    },
                    &control,
                )
                .await?;
                match response.choice {
                    UploadConflictChoice::OverwriteIfDifferent => {
                        if let Some(local_size) = total_bytes {
                            match remote_file_needs_upload_with_fast_compare(
                                &sftp,
                                &local_path_obj,
                                &remote_path,
                                local_size,
                                &control,
                            )
                            .await?
                            {
                                Some(true) => {}
                                Some(false) => {
                                    send_transfer_finished(
                                        control.event_tx,
                                        request_id,
                                        local_path,
                                        Some("Skipped identical remote file.".to_string()),
                                    );
                                    return Ok(());
                                }
                                None => {
                                    send_download_event(
                                        control.event_tx,
                                        DownloadManagerEvent::Canceled {
                                            request_id,
                                            local_path,
                                        },
                                    );
                                    return Ok(());
                                }
                            }
                        }
                    }
                    UploadConflictChoice::Skip => {
                        send_transfer_finished(
                            control.event_tx,
                            request_id,
                            local_path,
                            Some("Skipped existing remote file.".to_string()),
                        );
                        return Ok(());
                    }
                    UploadConflictChoice::CancelTransfer => {
                        send_download_event(
                            control.event_tx,
                            DownloadManagerEvent::Canceled {
                                request_id,
                                local_path,
                            },
                        );
                        return Ok(());
                    }
                }
            }
            Some(RemotePathInfo {
                kind: RemotePathKind::Directory,
                ..
            }) => {
                return Err(anyhow!(
                    "Remote upload destination exists as a folder: {remote_path}"
                ));
            }
            None => {}
        }
    }

    let mut remote = if resume_from_remote_temp {
        let existing_size = existing_temp_size;
        if existing_size > 0 {
            match total_bytes {
                Some(total) if existing_size == total => {
                    replace_remote_file(&sftp, &temp_remote_path, &remote_path).await?;
                    send_download_event(
                        control.event_tx,
                        DownloadManagerEvent::Started {
                            request_id,
                            remote_path: remote_path.clone(),
                            local_path: local_path.clone(),
                            downloaded_bytes: existing_size,
                            total_bytes,
                        },
                    );
                    send_transfer_finished(control.event_tx, request_id, local_path, None);
                    return Ok(());
                }
                Some(total) if existing_size > total => {
                    sftp.remove_file(temp_remote_path.clone())
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to reset oversized temporary remote upload: {temp_remote_path}"
                            )
                        })?;
                    sftp.create(temp_remote_path.clone())
                        .await
                        .with_context(|| {
                            format!("Failed to create remote temp file: {temp_remote_path}")
                        })?
                }
                _ => {
                    uploaded_bytes = existing_size;
                    local
                        .seek(std::io::SeekFrom::Start(existing_size))
                        .await
                        .with_context(|| {
                            format!("Failed to seek local file for resume: {local_path}")
                        })?;
                    let mut remote = sftp
                        .open_with_flags(
                            temp_remote_path.clone(),
                            OpenFlags::CREATE | OpenFlags::WRITE,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to open temporary remote file for resume: {temp_remote_path}"
                            )
                        })?;
                    remote
                        .seek(std::io::SeekFrom::Start(existing_size))
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to seek temporary remote file for resume: {temp_remote_path}"
                            )
                        })?;
                    remote
                }
            }
        } else {
            sftp.create(temp_remote_path.clone())
                .await
                .with_context(|| format!("Failed to create remote temp file: {temp_remote_path}"))?
        }
    } else {
        if sftp
            .try_exists(temp_remote_path.clone())
            .await
            .with_context(|| format!("Failed to check remote temp file: {temp_remote_path}"))?
        {
            sftp.remove_file(temp_remote_path.clone())
                .await
                .with_context(|| {
                    format!("Failed to clear previous temporary remote file: {temp_remote_path}")
                })?;
        }
        sftp.create(temp_remote_path.clone())
            .await
            .with_context(|| format!("Failed to create remote temp file: {temp_remote_path}"))?
    };

    send_download_event(
        control.event_tx,
        DownloadManagerEvent::Started {
            request_id,
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            downloaded_bytes: uploaded_bytes,
            total_bytes,
        },
    );

    let started_at = Instant::now();
    let mut buf = vec![0u8; TRANSFER_IO_BUFFER_SIZE];

    loop {
        if transfer_cancel_requested(control.cancel_rx) {
            send_download_event(
                control.event_tx,
                DownloadManagerEvent::Canceled {
                    request_id,
                    local_path: local_path.clone(),
                },
            );
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
        send_download_event(
            control.event_tx,
            DownloadManagerEvent::Progress {
                request_id,
                downloaded_bytes: uploaded_bytes,
                total_bytes,
                speed_bps,
            },
        );
    }

    remote
        .flush()
        .await
        .context("Failed to flush remote file")?;
    remote
        .shutdown()
        .await
        .context("Failed to close temporary remote file")?;
    replace_remote_file(&sftp, &temp_remote_path, &remote_path).await?;
    send_transfer_finished(control.event_tx, request_id, local_path, None);
    Ok(())
}

async fn run_sftp_client_async(
    client_id: u64,
    session: Rc<SshHandle>,
    sftp: SftpSession,
    ui_tx: Sender<SftpUiMessage>,
    mut worker_rx: UnboundedReceiver<SftpWorkerMessage>,
    log_path: String,
) -> Result<()> {
    logger::log_line(&log_path, &format!("SFTP client {client_id} connected."));
    send_message(
        &ui_tx,
        SftpUiMessage::Status(StatusUpdate::info("Connected")),
    );
    send_message(&ui_tx, SftpUiMessage::Connected(true));

    while let Some(message) = worker_rx.recv().await {
        match message {
            SftpWorkerMessage::Command(cmd) => {
                let request_id = sftp_command_request_id(&cmd);
                match execute_sftp_command(&sftp, Some(session.as_ref()), cmd, &log_path).await {
                    Ok(event) => {
                        send_message(&ui_tx, SftpUiMessage::Event(event));
                    }
                    Err(err) => {
                        logger::log_line(
                            &log_path,
                            &format!("SFTP client {client_id} command failed: {err}"),
                        );
                        send_message(
                            &ui_tx,
                            SftpUiMessage::Event(SftpEvent::OperationErr {
                                request_id,
                                issue: transfer_issue_from_error(&err),
                            }),
                        );
                    }
                }
            }
            SftpWorkerMessage::Disconnect => break,
        }
    }

    logger::log_line(&log_path, &format!("SFTP client {client_id} disconnected."));
    send_sftp_status(&ui_tx, IssueKind::Transport, "Disconnected");
    send_message(&ui_tx, SftpUiMessage::Connected(false));
    Ok(())
}

async fn run_terminal_client_async(
    client_id: u64,
    session: Rc<SshHandle>,
    ui_tx: Sender<UiMessage>,
    mut worker_rx: UnboundedReceiver<TerminalClientCommand>,
    scrollback_len: usize,
    log_path: String,
) -> Result<()> {
    logger::log_line(
        &log_path,
        &format!("Terminal client {client_id} opening channel."),
    );
    let mut channel = session
        .channel_open_session()
        .await
        .context("Failed to open SSH channel")?;

    logger::log_line(
        &log_path,
        &format!("Terminal client {client_id} requesting PTY."),
    );
    channel
        .request_pty(false, "xterm-256color", 80, 24, 0, 0, &[])
        .await
        .context("Failed to request PTY")?;

    let _ = channel.set_env(false, "TERM", "xterm-256color").await;

    logger::log_line(
        &log_path,
        &format!("Terminal client {client_id} starting shell."),
    );
    channel
        .request_shell(true)
        .await
        .context("Failed to start shell")?;

    send_ui_status(&ui_tx, IssueKind::Info, "Connected successfully.");
    send_message(&ui_tx, UiMessage::Connected(true));
    logger::log_line(
        &log_path,
        &format!("Terminal client {client_id} connected."),
    );

    let len = scrollback_len.clamp(0, 200_000);
    let len = if len == 0 { TERM_SCROLLBACK_LEN } else { len };
    let mut parser = Parser::new(24, 80, len);
    let mut scanner = CsiQueryScanner::default();
    let mut screen_dirty = true;
    let mut scrollback_dirty = true;
    let mut last_scrollback_max: Option<usize> = None;
    let mut screen_emit_interval = TERM_SCREEN_EMIT_INTERVAL_BASE;
    let mut last_screen_emit = Instant::now()
        .checked_sub(screen_emit_interval)
        .unwrap_or_else(Instant::now);
    let mut screen_rate_window_started = Instant::now();
    let mut screen_rate_window_bytes: u64 = 0;

    let mut writer = channel.make_writer();

    loop {
        let rate_window_elapsed = screen_rate_window_started.elapsed();
        if rate_window_elapsed >= TERM_SCREEN_RATE_WINDOW {
            screen_emit_interval =
                adaptive_screen_emit_interval(screen_rate_window_bytes, rate_window_elapsed);
            screen_rate_window_started = Instant::now();
            screen_rate_window_bytes = 0;
        }

        if scrollback_dirty {
            let scrollback_max = parser.screen().scrollback_max();
            if last_scrollback_max != Some(scrollback_max) {
                send_scrollback_max(&ui_tx, &mut parser);
                last_scrollback_max = Some(scrollback_max);
            }
            scrollback_dirty = false;
        }

        if screen_dirty && last_screen_emit.elapsed() >= screen_emit_interval {
            send_screen(&ui_tx, &mut parser);
            screen_dirty = false;
            last_screen_emit = Instant::now();
            continue;
        }

        let mut disconnected = false;
        tokio::select! {
            command = worker_rx.recv() => {
                match command {
                    Some(TerminalClientCommand::Input(data)) => {
                        writer
                            .write_all(&data)
                            .await
                            .context("Channel write failed")?;
                        writer.flush().await.context("Channel flush failed")?;
                    }
                    Some(TerminalClientCommand::Resize {
                        rows,
                        cols,
                        width_px,
                        height_px,
                    }) => {
                        channel
                            .window_change(cols.into(), rows.into(), width_px, height_px)
                            .await
                            .map_err(|err| anyhow!("Failed to resize PTY: {err}"))?;
                        parser.set_size(rows, cols);
                        screen_dirty = true;
                        scrollback_dirty = true;
                    }
                    Some(TerminalClientCommand::SetScrollback(rows)) => {
                        parser.set_scrollback(rows);
                        screen_dirty = true;
                        scrollback_dirty = true;
                    }
                    Some(TerminalClientCommand::Disconnect) | None => {
                        disconnected = true;
                    }
                }
            }
            message = channel.wait() => {
                match message {
                    Some(ChannelMsg::Data { data }) => {
                        process_with_query_responses(
                            &mut parser,
                            &mut scanner,
                            &mut writer,
                            data.as_ref(),
                        )
                        .await?;
                        screen_rate_window_bytes =
                            screen_rate_window_bytes.saturating_add(data.len() as u64);
                        screen_dirty = true;
                        scrollback_dirty = true;
                    }
                    Some(ChannelMsg::ExtendedData { data, .. }) => {
                        process_with_query_responses(
                            &mut parser,
                            &mut scanner,
                            &mut writer,
                            data.as_ref(),
                        )
                        .await?;
                        screen_rate_window_bytes =
                            screen_rate_window_bytes.saturating_add(data.len() as u64);
                        screen_dirty = true;
                        scrollback_dirty = true;
                    }
                    Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) => {
                        logger::log_line(
                            &log_path,
                            &format!("Terminal client {client_id} channel EOF."),
                        );
                        send_ui_status(
                            &ui_tx,
                            IssueKind::Transport,
                            "Transport disconnected. The SSH channel closed.",
                        );
                        send_message(&ui_tx, UiMessage::Connected(false));
                        return Ok(());
                    }
                    Some(_) => {}
                    None => {
                        logger::log_line(
                            &log_path,
                            &format!("Terminal client {client_id} channel closed."),
                        );
                        send_ui_status(
                            &ui_tx,
                            IssueKind::Transport,
                            "Transport disconnected. The SSH channel closed.",
                        );
                        send_message(&ui_tx, UiMessage::Connected(false));
                        return Ok(());
                    }
                }
            }
            _ = tokio::time::sleep(screen_emit_interval.saturating_sub(last_screen_emit.elapsed())), if screen_dirty => {
                send_screen(&ui_tx, &mut parser);
                screen_dirty = false;
                last_screen_emit = Instant::now();
            }
        }

        if disconnected {
            logger::log_line(
                &log_path,
                &format!("Terminal client {client_id} disconnected."),
            );
            let _ = channel.eof().await;
            let _ = channel.close().await;
            send_ui_status(&ui_tx, IssueKind::Info, "Disconnected.");
            send_message(&ui_tx, UiMessage::Connected(false));
            return Ok(());
        }
    }
}

fn terminal_client_by_id(active_clients: &[ActiveTerminalClient], client_id: u64) -> Option<usize> {
    active_clients
        .iter()
        .position(|client| client.client_id == client_id)
}

fn send_terminal_client_command(
    active_clients: &mut Vec<ActiveTerminalClient>,
    client_id: u64,
    command: TerminalClientCommand,
) {
    let Some(index) = terminal_client_by_id(active_clients, client_id) else {
        return;
    };
    if active_clients[index].command_tx.send(command).is_ok() {
        return;
    }
    let client = active_clients.swap_remove(index);
    abort_terminal_client(client);
}

fn spawn_terminal_client(
    active_clients: &mut Vec<ActiveTerminalClient>,
    client_id: u64,
    session: Rc<SshHandle>,
    ui_tx: Sender<UiMessage>,
    scrollback_len: usize,
    log_path: &str,
) {
    if let Some(index) = terminal_client_by_id(active_clients, client_id) {
        let client = active_clients.swap_remove(index);
        abort_terminal_client(client);
    }

    let (command_tx, command_rx) = unbounded_channel::<TerminalClientCommand>();
    let tracked_ui_tx = ui_tx.clone();
    let log_path = log_path.to_string();
    let task = tokio::task::spawn_local(async move {
        if let Err(err) = run_terminal_client_async(
            client_id,
            session,
            ui_tx.clone(),
            command_rx,
            scrollback_len,
            log_path.clone(),
        )
        .await
        {
            logger::log_line(
                &log_path,
                &format!("Terminal client {client_id} failed: {err}"),
            );
            let issue = transfer_issue_from_error(&err);
            send_ui_status(&ui_tx, issue.kind, issue.message);
            send_message(&ui_tx, UiMessage::Connected(false));
        }
    });
    active_clients.push(ActiveTerminalClient {
        client_id,
        ui_tx: tracked_ui_tx,
        command_tx,
        abort_handle: task.abort_handle(),
    });
    drop(task);
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
    send_ui_status(ui_tx, IssueKind::Authentication, "Authentication required");
    send_message(
        ui_tx,
        UiMessage::AuthPrompt(AuthPrompt {
            instructions: instructions.to_string(),
            prompts: prompts.clone(),
        }),
    );

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

async fn authenticate_keyboard_interactive_with_password(
    session: &mut SshHandle,
    username: &str,
    password: &str,
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
            KeyboardInteractiveAuthResponse::InfoRequest { prompts, .. } => {
                if !can_auto_fill_cached_password(&prompts, password) {
                    return Err(ConnectionError::DetachedTransferNeedsCredentials.into());
                }
                reply = session
                    .authenticate_keyboard_interactive_respond(vec![password.to_string()])
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

async fn open_authenticated_session_for_transfer(
    settings: &ConnectionSettings,
    log_path: &str,
) -> Result<SshHandle> {
    if settings.host.trim().is_empty() {
        return Err(ConnectionError::MissingHost.into());
    }
    if settings.username.trim().is_empty() {
        return Err(ConnectionError::MissingUsername.into());
    }

    let host = settings.host.trim().to_string();
    let port = settings.port;
    let addr = format!("{host}:{port}");
    logger::log_line(
        log_path,
        &format!("Detached transfer connecting TCP to {addr}."),
    );
    let tcp = tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("Failed to connect to {addr}"))?;
    let _ = tcp.set_nodelay(true);

    let config = client::Config {
        inactivity_timeout: None,
        keepalive_interval: Some(Duration::from_secs(20)),
        keepalive_max: 0,
        ..Default::default()
    };
    let config = Arc::new(config);

    logger::log_line(log_path, "Creating detached transfer SSH session.");
    let mut session = client::connect_stream(
        config,
        tcp,
        KnownHostsClient::non_interactive(host, port, log_path.to_string()),
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
        AuthResult::Success => authenticated = true,
        AuthResult::Failure {
            remaining_methods: methods,
            ..
        } => remaining_methods = methods,
    }

    let mut supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
    let mut supports_pass = supports_method(&remaining_methods, MethodKind::Password);
    let mut supports_pubkey = supports_method(&remaining_methods, MethodKind::PublicKey);

    let best_rsa_hash = if supports_pubkey {
        match session.best_supported_rsa_hash().await {
            Ok(v) => v.flatten(),
            Err(err) => {
                logger::log_line(
                    log_path,
                    &format!("Unable to query best RSA hash for detached transfer session: {err}"),
                );
                None
            }
        }
    } else {
        None
    };

    if !authenticated && supports_pubkey && !settings.private_key_path.trim().is_empty() {
        let private_key = Path::new(settings.private_key_path.trim());
        if private_key.exists() {
            let saved_passphrase = if settings.key_passphrase.trim().is_empty() {
                None
            } else {
                Some(settings.key_passphrase.as_str())
            };
            if let Ok(auth_result) = authenticate_with_private_key(
                &mut session,
                username,
                private_key,
                saved_passphrase,
                best_rsa_hash,
            )
            .await
            {
                let _ = apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);
                supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
                supports_pass = supports_method(&remaining_methods, MethodKind::Password);
                supports_pubkey = supports_method(&remaining_methods, MethodKind::PublicKey);
            }
        }
    }

    if !authenticated && supports_pubkey {
        if let Ok(auth_result) = authenticate_via_agent(&mut session, username, best_rsa_hash).await
        {
            let _ = apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);
            supports_kbd = supports_method(&remaining_methods, MethodKind::KeyboardInteractive);
            supports_pass = supports_method(&remaining_methods, MethodKind::Password);
        }
    }

    if !authenticated && supports_kbd && !settings.password.is_empty() {
        if let Ok(auth_result) = authenticate_keyboard_interactive_with_password(
            &mut session,
            username,
            &settings.password,
        )
        .await
        {
            let _ = apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);
            supports_pass = supports_method(&remaining_methods, MethodKind::Password);
        }
    }

    if !authenticated && supports_pass && !settings.password.is_empty() {
        let auth_result = session
            .authenticate_password(username, settings.password.clone())
            .await
            .context("SSH password authentication failed")?;
        let _ = apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);
    }

    if !authenticated {
        return Err(ConnectionError::DetachedTransferNeedsCredentials.into());
    }

    Ok(session)
}

type SshHandle = client::Handle<KnownHostsClient>;

struct ActiveTerminalClient {
    client_id: u64,
    ui_tx: Sender<UiMessage>,
    command_tx: UnboundedSender<TerminalClientCommand>,
    abort_handle: tokio::task::AbortHandle,
}

fn abort_terminal_client(client: ActiveTerminalClient) {
    if client.abort_handle.is_finished() {
        return;
    }
    client.abort_handle.abort();
}

fn stop_terminal_client(client: ActiveTerminalClient, message: &str) {
    if client.abort_handle.is_finished() {
        return;
    }
    client.abort_handle.abort();
    send_message(
        &client.ui_tx,
        UiMessage::Status(StatusUpdate {
            kind: IssueKind::Transport,
            message: message.to_string(),
        }),
    );
    send_message(&client.ui_tx, UiMessage::Connected(false));
}

fn stop_active_terminal_clients(active_clients: &mut Vec<ActiveTerminalClient>, message: &str) {
    for client in active_clients.drain(..) {
        stop_terminal_client(client, message);
    }
}

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
    send_message(
        &client.ui_tx,
        SftpUiMessage::Status(StatusUpdate {
            kind: IssueKind::Transport,
            message: message.to_string(),
        }),
    );
    send_message(&client.ui_tx, SftpUiMessage::Connected(false));
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
        send_message(
            &transfer.event_tx,
            DownloadManagerEvent::Paused {
                request_id: transfer.request_id,
                issue: TransferIssue {
                    kind: IssueKind::Transport,
                    message: message.to_string(),
                },
            },
        );
    }
}

enum HostKeyVerificationMode {
    Interactive {
        ui_tx: Sender<UiMessage>,
        decision_rx: Receiver<HostKeyDecision>,
        log_path: String,
    },
    NonInteractive {
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

    fn non_interactive(host: String, port: u16, log_path: String) -> Self {
        Self {
            host,
            port,
            known_hosts_path: app_known_hosts_path(),
            mode: HostKeyVerificationMode::NonInteractive { log_path },
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
                send_message(ui_tx, UiMessage::HostKeyPrompt(prompt));

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
            HostKeyVerificationMode::NonInteractive { log_path } => {
                let fingerprint =
                    format!("{}", server_public_key.fingerprint(keys::HashAlg::Sha256));
                if let Some(line) = changed_line {
                    logger::log_line(
                        log_path.as_str(),
                        &format!(
                            "Detached transfer rejected changed host key for {}:{} at known_hosts line {} (fingerprint {}).",
                            self.host, self.port, line, fingerprint
                        ),
                    );
                    return Err(keys::Error::KeyChanged { line }.into());
                }

                logger::log_line(
                    log_path.as_str(),
                    &format!(
                        "Detached transfer rejected unknown host key for {}:{} (fingerprint {}).",
                        self.host, self.port, fingerprint
                    ),
                );
                Ok(false)
            }
        }
    }
}

async fn run_detached_sftp_download(
    request: DetachedTransferRequest,
    resume_from_local: bool,
) -> Result<()> {
    let DetachedTransferRequest {
        settings,
        request_id,
        remote_path,
        local_path,
        event_tx,
        cancel_rx,
        conflict_response_rx: _,
        log_path,
    } = request;

    if transfer_cancel_requested(&cancel_rx) {
        send_download_event(
            &event_tx,
            DownloadManagerEvent::Canceled {
                request_id,
                local_path,
            },
        );
        return Ok(());
    }

    let session = open_authenticated_session_for_transfer(&settings, &log_path).await?;
    let sftp = open_sftp_channel(&session, &log_path).await?;
    run_sftp_download_with_session(
        sftp,
        request_id,
        remote_path,
        local_path,
        resume_from_local,
        &event_tx,
        &cancel_rx,
    )
    .await
}

async fn run_detached_sftp_upload(
    request: DetachedTransferRequest,
    resume_from_remote_temp: bool,
) -> Result<()> {
    let DetachedTransferRequest {
        settings,
        request_id,
        remote_path,
        local_path,
        event_tx,
        cancel_rx,
        conflict_response_rx,
        log_path,
    } = request;

    if transfer_cancel_requested(&cancel_rx) {
        send_download_event(
            &event_tx,
            DownloadManagerEvent::Canceled {
                request_id,
                local_path,
            },
        );
        return Ok(());
    }

    let session = open_authenticated_session_for_transfer(&settings, &log_path).await?;
    let sftp = open_sftp_channel(&session, &log_path).await?;
    run_sftp_upload_with_session(
        sftp,
        request_id,
        remote_path,
        local_path,
        resume_from_remote_temp,
        UploadTransferControl {
            request_id,
            session: &session,
            event_tx: &event_tx,
            cancel_rx: &cancel_rx,
            conflict_response_rx: &conflict_response_rx,
            log_path: &log_path,
        },
    )
    .await
}

pub fn start_sftp_download_detached(
    request: DetachedTransferRequest,
    resume_from_local: bool,
) -> thread::JoinHandle<()> {
    let request_id = request.request_id;
    let failure_tx = request.event_tx.clone();
    thread::spawn(move || {
        let result = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt.block_on(run_detached_sftp_download(request, resume_from_local)),
            Err(err) => Err(anyhow!("Failed to create async runtime: {err}")),
        };

        if let Err(err) = result {
            send_download_event(
                &failure_tx,
                DownloadManagerEvent::Failed {
                    request_id,
                    issue: transfer_issue_from_error(&err),
                },
            );
        }
    })
}

pub fn start_sftp_upload_detached(
    request: DetachedTransferRequest,
    resume_from_remote_temp: bool,
) -> thread::JoinHandle<()> {
    let request_id = request.request_id;
    let failure_tx = request.event_tx.clone();
    thread::spawn(move || {
        let result = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt.block_on(run_detached_sftp_upload(request, resume_from_remote_temp)),
            Err(err) => Err(anyhow!("Failed to create async runtime: {err}")),
        };

        if let Err(err) = result {
            send_download_event(
                &failure_tx,
                DownloadManagerEvent::Failed {
                    request_id,
                    issue: transfer_issue_from_error(&err),
                },
            );
        }
    })
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
    initial_client_id: u64,
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
                        initial_client_id,
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
            send_message(
                &ui_tx,
                UiMessage::Status(connection_status_from_error(&err)),
            );
            send_message(&ui_tx, UiMessage::Connected(false));
        }
    })
}

async fn run_shell_async(
    initial_client_id: u64,
    settings: ConnectionSettings,
    scrollback_len: usize,
    ui_tx: &Sender<UiMessage>,
    worker_rx: Receiver<WorkerMessage>,
    host_key_rx: Receiver<HostKeyDecision>,
    log_path: &str,
) -> Result<()> {
    logger::log_line(log_path, "Validating settings.");
    if settings.host.trim().is_empty() {
        return Err(ConnectionError::MissingHost.into());
    }
    if settings.username.trim().is_empty() {
        return Err(ConnectionError::MissingUsername.into());
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
                    send_ui_status(
                        ui_tx,
                        IssueKind::Authentication,
                        "Additional authentication required (server policy).",
                    );
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
                    send_ui_status(
                        ui_tx,
                        IssueKind::Authentication,
                        "Additional authentication required (server policy).",
                    );
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
            send_ui_status(ui_tx, IssueKind::Authentication, "Password required.");
        };

        let auth_result = session
            .authenticate_password(username, pw)
            .await
            .context("SSH password authentication failed")?;
        let _ = apply_auth_result(auth_result, &mut authenticated, &mut remaining_methods);
    }

    if !authenticated {
        return Err(ConnectionError::AuthenticationFailed.into());
    }

    let session = Rc::new(session);
    let mut worker_rx = bridge_receiver_to_async(worker_rx);
    let mut active_terminal_clients: Vec<ActiveTerminalClient> = Vec::new();
    let mut active_sftp_clients: Vec<ActiveSftpClient> = Vec::new();
    let mut active_transfers: Vec<ActiveTransfer> = Vec::new();
    let mut worker_sender_disconnected = false;
    spawn_terminal_client(
        &mut active_terminal_clients,
        initial_client_id,
        Rc::clone(&session),
        ui_tx.clone(),
        scrollback_len,
        log_path,
    );

    loop {
        active_terminal_clients.retain(|client| !client.abort_handle.is_finished());
        active_sftp_clients.retain(|client| !client.abort_handle.is_finished());
        active_transfers.retain(|transfer| !transfer.abort_handle.is_finished());

        let mut pending_messages = VecDeque::new();
        if worker_sender_disconnected {
            tokio::time::sleep(SESSION_HOUSEKEEPING_INTERVAL).await;
        } else {
            tokio::select! {
                maybe_message = worker_rx.recv() => {
                    match maybe_message {
                        Some(message) => pending_messages.push_back(message),
                        None => worker_sender_disconnected = true,
                    }
                }
                _ = tokio::time::sleep(SESSION_HOUSEKEEPING_INTERVAL) => {}
            }
        }

        if !worker_sender_disconnected {
            loop {
                match worker_rx.try_recv() {
                    Ok(message) => pending_messages.push_back(message),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        worker_sender_disconnected = true;
                        break;
                    }
                }
            }
        }

        let mut disconnected = false;
        while let Some(message) = pending_messages.pop_front() {
            match message {
                WorkerMessage::Input { client_id, data } => send_terminal_client_command(
                    &mut active_terminal_clients,
                    client_id,
                    TerminalClientCommand::Input(data),
                ),
                WorkerMessage::Resize {
                    client_id,
                    rows,
                    cols,
                    width_px,
                    height_px,
                } => send_terminal_client_command(
                    &mut active_terminal_clients,
                    client_id,
                    TerminalClientCommand::Resize {
                        rows,
                        cols,
                        width_px,
                        height_px,
                    },
                ),
                WorkerMessage::SetScrollback { client_id, rows } => send_terminal_client_command(
                    &mut active_terminal_clients,
                    client_id,
                    TerminalClientCommand::SetScrollback(rows),
                ),
                WorkerMessage::AttachTerminalClient {
                    client_id,
                    ui_tx,
                    scrollback_len,
                } => spawn_terminal_client(
                    &mut active_terminal_clients,
                    client_id,
                    Rc::clone(&session),
                    ui_tx,
                    scrollback_len,
                    log_path,
                ),
                WorkerMessage::DetachTerminalClient { client_id } => {
                    if let Some(index) = terminal_client_by_id(&active_terminal_clients, client_id)
                    {
                        let client = active_terminal_clients.swap_remove(index);
                        let _ = client.command_tx.send(TerminalClientCommand::Disconnect);
                        abort_terminal_client(client);
                    }
                }
                WorkerMessage::AttachSftpClient {
                    client_id,
                    ui_tx,
                    worker_rx,
                } => {
                    if let Some(index) = active_sftp_clients
                        .iter()
                        .position(|client| client.client_id == client_id)
                    {
                        let client = active_sftp_clients.swap_remove(index);
                        stop_sftp_client(client, "SFTP session restarted.");
                    }

                    send_sftp_status(&ui_tx, IssueKind::Info, "Connecting SFTP session...");

                    match open_sftp_channel(session.as_ref(), log_path).await {
                        Ok(sftp) => {
                            let log_path = log_path.to_string();
                            let tracked_ui_tx = ui_tx.clone();
                            let sftp_session = Rc::clone(&session);
                            let worker_rx = bridge_receiver_to_async(worker_rx);
                            let task = tokio::task::spawn_local(async move {
                                if let Err(err) = run_sftp_client_async(
                                    client_id,
                                    sftp_session,
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
                                    let issue = transfer_issue_from_error(&err);
                                    send_sftp_status(&ui_tx, issue.kind, issue.message);
                                    send_message(&ui_tx, SftpUiMessage::Connected(false));
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
                            send_sftp_status(
                                &ui_tx,
                                transfer_issue_from_error(&err).kind,
                                format!("Failed to open SFTP session. {err}"),
                            );
                            send_message(&ui_tx, SftpUiMessage::Connected(false));
                        }
                    }
                }
                WorkerMessage::TransferCommand(cmd) => match cmd {
                    TransferCommand::Download {
                        request_id,
                        remote_path,
                        local_path,
                        resume_from_local,
                        event_tx,
                        cancel_rx,
                    } => {
                        if transfer_cancel_requested(&cancel_rx) {
                            send_message(
                                &event_tx,
                                DownloadManagerEvent::Canceled {
                                    request_id,
                                    local_path,
                                },
                            );
                            continue;
                        }

                        match open_sftp_channel(session.as_ref(), log_path).await {
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
                                        send_message(
                                            &event_tx,
                                            DownloadManagerEvent::Failed {
                                                request_id,
                                                issue: transfer_issue_from_error(&err),
                                            },
                                        );
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
                                send_message(
                                    &event_tx,
                                    DownloadManagerEvent::Failed {
                                        request_id,
                                        issue: transfer_issue_from_error(&err),
                                    },
                                );
                            }
                        }
                    }
                    TransferCommand::Upload {
                        request_id,
                        remote_path,
                        local_path,
                        resume_from_remote_temp,
                        event_tx,
                        cancel_rx,
                        conflict_response_rx,
                    } => {
                        if transfer_cancel_requested(&cancel_rx) {
                            send_message(
                                &event_tx,
                                DownloadManagerEvent::Canceled {
                                    request_id,
                                    local_path,
                                },
                            );
                            continue;
                        }

                        match open_sftp_channel(session.as_ref(), log_path).await {
                            Ok(sftp) => {
                                let log_path = log_path.to_string();
                                let tracked_event_tx = event_tx.clone();
                                let transfer_session = Rc::clone(&session);
                                let task = tokio::task::spawn_local(async move {
                                    if let Err(err) = run_sftp_upload_with_session(
                                        sftp,
                                        request_id,
                                        remote_path,
                                        local_path,
                                        resume_from_remote_temp,
                                        UploadTransferControl {
                                            request_id,
                                            session: transfer_session.as_ref(),
                                            event_tx: &event_tx,
                                            cancel_rx: &cancel_rx,
                                            conflict_response_rx: &conflict_response_rx,
                                            log_path: &log_path,
                                        },
                                    )
                                    .await
                                    {
                                        logger::log_line(
                                            &log_path,
                                            &format!(
                                                "Live upload transfer {request_id} failed: {err}"
                                            ),
                                        );
                                        send_message(
                                            &event_tx,
                                            DownloadManagerEvent::Failed {
                                                request_id,
                                                issue: transfer_issue_from_error(&err),
                                            },
                                        );
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
                                send_message(
                                    &event_tx,
                                    DownloadManagerEvent::Failed {
                                        request_id,
                                        issue: transfer_issue_from_error(&err),
                                    },
                                );
                            }
                        }
                    }
                },
                WorkerMessage::Disconnect => {
                    disconnected = true;
                    break;
                }
                WorkerMessage::AuthResponse(_) => {
                    // Auth responses are only expected during authentication.
                }
            }
        }

        if disconnected
            || (worker_sender_disconnected
                && active_terminal_clients.is_empty()
                && active_sftp_clients.is_empty()
                && active_transfers.is_empty())
        {
            stop_active_terminal_clients(
                &mut active_terminal_clients,
                "Transport disconnected. The SSH session closed.",
            );
            stop_active_sftp_clients(
                &mut active_sftp_clients,
                "SFTP session disconnected. Reconnect the source terminal to continue.",
            );
            stop_active_transfers(
                &mut active_transfers,
                "Transfer paused because the SSH session disconnected. Reconnect the tab and click retry.",
            );
            let _ = session
                .disconnect(Disconnect::ByApplication, "", "English")
                .await;
            logger::log_line(log_path, "Disconnected on request.");
            return Ok(());
        }

        if worker_sender_disconnected && active_terminal_clients.is_empty() {
            stop_active_sftp_clients(
                &mut active_sftp_clients,
                "SFTP session disconnected. Reconnect the source terminal to continue.",
            );
            stop_active_transfers(
                &mut active_transfers,
                "Transfer paused because the SSH session disconnected. Reconnect the tab and click retry.",
            );
            let _ = session
                .disconnect(Disconnect::ByApplication, "", "English")
                .await;
            logger::log_line(
                log_path,
                "Session worker is idle; disconnecting SSH session.",
            );
            return Ok(());
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

    #[test]
    fn transfer_temp_paths_use_sidecar_suffix() {
        let local = local_transfer_temp_path("C:\\temp\\archive.zip");
        assert_eq!(local, PathBuf::from("C:\\temp\\archive.zip.rusty-part"));
        assert_eq!(
            remote_transfer_temp_path("/srv/archive.zip"),
            "/srv/archive.zip.rusty-part"
        );
    }

    #[test]
    fn apply_remote_permission_mode_preserves_file_type_bits() {
        assert_eq!(
            apply_remote_permission_mode(Some(0o100644), 0o755),
            0o100755
        );
        assert_eq!(
            apply_remote_permission_mode(Some(0o040700), 0o640),
            0o040640
        );
        assert_eq!(apply_remote_permission_mode(None, 0o600), 0o600);
    }

    #[test]
    fn build_remote_ownership_command_uses_names() {
        let paths = vec!["/srv/example.txt".to_string()];
        assert_eq!(
            build_remote_ownership_command(&paths, Some("alice"), Some("staff")).unwrap(),
            "chown -- 'alice:staff' '/srv/example.txt'"
        );
        assert_eq!(
            build_remote_ownership_command(&paths, Some("alice"), None).unwrap(),
            "chown -- 'alice' '/srv/example.txt'"
        );
        assert_eq!(
            build_remote_ownership_command(&paths, None, Some("staff")).unwrap(),
            "chgrp -- 'staff' '/srv/example.txt'"
        );
    }

    #[test]
    fn diff_upload_trees_marks_existing_files_for_later_decision() {
        let local_tree = LocalUploadTree {
            directories: vec!["sub".to_string()],
            files: vec![
                LocalUploadTreeFile {
                    local_path: PathBuf::from("root").join("new.txt"),
                    relative_path: "new.txt".to_string(),
                    size: 5,
                },
                LocalUploadTreeFile {
                    local_path: PathBuf::from("root").join("same.txt"),
                    relative_path: "same.txt".to_string(),
                    size: 7,
                },
                LocalUploadTreeFile {
                    local_path: PathBuf::from("root").join("sub").join("changed.txt"),
                    relative_path: "sub/changed.txt".to_string(),
                    size: 9,
                },
            ],
            total_bytes: 21,
        };
        let mut remote_directories = HashSet::new();
        remote_directories.insert("sub".to_string());
        let mut remote_files = HashSet::new();
        remote_files.insert("same.txt".to_string());
        remote_files.insert("sub/changed.txt".to_string());
        let remote_tree = RemoteUploadTree {
            directories: remote_directories,
            files: remote_files,
        };

        let diff = diff_upload_trees(&local_tree, "/remote", true, &remote_tree).unwrap();

        assert!(diff.directories_to_create.is_empty());
        assert_eq!(diff.initial_conflict_count, 2);
        assert_eq!(diff.files.len(), 3);
        assert!(diff.files.iter().any(|file| {
            file.remote_path == "/remote/new.txt" && !file.remote_exists && file.size == 5
        }));
        assert!(diff.files.iter().any(|file| {
            file.remote_path == "/remote/same.txt" && file.remote_exists && file.size == 7
        }));
        assert!(diff.files.iter().any(|file| {
            file.remote_path == "/remote/sub/changed.txt" && file.remote_exists && file.size == 9
        }));
    }

    #[test]
    fn parse_remote_upload_tree_output_parses_find_rows() {
        let tree =
            parse_remote_upload_tree_output("d\tsub\nf\talpha.txt\nf\tsub/beta.txt\n").unwrap();

        assert!(tree.directories.contains("sub"));
        assert!(tree.files.contains("alpha.txt"));
        assert!(tree.files.contains("sub/beta.txt"));
    }

    #[test]
    fn parse_remote_file_sizes_output_parses_lines() {
        let sizes = parse_remote_file_sizes_output("12\n34\n", 2).unwrap();

        assert_eq!(sizes, vec![12, 34]);
    }

    #[test]
    fn parse_remote_file_md5_output_parses_md5_variants() {
        let md5sum_hashes = parse_remote_file_md5_output(
            "d41d8cd98f00b204e9800998ecf8427e  /tmp/a\n900150983cd24fb0d6963f7d28e17f72  /tmp/b\n",
            2,
        )
        .unwrap();
        assert_eq!(
            md5sum_hashes,
            vec![
                "d41d8cd98f00b204e9800998ecf8427e".to_string(),
                "900150983cd24fb0d6963f7d28e17f72".to_string()
            ]
        );

        let openssl_hashes =
            parse_remote_file_md5_output("MD5(/tmp/a)= d41d8cd98f00b204e9800998ecf8427e\n", 1)
                .unwrap();
        assert_eq!(
            openssl_hashes,
            vec!["d41d8cd98f00b204e9800998ecf8427e".to_string()]
        );
    }

    #[test]
    fn diff_upload_trees_rejects_remote_type_mismatches() {
        let local_tree = LocalUploadTree {
            directories: vec!["sub".to_string()],
            files: Vec::new(),
            total_bytes: 0,
        };
        let mut remote_files = HashSet::new();
        remote_files.insert("sub".to_string());
        let remote_tree = RemoteUploadTree {
            directories: HashSet::new(),
            files: remote_files,
        };

        let err = diff_upload_trees(&local_tree, "/remote", true, &remote_tree).unwrap_err();

        assert!(err
            .to_string()
            .contains("Remote path exists as a file where a folder is required"));
    }
}
