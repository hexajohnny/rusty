use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};
use std::{fs, path::PathBuf};

use arboard::Clipboard;
use eframe::egui;
use eframe::egui::{
    text::{LayoutJob, TextFormat},
    Align, Color32, EventFilter, FontId, Id, Pos2, Rect, Response, Sense, Stroke, Vec2,
};
use egui_tiles::{
    Behavior as TilesBehavior, Container, LinearDir, SimplificationOptions, Tile, TileId, Tiles,
    Tree, UiResponse,
};

use crate::async_config::AsyncConfigSaver;
use crate::config;
use crate::model::ConnectionSettings;
use crate::ssh::{self, UiMessage, WorkerMessage};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PersistedPaneKind {
    #[default]
    Terminal,
    FileManager {
        source_terminal: TileId,
        path: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedTab {
    id: u64,
    user_title: Option<String>,
    color: Option<Color32>,
    profile_name: Option<String>,
    settings: ConnectionSettings,
    scrollback_len: usize,
    #[serde(default)]
    autoconnect: bool,
    #[serde(default)]
    pane_kind: PersistedPaneKind,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedSession {
    tree: Tree<PersistedTab>,
    active_tile: Option<TileId>,
}

const TERM_FONT_SIZE_DEFAULT: f32 = 14.0;
const TERM_FONT_SIZE_MIN: f32 = 10.0;
const TERM_FONT_SIZE_MAX: f32 = 28.0;
const TERM_PAD_X: f32 = 1.0;
const TERM_PAD_Y: f32 = 1.0;

const TITLE_BAR_H: f32 = 28.0;
const TITLE_PAD_X: f32 = 10.0;
const CONTENT_PAD: f32 = 0.0;
const RESIZE_MARGIN: f32 = 6.0;

const APP_TITLE_TEXT: &str = concat!("Rusty - v", env!("CARGO_PKG_VERSION"));
const UPDATE_CHECK_INTERVAL_SECS: u64 = 12 * 60 * 60;
const UPDATE_CHECK_API_URL: &str = "https://api.github.com/repos/hexajohnny/rusty/releases/latest";
const UPDATE_RELEASES_URL: &str = "https://github.com/hexajohnny/rusty/releases/latest";

#[derive(Debug)]
struct UpdateCheckResult {
    check_succeeded: bool,
    available_version: Option<String>,
    available_url: Option<String>,
}

#[derive(Clone, Copy)]
struct UiTheme {
    bg: Color32,
    fg: Color32,
    top_bg: Color32,
    top_border: Color32,
    accent: Color32,
    muted: Color32,
}

impl Default for UiTheme {
    fn default() -> Self {
        Self {
            bg: Color32::from_rgb(10, 12, 14),
            fg: Color32::from_rgb(220, 220, 220),
            top_bg: Color32::from_rgb(18, 20, 24),
            top_border: Color32::from_rgb(45, 50, 58),
            accent: Color32::from_rgb(255, 184, 108),
            muted: Color32::from_rgb(140, 150, 160),
        }
    }
}

#[derive(Clone, Copy)]
struct TermTheme {
    bg: Color32,
    fg: Color32,
    palette16: [Color32; 16],
    dim_blend: f32,
}

impl Default for TermTheme {
    fn default() -> Self {
        Self {
            bg: Color32::from_rgb(0, 0, 0),
            fg: Color32::from_rgb(220, 220, 220),
            palette16: [
                Color32::from_rgb(0, 0, 0),       // 0 black
                Color32::from_rgb(205, 49, 49),   // 1 red
                Color32::from_rgb(13, 188, 121),  // 2 green
                Color32::from_rgb(229, 229, 16),  // 3 yellow
                Color32::from_rgb(36, 114, 200),  // 4 blue
                Color32::from_rgb(188, 63, 188),  // 5 magenta
                Color32::from_rgb(17, 168, 205),  // 6 cyan
                Color32::from_rgb(229, 229, 229), // 7 white
                Color32::from_rgb(102, 102, 102), // 8 bright black (gray)
                Color32::from_rgb(241, 76, 76),   // 9 bright red
                Color32::from_rgb(35, 209, 139),  // 10 bright green
                Color32::from_rgb(245, 245, 67),  // 11 bright yellow
                Color32::from_rgb(59, 142, 234),  // 12 bright blue
                Color32::from_rgb(214, 112, 214), // 13 bright magenta
                Color32::from_rgb(41, 184, 219),  // 14 bright cyan
                Color32::from_rgb(255, 255, 255), // 15 bright white
            ],
            dim_blend: 0.38,
        }
    }
}

impl TermTheme {
    fn from_config(cfg: &config::TerminalColorsConfig) -> Self {
        let to_c32 = |c: config::RgbColor| Color32::from_rgb(c.r, c.g, c.b);
        let mut palette16 = [Color32::BLACK; 16];
        for (i, c) in cfg.palette16.iter().copied().enumerate().take(16) {
            palette16[i] = to_c32(c);
        }
        let dim_blend = if cfg.dim_blend.is_finite() {
            cfg.dim_blend.clamp(0.0, 0.90)
        } else {
            0.38
        };
        Self {
            bg: to_c32(cfg.bg),
            fg: to_c32(cfg.fg),
            palette16,
            dim_blend,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TermStyle {
    fg: Color32,
    bg: Color32,
    italic: bool,
    underline: bool,
    inverse: bool,
}

impl TermStyle {
    fn to_text_format(self, font_id: FontId) -> TextFormat {
        TextFormat {
            font_id,
            color: self.fg,
            background: self.bg,
            italics: self.italic,
            underline: if self.underline {
                Stroke::new(1.0, self.fg)
            } else {
                Stroke::NONE
            },
            ..Default::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TermSelection {
    anchor: (u16, u16), // (row, col)
    cursor: (u16, u16), // (row, col)
    dragging: bool,
}

impl TermSelection {
    fn is_empty(&self) -> bool {
        self.anchor == self.cursor
    }

    fn normalized(&self) -> ((u16, u16), (u16, u16)) {
        if (self.anchor.0, self.anchor.1) <= (self.cursor.0, self.cursor.1) {
            (self.anchor, self.cursor)
        } else {
            (self.cursor, self.anchor)
        }
    }
}

impl UiTheme {
    fn light_default() -> Self {
        Self {
            bg: Color32::from_rgb(244, 246, 249),
            fg: Color32::from_rgb(28, 34, 42),
            top_bg: Color32::from_rgb(231, 235, 241),
            top_border: Color32::from_rgb(170, 178, 190),
            accent: Color32::from_rgb(196, 120, 20),
            muted: Color32::from_rgb(102, 112, 124),
        }
    }
}

fn parse_theme_rgb(value: &str) -> Option<Color32> {
    let s = value.trim();
    if let Some(hex) = s.strip_prefix('#') {
        if hex.len() == 6 {
            let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
            let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
            let b = u8::from_str_radix(&hex[4..6], 16).ok()?;
            return Some(Color32::from_rgb(r, g, b));
        }
    }

    let mut parts = s.split(',').map(|p| p.trim().parse::<u8>().ok());
    let r = parts.next().flatten()?;
    let g = parts.next().flatten()?;
    let b = parts.next().flatten()?;
    if parts.next().is_some() {
        return None;
    }
    Some(Color32::from_rgb(r, g, b))
}

fn for_each_theme_kv(text: &str, mut f: impl FnMut(&str, &str)) {
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = raw.trim();
        if key.is_empty() || value.is_empty() {
            continue;
        }
        f(key, value);
    }
}

fn default_theme_file_name(mode: config::UiThemeMode) -> &'static str {
    match mode {
        config::UiThemeMode::Dark => "Dark.thm",
        config::UiThemeMode::Light => "Light.thm",
    }
}

fn theme_dir_paths() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut push_unique = |path: PathBuf| {
        if !dirs.iter().any(|p| p == &path) {
            dirs.push(path);
        }
    };

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            push_unique(dir.join("theme"));
            push_unique(dir.join("dist").join("theme"));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        push_unique(cwd.join("theme"));
        push_unique(cwd.join("dist").join("theme"));
    }
    dirs
}

fn user_profile_dir() -> Option<PathBuf> {
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
        .filter(|path| path.is_dir())
}

fn theme_file_paths_for_name(file_name: &str) -> Vec<PathBuf> {
    theme_dir_paths()
        .into_iter()
        .map(|dir| dir.join(file_name))
        .collect()
}

fn normalize_theme_file_name(file: Option<&str>) -> Option<String> {
    let raw = file?.trim();
    if raw.is_empty() {
        return None;
    }
    let file_name = std::path::Path::new(raw)
        .file_name()?
        .to_string_lossy()
        .to_string();
    if file_name.trim().is_empty() {
        None
    } else {
        Some(file_name)
    }
}

fn available_theme_file_names() -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for dir in theme_dir_paths() {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_thm = path
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("thm"))
                .unwrap_or(false);
            if !is_thm {
                continue;
            }
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let name = name.trim().to_string();
            if name.is_empty() {
                continue;
            }
            if !names.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
                names.push(name);
            }
        }
    }
    names.sort_by_key(|s| s.to_ascii_lowercase());
    names
}

fn load_ui_theme(
    mode: config::UiThemeMode,
    selected_file: Option<&str>,
) -> (UiTheme, Option<PathBuf>) {
    let mut theme = match mode {
        config::UiThemeMode::Dark => UiTheme::default(),
        config::UiThemeMode::Light => UiTheme::light_default(),
    };

    let mut candidates: Vec<String> = Vec::new();
    if let Some(selected) = normalize_theme_file_name(selected_file) {
        candidates.push(selected);
    }
    let mode_default = default_theme_file_name(mode).to_string();
    if !candidates
        .iter()
        .any(|f| f.eq_ignore_ascii_case(&mode_default))
    {
        candidates.push(mode_default);
    }

    for file_name in candidates {
        for path in theme_file_paths_for_name(&file_name) {
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };

            for_each_theme_kv(&text, |key, value| {
                let Some(color) = parse_theme_rgb(value) else {
                    return;
                };
                if key.eq_ignore_ascii_case("bg") {
                    theme.bg = color;
                } else if key.eq_ignore_ascii_case("fg") {
                    theme.fg = color;
                } else if key.eq_ignore_ascii_case("top_bg") {
                    theme.top_bg = color;
                } else if key.eq_ignore_ascii_case("top_border") {
                    theme.top_border = color;
                } else if key.eq_ignore_ascii_case("accent") {
                    theme.accent = color;
                } else if key.eq_ignore_ascii_case("muted") {
                    theme.muted = color;
                }
            });

            return (theme, Some(path));
        }
    }

    (theme, None)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TermAbsSelection {
    anchor: (i64, u16), // (absolute_row, col)
    cursor: (i64, u16), // (absolute_row, col)
    dragging: bool,
}

impl TermAbsSelection {
    fn is_empty(&self) -> bool {
        self.anchor == self.cursor
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PendingRemoteClick {
    start_pos: Pos2,
    start_cell: (u16, u16), // (row, col) 0-based
}

struct RenamePopup {
    tile_id: TileId,
    value: String,
    just_opened: bool,
}

struct AuthDialog {
    tile_id: TileId,
    profile_name: Option<String>,
    instructions: String,
    prompts: Vec<ssh::AuthPromptItem>,
    responses: Vec<String>,
    just_opened: bool,
    remember_key_passphrase: bool,
}

struct HostKeyDialog {
    tile_id: TileId,
    prompt: ssh::HostKeyPrompt,
}

struct TransferDeleteDialog {
    request_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SettingsPage {
    Autostart,
    Behavior,
    Appearance,
    Updates,
    TerminalColors,
    ProfilesAndAccount,
}

impl SettingsPage {
    fn label(self) -> &'static str {
        match self {
            Self::Autostart => "Autostart",
            Self::Behavior => "Behavior",
            Self::Appearance => "Appearance",
            Self::Updates => "Updates",
            Self::TerminalColors => "Terminal Colors",
            Self::ProfilesAndAccount => "Profiles and Account",
        }
    }
}

struct SettingsDialog {
    open: bool,
    page: SettingsPage,
    target_tile: Option<TileId>,
    selected_profile: Option<usize>,
    profile_name: String,
    remember_password: bool,
    remember_key_passphrase: bool,
    draft: ConnectionSettings,
    just_opened: bool,
}

impl SettingsDialog {
    fn closed() -> Self {
        Self {
            open: false,
            page: SettingsPage::ProfilesAndAccount,
            target_tile: None,
            selected_profile: None,
            profile_name: String::new(),
            remember_password: false,
            remember_key_passphrase: false,
            draft: ConnectionSettings::default(),
            just_opened: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DownloadState {
    Queued,
    Running,
    Paused,
    Finished,
    Failed,
    Canceled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransferDirection {
    Download,
    Upload,
}

#[derive(Clone, Debug)]
struct DownloadJob {
    request_id: u64,
    direction: TransferDirection,
    settings: ConnectionSettings,
    remote_path: String,
    local_path: String,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
    speed_bps: f64,
    state: DownloadState,
    message: String,
}

#[derive(Clone, Debug)]
enum PaneKind {
    Terminal,
    FileManager(FileBrowserState),
}

#[derive(Clone, Debug)]
struct FileBrowserState {
    source_terminal: TileId,
    source_connected: bool,
    cwd: String,
    path_input: String,
    entries: Vec<ssh::SftpEntry>,
    selected_name: Option<String>,
    rename_to: String,
    rename_from: Option<String>,
    rename_dialog_open: bool,
    mkdir_name: String,
    mkdir_dialog_open: bool,
    busy: bool,
    status: String,
}

impl FileBrowserState {
    fn new(source_terminal: TileId, path: String) -> Self {
        let path = if path.trim().is_empty() {
            ".".to_string()
        } else {
            path
        };
        Self {
            source_terminal,
            source_connected: false,
            cwd: path.clone(),
            path_input: path,
            entries: Vec::new(),
            selected_name: None,
            rename_to: String::new(),
            rename_from: None,
            rename_dialog_open: false,
            mkdir_name: String::new(),
            mkdir_dialog_open: false,
            busy: false,
            status: "Not connected".to_string(),
        }
    }
}

struct SshTab {
    id: u64,
    title: String,
    user_title: Option<String>,
    color: Option<Color32>,
    profile_name: Option<String>,
    scrollback_len: usize,
    settings: ConnectionSettings,
    connected: bool,
    connecting: bool,
    last_status: String,

    screen: crate::terminal_emulator::Screen,
    scroll_wheel_accum: f32,
    scrollback_max: usize,
    scrollbar_dragging: bool,
    copy_flash_until: Option<Instant>,

    ui_rx: Option<Receiver<UiMessage>>,
    worker_tx: Option<Sender<WorkerMessage>>,
    host_key_tx: Option<Sender<ssh::HostKeyDecision>>,

    last_sent_size: Option<(u16, u16, u32, u32)>,
    pending_resize: Option<(u16, u16, u32, u32)>,
    focus_terminal_next_frame: bool,

    log_path: String,

    selection: Option<TermSelection>,
    abs_selection: Option<TermAbsSelection>,
    pending_remote_click: Option<PendingRemoteClick>,

    pending_auth: Option<ssh::AuthPrompt>,
    pending_host_key: Option<ssh::HostKeyPrompt>,
    pending_scrollback: Option<usize>,
    last_selection_autoscroll: Instant,
    pending_sftp_events: Vec<ssh::SftpEvent>,
    kind: PaneKind,
}

impl SshTab {
    fn new(
        id: u64,
        settings: ConnectionSettings,
        profile_name: Option<String>,
        scrollback_len: usize,
        log_path: String,
    ) -> Self {
        let len = scrollback_len.clamp(0, 200_000);
        let len = if len == 0 {
            ssh::TERM_SCROLLBACK_LEN
        } else {
            len
        };
        let screen = crate::terminal_emulator::Parser::new(24, 80, len).screen().clone();
        let title = Self::title_for(id, &settings);
        Self {
            id,
            title,
            user_title: None,
            color: None,
            profile_name,
            scrollback_len: len,
            settings,
            connected: false,
            connecting: false,
            last_status: String::new(),
            screen,
            scroll_wheel_accum: 0.0,
            scrollback_max: 0,
            scrollbar_dragging: false,
            copy_flash_until: None,
            ui_rx: None,
            worker_tx: None,
            host_key_tx: None,
            last_sent_size: None,
            pending_resize: None,
            focus_terminal_next_frame: false,
            log_path,
            selection: None,
            abs_selection: None,
            pending_remote_click: None,
            pending_auth: None,
            pending_host_key: None,
            pending_scrollback: None,
            last_selection_autoscroll: Instant::now(),
            pending_sftp_events: Vec::new(),
            kind: PaneKind::Terminal,
        }
    }

    fn new_file_manager(
        id: u64,
        settings: ConnectionSettings,
        profile_name: Option<String>,
        color: Option<Color32>,
        source_terminal: TileId,
        path: String,
    ) -> Self {
        let mut tab = Self::new(
            id,
            settings,
            profile_name,
            ssh::TERM_SCROLLBACK_LEN,
            format!("logs\\file-tab-{id}.log"),
        );
        let user = tab.settings.username.trim();
        let host = tab.settings.host.trim();
        let identity = match (user.is_empty(), host.is_empty()) {
            (false, false) => format!("{user}@{host}"),
            (false, true) => format!("{user}@new"),
            (true, false) => format!("ssh@{host}"),
            (true, true) => "session".to_string(),
        };
        tab.title = format!("SFTP: {identity}");
        tab.color = color;
        tab.last_status = "Not connected".to_string();
        tab.kind = PaneKind::FileManager(FileBrowserState::new(source_terminal, path));
        tab
    }

    fn is_terminal(&self) -> bool {
        matches!(self.kind, PaneKind::Terminal)
    }

    fn is_file_manager(&self) -> bool {
        matches!(self.kind, PaneKind::FileManager(_))
    }

    fn file_browser(&self) -> Option<&FileBrowserState> {
        match &self.kind {
            PaneKind::FileManager(f) => Some(f),
            PaneKind::Terminal => None,
        }
    }

    fn file_browser_mut(&mut self) -> Option<&mut FileBrowserState> {
        match &mut self.kind {
            PaneKind::FileManager(f) => Some(f),
            PaneKind::Terminal => None,
        }
    }

    fn title_for(id: u64, settings: &ConnectionSettings) -> String {
        let host = settings.host.trim();
        let user = settings.username.trim();
        let base = match (user.is_empty(), host.is_empty()) {
            (false, false) => format!("{user}@{host}"),
            (false, true) => format!("{user}@new"),
            (true, false) => format!("ssh@{host}"),
            (true, true) => "new tab".to_string(),
        };
        format!("{base} #{id}")
    }

    fn start_connect(&mut self) {
        if self.connected || self.connecting {
            return;
        }

        // Force a fresh PTY resize sync on (re)connect.
        self.last_sent_size = None;
        self.pending_resize = None;

        self.pending_auth = None;
        self.pending_host_key = None;

        let (ui_tx, ui_rx) = mpsc::channel::<UiMessage>();
        let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>();
        let (host_key_tx, host_key_rx) = mpsc::channel::<ssh::HostKeyDecision>();

        self.ui_rx = Some(ui_rx);
        self.worker_tx = Some(worker_tx);
        self.host_key_tx = Some(host_key_tx);
        self.connecting = true;
        self.last_status = "Connecting...".to_string();
        if self.is_terminal() {
            self.title = Self::title_for(self.id, &self.settings);
        }

        let settings = self.settings.clone();
        let scrollback_len = self.scrollback_len;
        let log_path = self.log_path.clone();
        let _handle = ssh::start_shell(
            settings,
            scrollback_len,
            ui_tx,
            worker_rx,
            host_key_rx,
            log_path,
        );
    }

    fn disconnect(&mut self) {
        if let Some(tx) = self.worker_tx.take() {
            let _ = tx.send(WorkerMessage::Disconnect);
        }
        self.ui_rx = None;
        self.host_key_tx = None;
        self.connected = false;
        self.connecting = false;
        self.last_status = "Disconnected".to_string();
        self.last_sent_size = None;
        self.pending_resize = None;
        self.scroll_wheel_accum = 0.0;
        self.scrollback_max = 0;
        self.scrollbar_dragging = false;
        self.copy_flash_until = None;
        self.selection = None;
        self.abs_selection = None;
        self.pending_remote_click = None;
        self.pending_auth = None;
        self.pending_host_key = None;
        self.pending_scrollback = None;
        self.last_selection_autoscroll = Instant::now();
    }

    fn poll_messages(&mut self) -> bool {
        let Some(rx) = self.ui_rx.as_ref() else {
            return false;
        };
        const MAX_MSGS_PER_FRAME: usize = 256;
        let mut processed = 0usize;
        let mut saw_message = false;
        let mut latest_screen: Option<Box<crate::terminal_emulator::Screen>> = None;
        let mut latest_scrollback_max: Option<usize> = None;
        loop {
            if processed >= MAX_MSGS_PER_FRAME {
                break;
            }
            match rx.try_recv() {
                Ok(UiMessage::Status(s)) => {
                    saw_message = true;
                    self.last_status = s;
                }
                Ok(UiMessage::Screen(screen)) => {
                    saw_message = true;
                    latest_screen = Some(screen);
                }
                Ok(UiMessage::ScrollbackMax(max)) => {
                    saw_message = true;
                    latest_scrollback_max = Some(max);
                }
                Ok(UiMessage::Connected(ok)) => {
                    saw_message = true;
                    self.connected = ok;
                    self.connecting = false;
                    if ok {
                        // A new shell starts at server-default PTY size (often 80x24).
                        // Reset cached size so the next frame always sends current viewport size.
                        self.last_sent_size = None;
                        self.pending_resize = None;
                        self.focus_terminal_next_frame = true;
                    }
                }
                Ok(UiMessage::AuthPrompt(p)) => {
                    saw_message = true;
                    self.pending_auth = Some(p);
                }
                Ok(UiMessage::HostKeyPrompt(p)) => {
                    saw_message = true;
                    self.pending_host_key = Some(p);
                }
                Ok(UiMessage::SftpEvent(event)) => {
                    saw_message = true;
                    self.pending_sftp_events.push(event);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    saw_message = true;
                    self.connected = false;
                    self.connecting = false;
                    break;
                }
            }
            processed += 1;
        }

        if let Some(screen) = latest_screen {
            self.screen = *screen;
            if let Some(target) = self.pending_scrollback {
                let clamped = target.min(self.scrollback_max);
                self.screen.set_scrollback(clamped);
                if self.screen.scrollback() == clamped {
                    self.pending_scrollback = None;
                }
            }
            if self.is_terminal() && !self.screen.title().is_empty() {
                // Prefer the remote title when set. Keep the id suffix to avoid
                // confusing duplicates when opening multiple tabs with the same host.
                self.title = format!("{} #{id}", self.screen.title(), id = self.id);
            }
        }

        if let Some(max) = latest_scrollback_max {
            self.scrollback_max = max;
            if let Some(target) = self.pending_scrollback {
                let clamped = target.min(max);
                if clamped != target {
                    self.pending_scrollback = Some(clamped);
                }
                self.screen.set_scrollback(clamped);
                if self.screen.scrollback() == clamped {
                    self.pending_scrollback = None;
                }
            }
        }

        saw_message
    }
}

pub struct AppState {
    theme: UiTheme,
    theme_source: Option<PathBuf>,
    term_theme: TermTheme,

    config: config::AppConfig,
    config_saver: AsyncConfigSaver,
    settings_dialog: SettingsDialog,

    tree: Tree<SshTab>,
    active_tile: Option<TileId>,
    next_session_id: u64,

    last_cursor_blink: Instant,
    last_terminal_activity: Instant,
    cursor_visible: bool,

    tray: Option<crate::tray::TrayState>,
    tray_events: crossbeam_channel::Receiver<crate::tray::TrayAppEvent>,
    hidden_to_tray: bool,
    minimize_to_tray_requested: bool,

    clipboard: Option<Clipboard>,
    rename_popup: Option<RenamePopup>,
    auth_dialog: Option<AuthDialog>,
    host_key_dialog: Option<HostKeyDialog>,
    transfer_delete_dialog: Option<TransferDeleteDialog>,

    style_initialized: bool,

    layout_dirty: bool,
    last_layout_save: Instant,
    restored_window: bool,
    next_sftp_request_id: u64,
    pending_sftp_requests: HashMap<u64, TileId>,
    downloads_window_open: bool,
    downloads_window_just_opened: bool,
    download_jobs: Vec<DownloadJob>,
    download_event_tx: Sender<ssh::DownloadManagerEvent>,
    download_event_rx: Receiver<ssh::DownloadManagerEvent>,
    download_cancel_txs: HashMap<u64, Sender<()>>,
    upload_refresh_targets: HashMap<u64, TileId>,
    update_check_in_progress: bool,
    update_check_rx: Option<Receiver<UpdateCheckResult>>,
    update_next_check_at: Instant,
    update_available_version: Option<String>,
    update_available_url: Option<String>,
    update_manual_open_if_newer: bool,
    update_manual_status: Option<String>,
}
