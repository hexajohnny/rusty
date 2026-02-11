use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};

use arboard::Clipboard;
use egui_tiles::{
    Behavior as TilesBehavior, Container, LinearDir, SimplificationOptions, Tile, TileId, Tiles,
    Tree, UiResponse,
};
use eframe::egui;
use eframe::egui::{
    text::{LayoutJob, TextFormat},
    Align, Color32, EventFilter, FontId, Id, Pos2, Rect, Response, Sense, Stroke, Vec2,
};

use crate::config;
use crate::model::ConnectionSettings;
use crate::ssh::{self, UiMessage, WorkerMessage};
use crate::async_config::AsyncConfigSaver;

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
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedSession {
    tree: Tree<PersistedTab>,
    active_tile: Option<TileId>,
}

const TERM_FONT_SIZE_DEFAULT: f32 = 16.0;
const TERM_FONT_SIZE_MIN: f32 = 10.0;
const TERM_FONT_SIZE_MAX: f32 = 28.0;
const TERM_PAD_X: f32 = 1.0;
const TERM_PAD_Y: f32 = 1.0;

const TITLE_BAR_H: f32 = 28.0;
const WINDOW_RADIUS: f32 = 14.0;
const TITLE_PAD_X: f32 = 10.0;
const CONTENT_PAD: f32 = 0.0;
const RESIZE_MARGIN: f32 = 6.0;

const APP_TITLE_TEXT: &str = concat!("Rusty SSH - v", env!("CARGO_PKG_VERSION"));

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
                Color32::from_rgb(0, 0, 0),         // 0 black
                Color32::from_rgb(205, 49, 49),     // 1 red
                Color32::from_rgb(13, 188, 121),    // 2 green
                Color32::from_rgb(229, 229, 16),    // 3 yellow
                Color32::from_rgb(36, 114, 200),    // 4 blue
                Color32::from_rgb(188, 63, 188),    // 5 magenta
                Color32::from_rgb(17, 168, 205),    // 6 cyan
                Color32::from_rgb(229, 229, 229),   // 7 white
                Color32::from_rgb(102, 102, 102),   // 8 bright black (gray)
                Color32::from_rgb(241, 76, 76),     // 9 bright red
                Color32::from_rgb(35, 209, 139),    // 10 bright green
                Color32::from_rgb(245, 245, 67),    // 11 bright yellow
                Color32::from_rgb(59, 142, 234),    // 12 bright blue
                Color32::from_rgb(214, 112, 214),   // 13 bright magenta
                Color32::from_rgb(41, 184, 219),    // 14 bright cyan
                Color32::from_rgb(255, 255, 255),   // 15 bright white
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SettingsPage {
    Autostart,
    Appearance,
    TerminalColors,
    ProfilesAndAccount,
}

impl SettingsPage {
    fn label(self) -> &'static str {
        match self {
            Self::Autostart => "Autostart",
            Self::Appearance => "Appearance",
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

    screen: vt100::Screen,
    scroll_wheel_accum: f32,
    scrollback_max: usize,
    scrollbar_dragging: bool,
    copy_flash_until: Option<Instant>,

    ui_rx: Option<Receiver<UiMessage>>,
    worker_tx: Option<Sender<WorkerMessage>>,

    last_sent_size: Option<(u16, u16, u32, u32)>,
    pending_resize: Option<(u16, u16, u32, u32)>,
    focus_terminal_next_frame: bool,

    log_path: String,

    selection: Option<TermSelection>,
    pending_remote_click: Option<PendingRemoteClick>,

    pending_auth: Option<ssh::AuthPrompt>,
    pending_scrollback: Option<usize>,
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
        let len = if len == 0 { ssh::TERM_SCROLLBACK_LEN } else { len };
        let screen = vt100::Parser::new(24, 80, len).screen().clone();
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
            last_sent_size: None,
            pending_resize: None,
            focus_terminal_next_frame: false,
            log_path,
            selection: None,
            pending_remote_click: None,
            pending_auth: None,
            pending_scrollback: None,
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

        self.pending_auth = None;

        let (ui_tx, ui_rx) = mpsc::channel::<UiMessage>();
        let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>();

        self.ui_rx = Some(ui_rx);
        self.worker_tx = Some(worker_tx);
        self.connecting = true;
        self.last_status = "Connecting...".to_string();
        self.title = Self::title_for(self.id, &self.settings);

        let settings = self.settings.clone();
        let scrollback_len = self.scrollback_len;
        let log_path = self.log_path.clone();
        let _handle = ssh::start_shell(settings, scrollback_len, ui_tx, worker_rx, log_path);
    }

    fn disconnect(&mut self) {
        if let Some(tx) = self.worker_tx.take() {
            let _ = tx.send(WorkerMessage::Disconnect);
        }
        self.ui_rx = None;
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
        self.pending_remote_click = None;
        self.pending_auth = None;
        self.pending_scrollback = None;
    }

    fn poll_messages(&mut self) {
        let Some(rx) = self.ui_rx.as_ref() else { return };
        const MAX_MSGS_PER_FRAME: usize = 256;
        let mut processed = 0usize;
        let mut latest_screen: Option<Box<vt100::Screen>> = None;
        let mut latest_scrollback_max: Option<usize> = None;
        loop {
            if processed >= MAX_MSGS_PER_FRAME {
                break;
            }
            match rx.try_recv() {
                Ok(UiMessage::Status(s)) => {
                    self.last_status = s;
                }
                Ok(UiMessage::Screen(screen)) => {
                    latest_screen = Some(screen);
                }
                Ok(UiMessage::ScrollbackMax(max)) => {
                    latest_scrollback_max = Some(max);
                }
                Ok(UiMessage::Connected(ok)) => {
                    self.connected = ok;
                    self.connecting = false;
                    if ok {
                        self.focus_terminal_next_frame = true;
                    }
                }
                Ok(UiMessage::AuthPrompt(p)) => {
                    self.pending_auth = Some(p);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.connected = false;
                    self.connecting = false;
                    break;
                }
            }
            processed += 1;
        }

        if let Some(screen) = latest_screen {
            self.screen = *screen;
            if !self.screen.title().is_empty() {
                // Prefer the remote title when set. Keep the id suffix to avoid
                // confusing duplicates when opening multiple tabs with the same host.
                self.title = format!("{} #{id}", self.screen.title(), id = self.id);
            }
        }

        if let Some(max) = latest_scrollback_max {
            self.scrollback_max = max;
        }
    }
}

pub struct AppState {
    theme: UiTheme,
    term_theme: TermTheme,

    config: config::AppConfig,
    config_saver: AsyncConfigSaver,
    settings_dialog: SettingsDialog,

    tree: Tree<SshTab>,
    active_tile: Option<TileId>,
    next_session_id: u64,

    last_cursor_blink: Instant,
    cursor_visible: bool,

    tray: Option<crate::tray::TrayState>,
    tray_events: crossbeam_channel::Receiver<crate::tray::TrayAppEvent>,
    hidden_to_tray: bool,
    minimize_to_tray_requested: bool,

    clipboard: Option<Clipboard>,
    rename_popup: Option<RenamePopup>,
    auth_dialog: Option<AuthDialog>,

    style_initialized: bool,

    layout_dirty: bool,
    last_layout_save: Instant,
    restored_window: bool,
}

