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
    }

    fn poll_messages(&mut self) {
        let Some(rx) = self.ui_rx.as_ref() else { return };
        loop {
            match rx.try_recv() {
                Ok(UiMessage::Status(s)) => {
                    self.last_status = s;
                }
                Ok(UiMessage::Screen(screen)) => {
                    self.screen = screen;
                    if !self.screen.title().is_empty() {
                        // Prefer the remote title when set. Keep the id suffix to avoid
                        // confusing duplicates when opening multiple tabs with the same host.
                        self.title = format!("{} #{id}", self.screen.title(), id = self.id);
                    }
                }
                Ok(UiMessage::ScrollbackMax(max)) => {
                    self.scrollback_max = max;
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
        }
    }
}

pub struct AppState {
    theme: UiTheme,
    term_theme: TermTheme,

    config: config::AppConfig,
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
}

impl AppState {
    pub fn new() -> Self {
        let _ = std::fs::create_dir_all("logs");
        let mut config = config::load();

        // Resolve the default profile index (if any). If the saved default no longer exists,
        // clear it so we don't get stuck on startup behavior.
        let mut default_profile_idx: Option<usize> = None;
        if let Some(name) = config.default_profile.clone() {
            default_profile_idx = config::find_profile_index(&config, &name);
            if default_profile_idx.is_none() {
                config.default_profile = None;
                config::save(&config);
            }
        }

        let mut settings_dialog = SettingsDialog::closed();
        let mut initial_settings = ConnectionSettings::default();
        let mut initial_profile_name: Option<String> = None;
        if let Some(idx) = default_profile_idx {
            let p = config.profiles[idx].clone();
            let s = config::write_profile_settings(&p);
            initial_settings = s.clone();
            initial_profile_name = Some(p.name.clone());
            settings_dialog.selected_profile = Some(idx);
            settings_dialog.profile_name = p.name;
            settings_dialog.remember_password = p.remember_password;
            settings_dialog.remember_key_passphrase = p.remember_key_passphrase;
            settings_dialog.draft = s;
        } else {
            settings_dialog.draft = initial_settings.clone();
        }

        let mut next_session_id = 1u64;
        let mut first = SshTab::new(
            next_session_id,
            initial_settings,
            initial_profile_name,
            config.terminal_scrollback_lines,
            format!("logs\\tab-{next_session_id}.log"),
        );
        next_session_id += 1;

        let autostart_ok = config.autostart
            && default_profile_idx.is_some()
            && !settings_dialog.draft.host.trim().is_empty()
            && !settings_dialog.draft.username.trim().is_empty();
        if autostart_ok {
            first.start_connect();
            settings_dialog.open = false;
        } else {
            settings_dialog.open = true;
            settings_dialog.just_opened = true;
        }

        let mut tiles = egui_tiles::Tiles::default();
        let first_tile_id = tiles.insert_pane(first);
        let root = tiles.insert_tab_tile(vec![first_tile_id]);
        let tree = Tree::new("ssh_tree", root, tiles);
        settings_dialog.target_tile = Some(first_tile_id);

        Self {
            theme: UiTheme::default(),
            term_theme: TermTheme::from_config(&config.terminal_colors),
            config,
            settings_dialog,
            tree,
            active_tile: Some(first_tile_id),
            next_session_id,
            last_cursor_blink: Instant::now(),
            cursor_visible: true,
            tray: None,
            tray_events: crate::tray::install_handlers(),
            hidden_to_tray: false,
            minimize_to_tray_requested: false,
            clipboard: Clipboard::new().ok(),
            rename_popup: None,
            auth_dialog: None,
            style_initialized: false,
        }
    }

    fn apply_global_style(&self, ctx: &egui::Context) {
        // Ensure default widget visuals and window title bars are readable against our dark theme.
        let mut style = (*ctx.style()).clone();
        style.visuals = egui::Visuals::dark();
        style.visuals.override_text_color = Some(self.theme.fg);
        style.visuals.panel_fill = self.theme.bg;
        style.visuals.window_fill = adjust_color(self.theme.top_bg, 0.06);
        style.visuals.window_stroke = Stroke::new(1.0, self.theme.top_border);
        style.visuals.faint_bg_color = adjust_color(self.theme.top_bg, 0.04);
        style.visuals.extreme_bg_color = self.theme.bg;
        style.visuals.hyperlink_color = self.theme.accent;
        style.visuals.selection.bg_fill = self.theme.accent;
        style.visuals.selection.stroke = Stroke::new(1.0, contrast_text_color(self.theme.accent));

        let w = &mut style.visuals.widgets;
        w.noninteractive.bg_fill = self.theme.bg;
        w.noninteractive.fg_stroke.color = self.theme.fg;
        w.inactive.bg_fill = adjust_color(self.theme.top_bg, 0.08);
        w.inactive.fg_stroke.color = self.theme.fg;
        w.inactive.bg_stroke = Stroke::new(1.0, self.theme.top_border);
        w.hovered.bg_fill = adjust_color(self.theme.top_bg, 0.14);
        w.hovered.fg_stroke.color = self.theme.fg;
        w.hovered.bg_stroke = Stroke::new(1.0, self.theme.accent);
        w.active.bg_fill = adjust_color(self.theme.top_bg, 0.18);
        w.active.fg_stroke.color = self.theme.fg;
        w.active.bg_stroke = Stroke::new(1.0, self.theme.accent);

        style
            .text_styles
            .insert(egui::TextStyle::Heading, FontId::proportional(18.0));
        style
            .text_styles
            .insert(egui::TextStyle::Button, FontId::proportional(14.0));
        style
            .text_styles
            .insert(egui::TextStyle::Body, FontId::proportional(14.0));

        ctx.set_style(style);
    }

    fn pane_ids(&self) -> Vec<TileId> {
        self.tree
            .tiles
            .iter()
            .filter_map(|(id, tile)| match tile {
                Tile::Pane(_) => Some(*id),
                _ => None,
            })
            .collect()
    }

    fn first_pane_id(&self) -> Option<TileId> {
        self.tree.tiles.iter().find_map(|(id, tile)| match tile {
            Tile::Pane(_) => Some(*id),
            _ => None,
        })
    }

    fn pane(&self, tile_id: TileId) -> Option<&SshTab> {
        match self.tree.tiles.get(tile_id) {
            Some(Tile::Pane(pane)) => Some(pane),
            _ => None,
        }
    }

    fn pane_mut(&mut self, tile_id: TileId) -> Option<&mut SshTab> {
        match self.tree.tiles.get_mut(tile_id) {
            Some(Tile::Pane(pane)) => Some(pane),
            _ => None,
        }
    }

    fn ensure_tree_non_empty(&mut self) {
        if self.tree.root.is_some() && self.first_pane_id().is_some() {
            return;
        }

        let pane_id = self.create_pane(ConnectionSettings::default(), None, None, self.config.terminal_scrollback_lines);
        let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
        self.tree.root = Some(root);
        self.active_tile = Some(pane_id);
        self.settings_dialog.target_tile = Some(pane_id);
    }

    fn create_pane(
        &mut self,
        settings: ConnectionSettings,
        color: Option<Color32>,
        profile_name: Option<String>,
        scrollback_len: usize,
    ) -> TileId {
        let id = self.next_session_id;
        self.next_session_id += 1;

        let mut tab = SshTab::new(
            id,
            settings,
            profile_name,
            scrollback_len,
            format!("logs\\tab-{id}.log"),
        );
        tab.color = color;
        tab.focus_terminal_next_frame = true;
        if !tab.settings.host.trim().is_empty() && !tab.settings.username.trim().is_empty() {
            tab.start_connect();
        }

        self.tree.tiles.insert_pane(tab)
    }

    fn set_focus_next_frame(&mut self, tile_id: TileId) {
        if let Some(pane) = self.pane_mut(tile_id) {
            pane.focus_terminal_next_frame = true;
        }
    }

    fn add_new_pane_to_tabs(
        &mut self,
        tabs_container_id: TileId,
        base_pane_id: Option<TileId>,
    ) -> Option<TileId> {
        let (settings, color, profile_name, scrollback_len) = base_pane_id
            .and_then(|id| {
                self.pane(id).map(|p| {
                    (p.settings.clone(), p.color, p.profile_name.clone(), p.scrollback_len)
                })
            })
            .unwrap_or((ConnectionSettings::default(), None, None, self.config.terminal_scrollback_lines));

        let pane_id = self.create_pane(settings, color, profile_name, scrollback_len);

        if let Some(Tile::Container(Container::Tabs(tabs))) =
            self.tree.tiles.get_mut(tabs_container_id)
        {
            tabs.children.push(pane_id);
            tabs.set_active(pane_id);
        } else {
            // Fallback: replace the whole root with a tabs container.
            let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
            self.tree.root = Some(root);
        }

        self.active_tile = Some(pane_id);
        self.settings_dialog.target_tile = Some(pane_id);
        Some(pane_id)
    }

    fn ensure_tray_icon(&mut self) {
        if !self.config.minimize_to_tray {
            self.tray = None;
            self.hidden_to_tray = false;
            return;
        }

        if self.tray.is_none() {
            self.tray = crate::tray::create_tray().ok();
            if let Some(tray) = self.tray.as_ref() {
                // Start with a "Hide" label when visible.
                tray.show_hide_item.set_text("Hide Rusty");
            }
        }
    }

    fn hide_to_tray(&mut self, ctx: &egui::Context) {
        self.ensure_tray_icon();
        if self.tray.is_none() {
            // If tray creation failed, fall back to a normal minimize.
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
            return;
        }

        ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
        self.hidden_to_tray = true;
        if let Some(tray) = self.tray.as_ref() {
            tray.show_hide_item.set_text("Show Rusty");
        }
        // Keep the app responsive to tray events even while hidden.
        ctx.request_repaint_after(Duration::from_millis(200));
    }

    fn show_from_tray(&mut self, ctx: &egui::Context) {
        ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
        ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
        self.hidden_to_tray = false;

        // Force a fresh PTY resize on restore, in case the window was hidden/minimized with a
        // transient tiny size and the terminal would otherwise stay "blank" until output arrives.
        for id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(id) {
                tab.last_sent_size = None;
            }
        }
        ctx.request_repaint();

        if let Some(tray) = self.tray.as_ref() {
            tray.show_hide_item.set_text("Hide Rusty");
        }
    }

    fn handle_tray_events(&mut self, ctx: &egui::Context) {
        while let Ok(ev) = self.tray_events.try_recv() {
            match ev {
                crate::tray::TrayAppEvent::Menu(id) => {
                    let Some(tray) = self.tray.as_ref() else { continue };
                    if id == tray.show_hide_id {
                        if self.hidden_to_tray {
                            self.show_from_tray(ctx);
                        } else {
                            self.hide_to_tray(ctx);
                        }
                    } else if id == tray.exit_id {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                }
                crate::tray::TrayAppEvent::Tray(te) => {
                    if let tray_icon::TrayIconEvent::DoubleClick { button, .. } = te {
                        if button == tray_icon::MouseButton::Left {
                            if self.hidden_to_tray {
                                self.show_from_tray(ctx);
                            } else if self.config.minimize_to_tray {
                                self.hide_to_tray(ctx);
                            }
                        }
                    }
                }
            }
        }

        if self.hidden_to_tray {
            ctx.request_repaint_after(Duration::from_millis(200));
        }
    }

    fn add_new_pane_to_tabs_with_settings(
        &mut self,
        tabs_container_id: TileId,
        settings: ConnectionSettings,
        color: Option<Color32>,
        profile_name: Option<String>,
    ) -> Option<TileId> {
        let pane_id = self.create_pane(settings, color, profile_name, self.config.terminal_scrollback_lines);

        if let Some(Tile::Container(Container::Tabs(tabs))) =
            self.tree.tiles.get_mut(tabs_container_id)
        {
            tabs.children.push(pane_id);
            tabs.set_active(pane_id);
        } else {
            // Fallback: replace the whole root with a tabs container.
            let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
            self.tree.root = Some(root);
        }

        self.active_tile = Some(pane_id);
        self.settings_dialog.target_tile = Some(pane_id);

        // If the profile is incomplete (e.g. missing username), prompt immediately instead of
        // leaving the user at "Not connected" with no next step.
        let needs_settings = self
            .pane(pane_id)
            .map(|t| t.settings.host.trim().is_empty() || t.settings.username.trim().is_empty())
            .unwrap_or(true);
        if needs_settings {
            self.open_settings_dialog_for_tile(pane_id);
            self.settings_dialog.page = SettingsPage::ProfilesAndAccount;
        }
        Some(pane_id)
    }

    fn close_pane(&mut self, pane_id: TileId) {
        if let Some(pane) = self.pane_mut(pane_id) {
            pane.disconnect();
        }
        self.tree.remove_recursively(pane_id);

        if self.first_pane_id().is_none() {
            self.ensure_tree_non_empty();
            return;
        }

        if self.active_tile == Some(pane_id) {
            self.active_tile = self.first_pane_id();
        }
        if self.settings_dialog.target_tile == Some(pane_id) {
            self.settings_dialog.target_tile = self.active_tile;
        }
    }

    fn replace_child_in_container(
        container: &mut Container,
        old_child: TileId,
        new_child: TileId,
    ) {
        // Preserve ordering/position in the parent container.
        let idx = container.remove_child(old_child);
        match container {
            Container::Tabs(tabs) => {
                if let Some(i) = idx {
                    tabs.children.insert(i.min(tabs.children.len()), new_child);
                    if tabs.active == Some(old_child) || tabs.active.is_none() {
                        tabs.active = Some(new_child);
                    }
                } else {
                    tabs.children.push(new_child);
                }
            }
            Container::Linear(linear) => {
                if let Some(i) = idx {
                    linear.children.insert(i.min(linear.children.len()), new_child);
                } else {
                    linear.children.push(new_child);
                }
                linear.shares.replace_with(old_child, new_child);
            }
            Container::Grid(grid) => {
                if let Some(i) = idx {
                    let _ = grid.replace_at(i, new_child);
                } else {
                    grid.add_child(new_child);
                }
            }
        }
    }

    fn split_pane(&mut self, pane_id: TileId, dir: LinearDir) -> Option<TileId> {
        let (settings, color, profile_name, scrollback_len) = self
            .pane(pane_id)
            .map(|p| (p.settings.clone(), p.color, p.profile_name.clone(), p.scrollback_len))
            .unwrap_or((ConnectionSettings::default(), None, None, self.config.terminal_scrollback_lines));

        let Some(tabs_container_id) = self.tree.tiles.parent_of(pane_id) else {
            return None;
        };
        let parent_of_tabs = self.tree.tiles.parent_of(tabs_container_id);

        let new_pane_id = self.create_pane(settings, color, profile_name, scrollback_len);
        let new_tabs_id = self.tree.tiles.insert_tab_tile(vec![new_pane_id]);
        let new_linear_id = match dir {
            LinearDir::Horizontal => self
                .tree
                .tiles
                .insert_horizontal_tile(vec![tabs_container_id, new_tabs_id]),
            LinearDir::Vertical => self
                .tree
                .tiles
                .insert_vertical_tile(vec![tabs_container_id, new_tabs_id]),
        };

        if let Some(parent) = parent_of_tabs {
            if let Some(Tile::Container(container)) = self.tree.tiles.get_mut(parent) {
                Self::replace_child_in_container(container, tabs_container_id, new_linear_id);
            } else {
                self.tree.root = Some(new_linear_id);
            }
        } else {
            self.tree.root = Some(new_linear_id);
        }

        self.active_tile = Some(new_pane_id);
        self.settings_dialog.target_tile = Some(new_pane_id);
        Some(new_pane_id)
    }

    fn cell_metrics(ctx: &egui::Context, font_id: &FontId) -> (f32, f32) {
        let ppp = ctx.pixels_per_point();
        ctx.fonts(|fonts| {
            // Derive metrics from actual layout to better match pixel snapping in the text renderer.
            let sample = "WWWWWWWWWWWWWWWW";
            let galley = fonts.layout_no_wrap(sample.to_string(), font_id.clone(), Color32::WHITE);
            let mut w = (galley.size().x / sample.len() as f32).max(1.0);
            let mut h = galley.size().y.max(1.0);

            // Snap to physical pixels to keep the grid stable and align overlays.
            let w_px = (w * ppp).round().max(1.0);
            let h_px = (h * ppp).round().max(1.0);
            w = w_px / ppp;
            h = h_px / ppp;
            (w, h)
        })
    }

    fn vt_color_to_color32(c: vt100::Color, default: Color32, term_theme: &TermTheme) -> Color32 {
        match c {
            vt100::Color::Default => default,
            vt100::Color::Rgb(r, g, b) => Color32::from_rgb(r, g, b),
            vt100::Color::Idx(i) => xterm_256_color(i, &term_theme.palette16),
        }
    }

    fn cell_style(cell: &vt100::Cell, term_theme: &TermTheme) -> TermStyle {
        let mut fg = Self::vt_color_to_color32(cell.fgcolor(), term_theme.fg, term_theme);
        let mut bg = Self::vt_color_to_color32(cell.bgcolor(), term_theme.bg, term_theme);

        // Common terminal behavior: bold maps to bright variants for the first 8 colors.
        if cell.bold() {
            if let vt100::Color::Idx(i) = cell.fgcolor() {
                if i < 8 {
                    fg = xterm_256_color(i + 8, &term_theme.palette16);
                }
            }
        }

        let inverse = cell.inverse();
        if inverse {
            std::mem::swap(&mut fg, &mut bg);
        }

        // Some TUIs rely on SGR 2 (faint/dim) for secondary text. The vt100 state carries this,
        // but egui has no "dim" attribute, so we simulate it by blending the foreground toward
        // the cell background.
        if cell.dim() {
            fg = lerp_color(fg, bg, term_theme.dim_blend);
        }

        TermStyle {
            fg,
            bg,
            italic: cell.italic(),
            underline: cell.underline(),
            inverse,
        }
    }

    fn screen_to_layout_job(
        screen: &vt100::Screen,
        font_id: FontId,
        term_theme: &TermTheme,
    ) -> LayoutJob {
        let (rows, cols) = screen.size();
        let mut job = LayoutJob::default();
        // NOTE: TextWrapping::max_rows == 0 means "render nothing". Keep the defaults,
        // only ensuring we don't wrap within rows (newlines still break rows).
        job.wrap.max_width = f32::INFINITY;
        job.wrap.max_rows = usize::MAX;
        job.wrap.break_anywhere = false;

        let mut current_style: Option<TermStyle> = None;
        let mut run = String::new();

        for row in 0..rows {
            for col in 0..cols {
                let cell = match screen.cell(row, col) {
                    Some(c) => c,
                    None => continue,
                };

                let style = Self::cell_style(cell, term_theme);
                if current_style.map(|s| s != style).unwrap_or(true) {
                    if let Some(s) = current_style.take() {
                        if !run.is_empty() {
                            job.append(&run, 0.0, s.to_text_format(font_id.clone()));
                            run.clear();
                        }
                        current_style = Some(style);
                    } else {
                        current_style = Some(style);
                    }
                }

                // Wide characters occupy two cells. Render the continuation cell as a space
                // to preserve monospace alignment.
                if cell.is_wide_continuation() {
                    run.push(' ');
                } else if cell.has_contents() {
                    run.push_str(&cell.contents());
                } else {
                    run.push(' ');
                }
            }

            if row + 1 < rows {
                run.push('\n');
            }
        }

        if let Some(s) = current_style {
            if !run.is_empty() {
                job.append(&run, 0.0, s.to_text_format(font_id));
            }
        }

        job
    }

    fn send_bytes(tab: &mut SshTab, bytes: Vec<u8>) {
        if let Some(tx) = tab.worker_tx.as_ref() {
            let _ = tx.send(WorkerMessage::Input(bytes));
        }
    }

    fn set_scrollback(tab: &mut SshTab, rows: usize) {
        if let Some(tx) = tab.worker_tx.as_ref() {
            let _ = tx.send(WorkerMessage::SetScrollback(rows));
        }
    }

    fn send_paste_text(tab: &mut SshTab, s: &str) {
        if s.is_empty() {
            return;
        }
        let mut bytes = Vec::new();
        if tab.screen.bracketed_paste() {
            bytes.extend_from_slice(b"\x1b[200~");
            bytes.extend_from_slice(s.as_bytes());
            bytes.extend_from_slice(b"\x1b[201~");
        } else {
            bytes.extend_from_slice(s.as_bytes());
        }
        Self::send_bytes(tab, bytes);
    }

    fn copy_text_to_clipboard(ctx: &egui::Context, clipboard: &mut Option<Clipboard>, text: String) {
        ctx.output_mut(|o| o.copied_text = text.clone());
        if let Some(cb) = clipboard.as_mut() {
            let _ = cb.set_text(text);
        }
    }

    fn copy_selection_with_flash(
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        tab: &mut SshTab,
        text: String,
    ) {
        if text.is_empty() {
            return;
        }
        Self::copy_text_to_clipboard(ctx, clipboard, text);
        tab.copy_flash_until = Some(Instant::now() + Duration::from_millis(150));
    }

    fn key_to_ctrl_byte(key: egui::Key) -> Option<u8> {
        use egui::Key::*;
        let c = match key {
            A => b'a',
            B => b'b',
            C => b'c',
            D => b'd',
            E => b'e',
            F => b'f',
            G => b'g',
            H => b'h',
            I => b'i',
            J => b'j',
            K => b'k',
            L => b'l',
            M => b'm',
            N => b'n',
            O => b'o',
            P => b'p',
            Q => b'q',
            R => b'r',
            S => b's',
            T => b't',
            U => b'u',
            V => b'v',
            W => b'w',
            X => b'x',
            Y => b'y',
            Z => b'z',
            _ => return None,
        };
        Some(c & 0x1f)
    }

    fn send_key(tab: &mut SshTab, key: egui::Key, mods: egui::Modifiers) {
        // Terminal-style copy shortcut that doesn't collide with SIGINT.
        if mods.ctrl && mods.shift && key == egui::Key::C {
            return;
        }

        if mods.ctrl {
            if let Some(b) = Self::key_to_ctrl_byte(key) {
                Self::send_bytes(tab, vec![b]);
                return;
            }
        }

        let app_cursor = tab.screen.application_cursor();
        let bytes: Option<&'static [u8]> = match key {
            egui::Key::Enter => Some(b"\r"),
            egui::Key::Tab => Some(b"\t"),
            egui::Key::Backspace => Some(&[0x7f]),
            egui::Key::Escape => Some(&[0x1b]),
            egui::Key::ArrowUp => Some(if app_cursor { b"\x1bOA" } else { b"\x1b[A" }),
            egui::Key::ArrowDown => Some(if app_cursor { b"\x1bOB" } else { b"\x1b[B" }),
            egui::Key::ArrowRight => Some(if app_cursor { b"\x1bOC" } else { b"\x1b[C" }),
            egui::Key::ArrowLeft => Some(if app_cursor { b"\x1bOD" } else { b"\x1b[D" }),
            egui::Key::Home => Some(if app_cursor { b"\x1bOH" } else { b"\x1b[H" }),
            egui::Key::End => Some(if app_cursor { b"\x1bOF" } else { b"\x1b[F" }),
            egui::Key::PageUp => Some(b"\x1b[5~"),
            egui::Key::PageDown => Some(b"\x1b[6~"),
            egui::Key::Insert => Some(b"\x1b[2~"),
            egui::Key::Delete => Some(b"\x1b[3~"),
            _ => None,
        };

        if let Some(b) = bytes {
            Self::send_bytes(tab, b.to_vec());
        }
    }

    fn mouse_event_bytes(
        encoding: vt100::MouseProtocolEncoding,
        mode: vt100::MouseProtocolMode,
        pressed: bool,
        button: egui::PointerButton,
        col_1: u16,
        row_1: u16,
    ) -> Option<Vec<u8>> {
        if mode == vt100::MouseProtocolMode::None {
            return None;
        }

        let btn_code = match button {
            egui::PointerButton::Primary => 0u8,
            egui::PointerButton::Middle => 1u8,
            egui::PointerButton::Secondary => 2u8,
            _ => return None,
        };

        // In press-only mode, ignore releases.
        if mode == vt100::MouseProtocolMode::Press && !pressed {
            return None;
        }

        match encoding {
            vt100::MouseProtocolEncoding::Sgr => {
                let suffix = if pressed { b'M' } else { b'm' };
                let s = format!("\x1b[<{};{};{}{}", btn_code, col_1, row_1, suffix as char);
                Some(s.into_bytes())
            }
            _ => {
                // Default encoding: CSI M Cb Cx Cy. Release is encoded with Cb=3.
                let cb = 32u8 + if pressed { btn_code } else { 3u8 };
                let cx = 32u8.saturating_add(col_1.min(223) as u8);
                let cy = 32u8.saturating_add(row_1.min(223) as u8);
                Some(vec![0x1b, b'[', b'M', cb, cx, cy])
            }
        }
    }

    fn handle_terminal_io(
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        ui: &mut egui::Ui,
        tab: &mut SshTab,
        term_rect: Rect,
        origin: Pos2,
        cell_w: f32,
        cell_h: f32,
        galley: Option<&egui::Galley>,
        response: &egui::Response,
    ) {
        let events = ui.input(|i| i.events.clone());
        let global_mods = ui.input(|i| i.modifiers);
        let has_copy_event = events.iter().any(|e| matches!(e, egui::Event::Copy));
        let has_paste_event = events.iter().any(|e| matches!(e, egui::Event::Paste(_)));
        let has_text_event = events
            .iter()
            .any(|e| matches!(e, egui::Event::Text(t) if !t.is_empty()));

        let (screen_rows, screen_cols) = tab.screen.size();
        let remote_mouse_enabled =
            tab.connected && tab.screen.mouse_protocol_mode() != vt100::MouseProtocolMode::None;
        let local_select_enabled = !remote_mouse_enabled || global_mods.shift;
        let allow_remote_mouse = remote_mouse_enabled && !global_mods.shift;

        // Selection and remote mouse clicks:
        // - If the remote enabled mouse reporting, clicking interacts with the remote.
        // - Click-drag selects text locally (so you can still copy output).
        //
        // We intentionally avoid relying on `egui::Event::PointerMoved` here, because pointer move
        // events are less reliable across nested UI layouts. Instead we use pointer state and the
        // widget `Response` to keep selection responsive.
        let pointer_pos = response
            .interact_pointer_pos()
            .or_else(|| ui.input(|i| i.pointer.latest_pos()));
        let primary_pressed = ui.input(|i| i.pointer.primary_pressed());
        let primary_down = ui.input(|i| i.pointer.primary_down());
        let primary_released = ui.input(|i| i.pointer.primary_released());
        let hovering_term = pointer_pos.map(|pos| term_rect.contains(pos)).unwrap_or(false) || response.hovered();

        // Scrollbar interaction (hover-only; click-drag to scroll).
        // We keep this independent from "remote mouse mode" so you can always scroll locally.
        // Wider hit area so it's easy to click-drag.
        let scrollbar_w = 16.0;
        let scrollbar_rect = Rect::from_min_max(
            Pos2::new(term_rect.right() - scrollbar_w, term_rect.top()),
            Pos2::new(term_rect.right(), term_rect.bottom()),
        );
        let hovering_scrollbar = pointer_pos
            .map(|p| scrollbar_rect.contains(p))
            .unwrap_or(false);
        if primary_pressed && hovering_scrollbar && tab.connected && tab.scrollback_max > 0 {
            tab.scrollbar_dragging = true;
            response.request_focus();
        }
        if primary_released {
            tab.scrollbar_dragging = false;
        }
        if tab.scrollbar_dragging && primary_down && tab.connected && tab.scrollback_max > 0 {
            if let Some(pos) = pointer_pos {
                // Map pointer Y to scrollback offset.
                let visible_rows = tab.screen.size().0 as f32;
                let total_rows = visible_rows + tab.scrollback_max as f32;
                let track_h = term_rect.height().max(1.0);
                let handle_h = (track_h * (visible_rows / total_rows))
                    .clamp(18.0, track_h);
                let track_min = term_rect.top();
                let track_max = term_rect.bottom() - handle_h;
                let y = pos.y.clamp(track_min, track_max.max(track_min));
                let t = if track_max > track_min {
                    (y - track_min) / (track_max - track_min)
                } else {
                    0.0
                };
                // t=0 => top (max scrollback), t=1 => bottom (0 scrollback)
                let max = tab.scrollback_max as f32;
                let desired = ((1.0 - t) * max).round().clamp(0.0, max) as usize;
                if desired != tab.screen.scrollback() {
                    Self::set_scrollback(tab, desired);
                }
            }
        }

        // Local scrollback (mouse wheel / trackpad). This is independent of any remote app state.
        if hovering_term && tab.connected {
            // Use egui's aggregated deltas (points). Some backends don't always generate `Event::Scroll`.
            let mut dy = ui.input(|i| i.raw_scroll_delta.y);
            if dy == 0.0 {
                dy = ui.input(|i| i.smooth_scroll_delta.y);
            }
            if dy == 0.0 {
                for ev in events.iter() {
                    if let egui::Event::Scroll(delta) = ev {
                        dy += delta.y;
                    }
                }
            }

            if dy != 0.0 {
                // Accumulate into rows and apply integer deltas.
                let step = cell_h.max(1.0);
                tab.scroll_wheel_accum += dy / step;
                let rows_delta = tab.scroll_wheel_accum.trunc() as i32;
                if rows_delta != 0 {
                    tab.scroll_wheel_accum -= rows_delta as f32;
                    let cur = tab.screen.scrollback() as i32;
                    let next = (cur + rows_delta).max(0) as usize;
                    Self::set_scrollback(tab, next);
                }
            }
        }

        // Clamp to the text grid area (not the outer padding) so selections still work if
        // you start dragging inside the padding.
        let grid_min = origin;
        let mut grid_max = Pos2::new(term_rect.right() - TERM_PAD_X, term_rect.bottom() - TERM_PAD_Y);
        grid_max.x = grid_max.x.max(grid_min.x + 1.0);
        grid_max.y = grid_max.y.max(grid_min.y + 1.0);
        let clamp_pos_to_grid = |p: Pos2| -> Pos2 {
            let x = p.x.clamp(grid_min.x, grid_max.x - 0.001);
            let y = p.y.clamp(grid_min.y, grid_max.y - 0.001);
            Pos2::new(x, y)
        };

        if primary_pressed {
            if let Some(pos) = pointer_pos {
                if term_rect.contains(pos) {
                    response.request_focus();
                    let pos = clamp_pos_to_grid(pos);

                    if local_select_enabled {
                        tab.pending_remote_click = None;
                        if let Some((row, col)) = Self::pos_to_cell(
                            pos,
                            origin,
                            cell_w,
                            cell_h,
                            &tab.screen,
                            galley,
                            screen_rows,
                            screen_cols,
                        ) {
                            tab.selection = Some(TermSelection {
                                anchor: (row, col),
                                cursor: (row, col),
                                dragging: true,
                            });
                        }
                    } else if allow_remote_mouse {
                        // Remote mouse is enabled. Treat this as a remote click unless the user drags,
                        // in which case we switch into local selection mode.
                        tab.selection = None;
                        if let Some((row, col)) = Self::pos_to_cell(
                            pos,
                            origin,
                            cell_w,
                            cell_h,
                            &tab.screen,
                            galley,
                            screen_rows,
                            screen_cols,
                        ) {
                            tab.pending_remote_click = Some(PendingRemoteClick {
                                start_pos: pos,
                                start_cell: (row, col),
                            });
                        } else {
                            tab.pending_remote_click = None;
                        }
                    }
                } else {
                    // Clicking outside clears selection and any pending click.
                    tab.selection = None;
                    tab.pending_remote_click = None;
                }
            }
        }

        if primary_down {
            if let Some(pos) = pointer_pos {
                let pos = clamp_pos_to_grid(pos);
                if let Some(sel) = tab.selection.as_mut() {
                    if sel.dragging {
                        if let Some((row, col)) = Self::pos_to_cell(
                            pos,
                            origin,
                            cell_w,
                            cell_h,
                            &tab.screen,
                            galley,
                            screen_rows,
                            screen_cols,
                        ) {
                            sel.cursor = (row, col);
                        }
                    }
                } else if allow_remote_mouse {
                    // When remote mouse is enabled, a small drag switches the gesture into local selection mode.
                    if let Some(pending) = tab.pending_remote_click {
                        let d = pos - pending.start_pos;
                        if d.length_sq() >= 6.0 * 6.0 {
                            if let Some((row, col)) = Self::pos_to_cell(
                                pos,
                                origin,
                                cell_w,
                                cell_h,
                                &tab.screen,
                                galley,
                                screen_rows,
                                screen_cols,
                            ) {
                                tab.selection = Some(TermSelection {
                                    anchor: pending.start_cell,
                                    cursor: (row, col),
                                    dragging: true,
                                });
                                tab.pending_remote_click = None;
                            }
                        }
                    }
                }
            }
        }

        if primary_released {
            // End local selection if active.
            if let Some(sel) = tab.selection.as_mut() {
                if sel.dragging {
                    sel.dragging = false;
                    if sel.is_empty() {
                        tab.selection = None;
                    }
                }
                // Local selection consumes the gesture: do not send remote click.
                tab.pending_remote_click = None;
            } else if allow_remote_mouse {
                // Dispatch remote click if it was not turned into a local selection.
                if let Some(pending) = tab.pending_remote_click.take() {
                    let mode = tab.screen.mouse_protocol_mode();
                    let encoding = tab.screen.mouse_protocol_encoding();

                    let release_cell = pointer_pos
                        .map(clamp_pos_to_grid)
                        .and_then(|pos| {
                            Self::pos_to_cell(
                                pos,
                                origin,
                                cell_w,
                                cell_h,
                                &tab.screen,
                                galley,
                                screen_rows,
                                screen_cols,
                            )
                        })
                        .unwrap_or(pending.start_cell);

                    // xterm mouse protocol is 1-based coordinates.
                    let (sr, sc) = pending.start_cell;
                    let (rr, rc) = release_cell;
                    let sc_1 = sc.saturating_add(1);
                    let sr_1 = sr.saturating_add(1);
                    let rc_1 = rc.saturating_add(1);
                    let rr_1 = rr.saturating_add(1);

                    if let Some(bytes) = Self::mouse_event_bytes(
                        encoding,
                        mode,
                        true,
                        egui::PointerButton::Primary,
                        sc_1,
                        sr_1,
                    ) {
                        Self::send_bytes(tab, bytes);
                    }
                    if let Some(bytes) = Self::mouse_event_bytes(
                        encoding,
                        mode,
                        false,
                        egui::PointerButton::Primary,
                        rc_1,
                        rr_1,
                    ) {
                        Self::send_bytes(tab, bytes);
                    }
                }
            } else {
                tab.pending_remote_click = None;
            }
        }

        // Keyboard input only when our terminal region has focus.
        if response.has_focus() && tab.connected {
            for ev in events.iter() {
                match ev {
                    egui::Event::Copy => {
                        if let Some(sel) = tab.selection {
                            let text = Self::selection_text(&tab.screen, sel);
                            if !text.is_empty() {
                                Self::copy_selection_with_flash(ctx, clipboard, tab, text);
                            }
                        } else {
                            // Treat Ctrl+C as SIGINT when nothing is selected.
                            Self::send_bytes(tab, vec![0x03]);
                        }
                    }
                    egui::Event::Text(t) => {
                        if !t.is_empty() {
                            Self::send_bytes(tab, t.as_bytes().to_vec());
                        }
                    }
                    egui::Event::Paste(s) => {
                        Self::send_paste_text(tab, s);
                    }
                    egui::Event::Key {
                        key,
                        pressed: true,
                        modifiers,
                        ..
                    } => {
                        // Copy selection to clipboard (terminal-style shortcut).
                        if modifiers.ctrl && modifiers.shift && *key == egui::Key::C {
                            let text = if let Some(sel) = tab.selection {
                                Self::selection_text(&tab.screen, sel)
                            } else {
                                tab.screen.contents()
                            };
                            if !text.is_empty() {
                                Self::copy_text_to_clipboard(ctx, clipboard, text);
                            }
                            continue;
                        }

                        // If there is a selection, Ctrl+C should copy (like Windows Terminal)
                        // instead of sending SIGINT to the remote.
                        if modifiers.ctrl && !modifiers.shift && *key == egui::Key::C {
                            // Some platforms report Ctrl+C as `Event::Copy` instead of `Event::Key`.
                            // Let `Event::Copy` handle it to avoid double actions.
                            if has_copy_event {
                                continue;
                            }
                            if let Some(sel) = tab.selection {
                                let text = Self::selection_text(&tab.screen, sel);
                                if !text.is_empty() {
                                    Self::copy_selection_with_flash(ctx, clipboard, tab, text);
                                }
                            } else {
                                // No local selection: behave like a real terminal (SIGINT).
                                Self::send_bytes(tab, vec![0x03]);
                            }
                            continue;
                        }

                        // Paste shortcut. Prefer the platform integration's Paste event, but
                        // fall back to reading the OS clipboard directly if needed.
                        if (modifiers.ctrl && *key == egui::Key::V) || (modifiers.ctrl && modifiers.shift && *key == egui::Key::V) {
                            if has_paste_event || has_text_event {
                                continue;
                            }
                            if let Some(cb) = clipboard.as_mut() {
                                if let Ok(s) = cb.get_text() {
                                    Self::send_paste_text(tab, &s);
                                }
                            }
                            continue;
                        }

                        Self::send_key(tab, *key, *modifiers);
                    }
                    _ => {}
                }
            }
        }

    }

    fn pos_to_cell(
        pos: Pos2,
        origin: Pos2,
        cell_w: f32,
        cell_h: f32,
        screen: &vt100::Screen,
        galley: Option<&egui::Galley>,
        rows: u16,
        cols: u16,
    ) -> Option<(u16, u16)> {
        if let Some(g) = galley {
            if let Some((r, c)) = Self::pos_to_cell_galley(pos, origin, screen, g, rows, cols) {
                return Some((r, c));
            }
        }

        if rows == 0 || cols == 0 {
            return None;
        }

        let col = ((pos.x - origin.x) / cell_w).floor() as i32;
        let row = ((pos.y - origin.y) / cell_h).floor() as i32;
        if col < 0 || row < 0 {
            return None;
        }

        let col = (col as u16).min(cols.saturating_sub(1));
        let row = (row as u16).min(rows.saturating_sub(1));
        Some((row, col))
    }

    fn pos_to_cell_galley(
        pos: Pos2,
        origin: Pos2,
        screen: &vt100::Screen,
        galley: &egui::Galley,
        rows: u16,
        cols: u16,
    ) -> Option<(u16, u16)> {
        if rows == 0 || cols == 0 {
            return None;
        }

        let x = pos.x - origin.x;
        let y = pos.y - origin.y;
        if x < 0.0 || y < 0.0 {
            return None;
        }

        let max_rows = rows as usize;
        let usable_rows = galley.rows.len().min(max_rows);
        if usable_rows == 0 {
            return None;
        }

        let mut row_idx: Option<usize> = None;
        for (i, row) in galley.rows.iter().take(usable_rows).enumerate() {
            if y >= row.rect.top() && y < row.rect.bottom() {
                row_idx = Some(i);
                break;
            }
        }

        let row_idx = row_idx.unwrap_or_else(|| {
            if y >= galley.rows[usable_rows - 1].rect.bottom() {
                usable_rows - 1
            } else {
                0
            }
        });

        let row_g = &galley.rows[row_idx];
        let char_idx = row_g.char_at(x) as usize;

        let row_u16 = row_idx as u16;
        let map = Self::row_col_to_char_index_map(screen, row_u16);
        let mut col_idx = Self::char_index_to_col(&map, char_idx);
        if col_idx as u16 >= cols {
            col_idx = cols.saturating_sub(1) as usize;
        }
        Some((row_u16, col_idx as u16))
    }

    fn row_col_to_char_index_map(screen: &vt100::Screen, row: u16) -> Vec<usize> {
        let (_rows, cols) = screen.size();
        let cols_usize = cols as usize;
        let mut out = Vec::with_capacity(cols_usize.saturating_add(1));

        let mut idx = 0usize;
        out.push(0);
        for col in 0..cols {
            let add = match screen.cell(row, col) {
                Some(cell) => {
                    if cell.is_wide_continuation() {
                        1usize
                    } else if cell.has_contents() {
                        cell.contents().chars().count().max(1)
                    } else {
                        1usize
                    }
                }
                None => 1usize,
            };
            idx = idx.saturating_add(add);
            out.push(idx);
        }
        out
    }

    fn col_to_char_index(map: &[usize], col: u16) -> usize {
        let i = col as usize;
        if i < map.len() {
            map[i]
        } else {
            *map.last().unwrap_or(&0)
        }
    }

    fn char_index_to_col(map: &[usize], char_idx: usize) -> usize {
        if map.len() <= 1 {
            return 0;
        }

        let mut col = match map.binary_search(&char_idx) {
            Ok(i) => i,
            Err(next) => next.saturating_sub(1),
        };
        // `map.len() == cols + 1`, so clamp to the last visible column.
        if col >= map.len().saturating_sub(1) {
            col = map.len().saturating_sub(2);
        }
        col
    }

    fn selection_text(screen: &vt100::Screen, sel: TermSelection) -> String {
        let (rows, cols) = screen.size();
        if rows == 0 || cols == 0 {
            return String::new();
        }

        let ((mut sr, mut sc), (mut er, mut ec)) = sel.normalized();
        sr = sr.min(rows.saturating_sub(1));
        er = er.min(rows.saturating_sub(1));
        sc = sc.min(cols.saturating_sub(1));
        ec = ec.min(cols.saturating_sub(1));

        let mut out = String::new();
        for row in sr..=er {
            let start_col = if row == sr { sc } else { 0 };
            let end_col = if row == er { ec } else { cols.saturating_sub(1) };
            if start_col > end_col {
                continue;
            }

            let mut line = String::new();
            for col in start_col..=end_col {
                if let Some(cell) = screen.cell(row, col) {
                    if cell.is_wide_continuation() {
                        continue;
                    }
                    if cell.has_contents() {
                        line.push_str(&cell.contents());
                    } else {
                        line.push(' ');
                    }
                } else {
                    line.push(' ');
                }
            }

            let trimmed = line.trim_end_matches(' ');
            out.push_str(trimmed);
            if row != er {
                out.push('\n');
            }
        }

        out
    }

    fn draw_selection_galley(
        painter: &egui::Painter,
        tab: &SshTab,
        origin: Pos2,
        galley: &egui::Galley,
        sel: TermSelection,
    ) {
        // Slightly stronger than a typical text selection so it's visible over dense ANSI color output.
        let selection_bg = if tab.copy_flash_until.is_some() {
            // Flash brighter on copy, then selection disappears (handled in the AppState update loop).
            Color32::from_rgba_unmultiplied(255, 184, 108, 190)
        } else {
            Color32::from_rgba_unmultiplied(255, 184, 108, 96)
        };
        let (rows, cols) = tab.screen.size();
        if rows == 0 || cols == 0 {
            return;
        }

        let ((mut sr, mut sc), (mut er, mut ec)) = sel.normalized();
        sr = sr.min(rows.saturating_sub(1));
        er = er.min(rows.saturating_sub(1));
        sc = sc.min(cols.saturating_sub(1));
        ec = ec.min(cols.saturating_sub(1));

        let usable_rows = galley.rows.len().min(rows as usize);
        if usable_rows == 0 {
            return;
        }

        for row in sr..=er {
            let row_idx = row as usize;
            if row_idx >= usable_rows {
                break;
            }
            let row_g = &galley.rows[row_idx];
            let map = Self::row_col_to_char_index_map(&tab.screen, row);

            let start_col = if row == sr { sc } else { 0 };
            let end_col = if row == er { ec } else { cols.saturating_sub(1) };
            if start_col > end_col {
                continue;
            }

            let start_i = Self::col_to_char_index(&map, start_col);
            let end_i = Self::col_to_char_index(&map, end_col.saturating_add(1));
            let x0 = origin.x + row_g.x_offset(start_i);
            let x1 = origin.x + row_g.x_offset(end_i);
            let y0 = origin.y + row_g.rect.top();
            let y1 = origin.y + row_g.rect.bottom();
            let rect = Rect::from_min_max(Pos2::new(x0, y0), Pos2::new(x1, y1));
            painter.rect_filled(rect, 0.0, selection_bg);
        }
    }

    fn draw_cursor_galley(
        painter: &egui::Painter,
        tab: &SshTab,
        origin: Pos2,
        galley: &egui::Galley,
        cursor_visible: bool,
        cursor_color: Color32,
        ppp: f32,
    ) {
        if tab.screen.hide_cursor() || !cursor_visible {
            return;
        }

        let (rows, cols) = tab.screen.size();
        if rows == 0 || cols == 0 {
            return;
        }

        let (mut row, mut col) = tab.screen.cursor_position();
        row = row.min(rows.saturating_sub(1));
        col = col.min(cols.saturating_sub(1));

        let row_idx = row as usize;
        if row_idx >= galley.rows.len() {
            return;
        }
        let row_g = &galley.rows[row_idx];
        let map = Self::row_col_to_char_index_map(&tab.screen, row);

        let start_i = Self::col_to_char_index(&map, col);
        let end_i = Self::col_to_char_index(&map, col.saturating_add(1));
        let x0 = origin.x + row_g.x_offset(start_i);
        let x1 = origin.x + row_g.x_offset(end_i);
        let w = (x1 - x0).max(2.0 / ppp.max(1.0));

        let thickness = (2.0 * ppp).round().max(1.0) / ppp.max(1.0);
        let y1 = origin.y + row_g.rect.bottom();
        let rect = Rect::from_min_size(Pos2::new(x0, y1 - thickness), Vec2::new(w, thickness));
        painter.rect_filled(rect, 0.0, cursor_color);
    }

    fn draw_scrollback_bar(
        painter: &egui::Painter,
        rect: Rect,
        visible_rows: u16,
        scrollback: usize,
        max_scrollback: usize,
        theme: UiTheme,
    ) {
        if max_scrollback == 0 || rect.width() < 20.0 || rect.height() < 20.0 {
            return;
        }

        let with_alpha = |c: Color32, a: u8| Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a);

        let bar_w = 8.0;
        let pad = 1.0;
        let track = Rect::from_min_max(
            Pos2::new(rect.right() - bar_w - pad, rect.top() + pad),
            Pos2::new(rect.right() - pad, rect.bottom() - pad),
        );

        let total_rows = visible_rows as f32 + max_scrollback as f32;
        let visible = (visible_rows as f32).max(1.0);
        let ratio = (visible / total_rows.max(visible)).clamp(0.05, 1.0);
        let mut thumb_h = (track.height() * ratio).round().max(14.0);
        thumb_h = thumb_h.min(track.height());

        let t = (scrollback as f32 / max_scrollback as f32).clamp(0.0, 1.0);
        let y = track.bottom() - thumb_h - t * (track.height() - thumb_h);
        let thumb = Rect::from_min_size(Pos2::new(track.left(), y), Vec2::new(track.width(), thumb_h));

        painter.rect_filled(track, 3.0, with_alpha(theme.top_border, 70));
        painter.rect_filled(thumb, 3.0, with_alpha(theme.accent, 150));
    }

    fn update_cursor_blink(&mut self) {
        if self.last_cursor_blink.elapsed() >= Duration::from_millis(530) {
            self.cursor_visible = !self.cursor_visible;
            self.last_cursor_blink = Instant::now();
        }
    }

    fn open_settings_dialog_for_tile(&mut self, tile_id: TileId) {
        let Some(settings) = self.pane(tile_id).map(|t| t.settings.clone()) else {
            return;
        };

        self.settings_dialog.open = true;
        self.settings_dialog.just_opened = true;
        self.settings_dialog.target_tile = Some(tile_id);
        self.settings_dialog.draft = settings.clone();

        // If we don't have enough info to connect, open directly on the connection page.
        if settings.host.trim().is_empty() || settings.username.trim().is_empty() {
            self.settings_dialog.page = SettingsPage::ProfilesAndAccount;
        }

        // Best-effort: preselect a matching profile (host/port/user).
        if let Some((idx, p)) = self
            .config
            .profiles
            .iter()
            .enumerate()
            .find(|(_, p)| {
                p.settings.host.trim() == settings.host.trim()
                    && p.settings.port == settings.port
                    && p.settings.username.trim() == settings.username.trim()
            })
        {
            self.settings_dialog.selected_profile = Some(idx);
            self.settings_dialog.profile_name = p.name.clone();
            self.settings_dialog.remember_password = p.remember_password;
            self.settings_dialog.remember_key_passphrase = p.remember_key_passphrase;
        } else {
            self.settings_dialog.selected_profile = None;
            self.settings_dialog.profile_name.clear();
            self.settings_dialog.remember_password = false;
            self.settings_dialog.remember_key_passphrase = false;
        }
    }

    fn load_profile_into_dialog(&mut self, idx: usize) {
        let Some(p) = self.config.profiles.get(idx).cloned() else { return };
        self.settings_dialog.selected_profile = Some(idx);
        let draft = config::write_profile_settings(&p);
        self.settings_dialog.profile_name = p.name;
        self.settings_dialog.remember_password = p.remember_password;
        self.settings_dialog.remember_key_passphrase = p.remember_key_passphrase;
        self.settings_dialog.draft = draft;
        self.settings_dialog.just_opened = true;
    }

    fn upsert_profile_from_dialog(&mut self) {
        let name = config::sanitized_profile_name(&self.settings_dialog.profile_name);
        if name.is_empty() {
            return;
        }

        let profile = config::read_profile_from_settings(
            name.clone(),
            &self.settings_dialog.draft,
            self.settings_dialog.remember_password,
            self.settings_dialog.remember_key_passphrase,
        );

        if let Some(i) = config::find_profile_index(&self.config, &name) {
            self.config.profiles[i] = profile;
            self.settings_dialog.selected_profile = Some(i);
        } else {
            self.config.profiles.push(profile);
            self.settings_dialog.selected_profile = Some(self.config.profiles.len().saturating_sub(1));
        }

        config::save(&self.config);
    }

    fn delete_selected_profile(&mut self) {
        let Some(idx) = self.settings_dialog.selected_profile else { return };
        if idx >= self.config.profiles.len() {
            self.settings_dialog.selected_profile = None;
            return;
        }

        let removed_name = self.config.profiles[idx].name.clone();
        self.config.profiles.remove(idx);
        self.settings_dialog.selected_profile = None;

        if self
            .config
            .default_profile
            .as_deref()
            .map(|d| d.eq_ignore_ascii_case(&removed_name))
            .unwrap_or(false)
        {
            self.config.default_profile = None;
        }

        config::save(&self.config);
    }

    fn draw_settings_page_autostart(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        ui.label("Default profile");
        let selected_text = self
            .config
            .default_profile
            .clone()
            .unwrap_or_else(|| "None".to_string());
        egui::ComboBox::from_id_source("default_profile_combo")
            .selected_text(selected_text)
            .width(ui.available_width())
            .show_ui(ui, |ui| {
                if ui
                    .add(egui::SelectableLabel::new(
                        self.config.default_profile.is_none(),
                        egui::RichText::new("None").color(if self.config.default_profile.is_none() {
                            Color32::from_rgb(20, 20, 20)
                        } else {
                            theme.fg
                        }),
                    ))
                    .clicked()
                {
                    self.config.default_profile = None;
                    config::save(&self.config);
                }
                for p in self.config.profiles.iter() {
                    let selected = self
                        .config
                        .default_profile
                        .as_deref()
                        .map(|d| d.eq_ignore_ascii_case(&p.name))
                        .unwrap_or(false);
                    let text_color = if selected { Color32::from_rgb(20, 20, 20) } else { theme.fg };
                    if ui
                        .add(egui::SelectableLabel::new(
                            selected,
                            egui::RichText::new(&p.name).color(text_color),
                        ))
                        .clicked()
                    {
                        self.config.default_profile = Some(p.name.clone());
                        config::save(&self.config);
                    }
                }
            });

        ui.add_space(10.0);

        let before = self.config.autostart;
        ui.checkbox(&mut self.config.autostart, "Autostart on launch");
        if self.config.autostart != before {
            config::save(&self.config);
        }

        ui.add_space(4.0);
        let note = if !self.config.autostart {
            "When enabled, Rusty connects on launch using the default profile."
        } else if self.config.default_profile.is_none() {
            "Pick a default profile to make autostart work."
        } else {
            "If the default profile does not store a password, you'll be prompted at startup."
        };
        ui.label(egui::RichText::new(note).color(theme.muted));
    }

    fn draw_settings_page_appearance(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        self.config.terminal_font_size = self
            .config
            .terminal_font_size
            .clamp(TERM_FONT_SIZE_MIN, TERM_FONT_SIZE_MAX);
        let before = self.config.terminal_font_size;
        let resp = ui.add(
            egui::Slider::new(
                &mut self.config.terminal_font_size,
                TERM_FONT_SIZE_MIN..=TERM_FONT_SIZE_MAX,
            )
            .text("Terminal font size")
            .fixed_decimals(0),
        );
        // Persist on release to avoid writing to disk on every slider tick.
        // Note: on the release-frame `resp.changed()` may be false, so key off `drag_released`.
        if resp.drag_released() {
            config::save(&self.config);
        } else if resp.changed() && !resp.dragged() && (self.config.terminal_font_size - before).abs() > f32::EPSILON {
            config::save(&self.config);
        }

        ui.add_space(6.0);
        ui.label(
            egui::RichText::new("Updates live, even for existing sessions.").color(theme.muted),
        );

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(10.0);

        let before = self.config.terminal_scrollback_lines;
        ui.add(
            egui::DragValue::new(&mut self.config.terminal_scrollback_lines)
                .speed(100.0)
                .clamp_range(0..=200_000),
        );
        ui.label("Terminal scrollback lines (0 = default)");
        if self.config.terminal_scrollback_lines != before {
            // Applies to new tabs and future connects. Existing sessions must reconnect to change capacity.
            let new_len = self.config.terminal_scrollback_lines;
            for id in self.pane_ids() {
                if let Some(tab) = self.pane_mut(id) {
                    tab.scrollback_len = new_len;
                }
            }
            config::save(&self.config);
        }
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new("Applies on new connections (reconnect existing tabs to take effect).")
                .color(theme.muted),
        );

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(10.0);

        let before = self.config.minimize_to_tray;
        ui.checkbox(&mut self.config.minimize_to_tray, "Minimize to tray");
        if self.config.minimize_to_tray != before {
            if !self.config.minimize_to_tray && self.hidden_to_tray {
                // If we are currently hidden and the user disables tray minimize, bring the window back.
                // (This will be a no-op if the backend ignores it.)
                // Note: we don't have access to `ctx` here, so we just clear state and let update handle it.
                self.hidden_to_tray = false;
            }
            config::save(&self.config);
        }
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new("When enabled, the minimize button hides Rusty to the system tray.")
                .color(theme.muted),
        );
    }

    fn draw_settings_page_terminal_colors(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;

        ui.label(egui::RichText::new("Base colors").strong());
        ui.add_space(6.0);

        let mut changed = false;
        {
            let mut bg = self.config.terminal_colors.bg.to_array();
            ui.horizontal(|ui| {
                ui.label("Background");
                let resp = ui.color_edit_button_srgb(&mut bg);
                changed |= resp.changed();
            });
            if changed {
                self.config.terminal_colors.bg = config::RgbColor::from_array(bg);
            }
        }

        {
            let mut fg = self.config.terminal_colors.fg.to_array();
            ui.horizontal(|ui| {
                ui.label("Foreground");
                let resp = ui.color_edit_button_srgb(&mut fg);
                changed |= resp.changed();
            });
            if changed {
                self.config.terminal_colors.fg = config::RgbColor::from_array(fg);
            }
        }

        ui.add_space(8.0);
        ui.label(egui::RichText::new("Dim / faint").strong());
        ui.add_space(4.0);
        let before = self.config.terminal_colors.dim_blend;
        let resp = ui.add(
            egui::Slider::new(&mut self.config.terminal_colors.dim_blend, 0.0..=0.90)
                .text("Dim blend (toward background)")
                .fixed_decimals(2),
        );
        if resp.changed() && (resp.drag_released() || !resp.dragged()) {
            if (self.config.terminal_colors.dim_blend - before).abs() > f32::EPSILON {
                changed = true;
            }
        }
        ui.label(
            egui::RichText::new(
                "Higher values make SGR 2 (dim/faint) text darker. 0.0 disables dimming.",
            )
            .color(theme.muted)
            .size(12.0),
        );

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(10.0);

        ui.label(egui::RichText::new("ANSI 16-color palette").strong());
        ui.add_space(6.0);
        ui.label(
            egui::RichText::new("Used for most CLI theming (folders, prompts, etc).")
                .color(theme.muted)
                .size(12.0),
        );
        ui.add_space(8.0);

        const NAMES: [&str; 8] = ["Black", "Red", "Green", "Yellow", "Blue", "Magenta", "Cyan", "White"];
        egui::Grid::new("terminal_palette16_grid")
            .num_columns(4)
            .spacing(Vec2::new(12.0, 10.0))
            .show(ui, |ui| {
                for i in 0..8usize {
                    ui.label(NAMES[i]);
                    let mut c0 = self.config.terminal_colors.palette16[i].to_array();
                    let r0 = ui.color_edit_button_srgb(&mut c0);
                    if r0.changed() {
                        self.config.terminal_colors.palette16[i] = config::RgbColor::from_array(c0);
                        changed = true;
                    }

                    ui.label(format!("Bright {}", NAMES[i]));
                    let mut c1 = self.config.terminal_colors.palette16[i + 8].to_array();
                    let r1 = ui.color_edit_button_srgb(&mut c1);
                    if r1.changed() {
                        self.config.terminal_colors.palette16[i + 8] = config::RgbColor::from_array(c1);
                        changed = true;
                    }
                    ui.end_row();
                }
            });

        ui.add_space(10.0);
        ui.horizontal(|ui| {
            if ui.button("Reset to defaults").clicked() {
                self.config.terminal_colors = config::TerminalColorsConfig::default();
                changed = true;
            }
            ui.label(
                egui::RichText::new("256-color cube and grayscale are fixed.")
                    .color(theme.muted)
                    .size(12.0),
            );
        });

        if changed {
            config::save(&self.config);
        }
    }

    fn draw_settings_page_profiles_and_account(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        ui.label(egui::RichText::new("Profiles").strong());
        egui::ScrollArea::vertical().max_height(140.0).show(ui, |ui| {
            let mut load_idx: Option<usize> = None;
            let mut delete_idx: Option<usize> = None;
            for (i, p) in self.config.profiles.iter().enumerate() {
                let selected = self.settings_dialog.selected_profile == Some(i);
                let label = config::profile_display_name(p, &self.config);
                let text_color = if selected { Color32::from_rgb(20, 20, 20) } else { theme.fg };
                let resp = ui.add(egui::SelectableLabel::new(
                    selected,
                    egui::RichText::new(label).color(text_color),
                ));
                if resp.clicked() {
                    load_idx = Some(i);
                }
                resp.context_menu(|ui: &mut egui::Ui| {
                    if ui.button("Delete Profile").clicked() {
                        delete_idx = Some(i);
                        ui.close_menu();
                    }
                });
            }
            if self.config.profiles.is_empty() {
                ui.label(egui::RichText::new("No profiles yet.").color(theme.muted));
            }

            if let Some(i) = delete_idx {
                self.settings_dialog.selected_profile = Some(i);
                self.delete_selected_profile();
                return;
            }

            if let Some(i) = load_idx {
                self.load_profile_into_dialog(i);
            }
        });

        ui.add_space(8.0);
        ui.horizontal_wrapped(|ui| {
            if ui.button("New").clicked() {
                self.settings_dialog.selected_profile = None;
                self.settings_dialog.profile_name.clear();
                self.settings_dialog.remember_password = false;
                self.settings_dialog.remember_key_passphrase = false;
                self.settings_dialog.draft = ConnectionSettings::default();
                self.settings_dialog.just_opened = true;
            }

            if ui
                .add_enabled(
                    self.settings_dialog.selected_profile.is_some(),
                    egui::Button::new("Delete"),
                )
                .clicked()
            {
                self.delete_selected_profile();
            }

            let can_save = !self.settings_dialog.profile_name.trim().is_empty();
            if ui.add_enabled(can_save, egui::Button::new("Save")).clicked() {
                self.upsert_profile_from_dialog();
            }
        });

        ui.add_space(8.0);
        ui.label("Profile name");
        let resp = ui.add(
            egui::TextEdit::singleline(&mut self.settings_dialog.profile_name)
                .hint_text("e.g. prod-1")
                .desired_width(ui.available_width()),
        );
        if self.settings_dialog.just_opened {
            resp.request_focus();
            self.settings_dialog.just_opened = false;
        }

        ui.add_space(6.0);
        ui.checkbox(&mut self.settings_dialog.remember_password, "Remember password");
        if self.settings_dialog.remember_password {
            ui.label(
                egui::RichText::new("Stored encrypted in a local config file (Windows DPAPI).")
                    .color(theme.muted)
                    .size(12.0),
            );
        }

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(10.0);

        ui.label(egui::RichText::new("Connection").strong());
        ui.add_space(6.0);

        ui.label("Host");
        ui.add(
            egui::TextEdit::singleline(&mut self.settings_dialog.draft.host)
                .hint_text("example.com")
                .desired_width(ui.available_width()),
        );
        ui.add_space(6.0);

        ui.label("Port");
        ui.add(
            egui::DragValue::new(&mut self.settings_dialog.draft.port)
                .speed(1.0)
                .clamp_range(1..=65535),
        );
        ui.add_space(6.0);

        ui.label("User");
        ui.add(
            egui::TextEdit::singleline(&mut self.settings_dialog.draft.username)
                .hint_text("root")
                .desired_width(ui.available_width()),
        );
        ui.add_space(6.0);

        ui.label("Password (optional)");
        ui.add(
            egui::TextEdit::singleline(&mut self.settings_dialog.draft.password)
                .password(true)
                .desired_width(ui.available_width()),
        );
        ui.add_space(6.0);

        ui.label("Private key (optional)");
        ui.horizontal(|ui| {
            ui.add(
                egui::TextEdit::singleline(&mut self.settings_dialog.draft.private_key_path)
                    .hint_text("C:\\\\Users\\\\you\\\\.ssh\\\\id_ed25519")
                    .desired_width(ui.available_width() - 92.0),
            );
            if ui.button("Browse...").clicked() {
                let mut dlg = rfd::FileDialog::new();
                if let Some(home) = std::env::var_os("USERPROFILE").map(std::path::PathBuf::from) {
                    let ssh_dir = home.join(".ssh");
                    if ssh_dir.is_dir() {
                        dlg = dlg.set_directory(ssh_dir);
                    } else {
                        dlg = dlg.set_directory(home);
                    }
                }
                if let Some(path) = dlg.pick_file() {
                    self.settings_dialog.draft.private_key_path = path.display().to_string();
                }
            }
        });

        // Status (only show failures to keep noise down).
        let status_tile = self
            .settings_dialog
            .target_tile
            .or(self.active_tile)
            .or_else(|| self.first_pane_id());
        if let Some(tile_id) = status_tile {
            if let Some(tab) = self.pane(tile_id) {
                if tab.last_status.to_lowercase().contains("failed") {
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new(&tab.last_status).color(theme.muted));
                }
            }
        }

        ui.add_space(12.0);

        ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
            if ui.button("Close").clicked() {
                self.settings_dialog.open = false;
            }

            let connect_enabled = !self.settings_dialog.draft.host.trim().is_empty()
                && !self.settings_dialog.draft.username.trim().is_empty();
            if ui
                .add_enabled(connect_enabled, egui::Button::new("Connect"))
                .clicked()
            {
                let target = self
                    .settings_dialog
                    .target_tile
                    .or(self.active_tile)
                    .or_else(|| self.first_pane_id());
                if let Some(tile_id) = target {
                    let draft = self.settings_dialog.draft.clone();
                    let profile_name = self
                        .settings_dialog
                        .selected_profile
                        .and_then(|i| self.config.profiles.get(i).map(|p| p.name.clone()));
                    if let Some(tab) = self.pane_mut(tile_id) {
                        tab.profile_name = profile_name;
                        tab.settings = draft;
                        tab.disconnect();
                        tab.start_connect();
                        tab.focus_terminal_next_frame = true;
                    }
                    self.active_tile = Some(tile_id);
                }
                self.settings_dialog.open = false;
            }
        });
    }

    fn draw_settings_contents(&mut self, ui: &mut egui::Ui, theme: UiTheme, _section_frame: &egui::Frame) {
        ui.visuals_mut().override_text_color = Some(theme.fg);
        ui.spacing_mut().item_spacing = Vec2::new(8.0, 10.0);

        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Settings")
                    .strong()
                    .size(22.0)
                    .color(theme.accent),
            );
            ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                if ui.button("Close").clicked() {
                    self.settings_dialog.open = false;
                }
            });
        });
        ui.separator();
        ui.add_space(6.0);

        // Settings layout that scales: left section list (listbox-style) + right content pane.
        let avail_h = ui.available_height();
        let panel_rounding = egui::Rounding::same(10.0);
        let panel_stroke = Stroke::new(1.0, theme.top_border);

        ui.horizontal(|ui| {
            let nav_w = 220.0;
            let gap = 12.0;

            let (nav_rect, _) = ui.allocate_exact_size(Vec2::new(nav_w, avail_h), Sense::hover());
            ui.painter()
                .rect_filled(nav_rect, panel_rounding, adjust_color(theme.top_bg, 0.10));
            ui.painter().rect_stroke(nav_rect, panel_rounding, panel_stroke);
            let nav_inner = nav_rect.shrink(12.0);
            let mut nav_ui = ui.child_ui(nav_inner, egui::Layout::top_down(Align::Min));
            nav_ui.spacing_mut().item_spacing = Vec2::new(6.0, 6.0);
            nav_ui.label(egui::RichText::new("Sections").color(theme.muted).size(12.0));
            nav_ui.add_space(6.0);

            egui::ScrollArea::vertical().auto_shrink([false, false]).show(&mut nav_ui, |ui| {
                let item_h = 34.0;
                let rounding = egui::Rounding::same(10.0);
                let font_id = FontId::proportional(16.0);

                let mut item = |ui: &mut egui::Ui, page: SettingsPage| {
                    let selected = self.settings_dialog.page == page;
                    let text = egui::WidgetText::from(page.label());
                    let galley = text.into_galley(ui, Some(false), f32::INFINITY, font_id.clone());

                    let (rect, resp) =
                        ui.allocate_exact_size(Vec2::new(ui.available_width(), item_h), Sense::click());
                    let hovered = resp.hovered();

                    let fill = if selected {
                        adjust_color(theme.top_bg, 0.16)
                    } else if hovered {
                        adjust_color(theme.top_bg, 0.10)
                    } else {
                        Color32::TRANSPARENT
                    };
                    let stroke = if selected {
                        Stroke::new(1.0, theme.accent)
                    } else if hovered {
                        Stroke::new(1.0, theme.top_border)
                    } else {
                        Stroke::NONE
                    };
                    let text_color = if selected { theme.accent } else { theme.fg };

                    if ui.is_rect_visible(rect) {
                        if fill != Color32::TRANSPARENT {
                            ui.painter().rect_filled(rect, rounding, fill);
                        }
                        if stroke != Stroke::NONE {
                            ui.painter().rect_stroke(rect, rounding, stroke);
                        }

                        if selected {
                            let bar_w = 4.0;
                            let bar = Rect::from_min_max(
                                rect.min,
                                Pos2::new(rect.min.x + bar_w, rect.max.y),
                            );
                            let bar_rounding = egui::Rounding {
                                nw: rounding.nw,
                                sw: rounding.sw,
                                ne: 0.0,
                                se: 0.0,
                            };
                            ui.painter().rect_filled(bar, bar_rounding, theme.accent);
                        }

                        let text_pos = Pos2::new(rect.left() + 14.0, rect.center().y - galley.size().y * 0.5);
                        ui.painter().galley(text_pos, galley, text_color);
                    }

                    if resp.clicked() {
                        self.settings_dialog.page = page;
                    }
                };

                item(ui, SettingsPage::Autostart);
                item(ui, SettingsPage::Appearance);
                item(ui, SettingsPage::TerminalColors);
                item(ui, SettingsPage::ProfilesAndAccount);
            });

            ui.add_space(gap);

            let content_w = ui.available_width().max(10.0);
            let (content_rect, _) =
                ui.allocate_exact_size(Vec2::new(content_w, avail_h), Sense::hover());
            ui.painter()
                .rect_filled(content_rect, panel_rounding, adjust_color(theme.top_bg, 0.10));
            ui.painter()
                .rect_stroke(content_rect, panel_rounding, panel_stroke);
            let content_inner = content_rect.shrink(12.0);
            let mut content_ui = ui.child_ui(content_inner, egui::Layout::top_down(Align::Min));

            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(&mut content_ui, |ui| match self.settings_dialog.page {
                    SettingsPage::Autostart => self.draw_settings_page_autostart(ui),
                    SettingsPage::Appearance => self.draw_settings_page_appearance(ui),
                    SettingsPage::TerminalColors => self.draw_settings_page_terminal_colors(ui),
                    SettingsPage::ProfilesAndAccount => self.draw_settings_page_profiles_and_account(ui),
                });
        });
    }

    fn draw_settings_dialog(&mut self, ctx: &egui::Context) {
        if !self.settings_dialog.open {
            return;
        }

        let viewport_id = egui::ViewportId::from_hash_of("rusty_settings_viewport");
        let builder = egui::ViewportBuilder::default()
            .with_title("Rusty Settings")
            .with_inner_size(Vec2::new(640.0, 720.0))
            .with_min_inner_size(Vec2::new(420.0, 520.0))
            .with_resizable(true);

        ctx.show_viewport_immediate(viewport_id, builder, |ctx, class| {
            if ctx.input(|i| i.viewport().close_requested()) {
                self.settings_dialog.open = false;
            }
            if !self.settings_dialog.open {
                return;
            }

            let theme = self.theme;
            let outer_frame = egui::Frame::none()
                .fill(adjust_color(theme.top_bg, 0.06))
                .stroke(Stroke::new(1.0, theme.top_border))
                .inner_margin(egui::Margin::same(14.0));
            let section_frame = egui::Frame::none()
                .fill(adjust_color(theme.top_bg, 0.10))
                .stroke(Stroke::new(1.0, theme.top_border))
                .rounding(egui::Rounding::same(10.0))
                .inner_margin(egui::Margin::same(12.0));

            match class {
                egui::ViewportClass::Embedded => {
                    let mut open = true;
                    egui::Window::new("Settings")
                        .collapsible(false)
                        .resizable(true)
                        .open(&mut open)
                        .frame(outer_frame)
                        .show(ctx, |ui| {
                            self.draw_settings_contents(ui, theme, &section_frame);
                        });
                    if !open {
                        self.settings_dialog.open = false;
                    }
                }
                _ => {
                    egui::CentralPanel::default()
                        .frame(outer_frame)
                        .show(ctx, |ui| {
                            self.draw_settings_contents(ui, theme, &section_frame);
                        });
                }
            }
        });
    }

    fn draw_auth_dialog(&mut self, ctx: &egui::Context) {
        let Some(mut auth) = self.auth_dialog.take() else { return };
        auth.responses.resize(auth.prompts.len(), String::new());

        // If the tab went away (closed) or no longer exists, drop the prompt.
        if !matches!(self.tree.tiles.get(auth.tile_id), Some(Tile::Pane(_))) {
            return;
        }

        // Modal dim background.
        let screen_rect = ctx.screen_rect();
        // Paint the dim overlay above panels but below windows.
        let overlay_id = egui::LayerId::new(egui::Order::PanelResizeLine, Id::new("auth_modal_bg"));
        let painter = ctx.layer_painter(overlay_id);
        painter.rect_filled(
            screen_rect,
            0.0,
            Color32::from_rgba_unmultiplied(0, 0, 0, 160),
        );

        let mut send_responses: Option<(TileId, Vec<String>)> = None;
        let mut cancel_tab: Option<TileId> = None;
        let mut open = true;

        let is_key_passphrase_prompt = {
            let instr = auth.instructions.to_ascii_lowercase();
            let prompt_has_passphrase = auth
                .prompts
                .iter()
                .any(|p| p.text.to_ascii_lowercase().contains("passphrase"));
            let prompt_is_key = auth
                .prompts
                .iter()
                .any(|p| p.text.to_ascii_lowercase().contains("key passphrase"));
            (prompt_is_key || (instr.contains("private key") && prompt_has_passphrase))
                && auth.prompts.len() == 1
        };

        // Keep this compact; most prompts are 1-2 fields.
        let prompts = auth.prompts.len().max(1) as f32;
        let win_w = (screen_rect.width() * 0.80).clamp(340.0, 480.0);
        let mut win_h = 170.0 + prompts * 62.0;
        win_h = win_h.clamp(220.0, (screen_rect.height() * 0.80).clamp(240.0, 520.0));
        // Force the modal into view even if egui has remembered an off-screen position.
        let pad = 10.0;
        let mut x = screen_rect.center().x - win_w * 0.5;
        let mut y = screen_rect.center().y - win_h * 0.5;
        let min_x = screen_rect.left() + pad;
        let min_y = screen_rect.top() + pad;
        let max_x = (screen_rect.right() - win_w - pad).max(min_x);
        let max_y = (screen_rect.bottom() - win_h - pad).max(min_y);
        x = x.clamp(min_x, max_x);
        y = y.clamp(min_y, max_y);
        let win_rect = Rect::from_min_size(Pos2::new(x, y), Vec2::new(win_w, win_h));
        let frame = egui::Frame::none()
            .fill(adjust_color(self.theme.top_bg, 0.06))
            .stroke(Stroke::new(1.0, self.theme.top_border))
            .rounding(egui::Rounding::same(12.0))
            .shadow(egui::epaint::Shadow::big_dark())
            .inner_margin(egui::Margin::same(12.0));

        egui::Window::new("Authentication")
            .collapsible(false)
            .resizable(false)
            .title_bar(false)
            .fixed_rect(win_rect)
            .frame(frame)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.visuals_mut().override_text_color = Some(self.theme.fg);
                ui.spacing_mut().item_spacing = Vec2::new(8.0, 8.0);

                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("Authentication")
                            .strong()
                            .size(18.0)
                            .color(self.theme.accent),
                    );
                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        if ui.button("Cancel").clicked() {
                            cancel_tab = Some(auth.tile_id);
                        }
                    });
                });
                ui.separator();
                ui.add_space(4.0);

                if !auth.instructions.trim().is_empty() {
                    ui.label(egui::RichText::new(&auth.instructions).color(self.theme.muted));
                    ui.add_space(6.0);
                }

                egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                    let field_w = ui.available_width().min(360.0);
                    for (i, p) in auth.prompts.iter().enumerate() {
                        ui.label(egui::RichText::new(&p.text).strong());
                        let edit = egui::TextEdit::singleline(auth.responses.get_mut(i).unwrap())
                            .password(!p.echo)
                            .desired_width(field_w);
                        let resp = ui.add(edit);
                        if auth.just_opened && i == 0 {
                            resp.request_focus();
                        }
                        ui.add_space(8.0);
                    }
                    auth.just_opened = false;
                });

                let enter = ui.input(|i| i.key_pressed(egui::Key::Enter));
                let can_continue = if is_key_passphrase_prompt {
                    // Allow empty passphrase: if the key isn't encrypted, SSH will still work, and
                    // we can fall back to password auth if needed.
                    true
                } else {
                    auth.responses
                        .iter()
                        .zip(auth.prompts.iter())
                        .all(|(r, p)| p.text.trim().is_empty() || !r.trim().is_empty())
                };

                if is_key_passphrase_prompt {
                    ui.add_space(2.0);
                    let can_remember = auth.profile_name.is_some();
                    ui.add_enabled_ui(can_remember, |ui| {
                        ui.checkbox(&mut auth.remember_key_passphrase, "Remember key passphrase for this profile");
                    });
                    if !can_remember {
                        ui.label(egui::RichText::new("Open this session from a saved profile to enable remembering.").color(self.theme.muted).size(12.0));
                    } else {
                        ui.label(egui::RichText::new("Saved encrypted (Windows DPAPI).").color(self.theme.muted).size(12.0));
                    }
                }

                ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                    let cont = ui.add_enabled(can_continue, egui::Button::new("Continue"));
                    if cont.clicked() || (enter && can_continue) {
                        send_responses = Some((auth.tile_id, auth.responses.clone()));
                    }
                });
            });

        if !open {
            cancel_tab = Some(auth.tile_id);
        }

        if let Some((tab_id, responses)) = send_responses {
            let mut key_pw: Option<String> = None;
            if is_key_passphrase_prompt {
                key_pw = Some(responses.get(0).cloned().unwrap_or_default());

                if auth.remember_key_passphrase {
                    if let (Some(profile_name), Some(pw)) =
                        (auth.profile_name.as_deref(), key_pw.as_ref())
                    {
                        if let Some(i) = config::find_profile_index(&self.config, profile_name) {
                            self.config.profiles[i].remember_key_passphrase = true;
                            self.config.profiles[i].settings.key_passphrase = pw.clone();
                            config::save(&self.config);

                            if self.settings_dialog.selected_profile == Some(i) {
                                self.settings_dialog.remember_key_passphrase = true;
                                self.settings_dialog.draft.key_passphrase = pw.clone();
                            }
                        }
                    }
                }
            }

            if let Some(tab) = self.pane_mut(tab_id) {
                if let Some(pw) = key_pw {
                    tab.settings.key_passphrase = pw;
                }
                if let Some(tx) = tab.worker_tx.as_ref() {
                    let _ = tx.send(WorkerMessage::AuthResponse(responses));
                }
            }
        } else if let Some(tab_id) = cancel_tab {
            if let Some(tab) = self.pane_mut(tab_id) {
                if let Some(tx) = tab.worker_tx.as_ref() {
                    let _ = tx.send(WorkerMessage::Disconnect);
                }
            }
        } else {
            // Keep it open for the next frame.
            self.auth_dialog = Some(auth);
        }
    }

    fn terminal_view(
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        tab: &mut SshTab,
        is_active: bool,
        theme: UiTheme,
        term_theme: TermTheme,
        cursor_visible: bool,
        term_font_size: f32,
        allow_resize: bool,
    ) {
        let avail = ui.available_size();
        let (rect, _) = ui.allocate_exact_size(avail, Sense::hover());
        let term_id = Id::new(("terminal_view", tab.id));
        let response = ui.interact(rect, term_id, Sense::click_and_drag());

        // Keep terminal focus locked to the terminal for common terminal keys (arrows/tab/escape).
        // Without this, egui may move focus to other widgets (e.g. the settings cog) on arrow keys.
        ui.memory_mut(|mem| {
            mem.set_focus_lock_filter(
                term_id,
                EventFilter {
                    tab: true,
                    horizontal_arrows: true,
                    vertical_arrows: true,
                    escape: true,
                },
            );
        });

        if response.clicked() {
            response.request_focus();
        }
        if tab.focus_terminal_next_frame {
            response.request_focus();
            tab.focus_terminal_next_frame = false;
        }

        let painter = ui.painter().with_clip_rect(rect);
        // If this pane touches the window edge, round the pane background corners too so we don't
        // "fill in" the transparent rounded-corner pixels of the borderless window.
        let screen = ctx.screen_rect();
        let eps = 1.2;
        let left = (rect.left() - screen.left()).abs() <= eps;
        let right = (rect.right() - screen.right()).abs() <= eps;
        let top = (rect.top() - screen.top()).abs() <= eps;
        let bottom = (rect.bottom() - screen.bottom()).abs() <= eps;
        let rounding = egui::Rounding {
            nw: if top && left { WINDOW_RADIUS } else { 0.0 },
            ne: if top && right { WINDOW_RADIUS } else { 0.0 },
            sw: if bottom && left { WINDOW_RADIUS } else { 0.0 },
            se: if bottom && right { WINDOW_RADIUS } else { 0.0 },
        };
        painter.rect_filled(rect, rounding, term_theme.bg);

        // Compute visible rows/cols and keep the remote PTY in sync.
        let font_id = FontId::monospace(term_font_size);
        let (cell_w, cell_h) = Self::cell_metrics(ctx, &font_id);

        let inner_size = rect.size() - Vec2::new(TERM_PAD_X * 2.0, TERM_PAD_Y * 2.0);
        let cols = ((inner_size.x / cell_w).floor().max(1.0)) as u16;
        let rows = ((inner_size.y / cell_h).floor().max(1.0)) as u16;
        let width_px = (inner_size.x * ctx.pixels_per_point()).round().max(1.0) as u32;
        let height_px = (inner_size.y * ctx.pixels_per_point()).round().max(1.0) as u32;

        if allow_resize && tab.connected {
            if let Some(tx) = tab.worker_tx.as_ref() {
                // Avoid resizing to a "degenerate" 1x1 while minimized/hidden or during transient layouts.
                // Keeping the last good PTY size prevents the screen from effectively going blank.
                if inner_size.x >= cell_w && inner_size.y >= cell_h {
                    let new_size = (rows, cols, width_px, height_px);
                    if tab.last_sent_size != Some(new_size) {
                        tab.pending_resize = Some(new_size);
                    }
                }

                // During drag-resize (window edges or tile splitters), don't spam intermediate sizes.
                // Send only when the user releases the mouse, which prevents "cut off" screens when
                // expanding back out.
                let dragging = ctx.input(|i| i.pointer.any_down());
                if !dragging {
                    if let Some((rows, cols, width_px, height_px)) = tab.pending_resize.take() {
                        tab.last_sent_size = Some((rows, cols, width_px, height_px));
                        let _ = tx.send(WorkerMessage::Resize {
                            rows,
                            cols,
                            width_px,
                            height_px,
                        });
                    }
                }
            }
        } else {
            tab.pending_resize = None;
        }

        let origin = rect.min + Vec2::new(TERM_PAD_X, TERM_PAD_Y);
        // Snap to pixels so our overlays (cursor/selection) line up with the text tessellation.
        let ppp = ctx.pixels_per_point();
        let origin = Pos2::new((origin.x * ppp).round() / ppp, (origin.y * ppp).round() / ppp);

        if tab.connected {
            let job = Self::screen_to_layout_job(&tab.screen, font_id, &term_theme);
            let galley = ui.fonts(|fonts| fonts.layout_job(job));
            painter.galley(origin, galley.clone(), Color32::WHITE);
            if let Some(sel) = tab.selection {
                // Draw selection *after* the galley so it stays visible even when ANSI background
                // colors are present.
                Self::draw_selection_galley(&painter, tab, origin, &galley, sel);
            }
            Self::draw_cursor_galley(&painter, tab, origin, &galley, cursor_visible, term_theme.fg, ppp);

            Self::handle_terminal_io(ctx, clipboard, ui, tab, rect, origin, cell_w, cell_h, Some(&galley), &response);
        } else {
            // Minimal empty state.
            let text = if tab.connecting {
                "Connecting..."
            } else {
                "Not connected"
            };
            painter.text(
                origin,
                egui::Align2::LEFT_TOP,
                text,
                FontId::proportional(14.0),
                theme.muted,
            );

            Self::handle_terminal_io(ctx, clipboard, ui, tab, rect, origin, cell_w, cell_h, None, &response);
        }

        // Hover-only scrollback bar (right side).
        let hovering_term = ui
            .input(|i| i.pointer.hover_pos())
            .map(|pos| rect.contains(pos))
            .unwrap_or(false);
        if hovering_term && tab.scrollback_max > 0 {
            let visible_rows = tab.screen.size().0;
            Self::draw_scrollback_bar(
                &painter,
                rect,
                visible_rows,
                tab.screen.scrollback(),
                tab.scrollback_max,
                theme,
            );
        }

        // Active-pane affordance: subtle border/glow so it's obvious which terminal is "current".
        if is_active || response.has_focus() {
            let c = theme.accent;
            let (a_stroke, a_glow) = if response.has_focus() { (180u8, 70u8) } else { (110u8, 38u8) };
            let stroke = Stroke::new(1.0, Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a_stroke));
            let glow = Stroke::new(3.0, Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a_glow));
            let r0 = rect.shrink(1.0);
            let r1 = rect.shrink(3.0);
            painter.rect_stroke(r1, 6.0, glow);
            painter.rect_stroke(r0, 6.0, stroke);
        }
    }
}

#[derive(Debug, Clone)]
enum TilesAction {
    NewTab {
        tabs_container_id: TileId,
        base_pane_id: Option<TileId>,
    },
    NewTabWithSettings {
        tabs_container_id: TileId,
        settings: ConnectionSettings,
        color: Option<Color32>,
        profile_name: Option<String>,
    },
    TabActivated(TileId),
    Connect(TileId),
    ToggleConnect(TileId),
    OpenSettings(TileId),
    Rename(TileId),
    SetColor {
        pane_id: TileId,
        color: Option<Color32>,
    },
    Split {
        pane_id: TileId,
        dir: LinearDir,
    },
    Close(TileId),
    Exit,
}

struct SshTilesBehavior<'a> {
    theme: UiTheme,
    term_theme: TermTheme,
    cursor_visible: bool,
    term_font_size: f32,
    allow_resize: bool,
    profiles: Vec<(String, ConnectionSettings)>,
    clipboard: &'a mut Option<Clipboard>,
    actions: Vec<TilesAction>,
    active_tile: Option<TileId>,
}

impl<'a> SshTilesBehavior<'a> {
    fn new(
        theme: UiTheme,
        term_theme: TermTheme,
        cursor_visible: bool,
        term_font_size: f32,
        allow_resize: bool,
        profiles: Vec<(String, ConnectionSettings)>,
        clipboard: &'a mut Option<Clipboard>,
        active_tile: Option<TileId>,
    ) -> Self {
        Self {
            theme,
            term_theme,
            cursor_visible,
            term_font_size,
            allow_resize,
            profiles,
            clipboard,
            actions: Vec::new(),
            active_tile,
        }
    }

    fn set_active(&mut self, tile_id: TileId) {
        self.active_tile = Some(tile_id);
        self.actions.push(TilesAction::TabActivated(tile_id));
    }
}

impl<'a> TilesBehavior<SshTab> for SshTilesBehavior<'a> {
    fn pane_ui(&mut self, ui: &mut egui::Ui, tile_id: TileId, pane: &mut SshTab) -> UiResponse {
        // Track the last pane the user interacted with so menu actions have a sensible default.
        let clicked_here = ui.input(|i| i.pointer.primary_clicked())
            && ui
                .input(|i| i.pointer.interact_pos())
                .map(|pos| ui.max_rect().contains(pos))
                .unwrap_or(false);
        if clicked_here {
            self.active_tile = Some(tile_id);
        }

        let ctx = ui.ctx().clone();
        AppState::terminal_view(
            ui,
            &ctx,
            self.clipboard,
            pane,
            self.active_tile == Some(tile_id),
            self.theme,
            self.term_theme,
            self.cursor_visible,
            self.term_font_size,
            self.allow_resize,
        );
        UiResponse::None
    }

    fn tab_title_for_pane(&mut self, pane: &SshTab) -> egui::WidgetText {
        let mut label = pane
            .user_title
            .as_deref()
            .unwrap_or(&pane.title)
            .to_string();
        if pane.connecting {
            label.push_str(" ...");
        }
        label.into()
    }

    fn simplification_options(&self) -> SimplificationOptions {
        // We always want a tab bar (for +/gear/context menus) even for a single visible pane.
        SimplificationOptions {
            all_panes_must_have_tabs: true,
            ..Default::default()
        }
    }

    fn tab_bar_height(&self, _style: &egui::Style) -> f32 {
        26.0
    }

    fn tab_bar_color(&self, _visuals: &egui::Visuals) -> Color32 {
        self.theme.top_bg
    }

    fn gap_width(&self, _style: &egui::Style) -> f32 {
        1.0
    }

    #[allow(clippy::fn_params_excessive_bools)]
    fn tab_ui(
        &mut self,
        tiles: &Tiles<SshTab>,
        ui: &mut egui::Ui,
        id: Id,
        tile_id: TileId,
        active: bool,
        is_being_dragged: bool,
    ) -> Response {
        let text = self.tab_title_for_tile(tiles, tile_id);
        let font_id = FontId::proportional(15.0);
        let galley = text.into_galley(ui, Some(false), f32::INFINITY, font_id);

        let x_margin = 12.0;
        let (_, rect) = ui.allocate_space(egui::vec2(
            galley.size().x + 2.0 * x_margin,
            ui.available_height(),
        ));
        let response = ui.interact(rect, id, Sense::click_and_drag());

        if response.clicked() {
            if matches!(tiles.get(tile_id), Some(Tile::Pane(_))) {
                self.set_active(tile_id);
            } else {
                self.active_tile = Some(tile_id);
            }
        }

        // Show a gap when dragged.
        if ui.is_rect_visible(rect) && !is_being_dragged {
            let mut fill = if active {
                adjust_color(self.theme.top_bg, 0.16)
            } else {
                adjust_color(self.theme.top_bg, 0.08)
            };

            if let Some(Tile::Pane(pane)) = tiles.get(tile_id) {
                if let Some(custom) = pane.color {
                    fill = if active {
                        custom
                    } else {
                        lerp_color(custom, self.theme.top_bg, 0.35)
                    };
                }
            }

            let stroke = Stroke::new(
                1.0,
                if active {
                    self.theme.accent
                } else {
                    self.theme.top_border
                },
            );
            let rounding = egui::Rounding {
                nw: 10.0,
                ne: 10.0,
                sw: 4.0,
                se: 4.0,
            };

            let paint_rect = rect.shrink(1.0);
            ui.painter().rect(paint_rect, rounding, fill, stroke);
            if active {
                // Blend the active tab into the pane area by covering the bottom stroke.
                let h = 2.0;
                let r = Rect::from_min_max(
                    Pos2::new(paint_rect.left(), paint_rect.bottom() - h),
                    Pos2::new(paint_rect.right(), paint_rect.bottom() + 1.0),
                );
                ui.painter().rect_filled(r, 0.0, fill);
            }

            let text_color = contrast_text_color(fill);
            ui.painter().galley(
                egui::Align2::CENTER_CENTER
                    .align_size_within_rect(galley.size(), paint_rect)
                    .min,
                galley,
                text_color,
            );
        }

        // Tooltips and context menu only make sense for leaf panes.
        let response = match tiles.get(tile_id) {
            Some(Tile::Pane(pane)) if pane.user_title.is_some() => {
                response.on_hover_text(pane.title.clone())
            }
            _ => response,
        };

        if matches!(tiles.get(tile_id), Some(Tile::Pane(_))) {
            response.context_menu(|ui: &mut egui::Ui| {
                if ui.button("Rename Tab").clicked() {
                    self.actions.push(TilesAction::Rename(tile_id));
                    ui.close_menu();
                }

                ui.menu_button("Change Tab Color", |ui: &mut egui::Ui| {
                    if ui.button("Default").clicked() {
                        self.actions.push(TilesAction::SetColor {
                            pane_id: tile_id,
                            color: None,
                        });
                        ui.close_menu();
                    }
                    ui.separator();

                    for (name, color) in TAB_COLOR_PRESETS {
                        let t = contrast_text_color(color);
                        if ui
                            .add(
                                egui::Button::new(egui::RichText::new(name).color(t))
                                    .fill(color)
                                    .rounding(egui::Rounding::same(6.0))
                                    .min_size(Vec2::new(160.0, 0.0)),
                            )
                            .clicked()
                        {
                            self.actions.push(TilesAction::SetColor {
                                pane_id: tile_id,
                                color: Some(color),
                            });
                            ui.close_menu();
                        }
                    }
                });

                ui.separator();
                if ui.button("Split Right").clicked() {
                    self.actions.push(TilesAction::Split {
                        pane_id: tile_id,
                        dir: LinearDir::Horizontal,
                    });
                    ui.close_menu();
                }
                if ui.button("Split Down").clicked() {
                    self.actions.push(TilesAction::Split {
                        pane_id: tile_id,
                        dir: LinearDir::Vertical,
                    });
                    ui.close_menu();
                }

                ui.separator();
                if ui.button("Close Tab").clicked() {
                    self.actions.push(TilesAction::Close(tile_id));
                    ui.close_menu();
                }
            });
        }

        response
    }

    fn top_bar_right_ui(
        &mut self,
        tiles: &Tiles<SshTab>,
        ui: &mut egui::Ui,
        tile_id: TileId,
        tabs: &egui_tiles::Tabs,
        scroll_offset: &mut f32,
    ) {
        let active_pane_id = tabs
            .active
            .or_else(|| tabs.children.first().copied())
            .filter(|id| matches!(tiles.get(*id), Some(Tile::Pane(_))));

        let btn_fill = adjust_color(self.theme.top_bg, 0.10);
        let btn = |label: egui::RichText| {
            egui::Button::new(label)
                .fill(btn_fill)
                .stroke(Stroke::new(1.0, self.theme.top_border))
                .rounding(egui::Rounding::same(8.0))
                .min_size(Vec2::new(30.0, 26.0))
        };

        ui.scope(|ui| {
            let mut style = (**ui.style()).clone();
            style.spacing.button_padding = Vec2::new(8.0, 4.0);
            ui.set_style(style);

            let w = &mut ui.visuals_mut().widgets;
            w.inactive.bg_fill = btn_fill;
            w.inactive.bg_stroke = Stroke::new(1.0, self.theme.top_border);
            w.inactive.rounding = egui::Rounding::same(8.0);
            w.hovered.bg_fill = adjust_color(btn_fill, 0.06);
            w.hovered.bg_stroke = Stroke::new(1.0, self.theme.accent);
            w.hovered.rounding = egui::Rounding::same(8.0);
            w.active.bg_fill = adjust_color(btn_fill, 0.10);
            w.active.bg_stroke = Stroke::new(1.0, self.theme.accent);
            w.active.rounding = egui::Rounding::same(8.0);

            let plus_resp =
                ui.add(btn(egui::RichText::new("+").size(18.0).color(self.theme.fg)));
            if plus_resp.clicked() {
                self.actions.push(TilesAction::NewTab {
                    tabs_container_id: tile_id,
                    base_pane_id: active_pane_id,
                });
                *scroll_offset += 10_000.0;
            }

            // Right-click the + button to create a new tab from a saved profile.
            plus_resp.context_menu(|ui: &mut egui::Ui| {
                ui.label(egui::RichText::new("New Tab From Profile").strong());
                ui.separator();

                if self.profiles.is_empty() {
                    ui.label(egui::RichText::new("No profiles saved").color(self.theme.muted));
                    return;
                }

                for (name, settings) in self.profiles.clone() {
                    if ui.button(&name).clicked() {
                        self.actions.push(TilesAction::NewTabWithSettings {
                            tabs_container_id: tile_id,
                            settings,
                            // Default tab color; user can change via tab right-click menu.
                            color: None,
                            profile_name: Some(name),
                        });
                        *scroll_offset += 10_000.0;
                        ui.close_menu();
                    }
                }
            });
        });
    }
}

impl eframe::App for AppState {
    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        // We use a transparent window to get rounded corners on borderless windows.
        egui::Color32::TRANSPARENT.to_normalized_gamma_f32()
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if !self.style_initialized {
            self.apply_global_style(ctx);
            self.style_initialized = true;
        }

        // Tray integration (created lazily when enabled).
        // If tray minimize was turned off while hidden, bring the window back.
        if !self.config.minimize_to_tray && self.hidden_to_tray {
            self.show_from_tray(ctx);
            self.tray = None;
        }
        self.ensure_tray_icon();
        self.handle_tray_events(ctx);

        ctx.request_repaint_after(Duration::from_millis(16));
        self.update_cursor_blink();

        self.ensure_tree_non_empty();

        for tile_id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(tile_id) {
                tab.poll_messages();
            }
        }

        // Copy flash: after a successful copy, briefly flash the selection then clear it.
        let now = Instant::now();
        for tile_id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(tile_id) {
                if let Some(until) = tab.copy_flash_until {
                    if now >= until {
                        tab.copy_flash_until = None;
                        tab.selection = None;
                    }
                }
            }
        }

        // If any SSH worker is asking for keyboard-interactive input (password, OTP, etc),
        // pop a modal dialog to collect it.
        if self.auth_dialog.is_none() {
            let mut candidates: Vec<TileId> = Vec::new();
            if let Some(active) = self.active_tile {
                candidates.push(active);
            }
            for id in self.pane_ids() {
                if Some(id) != self.active_tile {
                    candidates.push(id);
                }
            }

            for id in candidates {
                let Some(tab) = self.pane_mut(id) else { continue };
                if let Some(p) = tab.pending_auth.take() {
                    let n = p.prompts.len();
                    self.auth_dialog = Some(AuthDialog {
                        tile_id: id,
                        profile_name: tab.profile_name.clone(),
                        instructions: p.instructions,
                        prompts: p.prompts,
                        responses: vec![String::new(); n],
                        just_opened: true,
                        remember_key_passphrase: false,
                    });
                    break;
                }
            }
        }

        let mut clipboard = self.clipboard.take();

        let theme = self.theme;
        self.term_theme = TermTheme::from_config(&self.config.terminal_colors);
        let term_theme = self.term_theme;
        let cursor_visible = self.cursor_visible;
        let mut term_font_size = self.config.terminal_font_size;
        if !term_font_size.is_finite() || term_font_size <= 0.0 {
            term_font_size = TERM_FONT_SIZE_DEFAULT;
        }
        term_font_size = term_font_size.clamp(TERM_FONT_SIZE_MIN, TERM_FONT_SIZE_MAX);

        paint_window_chrome(ctx, theme);

        // App-level title bar (used as the window chrome when native decorations are disabled).
        // Keep this global so we don't duplicate controls per split-pane tab bar.
        let mut global_actions: Vec<TilesAction> = Vec::new();
        egui::TopBottomPanel::top("rusty_title_bar")
            .exact_height(TITLE_BAR_H)
            .frame(
                egui::Frame::none()
                    .fill(Color32::TRANSPARENT)
                    .stroke(Stroke::NONE)
                    .inner_margin(egui::Margin::symmetric(TITLE_PAD_X, 1.0)),
            )
            .show(ctx, |ui| {
                ui.visuals_mut().override_text_color = Some(theme.fg);

                // Draggable title bar background. Buttons placed on top will "win" hit-testing.
                let bar_rect = ui.max_rect();
                let drag_resp =
                    ui.interact(bar_rect, Id::new("rusty_title_drag"), Sense::click_and_drag());
                if drag_resp.drag_started() {
                    ctx.send_viewport_cmd(egui::ViewportCommand::StartDrag);
                }
                if drag_resp.double_clicked() {
                    let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                }

                let btn_fill = adjust_color(theme.top_bg, 0.10);
                let icon_btn = |label: egui::RichText| {
                    egui::Button::new(label)
                        .fill(btn_fill)
                        .stroke(Stroke::new(1.0, theme.top_border))
                        .rounding(egui::Rounding::same(8.0))
                        .min_size(Vec2::new(30.0, 24.0))
                };

                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(APP_TITLE_TEXT)
                            .strong()
                            .color(theme.accent)
                            .size(16.0),
                    );

                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        if ui
                            .add(icon_btn(egui::RichText::new("X").size(16.0).color(theme.fg)))
                            .clicked()
                        {
                            global_actions.push(TilesAction::Exit);
                        }

                        let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                        let max_label = if is_max { "\u{2750}" } else { "\u{25A1}" }; // restore / maximize
                        if ui
                            .add(icon_btn(egui::RichText::new(max_label).size(14.0).color(theme.fg)))
                            .clicked()
                        {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                        }

                        // Minimize button (taskbar or tray, depending on settings).
                        if ui
                            .add(icon_btn(egui::RichText::new("_").size(16.0).color(theme.fg)))
                            .clicked()
                        {
                            if self.config.minimize_to_tray {
                                self.minimize_to_tray_requested = true;
                            } else {
                                ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                            }
                        }

                        let target = self.active_tile.or_else(|| self.first_pane_id());
                        let (connecting, connected) = target
                            .and_then(|id| self.pane(id))
                            .map(|t| (t.connecting, t.connected))
                            .unwrap_or((false, false));

                        ui.menu_button(
                            egui::RichText::new("\u{2699}").size(18.0).color(theme.fg),
                            |ui| {
                                let Some(tile_id) = target else {
                                    ui.label(egui::RichText::new("No session").color(theme.muted));
                                    return;
                                };

                                if ui.button("Connect").clicked() {
                                    global_actions.push(TilesAction::Connect(tile_id));
                                    ui.close_menu();
                                }

                                if connecting || connected {
                                    if ui.button("Disconnect").clicked() {
                                        global_actions.push(TilesAction::ToggleConnect(tile_id));
                                        ui.close_menu();
                                    }
                                }

                                ui.separator();
                                if ui.button("Settings").clicked() {
                                    global_actions.push(TilesAction::OpenSettings(tile_id));
                                    ui.close_menu();
                                }

                                ui.separator();
                                if ui.button("Exit").clicked() {
                                    global_actions.push(TilesAction::Exit);
                                    ui.close_menu();
                                }
                            },
                        );
                    });
                });
            });

        let mut behavior = SshTilesBehavior::new(
            theme,
            term_theme,
            cursor_visible,
            term_font_size,
            !self.hidden_to_tray,
            self.config
                .profiles
                .iter()
                .map(|p| (p.name.clone(), config::write_profile_settings(p)))
                .collect(),
            &mut clipboard,
            self.active_tile,
        );

        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(Color32::TRANSPARENT)
                    .inner_margin(egui::Margin {
                        left: CONTENT_PAD,
                        right: CONTENT_PAD,
                        top: 0.0,
                        bottom: CONTENT_PAD,
                    }),
            )
            .show(ctx, |ui| {
                self.tree.ui(&mut behavior, ui);
            });

        self.active_tile = behavior.active_tile;

        // Apply behavior actions after the tree has been drawn.
        let mut actions = global_actions;
        actions.extend(std::mem::take(&mut behavior.actions));
        for action in actions {
            match action {
                TilesAction::NewTab {
                    tabs_container_id,
                    base_pane_id,
                } => {
                    let _ = self.add_new_pane_to_tabs(tabs_container_id, base_pane_id);
                }
                TilesAction::NewTabWithSettings {
                    tabs_container_id,
                    settings,
                    color,
                    profile_name,
                } => {
                    let _ = self.add_new_pane_to_tabs_with_settings(tabs_container_id, settings, color, profile_name);
                }
                TilesAction::TabActivated(tile_id) => {
                    if matches!(self.tree.tiles.get(tile_id), Some(Tile::Pane(_))) {
                        self.active_tile = Some(tile_id);
                        self.settings_dialog.target_tile = Some(tile_id);
                        self.set_focus_next_frame(tile_id);
                    }
                }
                TilesAction::Connect(tile_id) => {
                    let (missing_settings, connected_or_connecting) = self
                        .pane(tile_id)
                        .map(|t| {
                            (
                                t.settings.host.trim().is_empty()
                                    || t.settings.username.trim().is_empty(),
                                t.connecting || t.connected,
                            )
                        })
                        .unwrap_or((true, false));

                    if missing_settings || connected_or_connecting {
                        self.open_settings_dialog_for_tile(tile_id);
                        self.settings_dialog.page = SettingsPage::ProfilesAndAccount;
                    } else if let Some(tab) = self.pane_mut(tile_id) {
                        tab.start_connect();
                        tab.focus_terminal_next_frame = true;
                    }
                    self.active_tile = Some(tile_id);
                    self.settings_dialog.target_tile = Some(tile_id);
                }
                TilesAction::ToggleConnect(tile_id) => {
                    let needs_settings = self
                        .pane(tile_id)
                        .map(|t| {
                            t.settings.host.trim().is_empty()
                                || t.settings.username.trim().is_empty()
                        })
                        .unwrap_or(true);
                    if needs_settings {
                        self.open_settings_dialog_for_tile(tile_id);
                    } else if let Some(tab) = self.pane_mut(tile_id) {
                        if tab.connecting || tab.connected {
                            tab.disconnect();
                        } else {
                            tab.start_connect();
                        }
                        tab.focus_terminal_next_frame = true;
                    }
                    self.active_tile = Some(tile_id);
                    self.settings_dialog.target_tile = Some(tile_id);
                }
                TilesAction::OpenSettings(tile_id) => {
                    self.open_settings_dialog_for_tile(tile_id);
                }
                TilesAction::Rename(tile_id) => {
                    if let Some(tab) = self.pane(tile_id) {
                        let initial = tab
                            .user_title
                            .clone()
                            .unwrap_or_else(|| tab.title.clone());
                        self.rename_popup = Some(RenamePopup {
                            tile_id,
                            value: initial,
                            just_opened: true,
                        });
                    }
                }
                TilesAction::SetColor { pane_id, color } => {
                    if let Some(tab) = self.pane_mut(pane_id) {
                        tab.color = color;
                    }
                }
                TilesAction::Split { pane_id, dir } => {
                    let _ = self.split_pane(pane_id, dir);
                }
                TilesAction::Close(tile_id) => {
                    self.close_pane(tile_id);
                }
                TilesAction::Exit => {
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                }
            }
        }

        // Ensure the active tile is still valid after actions (e.g. closing tabs).
        if let Some(active) = self.active_tile {
            if !matches!(self.tree.tiles.get(active), Some(Tile::Pane(_))) {
                self.active_tile = self.first_pane_id();
            }
        }

        // Rename popup (global).
        let mut rename_action: Option<(TileId, String)> = None;
        let mut close_popup = false;
        if let Some(popup) = &mut self.rename_popup {
            let mut open = true;
            egui::Window::new("Rename Tab")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ctx, |ui| {
                    ui.label("New name:");
                    let resp = ui.text_edit_singleline(&mut popup.value);
                    if popup.just_opened {
                        resp.request_focus();
                        popup.just_opened = false;
                    }

                    let enter = ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if enter {
                        rename_action = Some((popup.tile_id, popup.value.trim().to_string()));
                        close_popup = true;
                    }

                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            close_popup = true;
                        }
                        if ui.button("Rename").clicked() {
                            rename_action = Some((popup.tile_id, popup.value.trim().to_string()));
                            close_popup = true;
                        }
                    });
                });

            if !open {
                close_popup = true;
            }
        }

        if let Some((tile_id, name)) = rename_action {
            if let Some(tab) = self.pane_mut(tile_id) {
                tab.user_title = if name.is_empty() { None } else { Some(name) };
            }
        }

        if close_popup {
            self.rename_popup = None;
        }

        if self.minimize_to_tray_requested {
            self.minimize_to_tray_requested = false;
            self.hide_to_tray(ctx);
        }

        handle_window_resize(ctx);

        self.draw_settings_dialog(ctx);
        self.draw_auth_dialog(ctx);

        self.clipboard = clipboard;
    }
}

fn paint_window_chrome(ctx: &egui::Context, theme: UiTheme) {
    let rect = ctx.screen_rect();
    // Use the shared background layer so our fills are always behind panels/widgets.
    let painter_bg = ctx.layer_painter(egui::LayerId::background());
    // Draw the border/lines above all UI so minimal padding doesn't hide them.
    let painter_fg = ctx.layer_painter(egui::LayerId::new(
        egui::Order::Foreground,
        Id::new("window_chrome_fg"),
    ));

    painter_bg.rect_filled(rect, WINDOW_RADIUS, theme.bg);

    let bar_rect = Rect::from_min_size(rect.min, Vec2::new(rect.width(), TITLE_BAR_H));
    painter_bg.rect_filled(
        bar_rect,
        egui::Rounding {
            nw: WINDOW_RADIUS,
            ne: WINDOW_RADIUS,
            sw: 0.0,
            se: 0.0,
        },
        theme.top_bg,
    );

    painter_fg.rect_stroke(
        rect.shrink(0.5),
        WINDOW_RADIUS,
        Stroke::new(1.0, theme.top_border),
    );
}

fn handle_window_resize(ctx: &egui::Context) {
    let maximized = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
    let fullscreen = ctx.input(|i| i.viewport().fullscreen.unwrap_or(false));
    if maximized || fullscreen {
        return;
    }

    let rect = ctx.screen_rect();
    let Some(pos) = ctx.input(|i| i.pointer.latest_pos()) else {
        return;
    };

    let left = pos.x <= rect.left() + RESIZE_MARGIN;
    let right = pos.x >= rect.right() - RESIZE_MARGIN;
    let top = pos.y <= rect.top() + RESIZE_MARGIN;
    let bottom = pos.y >= rect.bottom() - RESIZE_MARGIN;

    let dir = match (left, right, top, bottom) {
        (true, _, true, _) => Some(egui::ResizeDirection::NorthWest),
        (_, true, true, _) => Some(egui::ResizeDirection::NorthEast),
        (true, _, _, true) => Some(egui::ResizeDirection::SouthWest),
        (_, true, _, true) => Some(egui::ResizeDirection::SouthEast),
        (true, _, _, _) => Some(egui::ResizeDirection::West),
        (_, true, _, _) => Some(egui::ResizeDirection::East),
        (_, _, true, _) => Some(egui::ResizeDirection::North),
        (_, _, _, true) => Some(egui::ResizeDirection::South),
        _ => None,
    };

    let Some(dir) = dir else { return };

    let icon = match dir {
        egui::ResizeDirection::East | egui::ResizeDirection::West => {
            egui::CursorIcon::ResizeHorizontal
        }
        egui::ResizeDirection::North | egui::ResizeDirection::South => {
            egui::CursorIcon::ResizeVertical
        }
        egui::ResizeDirection::NorthEast | egui::ResizeDirection::SouthWest => {
            egui::CursorIcon::ResizeNeSw
        }
        egui::ResizeDirection::NorthWest | egui::ResizeDirection::SouthEast => {
            egui::CursorIcon::ResizeNwSe
        }
    };
    ctx.output_mut(|o| o.cursor_icon = icon);

    if ctx.input(|i| i.pointer.primary_pressed()) {
        ctx.send_viewport_cmd(egui::ViewportCommand::BeginResize(dir));
    }
}

fn xterm_256_color(idx: u8, base: &[Color32; 16]) -> Color32 {
    match idx {
        0..=15 => base[idx as usize],
        16..=231 => {
            let i = idx - 16;
            let r = i / 36;
            let g = (i % 36) / 6;
            let b = i % 6;
            let conv = |v: u8| -> u8 {
                match v {
                    0 => 0,
                    1 => 95,
                    2 => 135,
                    3 => 175,
                    4 => 215,
                    _ => 255,
                }
            };
            Color32::from_rgb(conv(r), conv(g), conv(b))
        }
        232..=255 => {
            let v = 8u8.saturating_add((idx - 232).saturating_mul(10));
            Color32::from_rgb(v, v, v)
        }
    }
}

const TAB_COLOR_PRESETS: [(&str, Color32); 12] = [
    ("Ash", Color32::from_rgb(56, 62, 72)),
    ("Steel", Color32::from_rgb(72, 92, 116)),
    ("Cobalt", Color32::from_rgb(39, 80, 158)),
    ("Ocean", Color32::from_rgb(0, 118, 150)),
    ("Mint", Color32::from_rgb(35, 160, 132)),
    ("Forest", Color32::from_rgb(0, 132, 90)),
    ("Lime", Color32::from_rgb(120, 200, 72)),
    ("Sand", Color32::from_rgb(214, 168, 76)),
    ("Sunset", Color32::from_rgb(223, 99, 72)),
    ("Ember", Color32::from_rgb(190, 54, 54)),
    ("Rose", Color32::from_rgb(198, 64, 131)),
    ("Purple", Color32::from_rgb(120, 78, 191)),
];

fn contrast_text_color(bg: Color32) -> Color32 {
    // Fast gamma-space luma is good enough for picking black/white text.
    let lum = (0.299 * bg.r() as f32 + 0.587 * bg.g() as f32 + 0.114 * bg.b() as f32) / 255.0;
    if lum > 0.62 {
        Color32::from_rgb(18, 18, 18)
    } else {
        Color32::from_rgb(245, 245, 245)
    }
}

fn lerp_color(a: Color32, b: Color32, t: f32) -> Color32 {
    let (ar, ag, ab, aa) = a.to_tuple();
    let (br, bg, bb, _ba) = b.to_tuple();
    let t = t.clamp(0.0, 1.0);
    let lerp = |x: u8, y: u8| -> u8 {
        (x as f32 + (y as f32 - x as f32) * t)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    Color32::from_rgba_premultiplied(lerp(ar, br), lerp(ag, bg), lerp(ab, bb), aa)
}

fn adjust_color(c: Color32, delta: f32) -> Color32 {
    let (r, g, b, a) = c.to_tuple();
    let t = delta.abs().clamp(0.0, 1.0);
    let (tr, tg, tb) = if delta >= 0.0 { (255u8, 255u8, 255u8) } else { (0u8, 0u8, 0u8) };
    let lerp = |x: u8, y: u8| -> u8 {
        (x as f32 + (y as f32 - x as f32) * t)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    Color32::from_rgba_premultiplied(lerp(r, tr), lerp(g, tg), lerp(b, tb), a)
}
