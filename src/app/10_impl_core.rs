impl AppState {
    pub fn new() -> Self {
        let _ = std::fs::create_dir_all("logs");
        let mut config = config::load();
        let config_saver = AsyncConfigSaver::new();
        let (download_event_tx, download_event_rx) = mpsc::channel::<ssh::DownloadManagerEvent>();

        let mut download_jobs: Vec<DownloadJob> = config
            .transfer_history
            .iter()
            .map(Self::download_job_from_history_entry)
            .collect();
        let mut transfer_history_changed = false;
        for job in &mut download_jobs {
            if matches!(job.state, DownloadState::Queued | DownloadState::Running) {
                job.state = DownloadState::Canceled;
                job.speed_bps = 0.0;
                job.message = "Interrupted on previous app shutdown".to_string();
                transfer_history_changed = true;
            }
        }
        if transfer_history_changed {
            config.transfer_history = download_jobs
                .iter()
                .map(Self::download_job_to_history_entry)
                .collect();
            config_saver.request_save(config.clone());
        }
        let next_sftp_request_id = download_jobs
            .iter()
            .map(|j| j.request_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);

        // Resolve the default profile index (if any). If the saved default no longer exists,
        // clear it so we don't get stuck on startup behavior.
        let mut default_profile_idx: Option<usize> = None;
        if let Some(name) = config.default_profile.clone() {
            default_profile_idx = config::find_profile_index(&config, &name);
            if default_profile_idx.is_none() {
                config.default_profile = None;
                config_saver.request_save(config.clone());
            }
        }

        let mut settings_dialog = SettingsDialog::closed();
        let (theme, theme_source) =
            load_ui_theme(config.ui_theme_mode, config.ui_theme_file.as_deref());
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

        // If enabled, restore the previous session layout (splits/tabs) and window geometry.
        // This restores layout + connection settings, and optionally reconnects panes that were connected.
        let mut tree: Option<Tree<SshTab>> = None;
        let mut active_tile: Option<TileId> = None;
        if config.save_session_layout {
            if let Some(json) = config.saved_session_layout_json.clone() {
                if let Ok(restored) =
                    Self::restore_session_tree(&json, config.terminal_scrollback_lines, &config)
                {
                    next_session_id = restored.0;
                    tree = Some(restored.1);
                    active_tile = restored.2;
                }
            }
        }

        let restored = tree.and_then(|t| {
            let pane_id = active_tile
                .filter(|id| matches!(t.tiles.get(*id), Some(Tile::Pane(_))))
                .or_else(|| {
                    t.tiles
                        .iter()
                        .find_map(|(id, tile)| matches!(tile, Tile::Pane(_)).then_some(*id))
                });
            pane_id.map(|pane_id| (t, pane_id))
        });

        let (tree, _first_tile_id, _initial_active_tile) = if let Some((t, pane_id)) = restored {
            settings_dialog.target_tile = Some(pane_id);
            settings_dialog.open = false;
            (t, pane_id, Some(pane_id))
        } else {
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
            let pane_id = tiles.insert_pane(first);
            let root = tiles.insert_tab_tile(vec![pane_id]);
            let tree = Tree::new("ssh_tree", root, tiles);
            settings_dialog.target_tile = Some(pane_id);
            (tree, pane_id, Some(pane_id))
        };

        let mut update_available_version = config.update_available_version.clone();
        let mut update_available_url = config.update_available_url.clone();
        let cached_update_valid = update_available_version
            .as_deref()
            .map(Self::is_newer_version_than_current)
            .unwrap_or(false);
        if !cached_update_valid {
            update_available_version = None;
            update_available_url = None;
            if config.update_available_version.is_some() || config.update_available_url.is_some() {
                config.update_available_version = None;
                config.update_available_url = None;
                config_saver.request_save(config.clone());
            }
        }
        let update_next_check_at = Self::next_update_check_at(config.update_last_check_unix);

        Self {
            theme,
            theme_source,
            term_theme: TermTheme::from_config(&config.terminal_colors),
            config,
            config_saver,
            settings_dialog,
            tree,
            // Start without a focused/active pane; user interaction picks one.
            active_tile: None,
            next_session_id,
            last_cursor_blink: Instant::now(),
            last_terminal_activity: Instant::now(),
            cursor_visible: true,
            tray: None,
            tray_events: crate::tray::install_handlers(),
            hidden_to_tray: false,
            minimize_to_tray_requested: false,
            clipboard: Clipboard::new().ok(),
            rename_popup: None,
            auth_dialog: None,
            host_key_dialog: None,
            transfer_delete_dialog: None,
            style_initialized: false,
            layout_dirty: false,
            last_layout_save: Instant::now(),
            restored_window: false,
            next_sftp_request_id,
            pending_sftp_requests: HashMap::new(),
            downloads_window_open: false,
            downloads_window_just_opened: false,
            download_jobs,
            download_event_tx,
            download_event_rx,
            download_cancel_txs: HashMap::new(),
            upload_refresh_targets: HashMap::new(),
            update_check_in_progress: false,
            update_check_rx: None,
            update_next_check_at,
            update_available_version,
            update_available_url,
            update_manual_open_if_newer: false,
            update_manual_status: None,
        }
    }

    fn now_unix_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    fn parse_version_triplet(raw: &str) -> Option<(u64, u64, u64)> {
        let base = raw
            .trim()
            .trim_start_matches('v')
            .split_once('-')
            .map(|(v, _)| v)
            .unwrap_or(raw.trim().trim_start_matches('v'))
            .split_once('+')
            .map(|(v, _)| v)
            .unwrap_or(raw.trim().trim_start_matches('v'));
        let mut parts = base.split('.');
        let major = parts.next()?.parse::<u64>().ok()?;
        let minor = parts.next()?.parse::<u64>().ok()?;
        let patch = parts.next()?.parse::<u64>().ok()?;
        Some((major, minor, patch))
    }

    fn is_newer_version_than_current(version: &str) -> bool {
        let Some(latest) = Self::parse_version_triplet(version) else {
            return false;
        };
        let Some(current) = Self::parse_version_triplet(env!("CARGO_PKG_VERSION")) else {
            return false;
        };
        latest > current
    }

    fn next_update_check_at(last_check_unix: Option<u64>) -> Instant {
        let now_unix = Self::now_unix_secs();
        let wait_secs = last_check_unix
            .and_then(|last| last.checked_add(UPDATE_CHECK_INTERVAL_SECS))
            .map(|next| next.saturating_sub(now_unix))
            .unwrap_or(0);
        Instant::now() + Duration::from_secs(wait_secs)
    }

    fn start_update_check_if_due(&mut self) {
        self.start_update_check(false);
    }

    fn start_update_check_now_open_if_newer(&mut self) {
        self.update_manual_open_if_newer = true;
        self.update_manual_status = Some("Checking for updates...".to_string());
        self.start_update_check(true);
    }

    fn start_update_check(&mut self, force: bool) {
        if self.update_check_in_progress {
            return;
        }
        if !force && Instant::now() < self.update_next_check_at {
            return;
        }

        self.update_next_check_at = Instant::now() + Duration::from_secs(UPDATE_CHECK_INTERVAL_SECS);
        self.config.update_last_check_unix = Some(Self::now_unix_secs());
        self.config_saver.request_save(self.config.clone());

        let (tx, rx) = mpsc::channel::<UpdateCheckResult>();
        self.update_check_rx = Some(rx);
        self.update_check_in_progress = true;

        std::thread::spawn(move || {
            let mut result = UpdateCheckResult {
                check_succeeded: false,
                available_version: None,
                available_url: None,
            };

            let response = ureq::get(UPDATE_CHECK_API_URL)
                .set("Accept", "application/vnd.github+json")
                .set("User-Agent", "rusty-update-check")
                .call();

            if let Ok(resp) = response {
                if let Ok(json) = resp.into_json::<serde_json::Value>() {
                    let latest_tag = json
                        .get("tag_name")
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string);
                    let latest_url = json
                        .get("html_url")
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| UPDATE_RELEASES_URL.to_string());

                    if let Some(tag) = latest_tag {
                        result.check_succeeded = true;
                        if Self::is_newer_version_than_current(&tag) {
                            result.available_version = Some(tag);
                            result.available_url = Some(latest_url);
                        }
                    }
                }
            }

            let _ = tx.send(result);
        });
    }

    fn poll_update_check_result(&mut self) {
        let Some(rx) = self.update_check_rx.as_ref() else {
            return;
        };

        match rx.try_recv() {
            Ok(result) => {
                let manual_open_if_newer = self.update_manual_open_if_newer;
                self.update_manual_open_if_newer = false;
                self.update_check_in_progress = false;
                self.update_check_rx = None;

                if result.check_succeeded {
                    self.update_available_version = result.available_version.clone();
                    self.update_available_url = result.available_url.clone();
                    self.config.update_available_version = result.available_version;
                    self.config.update_available_url = result.available_url;
                    self.config_saver.request_save(self.config.clone());

                    if self.update_available_version.is_some() {
                        let shown_version = self
                            .update_available_version
                            .as_deref()
                            .map(|v| {
                                if v.starts_with('v') {
                                    v.to_string()
                                } else {
                                    format!("v{v}")
                                }
                            })
                            .unwrap_or_else(|| "newer release".to_string());
                        self.update_manual_status =
                            Some(format!("Update available: {shown_version}"));
                        if manual_open_if_newer {
                            self.open_update_release_page();
                        }
                    } else {
                        self.update_manual_status =
                            Some("You are already on the latest release.".to_string());
                    }
                } else {
                    self.update_manual_status =
                        Some("Update check failed. Please try again.".to_string());
                }
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                self.update_manual_open_if_newer = false;
                self.update_manual_status =
                    Some("Update check failed. Please try again.".to_string());
                self.update_check_in_progress = false;
                self.update_check_rx = None;
            }
        }
    }

    fn open_update_release_page(&self) {
        let url = self
            .update_available_url
            .as_deref()
            .unwrap_or(UPDATE_RELEASES_URL);

        #[cfg(target_os = "windows")]
        let result = std::process::Command::new("cmd")
            .arg("/C")
            .arg("start")
            .arg("")
            .arg(url)
            .spawn();

        #[cfg(target_os = "macos")]
        let result = std::process::Command::new("open").arg(url).spawn();

        #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
        let result = std::process::Command::new("xdg-open").arg(url).spawn();

        if let Err(err) = result {
            crate::logger::log_line(
                "logs\\update.log",
                &format!("Failed to open update URL {url}: {err}"),
            );
        }
    }

    fn transfer_direction_from_config(
        direction: config::TransferDirectionConfig,
    ) -> TransferDirection {
        match direction {
            config::TransferDirectionConfig::Download => TransferDirection::Download,
            config::TransferDirectionConfig::Upload => TransferDirection::Upload,
        }
    }

    fn transfer_direction_to_config(
        direction: TransferDirection,
    ) -> config::TransferDirectionConfig {
        match direction {
            TransferDirection::Download => config::TransferDirectionConfig::Download,
            TransferDirection::Upload => config::TransferDirectionConfig::Upload,
        }
    }

    fn transfer_state_from_config(state: config::TransferStateConfig) -> DownloadState {
        match state {
            config::TransferStateConfig::Queued => DownloadState::Queued,
            config::TransferStateConfig::Running => DownloadState::Running,
            config::TransferStateConfig::Paused => DownloadState::Paused,
            config::TransferStateConfig::Finished => DownloadState::Finished,
            config::TransferStateConfig::Failed => DownloadState::Failed,
            config::TransferStateConfig::Canceled => DownloadState::Canceled,
        }
    }

    fn transfer_state_to_config(state: DownloadState) -> config::TransferStateConfig {
        match state {
            DownloadState::Queued => config::TransferStateConfig::Queued,
            DownloadState::Running => config::TransferStateConfig::Running,
            DownloadState::Paused => config::TransferStateConfig::Paused,
            DownloadState::Finished => config::TransferStateConfig::Finished,
            DownloadState::Failed => config::TransferStateConfig::Failed,
            DownloadState::Canceled => config::TransferStateConfig::Canceled,
        }
    }

    fn download_job_from_history_entry(entry: &config::TransferHistoryEntry) -> DownloadJob {
        DownloadJob {
            request_id: entry.request_id,
            direction: Self::transfer_direction_from_config(entry.direction),
            settings: entry.settings.clone(),
            remote_path: entry.remote_path.clone(),
            local_path: entry.local_path.clone(),
            downloaded_bytes: entry.transferred_bytes,
            total_bytes: entry.total_bytes,
            speed_bps: entry.speed_bps,
            state: Self::transfer_state_from_config(entry.state),
            message: entry.message.clone(),
        }
    }

    fn download_job_to_history_entry(job: &DownloadJob) -> config::TransferHistoryEntry {
        config::TransferHistoryEntry {
            request_id: job.request_id,
            direction: Self::transfer_direction_to_config(job.direction),
            settings: job.settings.clone(),
            remote_path: job.remote_path.clone(),
            local_path: job.local_path.clone(),
            transferred_bytes: job.downloaded_bytes,
            total_bytes: job.total_bytes,
            speed_bps: job.speed_bps,
            state: Self::transfer_state_to_config(job.state),
            message: job.message.clone(),
        }
    }

    fn persist_transfer_history(&mut self) {
        self.config.transfer_history = self
            .download_jobs
            .iter()
            .map(Self::download_job_to_history_entry)
            .collect();
        self.config_saver.request_save(self.config.clone());
    }

    fn open_downloads_window(&mut self) {
        if !self.downloads_window_open {
            self.downloads_window_just_opened = true;
        }
        self.downloads_window_open = true;
    }

    fn apply_global_style(&self, ctx: &egui::Context) {
        // Ensure default widget visuals and window title bars are readable against our dark theme.
        let mut style = (*ctx.style()).clone();
        let bg_luma = (0.299 * self.theme.bg.r() as f32
            + 0.587 * self.theme.bg.g() as f32
            + 0.114 * self.theme.bg.b() as f32)
            / 255.0;
        style.visuals = if bg_luma < 0.52 {
            egui::Visuals::dark()
        } else {
            egui::Visuals::light()
        };
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

    fn terminal_pane_ids(&self) -> Vec<TileId> {
        self.pane_ids()
            .into_iter()
            .filter(|id| self.pane(*id).map(|t| t.is_terminal()).unwrap_or(false))
            .collect()
    }

    fn first_pane_id(&self) -> Option<TileId> {
        self.tree.tiles.iter().find_map(|(id, tile)| match tile {
            Tile::Pane(_) => Some(*id),
            _ => None,
        })
    }

    fn first_terminal_pane_id(&self) -> Option<TileId> {
        self.terminal_pane_ids().into_iter().next()
    }

    fn cog_target_tile(&self) -> Option<TileId> {
        let valid_terminal_pane = |id: TileId| {
            matches!(
                self.tree.tiles.get(id),
                Some(Tile::Pane(tab)) if tab.is_terminal()
            )
        };

        if let Some(id) = self.settings_dialog.target_tile {
            if valid_terminal_pane(id) {
                return Some(id);
            }
        }
        if let Some(id) = self.active_tile {
            if valid_terminal_pane(id) {
                return Some(id);
            }
        }

        if let Some(id) = self.terminal_pane_ids().into_iter().find(|id| {
            self.pane(*id)
                .map(|t| t.connected || t.connecting)
                .unwrap_or(false)
        }) {
            return Some(id);
        }

        self.first_terminal_pane_id()
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

    fn terminal_pane(&self, tile_id: TileId) -> Option<&SshTab> {
        let pane = self.pane(tile_id)?;
        pane.is_terminal().then_some(pane)
    }

    fn terminal_pane_mut(&mut self, tile_id: TileId) -> Option<&mut SshTab> {
        let pane = self.pane_mut(tile_id)?;
        if pane.is_terminal() {
            Some(pane)
        } else {
            None
        }
    }

    fn file_pane(&self, tile_id: TileId) -> Option<&FileBrowserState> {
        self.pane(tile_id).and_then(|tab| tab.file_browser())
    }

    fn file_pane_mut(&mut self, tile_id: TileId) -> Option<&mut FileBrowserState> {
        self.pane_mut(tile_id)
            .and_then(|tab| tab.file_browser_mut())
    }

    fn ensure_tree_non_empty(&mut self) {
        if self.tree.root.is_some() && self.first_pane_id().is_some() {
            return;
        }

        let pane_id = self.create_pane(
            ConnectionSettings::default(),
            None,
            None,
            self.config.terminal_scrollback_lines,
        );
        let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
        self.tree.root = Some(root);
        self.active_tile = Some(pane_id);
        self.settings_dialog.target_tile = Some(pane_id);
    }

    fn restore_session_tree(
        json: &str,
        default_scrollback_len: usize,
        cfg: &config::AppConfig,
    ) -> anyhow::Result<(u64, Tree<SshTab>, Option<TileId>)> {
        let restored: PersistedSession = serde_json::from_str(json)?;
        let Some(root) = restored.tree.root else {
            return Err(anyhow::anyhow!("Saved layout had no root"));
        };

        let mut tiles: Tiles<SshTab> = Tiles::default();
        let mut max_session_id = 0u64;
        let mut to_autoconnect_terminals: Vec<TileId> = Vec::new();
        let mut to_connect_file_managers: Vec<TileId> = Vec::new();

        for (tile_id, tile) in restored.tree.tiles.iter() {
            match tile {
                Tile::Pane(p) => {
                    max_session_id = max_session_id.max(p.id);
                    let scrollback_len = if p.scrollback_len == 0 {
                        default_scrollback_len
                    } else {
                        p.scrollback_len
                    };
                    let settings = p
                        .profile_name
                        .as_deref()
                        .and_then(|name| config::find_profile_index(cfg, name))
                        .and_then(|i| cfg.profiles.get(i))
                        .map(config::write_profile_settings)
                        .unwrap_or_else(|| p.settings.clone());
                    let mut tab = match &p.pane_kind {
                        PersistedPaneKind::Terminal => {
                            let mut tab = SshTab::new(
                                p.id,
                                settings,
                                p.profile_name.clone(),
                                scrollback_len,
                                format!("logs\\tab-{}.log", p.id),
                            );
                            tab.title = SshTab::title_for(p.id, &tab.settings);
                            if p.autoconnect {
                                to_autoconnect_terminals.push(*tile_id);
                            }
                            tab
                        }
                        PersistedPaneKind::FileManager {
                            source_terminal,
                            path,
                        } => SshTab::new_file_manager(
                            p.id,
                            settings,
                            p.profile_name.clone(),
                            p.color,
                            *source_terminal,
                            path.clone(),
                        ),
                    };
                    tab.user_title = p.user_title.clone();
                    tab.color = p.color;
                    tab.focus_terminal_next_frame = false;
                    if tab.is_file_manager() {
                        tab.last_status = "Not connected".to_string();
                        to_connect_file_managers.push(*tile_id);
                    }
                    tiles.insert(*tile_id, Tile::Pane(tab));
                }
                Tile::Container(c) => {
                    tiles.insert(*tile_id, Tile::Container(c.clone()));
                }
            }
        }

        let mut tree = Tree::new("ssh_tree", root, tiles);

        for id in to_autoconnect_terminals {
            if let Some(Tile::Pane(tab)) = tree.tiles.get_mut(id) {
                if tab.is_terminal()
                    && !tab.settings.host.trim().is_empty()
                    && !tab.settings.username.trim().is_empty()
                {
                    tab.start_connect();
                }
            }
        }
        for id in to_connect_file_managers {
            if let Some(Tile::Pane(tab)) = tree.tiles.get_mut(id) {
                if tab.is_file_manager()
                    && !tab.settings.host.trim().is_empty()
                    && !tab.settings.username.trim().is_empty()
                {
                    tab.start_connect();
                }
            }
        }

        let next_session_id = max_session_id.saturating_add(1).max(1);
        Ok((next_session_id, tree, restored.active_tile))
    }

    fn persist_session_tree(&self) -> Option<String> {
        let root = self.tree.root?;
        let mut tiles: Tiles<PersistedTab> = Tiles::default();

        for (tile_id, tile) in self.tree.tiles.iter() {
            match tile {
                Tile::Pane(tab) => {
                    let pane_kind = match &tab.kind {
                        PaneKind::Terminal => PersistedPaneKind::Terminal,
                        PaneKind::FileManager(file) => PersistedPaneKind::FileManager {
                            source_terminal: file.source_terminal,
                            path: file.cwd.clone(),
                        },
                    };
                    let p = PersistedTab {
                        id: tab.id,
                        user_title: tab.user_title.clone(),
                        color: tab.color,
                        profile_name: tab.profile_name.clone(),
                        settings: tab.settings.clone(),
                        scrollback_len: tab.scrollback_len,
                        autoconnect: tab.is_terminal() && (tab.connected || tab.connecting),
                        pane_kind,
                    };
                    tiles.insert(*tile_id, Tile::Pane(p));
                }
                Tile::Container(c) => {
                    tiles.insert(*tile_id, Tile::Container(c.clone()));
                }
            }
        }

        let session = PersistedSession {
            tree: Tree::new("ssh_tree", root, tiles),
            active_tile: self.active_tile,
        };

        serde_json::to_string(&session).ok()
    }

    fn maybe_restore_window(&mut self, ctx: &egui::Context) {
        if self.restored_window {
            return;
        }
        self.restored_window = true;

        if !self.config.save_session_layout {
            return;
        }
        let Some(sw) = self.config.saved_window else {
            return;
        };

        // Restore normal geometry first, then maximize if requested.
        ctx.send_viewport_cmd(egui::ViewportCommand::OuterPosition(Pos2::new(
            sw.outer_pos[0],
            sw.outer_pos[1],
        )));
        ctx.send_viewport_cmd(egui::ViewportCommand::InnerSize(Vec2::new(
            sw.inner_size[0].max(100.0),
            sw.inner_size[1].max(100.0),
        )));
        if sw.maximized {
            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(true));
        }
    }

    fn maybe_save_session_layout(&mut self, ctx: &egui::Context) {
        if !self.config.save_session_layout {
            return;
        }

        // Save at most twice per second to avoid excessive disk writes.
        if !self.layout_dirty && self.last_layout_save.elapsed() < Duration::from_secs(2) {
            return;
        }
        if self.last_layout_save.elapsed() < Duration::from_millis(500) {
            return;
        }

        // Capture window geometry, but avoid overwriting the saved normal size while maximized/fullscreen.
        let maximized = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
        let fullscreen = ctx.input(|i| i.viewport().fullscreen.unwrap_or(false));
        if !fullscreen {
            if let (Some(outer), Some(inner)) =
                ctx.input(|i| (i.viewport().outer_rect, i.viewport().inner_rect))
            {
                if !maximized {
                    self.config.saved_window = Some(config::SavedWindow {
                        outer_pos: [outer.min.x, outer.min.y],
                        inner_size: [inner.width(), inner.height()],
                        maximized: false,
                    });
                } else if let Some(mut sw) = self.config.saved_window {
                    sw.maximized = true;
                    self.config.saved_window = Some(sw);
                } else {
                    self.config.saved_window = Some(config::SavedWindow {
                        outer_pos: [outer.min.x, outer.min.y],
                        inner_size: [inner.width(), inner.height()],
                        maximized: true,
                    });
                }
            }
        }

        if let Some(json) = self.persist_session_tree() {
            self.config.saved_session_layout_json = Some(json);
            self.config_saver.request_save(self.config.clone());
            self.layout_dirty = false;
            self.last_layout_save = Instant::now();
        }
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

    fn remote_join_path(base: &str, name: &str) -> String {
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

    fn remote_parent_path(path: &str) -> String {
        let path = path.trim();
        if path.is_empty() || path == "." || path == "/" {
            return "/".to_string();
        }
        let trimmed = path.trim_end_matches('/');
        if let Some((parent, _)) = trimmed.rsplit_once('/') {
            if parent.is_empty() {
                "/".to_string()
            } else {
                parent.to_string()
            }
        } else {
            ".".to_string()
        }
    }

    fn sender_for_file_tile(
        &self,
        file_tile: TileId,
    ) -> Option<(TileId, Sender<WorkerMessage>)> {
        let pane = self.pane(file_tile)?;
        if !pane.is_file_manager() || !pane.connected {
            return None;
        }
        let tx = pane.worker_tx.as_ref()?.clone();
        Some((file_tile, tx))
    }

    fn next_sftp_request_for_tile(&mut self, tile_id: TileId) -> u64 {
        let request_id = self.alloc_sftp_request_id();
        self.pending_sftp_requests.insert(request_id, tile_id);
        request_id
    }

    fn alloc_sftp_request_id(&mut self) -> u64 {
        let request_id = self.next_sftp_request_id;
        self.next_sftp_request_id = self.next_sftp_request_id.saturating_add(1).max(1);
        request_id
    }

    fn send_file_command(
        &mut self,
        file_tile: TileId,
        make_cmd: impl FnOnce(u64) -> ssh::SftpCommand,
        busy_status: String,
    ) {
        let request_id = self.next_sftp_request_for_tile(file_tile);
        if let Some(file) = self.file_pane_mut(file_tile) {
            file.busy = true;
            file.status = busy_status;
        }

        let Some((_source_tile, tx)) = self.sender_for_file_tile(file_tile) else {
            self.pending_sftp_requests.remove(&request_id);
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.busy = false;
                file.status = "SFTP session is not connected".to_string();
                file.source_connected = false;
            }
            return;
        };

        let cmd = make_cmd(request_id);
        if tx.send(WorkerMessage::SftpCommand(cmd)).is_err() {
            self.pending_sftp_requests.remove(&request_id);
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.busy = false;
                file.status = "Failed to send SFTP command".to_string();
            }
        }
    }

    fn open_file_manager_for_terminal(&mut self, source_tile: TileId) -> Option<TileId> {
        let source = self.terminal_pane(source_tile)?;
        let settings = source.settings.clone();
        let profile_name = source.profile_name.clone();
        let color = source.color;

        let id = self.next_session_id;
        self.next_session_id += 1;
        let pane = SshTab::new_file_manager(
            id,
            settings,
            profile_name,
            color,
            source_tile,
            ".".to_string(),
        );
        let pane_id = self.tree.tiles.insert_pane(pane);

        if let Some(parent_id) = self.tree.tiles.parent_of(source_tile) {
            if let Some(Tile::Container(Container::Tabs(tabs))) = self.tree.tiles.get_mut(parent_id)
            {
                tabs.children.push(pane_id);
                tabs.set_active(pane_id);
            } else {
                let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
                self.tree.root = Some(root);
            }
        } else {
            let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
            self.tree.root = Some(root);
        }

        self.active_tile = Some(pane_id);
        self.settings_dialog.target_tile = Some(source_tile);
        if let Some(tab) = self.pane_mut(pane_id) {
            tab.start_connect();
        }
        Some(pane_id)
    }

    fn request_file_list(&mut self, file_tile: TileId, path: String) {
        let path = path.trim();
        let path = if path.is_empty() { "." } else { path };
        if let Some(file) = self.file_pane_mut(file_tile) {
            file.path_input = path.to_string();
        }
        self.send_file_command(
            file_tile,
            |request_id| ssh::SftpCommand::ListDir {
                request_id,
                path: path.to_string(),
            },
            format!("Listing {path} ..."),
        );
    }

    fn request_file_up(&mut self, file_tile: TileId) {
        let path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_parent_path(&f.cwd))
            .unwrap_or_else(|| "/".to_string());
        self.request_file_list(file_tile, path);
    }

    fn request_file_mkdir(&mut self, file_tile: TileId, dir_name: String) {
        let dir_name = dir_name.trim().to_string();
        if dir_name.is_empty() {
            return;
        }
        let full_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &dir_name))
            .unwrap_or(dir_name.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::MakeDir {
                request_id,
                path: full_path.clone(),
            },
            format!("Creating folder {} ...", dir_name),
        );
    }

    fn request_file_rename(&mut self, file_tile: TileId, from_name: String, to_name: String) {
        let from_name = from_name.trim().to_string();
        let to_name = to_name.trim().to_string();
        if from_name.is_empty() || to_name.is_empty() {
            return;
        }
        let (old_path, new_path) = self
            .file_pane(file_tile)
            .map(|f| {
                (
                    Self::remote_join_path(&f.cwd, &from_name),
                    Self::remote_join_path(&f.cwd, &to_name),
                )
            })
            .unwrap_or_else(|| (from_name.clone(), to_name.clone()));
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::Rename {
                request_id,
                old_path: old_path.clone(),
                new_path: new_path.clone(),
            },
            format!("Renaming {} ...", from_name),
        );
    }

    fn request_file_delete(&mut self, file_tile: TileId, name: String, is_dir: bool) {
        let name = name.trim().to_string();
        if name.is_empty() {
            return;
        }
        let full_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &name))
            .unwrap_or(name.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::Delete {
                request_id,
                path: full_path.clone(),
                is_dir,
            },
            format!("Deleting {} ...", name),
        );
    }

    fn start_upload_for_file(&mut self, file_tile: TileId) -> bool {
        let settings = self.pane(file_tile).map(|p| p.settings.clone());
        let Some(settings) = settings else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status = "Cannot resolve connection settings for upload".to_string();
            }
            return false;
        };

        let mut dlg = rfd::FileDialog::new();
        if let Some(profile_dir) = user_profile_dir() {
            dlg = dlg.set_directory(profile_dir);
        }
        let Some(local_path) = dlg.pick_file() else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status = "Upload cancelled".to_string();
            }
            return false;
        };
        let local_path = local_path.display().to_string();
        if !std::path::Path::new(&local_path).is_file() {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status = format!("Local file does not exist: {local_path}");
            }
            return false;
        }

        let Some(file_name) = std::path::Path::new(&local_path)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .filter(|s| !s.trim().is_empty())
        else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status = "Invalid local upload file name".to_string();
            }
            return false;
        };

        let remote_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &file_name))
            .unwrap_or(file_name.clone());
        let request_id = self.alloc_sftp_request_id();
        self.download_jobs.push(DownloadJob {
            request_id,
            direction: TransferDirection::Upload,
            settings: settings.clone(),
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            downloaded_bytes: 0,
            total_bytes: None,
            speed_bps: 0.0,
            state: DownloadState::Queued,
            message: "Queued".to_string(),
        });

        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        self.download_cancel_txs.insert(request_id, cancel_tx);
        self.upload_refresh_targets.insert(request_id, file_tile);
        let log_path = format!("logs\\upload-{request_id}.log");
        let _ = ssh::start_sftp_upload_detached(
            settings,
            request_id,
            remote_path,
            local_path,
            self.download_event_tx.clone(),
            cancel_rx,
            log_path,
        );
        self.persist_transfer_history();

        if let Some(file) = self.file_pane_mut(file_tile) {
            file.status = format!("Upload queued: {file_name}");
        }
        self.open_downloads_window();
        true
    }

    fn start_download_for_file(&mut self, file_tile: TileId, name: String) {
        let name = name.trim().to_string();
        if name.is_empty() {
            return;
        }
        let remote_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &name))
            .unwrap_or(name.clone());
        let settings = self.pane(file_tile).map(|p| p.settings.clone());
        let Some(settings) = settings else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status = "Cannot resolve connection settings for download".to_string();
            }
            return;
        };

        let default_name = std::path::Path::new(&name)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(name.clone());
        let mut dlg = rfd::FileDialog::new().set_file_name(&default_name);
        if let Some(profile_dir) = user_profile_dir() {
            dlg = dlg.set_directory(profile_dir);
        }
        let Some(local_path) = dlg.save_file() else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status = "Download cancelled".to_string();
            }
            return;
        };
        let local_path = local_path.display().to_string();

        let request_id = self.alloc_sftp_request_id();
        self.download_jobs.push(DownloadJob {
            request_id,
            direction: TransferDirection::Download,
            settings: settings.clone(),
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            downloaded_bytes: 0,
            total_bytes: None,
            speed_bps: 0.0,
            state: DownloadState::Queued,
            message: "Queued".to_string(),
        });

        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        self.download_cancel_txs.insert(request_id, cancel_tx);
        let log_path = format!("logs\\download-{request_id}.log");

        let _ = ssh::start_sftp_download_detached(
            settings,
            request_id,
            remote_path,
            local_path,
            false,
            self.download_event_tx.clone(),
            cancel_rx,
            log_path,
        );
        self.persist_transfer_history();

        if let Some(file) = self.file_pane_mut(file_tile) {
            file.status = format!("Download queued: {name}");
        }
        self.open_downloads_window();
    }

    fn retry_download_job(&mut self, request_id: u64) {
        let Some(job_idx) = self
            .download_jobs
            .iter()
            .position(|j| j.request_id == request_id)
        else {
            return;
        };

        if matches!(
            self.download_jobs[job_idx].state,
            DownloadState::Queued | DownloadState::Running
        ) {
            return;
        }

        let settings = self.download_jobs[job_idx].settings.clone();
        let direction = self.download_jobs[job_idx].direction;
        let remote_path = self.download_jobs[job_idx].remote_path.clone();
        let local_path = self.download_jobs[job_idx].local_path.clone();

        self.download_cancel_txs.remove(&request_id);
        let upload_refresh_target = self.upload_refresh_targets.remove(&request_id);

        let new_request_id = self.alloc_sftp_request_id();
        {
            let job = &mut self.download_jobs[job_idx];
            job.request_id = new_request_id;
            job.downloaded_bytes = 0;
            job.total_bytes = None;
            job.speed_bps = 0.0;
            job.state = DownloadState::Queued;
            job.message = "Retrying...".to_string();
        }

        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        self.download_cancel_txs.insert(new_request_id, cancel_tx);
        let _ = match direction {
            TransferDirection::Download => {
                let log_path = format!("logs\\download-{new_request_id}.log");
                ssh::start_sftp_download_detached(
                    settings,
                    new_request_id,
                    remote_path,
                    local_path,
                    true,
                    self.download_event_tx.clone(),
                    cancel_rx,
                    log_path,
                )
            }
            TransferDirection::Upload => {
                if let Some(tile_id) = upload_refresh_target {
                    self.upload_refresh_targets.insert(new_request_id, tile_id);
                }
                let log_path = format!("logs\\upload-{new_request_id}.log");
                ssh::start_sftp_upload_detached(
                    settings,
                    new_request_id,
                    remote_path,
                    local_path,
                    self.download_event_tx.clone(),
                    cancel_rx,
                    log_path,
                )
            }
        };
        self.persist_transfer_history();
    }

    fn open_download_job_folder(&mut self, request_id: u64) {
        let Some(job) = self.download_jobs.iter_mut().find(|j| j.request_id == request_id) else {
            return;
        };

        let local_path = std::path::PathBuf::from(job.local_path.clone());
        let file_exists = local_path.is_file();
        let mut target_dir = local_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        if target_dir.as_os_str().is_empty() {
            target_dir = std::path::PathBuf::from(".");
        }

        #[cfg(target_os = "windows")]
        let result = if file_exists {
            std::process::Command::new("explorer")
                .arg(format!("/select,{}", local_path.display()))
                .spawn()
        } else {
            std::process::Command::new("explorer")
                .arg(target_dir.as_os_str())
                .spawn()
        };

        #[cfg(target_os = "macos")]
        let result = if file_exists {
            std::process::Command::new("open")
                .arg("-R")
                .arg(local_path.as_os_str())
                .spawn()
        } else {
            std::process::Command::new("open")
                .arg(target_dir.as_os_str())
                .spawn()
        };

        #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
        let result = std::process::Command::new("xdg-open")
            .arg(target_dir.as_os_str())
            .spawn();

        if let Err(err) = result {
            job.message = format!("Failed to open folder: {err}");
        }
    }

    fn remove_download_job(&mut self, request_id: u64) {
        if let Some(tx) = self.download_cancel_txs.remove(&request_id) {
            let _ = tx.send(());
        }
        self.upload_refresh_targets.remove(&request_id);

        let Some(job_idx) = self
            .download_jobs
            .iter()
            .position(|j| j.request_id == request_id)
        else {
            return;
        };

        let direction = self.download_jobs[job_idx].direction;
        let local_path = self.download_jobs[job_idx].local_path.clone();
        if direction == TransferDirection::Download && !local_path.trim().is_empty() {
            std::thread::spawn(move || {
                let path = std::path::PathBuf::from(local_path);
                for _ in 0..20 {
                    match std::fs::remove_file(&path) {
                        Ok(_) => return,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
                        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                        Err(_) => return,
                    }
                }
                let _ = std::fs::remove_file(&path);
            });
        }
        self.download_jobs.remove(job_idx);
        self.persist_transfer_history();
    }

    fn dismiss_transfer_job(&mut self, request_id: u64) {
        let Some(job_idx) = self
            .download_jobs
            .iter()
            .position(|j| j.request_id == request_id)
        else {
            return;
        };
        if matches!(
            self.download_jobs[job_idx].state,
            DownloadState::Queued | DownloadState::Running
        ) {
            return;
        }
        self.download_cancel_txs.remove(&request_id);
        self.upload_refresh_targets.remove(&request_id);
        self.download_jobs.remove(job_idx);
        self.persist_transfer_history();
    }

    fn has_active_downloads(&self) -> bool {
        self.download_jobs
            .iter()
            .any(|j| matches!(j.state, DownloadState::Queued | DownloadState::Running))
    }

    fn cancel_download_job(&mut self, request_id: u64) {
        if let Some(tx) = self.download_cancel_txs.get(&request_id) {
            let _ = tx.send(());
        }
        let mut changed = false;
        if let Some(job) = self
            .download_jobs
            .iter_mut()
            .find(|j| j.request_id == request_id)
        {
            if matches!(job.state, DownloadState::Queued | DownloadState::Running) {
                job.message = "Canceling...".to_string();
                changed = true;
            }
        }
        if changed {
            self.persist_transfer_history();
        }
    }

    fn poll_download_manager_events(&mut self) {
        let mut persist_needed = false;
        let mut refresh_upload_tiles: Vec<TileId> = Vec::new();
        while let Ok(event) = self.download_event_rx.try_recv() {
            match event {
                ssh::DownloadManagerEvent::Started {
                    request_id,
                    remote_path,
                    local_path,
                    downloaded_bytes,
                    total_bytes,
                } => {
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Running;
                        job.remote_path = remote_path;
                        job.local_path = local_path;
                        job.downloaded_bytes = downloaded_bytes;
                        job.total_bytes = total_bytes;
                        job.speed_bps = 0.0;
                        job.message = match job.direction {
                            TransferDirection::Download => {
                                if downloaded_bytes > 0 {
                                    "Resuming download...".to_string()
                                } else {
                                    "Downloading...".to_string()
                                }
                            }
                            TransferDirection::Upload => {
                                if downloaded_bytes > 0 {
                                    "Resuming upload...".to_string()
                                } else {
                                    "Uploading...".to_string()
                                }
                            }
                        };
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Progress {
                    request_id,
                    downloaded_bytes,
                    total_bytes,
                    speed_bps,
                } => {
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Running;
                        job.downloaded_bytes = downloaded_bytes;
                        job.total_bytes = total_bytes;
                        job.speed_bps = speed_bps;
                        job.message = match job.direction {
                            TransferDirection::Download => "Downloading...".to_string(),
                            TransferDirection::Upload => "Uploading...".to_string(),
                        };
                    }
                }
                ssh::DownloadManagerEvent::Retrying {
                    request_id,
                    attempt,
                    max_attempts,
                    delay_ms,
                    message,
                } => {
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Running;
                        job.speed_bps = 0.0;
                        let delay_secs = delay_ms as f64 / 1000.0;
                        job.message = format!(
                            "{message} (retry {attempt}/{max_attempts} in {delay_secs:.1}s)"
                        );
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Finished {
                    request_id,
                    local_path,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    let upload_refresh_target = self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Finished;
                        job.speed_bps = 0.0;
                        if let Some(total) = job.total_bytes {
                            job.downloaded_bytes = total;
                        }
                        job.message = match job.direction {
                            TransferDirection::Download => format!("Saved to {local_path}"),
                            TransferDirection::Upload => {
                                format!("Uploaded to {}", job.remote_path)
                            }
                        };
                        if job.direction == TransferDirection::Upload {
                            if let Some(tile_id) = upload_refresh_target {
                                refresh_upload_tiles.push(tile_id);
                            }
                        }
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Failed {
                    request_id,
                    message,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Failed;
                        job.speed_bps = 0.0;
                        job.message = message;
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Paused {
                    request_id,
                    message,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Paused;
                        job.speed_bps = 0.0;
                        job.message = message;
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Canceled {
                    request_id,
                    local_path,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Canceled;
                        job.speed_bps = 0.0;
                        job.message = match job.direction {
                            TransferDirection::Download => format!("Canceled ({local_path})"),
                            TransferDirection::Upload => {
                                format!("Upload canceled ({local_path})")
                            }
                        };
                        persist_needed = true;
                    }
                }
            }
        }
        if persist_needed {
            self.persist_transfer_history();
        }
        for tile_id in refresh_upload_tiles {
            if self.file_pane(tile_id).is_none() {
                continue;
            }
            let path = self
                .file_pane(tile_id)
                .map(|f| f.cwd.clone())
                .unwrap_or_else(|| ".".to_string());
            self.request_file_list(tile_id, path);
        }
    }

    fn sync_file_panes_with_sources(&mut self) {
        let pane_ids = self.pane_ids();
        let mut clear_pending_for: Vec<TileId> = Vec::new();
        let mut refresh_on_connect: Vec<(TileId, String)> = Vec::new();
        for pane_id in pane_ids {
            let Some(tab) = self.pane(pane_id) else {
                continue;
            };
            if !tab.is_file_manager() {
                continue;
            }
            let connected = tab.connected;
            let connecting = tab.connecting;
            let last_status = tab.last_status.clone();

            if let Some(file) = self.file_pane_mut(pane_id) {
                let was_connected = file.source_connected;
                file.source_connected = connected;

                if connected && !was_connected && !file.busy {
                    let pending_path = file.path_input.trim();
                    let path = if pending_path.is_empty() {
                        file.cwd.clone()
                    } else {
                        pending_path.to_string()
                    };
                    refresh_on_connect.push((pane_id, path));
                }

                if !connected && !connecting {
                    if file.busy {
                        file.busy = false;
                    }
                    clear_pending_for.push(pane_id);
                }

                if !connected && !file.busy {
                    file.status = if connecting {
                        "Connecting SFTP session...".to_string()
                    } else if !last_status.trim().is_empty() {
                        last_status.clone()
                    } else {
                        "SFTP session is not connected".to_string()
                    };
                }
            }
        }

        if !clear_pending_for.is_empty() {
            self.pending_sftp_requests
                .retain(|_, file_tile| !clear_pending_for.contains(file_tile));
        }
        for (tile_id, path) in refresh_on_connect {
            if self.file_pane(tile_id).is_some() {
                self.request_file_list(tile_id, path);
            }
        }
    }

    fn route_sftp_events(&mut self) {
        let pane_ids = self.pane_ids();
        let mut events: Vec<ssh::SftpEvent> = Vec::new();

        for tile_id in pane_ids {
            if let Some(tab) = self.pane_mut(tile_id) {
                events.extend(std::mem::take(&mut tab.pending_sftp_events));
            }
        }

        for event in events {
            match event {
                ssh::SftpEvent::ListDir {
                    request_id,
                    path,
                    entries,
                } => {
                    let target_tile = self.pending_sftp_requests.remove(&request_id);
                    let Some(tile_id) = target_tile else { continue };
                    let Some(file) = self.file_pane_mut(tile_id) else {
                        continue;
                    };
                    file.busy = false;
                    file.cwd = path.clone();
                    file.path_input = path;
                    file.entries = entries;
                    file.selected_name = None;
                    file.rename_to.clear();
                    file.status = format!("{} item(s)", file.entries.len());
                }
                ssh::SftpEvent::OperationOk {
                    request_id,
                    message,
                } => {
                    let target_tile = self.pending_sftp_requests.remove(&request_id);
                    let Some(tile_id) = target_tile else { continue };
                    let Some(file) = self.file_pane_mut(tile_id) else {
                        continue;
                    };
                    file.busy = false;
                    file.status = message;
                }
                ssh::SftpEvent::OperationErr {
                    request_id,
                    message,
                } => {
                    let target_tile = self.pending_sftp_requests.remove(&request_id);
                    let Some(tile_id) = target_tile else { continue };
                    let Some(file) = self.file_pane_mut(tile_id) else {
                        continue;
                    };
                    file.busy = false;
                    file.status = message;
                }
            }
        }
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
                    (
                        p.settings.clone(),
                        p.color,
                        p.profile_name.clone(),
                        p.scrollback_len,
                    )
                })
            })
            .unwrap_or((
                ConnectionSettings::default(),
                None,
                None,
                self.config.terminal_scrollback_lines,
            ));

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
            if let Some(tray) = self.tray.as_ref() {
                tray.set_visible(false);
            }
            self.tray = None;
            self.hidden_to_tray = false;
            crate::tray::set_hidden_to_tray_state(false);
            return;
        }

        if self.tray.is_none() {
            self.tray = crate::tray::create_tray().ok();
            if let Some(tray) = self.tray.as_ref() {
                // Keep this as an idempotent action label.
                tray.show_hide_item.set_text("Show Rusty");
                tray.set_visible(false);
            }
        }
    }

    fn hide_to_tray(&mut self, ctx: &egui::Context) {
        crate::tray::capture_foreground_hwnd();
        self.ensure_tray_icon();
        if self.tray.is_none() {
            // If tray creation failed, fall back to a normal minimize.
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
            return;
        }

        // Minimize first, then hide so we avoid some backends getting "stuck" invisible.
        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
        ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
        self.hidden_to_tray = true;
        crate::tray::set_hidden_to_tray_state(true);
        if let Some(tray) = self.tray.as_ref() {
            tray.show_hide_item.set_text("Show Rusty");
            tray.set_visible(true);
        }
        // Keep the app responsive to tray events even while hidden.
        ctx.request_repaint_after(Duration::from_millis(200));
        ctx.request_repaint();
    }

    fn show_from_tray(&mut self, ctx: &egui::Context) {
        // Un-minimize first (important for some platforms where hidden+minimized windows won't show)
        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
        // Then make visible and focus
        ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
        ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
        self.hidden_to_tray = false;
        crate::tray::set_hidden_to_tray_state(false);

        // Force a fresh PTY resize on restore, in case the window was hidden/minimized with a
        // transient tiny size and the terminal would otherwise stay "blank" until output arrives.
        for id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(id) {
                tab.last_sent_size = None;
            }
        }
        ctx.request_repaint();
        ctx.request_repaint_after(Duration::from_millis(16));

        if let Some(tray) = self.tray.as_ref() {
            tray.show_hide_item.set_text("Show Rusty");
            tray.set_visible(false);
        }
    }

    fn handle_tray_events(&mut self, ctx: &egui::Context) {
        while let Ok(ev) = self.tray_events.try_recv() {
            match ev {
                crate::tray::TrayAppEvent::Menu(menu_id) => {
                    let show_hide_id = self.tray.as_ref().map(|t| t.show_hide_id.clone());
                    let exit_id = self.tray.as_ref().map(|t| t.exit_id.clone());

                    if show_hide_id.as_ref().is_some_and(|id| menu_id == *id) {
                        if self.hidden_to_tray {
                            self.show_from_tray(ctx);
                        } else {
                            // Idempotent "show/raise" behavior.
                            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
                            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                            ctx.request_repaint();
                        }
                        if !self.hidden_to_tray {
                            for id in self.pane_ids() {
                                if let Some(tab) = self.pane_mut(id) {
                                    tab.last_sent_size = None;
                                }
                            }
                        }
                        if let Some(tray) = self.tray.as_ref() {
                            tray.show_hide_item.set_text("Show Rusty");
                        }
                    } else if exit_id.as_ref().is_some_and(|id| menu_id == *id) {
                        if self.hidden_to_tray {
                            // Ensure the viewport is restorable before close on platforms that
                            // ignore close for fully hidden windows.
                            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
                        }
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                }
                crate::tray::TrayAppEvent::Tray(te) => {
                    if let tray_icon::TrayIconEvent::Click {
                        button,
                        button_state,
                        ..
                    } = te
                    {
                        // Single left-click should immediately restore/raise.
                        if button == tray_icon::MouseButton::Left
                            && button_state == tray_icon::MouseButtonState::Up
                        {
                            if self.hidden_to_tray {
                                self.show_from_tray(ctx);
                            } else {
                                ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                                ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
                                ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                            }
                        }
                    }
                }
            }
            ctx.request_repaint();
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
        let pane_id = self.create_pane(
            settings,
            color,
            profile_name,
            self.config.terminal_scrollback_lines,
        );

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
        let removed_tiles: Vec<TileId> = vec![pane_id];
        if let Some(pane) = self.pane_mut(pane_id) {
            pane.disconnect();
        }

        self.tree.remove_recursively(pane_id);
        self.pending_sftp_requests
            .retain(|_, file_tile| !removed_tiles.contains(file_tile));

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

    fn replace_child_in_container(container: &mut Container, old_child: TileId, new_child: TileId) {
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
                    linear
                        .children
                        .insert(i.min(linear.children.len()), new_child);
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
            .map(|p| {
                (
                    p.settings.clone(),
                    p.color,
                    p.profile_name.clone(),
                    p.scrollback_len,
                )
            })
            .unwrap_or((
                ConnectionSettings::default(),
                None,
                None,
                self.config.terminal_scrollback_lines,
            ));

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
}
