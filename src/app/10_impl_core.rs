impl AppState {
    fn lowest_unused_terminal_title_index<I>(used_indices: I) -> u64
    where
        I: IntoIterator<Item = u64>,
    {
        let mut next = 1u64;
        let mut used_indices: Vec<u64> = used_indices
            .into_iter()
            .filter(|index| *index > 0)
            .collect();
        used_indices.sort_unstable();
        used_indices.dedup();
        for index in used_indices {
            if index == next {
                next += 1;
            } else if index > next {
                break;
            }
        }
        next
    }

    fn next_terminal_title_index(&self) -> u64 {
        Self::lowest_unused_terminal_title_index(
            self.pane_ids()
                .into_iter()
                .filter_map(|tile_id| self.terminal_pane(tile_id).map(|tab| tab.title_index)),
        )
    }

    pub fn new() -> Self {
        let _ = std::fs::create_dir_all("logs");
        if crate::logger::ui_profile_enabled() {
            let _ = std::fs::remove_file("logs\\ui-profile.log");
            crate::logger::log_ui_profile(&format!("=== startup pid={} ===", std::process::id()));
        }
        let load_outcome = config::load();
        let mut config = load_outcome.config;
        let startup_notice = load_outcome.notice;
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
                job.issue_kind = Some(ssh::IssueKind::Transport);
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

        let terminal_theme_registry = ThemeRegistry::load();
        let mut terminal_theme_changed = false;
        if let Some(selected_raw) = config.selected_terminal_theme.clone() {
            let selected = selected_raw.trim();
            if selected.is_empty() {
                config.selected_terminal_theme = None;
                terminal_theme_changed = true;
            } else if let Some(theme) = terminal_theme_registry.find_by_id_or_name(selected) {
                let themed_colors =
                    theme.to_terminal_colors_config(config.terminal_colors.dim_blend);
                if config.terminal_colors != themed_colors {
                    config.terminal_colors = themed_colors;
                    terminal_theme_changed = true;
                }
                if !selected.eq_ignore_ascii_case(&theme.id) {
                    config.selected_terminal_theme = Some(theme.id.clone());
                    terminal_theme_changed = true;
                }
            } else {
                let fallback = terminal_theme_registry.default_theme();
                config.terminal_colors =
                    fallback.to_terminal_colors_config(config.terminal_colors.dim_blend);
                crate::logger::log_line(
                    "logs\\terminal-themes.log",
                    &format!(
                        "Selected terminal theme '{selected}' was not found; applied fallback theme '{}'.",
                        fallback.name
                    ),
                );
                config.selected_terminal_theme = None;
                terminal_theme_changed = true;
            }
        }
        if terminal_theme_changed {
            config_saver.request_save(config.clone());
        }

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
        let low_power_renderer = std::env::var_os("RUSTY_LOW_POWER_RENDERER")
            .map(|value| value == "1")
            .unwrap_or(false);
        let snap_fractional_dpi = std::env::var_os("RUSTY_RENDERER_KIND")
            .map(|value| value == "wgpu")
            .unwrap_or(false)
            && std::env::var_os("RUSTY_WGPU_BACKEND")
                .map(|value| value.to_string_lossy().eq_ignore_ascii_case("gl"))
                .unwrap_or(false);
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

        let (tree, _first_tile_id, initial_active_tile) = if let Some((t, pane_id)) = restored {
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
        Self {
            theme,
            theme_source,
            term_theme: TermTheme::from_config(&config.terminal_colors),
            terminal_theme_registry,
            config,
            config_saver,
            settings_dialog,
            tree,
            active_tile: initial_active_tile,
            next_session_id,
            last_cursor_blink: Instant::now(),
            last_terminal_activity: Instant::now(),
            last_download_activity: Instant::now(),
            cursor_visible: true,
            low_power_renderer,
            snap_fractional_dpi,
            tray: None,
            tray_events: crate::tray::install_handlers(),
            hidden_to_tray: false,
            minimize_to_tray_requested: false,
            clipboard: Clipboard::new().ok(),
            rename_popup: None,
            auth_dialog: None,
            host_key_dialog: None,
            transfer_delete_dialog: None,
            upload_conflict_dialog: None,
            pending_upload_conflict_prompts: VecDeque::new(),
            style_initialized: false,
            style_scale_key: 0,
            layout_dirty: false,
            active_tile_dirty: false,
            last_layout_save: Instant::now(),
            last_active_tile_change: Instant::now(),
            ui_profile_hot_frames: 0,
            ui_profile_frame_index: 0,
            restored_window: false,
            next_sftp_request_id,
            pending_sftp_requests: HashMap::new(),
            downloads_window_open: false,
            downloads_window_just_opened: false,
            download_jobs,
            download_event_tx,
            download_event_rx,
            download_cancel_txs: HashMap::new(),
            upload_conflict_response_txs: HashMap::new(),
            upload_refresh_targets: HashMap::new(),
            update_check_in_progress: false,
            update_check_rx: None,
            update_available_version,
            update_available_url,
            update_manual_open_if_newer: false,
            update_manual_status: None,
            startup_notice,
        }
    }

    fn refresh_terminal_theme_registry(&mut self) {
        self.terminal_theme_registry = ThemeRegistry::load();
        if let Some(selected) = self.config.selected_terminal_theme.clone() {
            if self
                .terminal_theme_registry
                .find_by_id_or_name(&selected)
                .is_none()
            {
                crate::logger::log_line(
                    "logs\\terminal-themes.log",
                    &format!(
                        "Selected terminal theme '{selected}' is no longer available after reload."
                    ),
                );
                self.config.selected_terminal_theme = None;
                self.config_saver.request_save(self.config.clone());
            }
        }
    }

    fn apply_terminal_theme_selection(&mut self, selected: &str, persist: bool) -> bool {
        let Some(theme) = self
            .terminal_theme_registry
            .find_by_id_or_name(selected)
            .cloned()
        else {
            crate::logger::log_line(
                "logs\\terminal-themes.log",
                &format!("Requested terminal theme '{selected}' was not found."),
            );
            return false;
        };

        let mut changed = false;
        let new_colors = theme.to_terminal_colors_config(self.config.terminal_colors.dim_blend);
        if self.config.terminal_colors != new_colors {
            self.config.terminal_colors = new_colors;
            changed = true;
        }

        if self
            .config
            .selected_terminal_theme
            .as_deref()
            .map(|s| !s.eq_ignore_ascii_case(&theme.id))
            .unwrap_or(true)
        {
            self.config.selected_terminal_theme = Some(theme.id.clone());
            changed = true;
        }

        if changed && persist {
            self.config_saver.request_save(self.config.clone());
        }
        changed
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

    fn start_update_check_now_open_if_newer(&mut self) {
        self.update_manual_open_if_newer = true;
        self.update_manual_status = Some("Checking for updates...".to_string());
        self.start_update_check();
    }

    fn start_update_check(&mut self) {
        if self.update_check_in_progress {
            return;
        }

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
            crate::tray::request_app_repaint();
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
            source_terminal: None,
            downloaded_bytes: entry.transferred_bytes,
            total_bytes: entry.total_bytes,
            speed_bps: entry.speed_bps,
            state: Self::transfer_state_from_config(entry.state),
            issue_kind: None,
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

    fn set_tab_status_for_tile(
        &mut self,
        tile_id: TileId,
        kind: ssh::IssueKind,
        message: impl Into<String>,
    ) {
        let message = message.into();
        if let Some(tab) = self.pane_mut(tile_id) {
            tab.last_status_kind = kind;
            tab.last_status = message;
        }
    }

    fn set_file_status(
        &mut self,
        tile_id: TileId,
        kind: ssh::IssueKind,
        message: impl Into<String>,
    ) {
        let message = message.into();
        if let Some(file) = self.file_pane_mut(tile_id) {
            file.status_kind = kind;
            file.status = message;
        }
    }

    fn set_download_job_message(
        job: &mut DownloadJob,
        kind: ssh::IssueKind,
        message: impl Into<String>,
    ) {
        job.issue_kind = Some(kind);
        job.message = message.into();
    }

    fn upload_status_message(local_path: &str, resuming: bool) -> String {
        let file_name = std::path::Path::new(local_path)
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or(local_path);
        if resuming {
            format!("Resuming {file_name}")
        } else {
            format!("Uploading {file_name}")
        }
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

        let ppp = ctx.pixels_per_point().max(1.0);
        let snap = |sz: f32| ((sz * ppp).round() / ppp).max(8.0);
        style
            .text_styles
            .insert(egui::TextStyle::Heading, FontId::proportional(snap(18.0)));
        style
            .text_styles
            .insert(egui::TextStyle::Button, FontId::proportional(snap(14.0)));
        style
            .text_styles
            .insert(egui::TextStyle::Body, FontId::proportional(snap(14.0)));

        ctx.set_style(style);
    }

    fn style_scale_key(pixels_per_point: f32) -> u32 {
        let ppp = pixels_per_point.max(1.0);
        let heading_px = (18.0 * ppp).round().clamp(0.0, u16::MAX as f32) as u32;
        let body_px = (14.0 * ppp).round().clamp(0.0, u16::MAX as f32) as u32;
        (heading_px << 16) | body_px
    }

    fn ensure_global_style(&mut self, ctx: &egui::Context) {
        if !self.style_initialized {
            egui_extras::install_image_loaders(ctx);
            self.style_initialized = true;
        }

        // Refresh style only when the snapped text pixel sizes would actually change.
        let scale_key = Self::style_scale_key(ctx.pixels_per_point());
        if self.style_scale_key != scale_key {
            self.apply_global_style(ctx);
            self.style_scale_key = scale_key;
        }
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

    fn visible_terminal_focus_order(&self) -> Vec<TileId> {
        let mut terminals = Vec::new();
        if let Some(root) = self.tree.root {
            self.collect_visible_terminals(root, &mut terminals);
        }

        let mut ordered: Vec<(usize, TileId, Option<Rect>)> = terminals
            .into_iter()
            .enumerate()
            .map(|(index, tile_id)| {
                let rect = self.terminal_pane(tile_id).and_then(|pane| pane.last_view_rect);
                (index, tile_id, rect)
            })
            .collect();

        ordered.sort_by(|a, b| match (a.2, b.2) {
            (Some(a_rect), Some(b_rect)) => {
                let top_cmp = a_rect.min.y.total_cmp(&b_rect.min.y);
                if top_cmp != std::cmp::Ordering::Equal {
                    top_cmp
                } else {
                    let left_cmp = a_rect.min.x.total_cmp(&b_rect.min.x);
                    if left_cmp != std::cmp::Ordering::Equal {
                        left_cmp
                    } else {
                        a.0.cmp(&b.0)
                    }
                }
            }
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.0.cmp(&b.0),
        });

        ordered.into_iter().map(|(_, tile_id, _)| tile_id).collect()
    }

    fn collect_visible_terminals(&self, tile_id: TileId, out: &mut Vec<TileId>) {
        let Some(tile) = self.tree.tiles.get(tile_id) else {
            return;
        };

        match tile {
            Tile::Pane(tab) => {
                if tab.is_terminal() {
                    out.push(tile_id);
                }
            }
            Tile::Container(Container::Tabs(tabs)) => {
                let active = tabs
                    .active
                    .filter(|id| self.tree.tiles.is_visible(*id))
                    .or_else(|| {
                        tabs.children
                            .iter()
                            .copied()
                            .find(|id| self.tree.tiles.is_visible(*id))
                    });
                if let Some(active) = active {
                    self.collect_visible_terminals(active, out);
                }
            }
            Tile::Container(Container::Linear(linear)) => {
                for child in linear.children.iter().copied() {
                    if self.tree.tiles.is_visible(child) {
                        self.collect_visible_terminals(child, out);
                    }
                }
            }
            Tile::Container(Container::Grid(grid)) => {
                for child in grid.children().copied() {
                    if self.tree.tiles.is_visible(child) {
                        self.collect_visible_terminals(child, out);
                    }
                }
            }
        }
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
            false,
        );
        let root = self.tree.tiles.insert_tab_tile(vec![pane_id]);
        self.tree.root = Some(root);
        self.set_active_tile(Some(pane_id));
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
        let mut to_autoconnect_terminals: HashMap<u64, TileId> = HashMap::new();
        let mut to_attach_shared_terminals: Vec<TileId> = Vec::new();
        let mut restored_terminal_groups: HashMap<TileId, u64> = HashMap::new();
        let mut used_title_indices: BTreeSet<u64> = BTreeSet::new();

        for (tile_id, tile) in restored.tree.tiles.iter() {
            let Tile::Pane(p) = tile else {
                continue;
            };
            if matches!(p.pane_kind, PersistedPaneKind::Terminal) {
                restored_terminal_groups.insert(
                    *tile_id,
                    if p.connection_group_id == 0 {
                        p.id
                    } else {
                        p.connection_group_id
                    },
                );
            }
        }

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
                            let desired_title_index = if p.title_index == 0 {
                                p.id
                            } else {
                                p.title_index
                            };
                            tab.title_index = if desired_title_index > 0
                                && !used_title_indices.contains(&desired_title_index)
                            {
                                desired_title_index
                            } else {
                                Self::lowest_unused_terminal_title_index(
                                    used_title_indices.iter().copied(),
                                )
                            };
                            used_title_indices.insert(tab.title_index);
                            tab.connection_group_id = if p.connection_group_id == 0 {
                                p.id
                            } else {
                                p.connection_group_id
                            };
                            tab.title = SshTab::title_for(tab.title_index, &tab.settings);
                            if p.autoconnect {
                                match to_autoconnect_terminals.entry(tab.connection_group_id) {
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        entry.insert(*tile_id);
                                    }
                                    std::collections::hash_map::Entry::Occupied(_) => {
                                        to_attach_shared_terminals.push(*tile_id);
                                    }
                                }
                            }
                            tab
                        }
                        PersistedPaneKind::FileManager {
                            source_terminal,
                            source_connection_group_id,
                            path,
                        } => SshTab::new_file_manager(
                            p.id,
                            settings,
                            p.profile_name.clone(),
                            p.color,
                            *source_terminal,
                            if *source_connection_group_id == 0 {
                                restored_terminal_groups
                                    .get(source_terminal)
                                    .copied()
                                    .unwrap_or(p.id)
                            } else {
                                *source_connection_group_id
                            },
                            path.clone(),
                        ),
                    };
                    tab.user_title = p.user_title.clone();
                    tab.color = p.color;
                    tab.focus_terminal_next_frame = false;
                    tiles.insert(*tile_id, Tile::Pane(tab));
                }
                Tile::Container(c) => {
                    tiles.insert(*tile_id, Tile::Container(c.clone()));
                }
            }
        }

        let mut tree = Tree::new("ssh_tree", root, tiles);

        for id in to_autoconnect_terminals.into_values() {
            if let Some(Tile::Pane(tab)) = tree.tiles.get_mut(id) {
                if tab.is_terminal()
                    && !tab.settings.host.trim().is_empty()
                    && !tab.settings.username.trim().is_empty()
                {
                    tab.start_connect();
                }
            }
        }
        for id in to_attach_shared_terminals {
            if let Some(Tile::Pane(tab)) = tree.tiles.get_mut(id) {
                if tab.is_terminal() {
                    tab.connecting = true;
                    tab.last_status_kind = ssh::IssueKind::Info;
                    tab.last_status = "Waiting for shared SSH session...".to_string();
                    tab.pending_restore_attach_group = Some(tab.connection_group_id);
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
                            source_connection_group_id: file.source_connection_group_id,
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
                        title_index: tab.title_index,
                        connection_group_id: tab.connection_group_id,
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

        // Saved geometry can be invalid when moving between host/VM setups with different displays.
        // Clamp to a conservative visible range so startup can't end up off-screen.
        let mut x = sw.outer_pos[0];
        let mut y = sw.outer_pos[1];
        if !x.is_finite()
            || !y.is_finite()
            || x < -2000.0
            || y < -2000.0
            || x > 10000.0
            || y > 10000.0
        {
            x = 40.0;
            y = 40.0;
        }
        x = x.max(0.0);
        y = y.max(0.0);
        let w = sw.inner_size[0].clamp(360.0, 3840.0);
        let h = sw.inner_size[1].clamp(240.0, 2160.0);

        // Restore normal geometry first, then maximize if requested.
        ctx.send_viewport_cmd(egui::ViewportCommand::OuterPosition(Pos2::new(
            x, y,
        )));
        ctx.send_viewport_cmd(egui::ViewportCommand::InnerSize(Vec2::new(
            w, h,
        )));
        if sw.maximized {
            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(true));
        }
        ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
        ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
    }

    fn snapshot_saved_window(&self, ctx: &egui::Context) -> Option<config::SavedWindow> {
        let maximized = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
        let fullscreen = ctx.input(|i| i.viewport().fullscreen.unwrap_or(false));
        if fullscreen {
            return None;
        }

        let (outer, inner) = ctx.input(|i| (i.viewport().outer_rect, i.viewport().inner_rect));
        let (outer, inner) = (outer?, inner?);
        let snap = |value: f32| value.round();
        Some(config::SavedWindow {
            outer_pos: [snap(outer.min.x), snap(outer.min.y)],
            inner_size: [snap(inner.width()), snap(inner.height())],
            maximized,
        })
    }

    fn maybe_save_session_layout(&mut self, ctx: &egui::Context) {
        if !self.config.save_session_layout {
            return;
        }

        let pending_window = self.snapshot_saved_window(ctx);
        let window_dirty = pending_window
            .as_ref()
            .map(|window| self.config.saved_window != Some(*window))
            .unwrap_or(false);
        let active_tile_save_due =
            self.active_tile_dirty && self.last_active_tile_change.elapsed() >= Duration::from_secs(8);

        if !self.layout_dirty && !window_dirty && !active_tile_save_due {
            return;
        }
        if self.last_layout_save.elapsed() < Duration::from_millis(500) {
            return;
        }

        if let Some(window) = pending_window {
            self.config.saved_window = Some(window);
        }

        if let Some(json) = self.persist_session_tree() {
            self.config.saved_session_layout_json = Some(json);
            self.config_saver.request_save(self.config.clone());
            self.layout_dirty = false;
            self.active_tile_dirty = false;
            self.last_layout_save = Instant::now();
        }
    }

    fn create_pane(
        &mut self,
        settings: ConnectionSettings,
        color: Option<Color32>,
        profile_name: Option<String>,
        scrollback_len: usize,
        auto_connect: bool,
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
        tab.title_index = self.next_terminal_title_index();
        tab.title = SshTab::title_for(tab.title_index, &tab.settings);
        tab.color = color;
        tab.focus_terminal_next_frame = true;
        if auto_connect
            && !tab.settings.host.trim().is_empty()
            && !tab.settings.username.trim().is_empty()
        {
            tab.start_connect();
        }

        self.tree.tiles.insert_pane(tab)
    }

    fn attach_terminal_to_existing_session(
        &mut self,
        pane_id: TileId,
        worker_tx: Sender<ssh::WorkerMessage>,
    ) -> bool {
        let Some(tab) = self.terminal_pane_mut(pane_id) else {
            return false;
        };

        let (ui_tx, ui_rx) = mpsc::channel::<ssh::UiMessage>();
        tab.ui_rx = Some(ui_rx);
        tab.worker_tx = Some(worker_tx.clone());
        tab.host_key_tx = None;
        tab.connected = false;
        tab.connecting = true;
        tab.last_status_kind = ssh::IssueKind::Info;
        tab.last_status = "Opening shared shell...".to_string();
        tab.last_sent_size = None;
        tab.pending_resize = None;
        tab.pending_auth = None;
        tab.pending_host_key = None;
        tab.pending_scrollback = None;
        tab.title = SshTab::title_for(tab.title_index, &tab.settings);

        let send_result = worker_tx.send(ssh::WorkerMessage::AttachTerminalClient {
            client_id: tab.id,
            ui_tx,
            scrollback_len: tab.scrollback_len,
        });
        if send_result.is_ok() {
            return true;
        }

        tab.ui_rx = None;
        tab.worker_tx = None;
        tab.connecting = false;
        tab.last_status_kind = ssh::IssueKind::Transport;
        tab.last_status = "Failed to attach to the shared SSH session".to_string();
        false
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

    fn local_transfer_temp_path(local_path: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(format!("{local_path}.rusty-part"))
    }

    fn delete_local_download_artifacts(local_path: &str) {
        let primary_path = std::path::PathBuf::from(local_path);
        let temp_path = Self::local_transfer_temp_path(local_path);
        let mut paths = vec![primary_path];
        if paths[0] != temp_path {
            paths.push(temp_path);
        }

        for path in paths {
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(&path);
                continue;
            }

            for _ in 0..20 {
                match std::fs::remove_file(&path) {
                    Ok(_) => break,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                    Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    Err(_) => break,
                }
            }
        }
    }

    fn sender_for_terminal_tile(&self, terminal_tile: TileId) -> Option<Sender<WorkerMessage>> {
        let pane = self.terminal_pane(terminal_tile)?;
        if !pane.connected {
            return None;
        }
        pane.worker_tx.as_ref().cloned()
    }

    fn shared_terminal_session_for_pane(
        &self,
        pane_id: TileId,
    ) -> Option<(Sender<WorkerMessage>, u64)> {
        let pane = self.pane(pane_id)?;
        if pane.is_terminal() && pane.connected {
            return pane
                .worker_tx
                .as_ref()
                .cloned()
                .map(|tx| (tx, pane.connection_group_id));
        }

        let file = self.file_pane(pane_id)?;
        if !file.source_connected {
            return None;
        }

        self.source_worker_sender_for_file_tile(pane_id)
            .map(|tx| (tx, file.source_connection_group_id))
    }

    fn sender_for_connection_group(&self, connection_group_id: u64) -> Option<Sender<WorkerMessage>> {
        self.pane_ids().into_iter().find_map(|tile_id| {
            let pane = self.terminal_pane(tile_id)?;
            (pane.connection_group_id == connection_group_id && pane.connected)
                .then(|| pane.worker_tx.as_ref().cloned())
                .flatten()
        })
    }

    fn connection_group_state(&self, connection_group_id: u64) -> (bool, bool, String) {
        let mut connected = false;
        let mut connecting = false;
        let mut status = String::new();
        for tile_id in self.pane_ids() {
            let Some(pane) = self.terminal_pane(tile_id) else {
                continue;
            };
            if pane.connection_group_id != connection_group_id {
                continue;
            }
            connected |= pane.connected;
            connecting |= pane.connecting;
            if status.trim().is_empty() && !pane.last_status.trim().is_empty() {
                status = pane.last_status.clone();
            }
        }
        (connected, connecting, status)
    }

    fn sync_shared_terminal_groups(&mut self) {
        let pane_ids = self.pane_ids();
        let mut attach_pending: Vec<(TileId, u64, Sender<WorkerMessage>)> = Vec::new();
        let mut clear_pending: Vec<TileId> = Vec::new();

        for pane_id in pane_ids {
            let Some(tab) = self.terminal_pane(pane_id) else {
                continue;
            };
            let Some(connection_group_id) = tab.pending_restore_attach_group else {
                continue;
            };

            if tab.connected {
                clear_pending.push(pane_id);
                continue;
            }
            if tab.worker_tx.is_some() || tab.ui_rx.is_some() {
                continue;
            }
            if let Some(worker_tx) = self.sender_for_connection_group(connection_group_id) {
                attach_pending.push((pane_id, connection_group_id, worker_tx));
            }
        }

        for pane_id in clear_pending {
            if let Some(tab) = self.terminal_pane_mut(pane_id) {
                tab.pending_restore_attach_group = None;
            }
        }

        for (pane_id, connection_group_id, worker_tx) in attach_pending {
            if self
                .terminal_pane(pane_id)
                .and_then(|tab| tab.pending_restore_attach_group)
                != Some(connection_group_id)
            {
                continue;
            }
            if self.attach_terminal_to_existing_session(pane_id, worker_tx) {
                if let Some(tab) = self.terminal_pane_mut(pane_id) {
                    tab.connection_group_id = connection_group_id;
                    tab.pending_restore_attach_group = None;
                }
            }
        }
    }

    fn source_worker_sender_for_file_tile(&self, file_tile: TileId) -> Option<Sender<WorkerMessage>> {
        let file = self.file_pane(file_tile)?;
        file.source_worker_tx
            .as_ref()
            .cloned()
            .or_else(|| self.sender_for_connection_group(file.source_connection_group_id))
            .or_else(|| self.pane(file.source_terminal)?.worker_tx.as_ref().cloned())
    }

    fn transfer_sender_for_file_tile(
        &self,
        file_tile: TileId,
    ) -> Option<(TileId, Sender<WorkerMessage>)> {
        let source_tile = self.file_pane(file_tile)?.source_terminal;
        let tx = self.source_worker_sender_for_file_tile(file_tile)?;
        Some((source_tile, tx))
    }

    fn sender_for_file_tile(&self, file_tile: TileId) -> Option<Sender<ssh::SftpWorkerMessage>> {
        let pane = self.file_pane(file_tile)?;
        let tab = self.pane(file_tile)?;
        if !tab.connected {
            return None;
        }
        pane.worker_tx.as_ref().cloned()
    }

    fn attach_file_manager_to_source(&mut self, file_tile: TileId) -> bool {
        if self.file_pane(file_tile).is_none() {
            return false;
        }
        let source_tx = self
            .file_pane(file_tile)
            .and_then(|file| self.sender_for_connection_group(file.source_connection_group_id))
            .or_else(|| self.source_worker_sender_for_file_tile(file_tile));
        let Some(source_tx) = source_tx else {
            if let Some(tab) = self.pane_mut(file_tile) {
                tab.connected = false;
                tab.connecting = false;
            }
            self.set_tab_status_for_tile(
                file_tile,
                ssh::IssueKind::Transport,
                "SFTP session is not connected",
            );
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.source_connected = false;
                file.source_worker_tx = None;
                file.status_kind = ssh::IssueKind::Transport;
                file.status = "SFTP session is not connected".to_string();
                file.ui_rx = None;
                file.worker_tx = None;
            }
            return false;
        };

        let client_id = self.pane(file_tile).map(|pane| pane.id).unwrap_or(0);
        let (ui_tx, ui_rx) = mpsc::channel::<ssh::SftpUiMessage>();
        let (worker_tx, worker_rx) = mpsc::channel::<ssh::SftpWorkerMessage>();
        let send_result = source_tx.send(WorkerMessage::AttachSftpClient {
            client_id,
            ui_tx,
            worker_rx,
        });
        if send_result.is_err() {
            if let Some(tab) = self.pane_mut(file_tile) {
                tab.connected = false;
                tab.connecting = false;
            }
            self.set_tab_status_for_tile(
                file_tile,
                ssh::IssueKind::Transport,
                "Failed to open SFTP session",
            );
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.source_connected = false;
                file.source_worker_tx = None;
                file.status_kind = ssh::IssueKind::Transport;
                file.status = "Failed to open SFTP session".to_string();
                file.ui_rx = None;
                file.worker_tx = None;
            }
            return false;
        }

        if let Some(tab) = self.pane_mut(file_tile) {
            tab.connected = false;
            tab.connecting = true;
        }
        self.set_tab_status_for_tile(file_tile, ssh::IssueKind::Info, "Connecting SFTP session...");
        if let Some(file) = self.file_pane_mut(file_tile) {
            file.source_connected = false;
            file.source_worker_tx = Some(source_tx);
            file.status_kind = ssh::IssueKind::Info;
            file.status = "Connecting SFTP session...".to_string();
            file.ui_rx = Some(ui_rx);
            file.worker_tx = Some(worker_tx);
        }
        true
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
            file.status_kind = ssh::IssueKind::Info;
            file.status = busy_status;
        }

        let Some(tx) = self.sender_for_file_tile(file_tile) else {
            self.pending_sftp_requests.remove(&request_id);
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.busy = false;
                file.status_kind = ssh::IssueKind::Transport;
                file.status = "SFTP session is not connected".to_string();
                file.source_connected = false;
            }
            return;
        };

        let cmd = make_cmd(request_id);
        if tx.send(ssh::SftpWorkerMessage::Command(cmd)).is_err() {
            self.pending_sftp_requests.remove(&request_id);
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.busy = false;
                file.status_kind = ssh::IssueKind::Transport;
                file.status = "Failed to send SFTP command".to_string();
                file.source_connected = false;
            }
            if let Some(tab) = self.pane_mut(file_tile) {
                tab.connected = false;
                tab.connecting = false;
            }
        }
    }

    fn open_file_manager_for_terminal(&mut self, source_tile: TileId) -> Option<TileId> {
        let source = self.terminal_pane(source_tile)?;
        let settings = source.settings.clone();
        let profile_name = source.profile_name.clone();
        let color = source.color;
        let source_connected = source.connected;
        let source_connecting = source.connecting;
        let source_status = source.last_status.clone();
        let source_worker_tx = source.worker_tx.as_ref().cloned();
        let source_connection_group_id = source.connection_group_id;

        let id = self.next_session_id;
        self.next_session_id += 1;
        let pane = SshTab::new_file_manager(
            id,
            settings,
            profile_name,
            color,
            source_tile,
            source_connection_group_id,
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

        self.set_active_tile(Some(pane_id));
        self.settings_dialog.target_tile = Some(source_tile);
        if source_connected {
            let _ = self.attach_file_manager_to_source(pane_id);
        } else if let Some(file) = self.file_pane_mut(pane_id) {
            file.source_worker_tx = source_worker_tx;
            file.source_connected = false;
            file.status_kind = if source_connecting {
                ssh::IssueKind::Info
            } else {
                ssh::IssueKind::Transport
            };
            file.status = if source_connecting {
                "Connecting source terminal...".to_string()
            } else if !source_status.trim().is_empty() {
                source_status
            } else {
                "SFTP session is not connected".to_string()
            };
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

    fn selected_file_entries(&self, file_tile: TileId) -> Vec<ssh::SftpEntry> {
        let Some(file) = self.file_pane(file_tile) else {
            return Vec::new();
        };
        let selected = file.selected_names_in_entry_order();
        file.entries
            .iter()
            .filter(|entry| selected.iter().any(|name| name == &entry.file_name))
            .cloned()
            .collect()
    }

    fn transfer_context_for_file_transfer(
        &mut self,
        file_tile: TileId,
        action: &str,
    ) -> Option<(TileId, Sender<WorkerMessage>, ConnectionSettings)> {
        let Some((source_tile, tx)) = self.transfer_sender_for_file_tile(file_tile) else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status_kind = ssh::IssueKind::Transport;
                file.status = "SFTP session is not connected".to_string();
                file.source_connected = false;
            }
            return None;
        };
        let settings = self.pane(file_tile).map(|p| p.settings.clone());
        let Some(settings) = settings else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status_kind = ssh::IssueKind::Configuration;
                file.status = format!("Cannot resolve connection settings for {action}");
            }
            return None;
        };
        Some((source_tile, tx, settings))
    }

    fn request_file_delete(&mut self, file_tile: TileId, names: Vec<String>) {
        let names: Vec<String> = names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        if names.is_empty() {
            return;
        }
        let full_paths = self
            .file_pane(file_tile)
            .map(|f| {
                names.iter()
                    .map(|name| Self::remote_join_path(&f.cwd, name))
                    .collect()
            })
            .unwrap_or_else(|| names.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::Delete {
                request_id,
                paths: full_paths.clone(),
            },
            if names.len() == 1 {
                format!("Deleting {} ...", names[0])
            } else {
                format!("Deleting {} items ...", names.len())
            },
        );
    }

    fn request_file_copy(
        &mut self,
        file_tile: TileId,
        names: Vec<String>,
        destination_dir: String,
    ) {
        let names: Vec<String> = names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        let destination_dir = destination_dir.trim().to_string();
        if names.is_empty() || destination_dir.is_empty() {
            return;
        }
        let source_paths = self
            .file_pane(file_tile)
            .map(|f| {
                names.iter()
                    .map(|name| Self::remote_join_path(&f.cwd, name))
                    .collect()
            })
            .unwrap_or_else(|| names.clone());
        let destination_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &destination_dir))
            .unwrap_or(destination_dir.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::Copy {
                request_id,
                source_paths: source_paths.clone(),
                destination_dir: destination_path.clone(),
            },
            if names.len() == 1 {
                format!("Copying {} ...", names[0])
            } else {
                format!("Copying {} items ...", names.len())
            },
        );
    }

    fn request_file_move(
        &mut self,
        file_tile: TileId,
        names: Vec<String>,
        destination_dir: String,
    ) {
        let names: Vec<String> = names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        let destination_dir = destination_dir.trim().to_string();
        if names.is_empty() || destination_dir.is_empty() {
            return;
        }
        let source_paths = self
            .file_pane(file_tile)
            .map(|f| {
                names.iter()
                    .map(|name| Self::remote_join_path(&f.cwd, name))
                    .collect()
            })
            .unwrap_or_else(|| names.clone());
        let destination_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &destination_dir))
            .unwrap_or(destination_dir.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::Move {
                request_id,
                source_paths: source_paths.clone(),
                destination_dir: destination_path.clone(),
            },
            if names.len() == 1 {
                format!("Moving {} ...", names[0])
            } else {
                format!("Moving {} items ...", names.len())
            },
        );
    }

    fn request_file_set_permissions(&mut self, file_tile: TileId, names: Vec<String>, mode: u32) {
        let names: Vec<String> = names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        if names.is_empty() {
            return;
        }
        let full_paths = self
            .file_pane(file_tile)
            .map(|f| {
                names.iter()
                    .map(|name| Self::remote_join_path(&f.cwd, name))
                    .collect()
            })
            .unwrap_or_else(|| names.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::SetPermissions {
                request_id,
                paths: full_paths.clone(),
                mode,
            },
            if names.len() == 1 {
                format!("Updating permissions for {} ...", names[0])
            } else {
                format!("Updating permissions for {} items ...", names.len())
            },
        );
    }

    fn request_file_set_ownership(
        &mut self,
        file_tile: TileId,
        names: Vec<String>,
        owner: Option<String>,
        group: Option<String>,
    ) {
        let names: Vec<String> = names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        let owner = owner.and_then(|value| {
            let trimmed = value.trim().to_string();
            (!trimmed.is_empty()).then_some(trimmed)
        });
        let group = group.and_then(|value| {
            let trimmed = value.trim().to_string();
            (!trimmed.is_empty()).then_some(trimmed)
        });
        if names.is_empty() || (owner.is_none() && group.is_none()) {
            return;
        }
        let full_paths = self
            .file_pane(file_tile)
            .map(|f| {
                names.iter()
                    .map(|name| Self::remote_join_path(&f.cwd, name))
                    .collect()
            })
            .unwrap_or_else(|| names.clone());
        self.send_file_command(
            file_tile,
            move |request_id| ssh::SftpCommand::SetOwnership {
                request_id,
                paths: full_paths.clone(),
                owner: owner.clone(),
                group: group.clone(),
            },
            if names.len() == 1 {
                format!("Updating ownership for {} ...", names[0])
            } else {
                format!("Updating ownership for {} items ...", names.len())
            },
        );
    }

    fn queue_upload_transfer(
        &mut self,
        file_tile: TileId,
        source_tile: TileId,
        tx: &Sender<WorkerMessage>,
        settings: &ConnectionSettings,
        local_path: PathBuf,
    ) -> Result<String, (ssh::IssueKind, String)> {
        let local_path_label = local_path.display().to_string();
        let metadata = std::fs::metadata(&local_path).map_err(|err| {
            (
                ssh::IssueKind::Path,
                format!("Local upload path is not accessible: {local_path_label} ({err})"),
            )
        })?;
        if !metadata.is_file() && !metadata.is_dir() {
            return Err((
                ssh::IssueKind::Path,
                format!("Unsupported local upload path: {local_path_label}"),
            ));
        }

        let Some(file_name) = local_path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .filter(|s| !s.trim().is_empty())
        else {
            return Err((
                ssh::IssueKind::Path,
                "Invalid local upload file name".to_string(),
            ));
        };

        let remote_path = self
            .file_pane(file_tile)
            .map(|f| Self::remote_join_path(&f.cwd, &file_name))
            .unwrap_or(file_name.clone());
        let local_path = local_path_label;
        let request_id = self.alloc_sftp_request_id();
        self.download_jobs.push(DownloadJob {
            request_id,
            direction: TransferDirection::Upload,
            settings: settings.clone(),
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            source_terminal: Some(source_tile),
            downloaded_bytes: 0,
            total_bytes: None,
            speed_bps: 0.0,
            state: DownloadState::Queued,
            issue_kind: Some(ssh::IssueKind::Info),
            message: "Queued".to_string(),
        });

        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        self.download_cancel_txs.insert(request_id, cancel_tx);
        let (conflict_tx, conflict_rx) = mpsc::channel::<ssh::UploadConflictResponse>();
        self.upload_conflict_response_txs.insert(request_id, conflict_tx);
        self.upload_refresh_targets.insert(request_id, file_tile);
        let send_result = tx.send(WorkerMessage::TransferCommand(ssh::TransferCommand::Upload {
            request_id,
            remote_path,
            local_path,
            resume_from_remote_temp: false,
            event_tx: self.download_event_tx.clone(),
            cancel_rx,
            conflict_response_rx: conflict_rx,
        }));
        if send_result.is_err() {
            self.download_cancel_txs.remove(&request_id);
            self.upload_conflict_response_txs.remove(&request_id);
            self.upload_refresh_targets.remove(&request_id);
            if let Some(job) = self
                .download_jobs
                .iter_mut()
                .find(|job| job.request_id == request_id)
            {
                job.state = DownloadState::Failed;
                Self::set_download_job_message(
                    job,
                    ssh::IssueKind::Transport,
                    "Failed to queue upload on the live SSH session",
                );
            }
            self.persist_transfer_history();
            return Err((
                ssh::IssueKind::Transport,
                "Failed to queue upload on the live SSH session".to_string(),
            ));
        }
        self.persist_transfer_history();
        Ok(file_name)
    }

    fn start_upload_from_files_picker(&mut self, file_tile: TileId) -> bool {
        let mut dlg = rfd::FileDialog::new();
        if let Some(profile_dir) = user_profile_dir() {
            dlg = dlg.set_directory(profile_dir);
        }
        let Some(local_paths) = dlg.pick_files() else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status_kind = ssh::IssueKind::Info;
                file.status = "Upload cancelled".to_string();
            }
            return false;
        };
        self.start_upload_paths(file_tile, local_paths)
    }

    fn start_upload_from_folder_picker(&mut self, file_tile: TileId) -> bool {
        let mut dlg = rfd::FileDialog::new();
        if let Some(profile_dir) = user_profile_dir() {
            dlg = dlg.set_directory(profile_dir);
        }
        let Some(local_path) = dlg.pick_folder() else {
            if let Some(file) = self.file_pane_mut(file_tile) {
                file.status_kind = ssh::IssueKind::Info;
                file.status = "Upload cancelled".to_string();
            }
            return false;
        };
        self.start_upload_paths(file_tile, vec![local_path])
    }

    fn start_upload_paths(&mut self, file_tile: TileId, local_paths: Vec<PathBuf>) -> bool {
        let local_paths: Vec<PathBuf> = local_paths
            .into_iter()
            .filter(|path| !path.as_os_str().is_empty())
            .collect();
        if local_paths.is_empty() {
            self.set_file_status(file_tile, ssh::IssueKind::Info, "Upload cancelled");
            return false;
        }

        let total_requested = local_paths.len();
        let Some((source_tile, tx, settings)) =
            self.transfer_context_for_file_transfer(file_tile, "upload")
        else {
            return false;
        };

        let mut queued = 0usize;
        let mut last_error: Option<(ssh::IssueKind, String)> = None;
        for local_path in local_paths {
            match self.queue_upload_transfer(file_tile, source_tile, &tx, &settings, local_path) {
                Ok(_) => queued = queued.saturating_add(1),
                Err(err) => last_error = Some(err),
            }
        }

        if queued > 0 {
            self.open_downloads_window();
        }

        match (queued, total_requested, last_error) {
            (0, _, Some((kind, message))) => self.set_file_status(file_tile, kind, message),
            (0, _, None) => self.set_file_status(file_tile, ssh::IssueKind::Info, "Upload cancelled"),
            (count, total, Some((_kind, message))) if count < total => self.set_file_status(
                file_tile,
                ssh::IssueKind::Info,
                format!("Queued {count}/{total} uploads. {message}"),
            ),
            (1, 1, _) => self.set_file_status(file_tile, ssh::IssueKind::Info, "Upload queued"),
            (count, _, _) => self.set_file_status(
                file_tile,
                ssh::IssueKind::Info,
                format!("Queued {count} uploads"),
            ),
        }

        queued > 0
    }

    fn queue_download_transfer(
        &mut self,
        source_tile: TileId,
        tx: &Sender<WorkerMessage>,
        settings: &ConnectionSettings,
        remote_path: String,
        local_path: PathBuf,
    ) -> Result<(), (ssh::IssueKind, String)> {
        let local_path = local_path.display().to_string();
        if local_path.trim().is_empty() {
            return Err((
                ssh::IssueKind::Path,
                "Invalid local download path".to_string(),
            ));
        }

        let request_id = self.alloc_sftp_request_id();
        self.download_jobs.push(DownloadJob {
            request_id,
            direction: TransferDirection::Download,
            settings: settings.clone(),
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            source_terminal: Some(source_tile),
            downloaded_bytes: 0,
            total_bytes: None,
            speed_bps: 0.0,
            state: DownloadState::Queued,
            issue_kind: Some(ssh::IssueKind::Info),
            message: "Queued".to_string(),
        });

        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        self.download_cancel_txs.insert(request_id, cancel_tx);
        let send_result = tx.send(WorkerMessage::TransferCommand(ssh::TransferCommand::Download {
            request_id,
            remote_path,
            local_path,
            resume_from_local: false,
            event_tx: self.download_event_tx.clone(),
            cancel_rx,
        }));
        if send_result.is_err() {
            self.download_cancel_txs.remove(&request_id);
            if let Some(job) = self
                .download_jobs
                .iter_mut()
                .find(|job| job.request_id == request_id)
            {
                job.state = DownloadState::Failed;
                Self::set_download_job_message(
                    job,
                    ssh::IssueKind::Transport,
                    "Failed to queue download on the live SSH session",
                );
            }
            self.persist_transfer_history();
            return Err((
                ssh::IssueKind::Transport,
                "Failed to queue download on the live SSH session".to_string(),
            ));
        }
        self.persist_transfer_history();
        Ok(())
    }

    fn start_download_for_entries(
        &mut self,
        file_tile: TileId,
        entries: Vec<ssh::SftpEntry>,
    ) -> bool {
        if entries.is_empty() {
            return false;
        }
        let Some((source_tile, tx, settings)) =
            self.transfer_context_for_file_transfer(file_tile, "download")
        else {
            return false;
        };
        let cwd = self
            .file_pane(file_tile)
            .map(|f| f.cwd.clone())
            .unwrap_or_else(|| ".".to_string());
        let multi_target = entries.len() > 1 || entries.iter().any(|entry| entry.is_dir);

        let download_targets: Vec<(String, PathBuf)> = if multi_target {
            let mut dlg = rfd::FileDialog::new();
            if let Some(profile_dir) = user_profile_dir() {
                dlg = dlg.set_directory(profile_dir);
            }
            let Some(base_dir) = dlg.pick_folder() else {
                self.set_file_status(file_tile, ssh::IssueKind::Info, "Download cancelled");
                return false;
            };
            entries
                .into_iter()
                .map(|entry| {
                    (
                        Self::remote_join_path(&cwd, &entry.file_name),
                        base_dir.join(&entry.file_name),
                    )
                })
                .collect()
        } else {
            let entry = entries.into_iter().next().unwrap();
            let default_name = entry.file_name.clone();
            let mut dlg = rfd::FileDialog::new().set_file_name(&default_name);
            if let Some(profile_dir) = user_profile_dir() {
                dlg = dlg.set_directory(profile_dir);
            }
            let Some(local_path) = dlg.save_file() else {
                self.set_file_status(file_tile, ssh::IssueKind::Info, "Download cancelled");
                return false;
            };
            vec![(Self::remote_join_path(&cwd, &default_name), local_path)]
        };

        let total_requested = download_targets.len();
        let mut queued = 0usize;
        let mut last_error: Option<(ssh::IssueKind, String)> = None;
        for (remote_path, local_path) in download_targets {
            match self.queue_download_transfer(source_tile, &tx, &settings, remote_path, local_path) {
                Ok(()) => queued = queued.saturating_add(1),
                Err(err) => last_error = Some(err),
            }
        }

        if queued > 0 {
            self.open_downloads_window();
        }

        match (queued, total_requested, last_error) {
            (0, _, Some((kind, message))) => self.set_file_status(file_tile, kind, message),
            (0, _, None) => self.set_file_status(file_tile, ssh::IssueKind::Info, "Download cancelled"),
            (count, total, Some((_kind, message))) if count < total => self.set_file_status(
                file_tile,
                ssh::IssueKind::Info,
                format!("Queued {count}/{total} downloads. {message}"),
            ),
            (1, 1, _) => self.set_file_status(file_tile, ssh::IssueKind::Info, "Download queued"),
            (count, _, _) => self.set_file_status(
                file_tile,
                ssh::IssueKind::Info,
                format!("Queued {count} downloads"),
            ),
        }

        queued > 0
    }

    fn start_download_for_selected(&mut self, file_tile: TileId) -> bool {
        let entries = self.selected_file_entries(file_tile);
        self.start_download_for_entries(file_tile, entries)
    }

    fn start_download_for_file(&mut self, file_tile: TileId, name: String) -> bool {
        let name = name.trim().to_string();
        if name.is_empty() {
            return false;
        }
        let entries = self
            .file_pane(file_tile)
            .map(|file| {
                file.entries
                    .iter()
                    .filter(|entry| entry.file_name == name)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.start_download_for_entries(file_tile, entries)
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
        let source_terminal = self.download_jobs[job_idx].source_terminal;
        let live_transfer = source_terminal
            .and_then(|tile_id| self.sender_for_terminal_tile(tile_id).map(|tx| (tile_id, tx)));

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
            job.issue_kind = Some(ssh::IssueKind::Info);
            job.message = "Retrying...".to_string();
            job.source_terminal = source_terminal;
        }

        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        self.download_cancel_txs.insert(new_request_id, cancel_tx);
        let mut cancel_rx = Some(cancel_rx);
        let mut conflict_rx = None;
        if direction == TransferDirection::Upload {
            let (conflict_tx, new_conflict_rx) = mpsc::channel::<ssh::UploadConflictResponse>();
            self.upload_conflict_response_txs
                .insert(new_request_id, conflict_tx);
            conflict_rx = Some(new_conflict_rx);
        } else {
            self.upload_conflict_response_txs.remove(&new_request_id);
        }
        if let Some(tile_id) = upload_refresh_target {
            self.upload_refresh_targets.insert(new_request_id, tile_id);
        }

        let queued = if let Some((source_tile, tx)) = live_transfer {
            let cmd = match direction {
                TransferDirection::Download => ssh::TransferCommand::Download {
                    request_id: new_request_id,
                    remote_path: remote_path.clone(),
                    local_path: local_path.clone(),
                    resume_from_local: true,
                    event_tx: self.download_event_tx.clone(),
                    cancel_rx: cancel_rx
                        .take()
                        .expect("retry cancel receiver should be present"),
                },
                TransferDirection::Upload => ssh::TransferCommand::Upload {
                    request_id: new_request_id,
                    remote_path: remote_path.clone(),
                    local_path: local_path.clone(),
                    resume_from_remote_temp: true,
                    event_tx: self.download_event_tx.clone(),
                    cancel_rx: cancel_rx
                        .take()
                        .expect("retry cancel receiver should be present"),
                    conflict_response_rx: conflict_rx
                        .take()
                        .expect("retry conflict receiver should be present"),
                },
            };
            match tx.send(WorkerMessage::TransferCommand(cmd)) {
                Ok(()) => {
                    if let Some(job) = self.download_jobs.get_mut(job_idx) {
                        job.source_terminal = Some(source_tile);
                    }
                    true
                }
                Err(_) => {
                    let (fallback_cancel_tx, fallback_cancel_rx) = mpsc::channel::<()>();
                    self.download_cancel_txs
                        .insert(new_request_id, fallback_cancel_tx);
                    cancel_rx = Some(fallback_cancel_rx);
                    if direction == TransferDirection::Upload {
                        let (fallback_conflict_tx, fallback_conflict_rx) =
                            mpsc::channel::<ssh::UploadConflictResponse>();
                        self.upload_conflict_response_txs
                            .insert(new_request_id, fallback_conflict_tx);
                        conflict_rx = Some(fallback_conflict_rx);
                    }
                    false
                }
            }
        } else {
            false
        };

        if !queued {
            let cancel_rx = cancel_rx
                .take()
                .expect("detached retry cancel receiver should be present");
            let log_path = match direction {
                TransferDirection::Download => format!("logs\\download-{new_request_id}.log"),
                TransferDirection::Upload => format!("logs\\upload-{new_request_id}.log"),
            };
            let request = ssh::DetachedTransferRequest {
                settings,
                request_id: new_request_id,
                remote_path,
                local_path,
                event_tx: self.download_event_tx.clone(),
                cancel_rx,
                conflict_response_rx: conflict_rx.take().unwrap_or_else(|| {
                    let (_tx, rx) = mpsc::channel::<ssh::UploadConflictResponse>();
                    rx
                }),
                log_path,
            };
            let _handle = match direction {
                TransferDirection::Download => ssh::start_sftp_download_detached(
                    request,
                    true,
                ),
                TransferDirection::Upload => ssh::start_sftp_upload_detached(
                    request,
                    true,
                ),
            };
        }
        self.persist_transfer_history();
    }

    fn open_download_job_folder(&mut self, request_id: u64) {
        let Some(job) = self.download_jobs.iter_mut().find(|j| j.request_id == request_id) else {
            return;
        };

        let local_path = std::path::PathBuf::from(job.local_path.clone());
        let temp_path = Self::local_transfer_temp_path(&job.local_path);
        let selected_path = if local_path.is_file() {
            local_path.clone()
        } else if job.direction == TransferDirection::Download && temp_path.is_file() {
            temp_path
        } else {
            local_path.clone()
        };
        let file_exists = selected_path.is_file();
        let dir_exists = selected_path.is_dir();
        let mut target_dir = if dir_exists {
            selected_path.clone()
        } else {
            selected_path
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| std::path::PathBuf::from("."))
        };
        if target_dir.as_os_str().is_empty() {
            target_dir = std::path::PathBuf::from(".");
        }

        #[cfg(target_os = "windows")]
        let result = if file_exists {
            std::process::Command::new("explorer")
                .arg(format!("/select,{}", selected_path.display()))
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
            Self::set_download_job_message(
                job,
                ssh::IssueKind::Unknown,
                format!("Failed to open folder: {err}"),
            );
        }
    }

    fn remove_download_job(&mut self, request_id: u64) {
        if let Some(tx) = self.download_cancel_txs.remove(&request_id) {
            let _ = tx.send(());
        }
        self.respond_to_upload_conflict(
            request_id,
            ssh::UploadConflictChoice::CancelTransfer,
            false,
        );
        self.upload_conflict_response_txs.remove(&request_id);
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
                Self::delete_local_download_artifacts(&local_path);
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
        self.upload_conflict_response_txs.remove(&request_id);
        self.clear_upload_conflict_request(request_id);
        self.upload_refresh_targets.remove(&request_id);
        self.download_jobs.remove(job_idx);
        self.persist_transfer_history();
    }

    fn show_next_upload_conflict_prompt(&mut self) {
        if self.upload_conflict_dialog.is_some() {
            return;
        }
        if let Some(prompt) = self.pending_upload_conflict_prompts.pop_front() {
            self.upload_conflict_dialog = Some(UploadConflictDialog {
                prompt,
                apply_to_all: false,
            });
        }
    }

    fn queue_upload_conflict_prompt(&mut self, prompt: ssh::UploadConflictPrompt) {
        self.open_downloads_window();
        self.pending_upload_conflict_prompts.push_back(prompt);
        self.show_next_upload_conflict_prompt();
    }

    fn clear_upload_conflict_request(&mut self, request_id: u64) {
        if self
            .upload_conflict_dialog
            .as_ref()
            .map(|dialog| dialog.prompt.request_id == request_id)
            .unwrap_or(false)
        {
            self.upload_conflict_dialog = None;
        }
        self.pending_upload_conflict_prompts
            .retain(|prompt| prompt.request_id != request_id);
        self.show_next_upload_conflict_prompt();
    }

    fn respond_to_upload_conflict(
        &mut self,
        request_id: u64,
        choice: ssh::UploadConflictChoice,
        apply_to_all: bool,
    ) {
        let send_result = self
            .upload_conflict_response_txs
            .get(&request_id)
            .map(|tx| {
                tx.send(ssh::UploadConflictResponse {
                    choice,
                    apply_to_all,
                })
            })
            .unwrap_or_else(|| Err(mpsc::SendError(ssh::UploadConflictResponse {
                choice,
                apply_to_all,
            })));
        if send_result.is_err() {
            self.upload_conflict_response_txs.remove(&request_id);
        }
        self.clear_upload_conflict_request(request_id);
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
        if self.upload_conflict_response_txs.contains_key(&request_id) {
            self.respond_to_upload_conflict(
                request_id,
                ssh::UploadConflictChoice::CancelTransfer,
                false,
            );
        }
        let mut changed = false;
        if let Some(job) = self
            .download_jobs
            .iter_mut()
            .find(|j| j.request_id == request_id)
        {
            if matches!(job.state, DownloadState::Queued | DownloadState::Running) {
                job.issue_kind = Some(ssh::IssueKind::Info);
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
        let mut saw_event = false;
        while let Ok(event) = self.download_event_rx.try_recv() {
            saw_event = true;
            match event {
                ssh::DownloadManagerEvent::Preparing {
                    request_id,
                    total_bytes,
                    message,
                } => {
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Running;
                        job.total_bytes = total_bytes;
                        job.speed_bps = 0.0;
                        job.issue_kind = Some(ssh::IssueKind::Info);
                        job.message = message;
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::UploadConflictPrompt { prompt } => {
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == prompt.request_id)
                    {
                        job.state = DownloadState::Running;
                        job.speed_bps = 0.0;
                        job.issue_kind = Some(ssh::IssueKind::Info);
                        job.message = format!(
                            "Waiting for upload decision ({}/{})...",
                            prompt.conflict_index, prompt.conflict_total
                        );
                        persist_needed = true;
                    }
                    self.queue_upload_conflict_prompt(prompt);
                }
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
                        job.issue_kind = Some(ssh::IssueKind::Info);
                        job.message = match job.direction {
                            TransferDirection::Download => {
                                if downloaded_bytes > 0 {
                                    "Resuming download...".to_string()
                                } else {
                                    "Downloading...".to_string()
                                }
                            }
                            TransferDirection::Upload => {
                                Self::upload_status_message(&job.local_path, downloaded_bytes > 0)
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
                        job.issue_kind = Some(ssh::IssueKind::Info);
                        job.message = match job.direction {
                            TransferDirection::Download => "Downloading...".to_string(),
                            TransferDirection::Upload => {
                                Self::upload_status_message(&job.local_path, downloaded_bytes > 0)
                            }
                        };
                    }
                }
                ssh::DownloadManagerEvent::Finished {
                    request_id,
                    local_path,
                    message,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_conflict_response_txs.remove(&request_id);
                    self.clear_upload_conflict_request(request_id);
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
                        job.issue_kind = Some(ssh::IssueKind::Info);
                        job.message = message.unwrap_or_else(|| match job.direction {
                            TransferDirection::Download => format!("Saved to {local_path}"),
                            TransferDirection::Upload => {
                                format!("Uploaded to {}", job.remote_path)
                            }
                        });
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
                    issue,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_conflict_response_txs.remove(&request_id);
                    self.clear_upload_conflict_request(request_id);
                    self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Failed;
                        job.speed_bps = 0.0;
                        Self::set_download_job_message(job, issue.kind, match job.direction {
                            TransferDirection::Download => {
                                format!(
                                    "{} Partial download data was kept for retry.",
                                    issue.message
                                )
                            }
                            TransferDirection::Upload => {
                                format!(
                                    "{} Partial remote upload data was kept for retry.",
                                    issue.message
                                )
                            }
                        });
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Paused {
                    request_id,
                    issue,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_conflict_response_txs.remove(&request_id);
                    self.clear_upload_conflict_request(request_id);
                    self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Paused;
                        job.speed_bps = 0.0;
                        Self::set_download_job_message(job, issue.kind, match job.direction {
                            TransferDirection::Download => {
                                format!(
                                    "{} Partial download data was kept for retry.",
                                    issue.message
                                )
                            }
                            TransferDirection::Upload => {
                                format!(
                                    "{} Partial remote upload data was kept for retry.",
                                    issue.message
                                )
                            }
                        });
                        persist_needed = true;
                    }
                }
                ssh::DownloadManagerEvent::Canceled {
                    request_id,
                    local_path,
                } => {
                    self.download_cancel_txs.remove(&request_id);
                    self.upload_conflict_response_txs.remove(&request_id);
                    self.clear_upload_conflict_request(request_id);
                    self.upload_refresh_targets.remove(&request_id);
                    if let Some(job) = self
                        .download_jobs
                        .iter_mut()
                        .find(|j| j.request_id == request_id)
                    {
                        job.state = DownloadState::Canceled;
                        job.speed_bps = 0.0;
                        Self::set_download_job_message(job, ssh::IssueKind::Info, match job.direction {
                            TransferDirection::Download => {
                                format!(
                                    "Canceled ({local_path}). Partial download data was kept for retry."
                                )
                            }
                            TransferDirection::Upload => {
                                format!(
                                    "Upload canceled ({local_path}). Partial remote upload data was kept for retry."
                                )
                            }
                        });
                        persist_needed = true;
                    }
                }
            }
        }
        if saw_event {
            self.last_download_activity = Instant::now();
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
        let mut attach_pending: Vec<TileId> = Vec::new();
        let mut detach_pending: Vec<TileId> = Vec::new();
        let mut refresh_on_connect: Vec<(TileId, String)> = Vec::new();
        for pane_id in pane_ids {
            let Some(file) = self.file_pane(pane_id) else {
                continue;
            };
            let source_tile = file.source_terminal;
            let has_worker = file.worker_tx.is_some() || file.ui_rx.is_some();
            let source_sender = file
                .source_worker_tx
                .as_ref()
                .cloned()
                .or_else(|| self.sender_for_connection_group(file.source_connection_group_id));
            let (_group_connected, group_connecting, group_status) =
                self.connection_group_state(file.source_connection_group_id);
            let (source_connected, source_connecting, source_status) = if source_sender.is_some() {
                (
                    true,
                    group_connecting,
                    if group_status.trim().is_empty() {
                        String::new()
                    } else {
                        group_status
                    },
                )
            } else {
                (
                    false,
                    group_connecting,
                    if !group_status.trim().is_empty() {
                        group_status
                    } else if let Some(source) = self.terminal_pane(source_tile) {
                        source.last_status.clone()
                    } else {
                        "Source terminal is not available".to_string()
                    },
                )
            };
            let (sftp_connected, sftp_connecting, pane_status, pane_status_kind) = self
                .pane(pane_id)
                .map(|pane| {
                    (
                        pane.connected,
                        pane.connecting,
                        pane.last_status.clone(),
                        pane.last_status_kind,
                    )
                })
                .unwrap_or((false, false, String::new(), ssh::IssueKind::Transport));

            if let Some(file) = self.file_pane_mut(pane_id) {
                if source_sender.is_some() {
                    file.source_worker_tx = source_sender.clone();
                }
                let was_connected = file.source_connected;
                let now_connected = sftp_connected;
                file.source_connected = now_connected;

                if now_connected && !was_connected && !file.busy {
                    let pending_path = file.path_input.trim();
                    let path = if pending_path.is_empty() {
                        file.cwd.clone()
                    } else {
                        pending_path.to_string()
                    };
                    refresh_on_connect.push((pane_id, path));
                }

                if (!source_connected || !sftp_connected) && !source_connecting && !sftp_connecting {
                    if file.busy {
                        file.busy = false;
                    }
                    clear_pending_for.push(pane_id);
                    if !source_connected {
                        file.source_worker_tx = None;
                    }
                }

                if !now_connected && !file.busy {
                    file.status_kind = if !source_connected && !source_connecting {
                        ssh::IssueKind::Transport
                    } else if sftp_connecting || source_connecting {
                        ssh::IssueKind::Info
                    } else if !pane_status.trim().is_empty() {
                        pane_status_kind
                    } else {
                        ssh::IssueKind::Transport
                    };
                    file.status = if !source_connected {
                        if source_connecting {
                            "Connecting source terminal...".to_string()
                        } else if !source_status.trim().is_empty() {
                            source_status.clone()
                        } else {
                            "SFTP session is not connected".to_string()
                        }
                    } else if sftp_connecting {
                        "Connecting SFTP session...".to_string()
                    } else if !pane_status.trim().is_empty() {
                        pane_status.clone()
                    } else if source_connecting {
                        "Connecting source terminal...".to_string()
                    } else {
                        "SFTP session is not connected".to_string()
                    };
                }
            }

            if source_connected {
                if !sftp_connected && !sftp_connecting && !has_worker {
                    attach_pending.push(pane_id);
                }
            } else if sftp_connected || sftp_connecting || has_worker {
                detach_pending.push(pane_id);
            }
        }

        for tile_id in detach_pending {
            if let Some(tab) = self.pane_mut(tile_id) {
                tab.disconnect();
            }
        }
        for tile_id in attach_pending {
            if self.file_pane(tile_id).is_some() {
                let _ = self.attach_file_manager_to_source(tile_id);
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
        let mut refresh_after_success: Vec<(TileId, String)> = Vec::new();

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
                    file.clear_selection();
                    file.rename_to.clear();
                    file.rename_from = None;
                    file.delete_confirm = None;
                    file.permissions_dialog = None;
                    file.ownership_dialog = None;
                    file.batch_target_dir = file.cwd.clone();
                    file.batch_destination_mode = None;
                    file.status_kind = ssh::IssueKind::Info;
                    file.status = format!("{} item(s)", file.entries.len());
                }
                ssh::SftpEvent::OperationOk {
                    request_id,
                    message,
                } => {
                    let target_tile = self.pending_sftp_requests.remove(&request_id);
                    let Some(tile_id) = target_tile else { continue };
                    let refresh_path = self
                        .file_pane(tile_id)
                        .map(|file| file.cwd.clone())
                        .unwrap_or_else(|| ".".to_string());
                    let Some(file) = self.file_pane_mut(tile_id) else {
                        continue;
                    };
                    file.busy = false;
                    file.status_kind = ssh::IssueKind::Info;
                    file.status = message;
                    refresh_after_success.push((tile_id, refresh_path));
                }
                ssh::SftpEvent::OperationErr {
                    request_id,
                    issue,
                } => {
                    let target_tile = self.pending_sftp_requests.remove(&request_id);
                    let Some(tile_id) = target_tile else { continue };
                    let Some(file) = self.file_pane_mut(tile_id) else {
                        continue;
                    };
                    file.busy = false;
                    file.status_kind = issue.kind;
                    file.status = issue.message;
                }
            }
        }

        for (tile_id, path) in refresh_after_success {
            if self.file_pane(tile_id).is_some() {
                self.request_file_list(tile_id, path);
            }
        }
    }

    fn set_focus_next_frame(&mut self, tile_id: TileId) {
        if let Some(pane) = self.pane_mut(tile_id) {
            pane.focus_terminal_next_frame = true;
        }
    }

    fn note_ui_profile_activity(&mut self, reason: &str) {
        if crate::logger::ui_profile_enabled() {
            self.ui_profile_hot_frames = self.ui_profile_hot_frames.max(90);
            crate::logger::log_ui_profile(&format!(
                "activity frame={} active_tile={:?} reason={reason}",
                self.ui_profile_frame_index, self.active_tile
            ));
        }
    }

    fn set_active_tile(&mut self, tile_id: Option<TileId>) {
        if self.active_tile != tile_id {
            self.active_tile = tile_id;
            self.active_tile_dirty = true;
            self.last_active_tile_change = Instant::now();
            self.note_ui_profile_activity(&format!("set_active_tile -> {tile_id:?}"));
        }
    }

    fn clear_transient_prompts_for_tile(&mut self, tile_id: TileId) {
        if self
            .auth_dialog
            .as_ref()
            .map(|dialog| dialog.tile_id == tile_id)
            .unwrap_or(false)
        {
            self.auth_dialog = None;
        }
        if self
            .host_key_dialog
            .as_ref()
            .map(|dialog| dialog.tile_id == tile_id)
            .unwrap_or(false)
        {
            self.host_key_dialog = None;
        }
        if let Some(tab) = self.pane_mut(tile_id) {
            tab.pending_auth = None;
            tab.pending_host_key = None;
        }
    }

    fn cycle_active_terminal_focus(&mut self, reverse: bool) -> bool {
        let order = self.visible_terminal_focus_order();
        if order.is_empty() {
            return false;
        }

        let next_tile = self
            .active_tile
            .and_then(|active| order.iter().position(|candidate| *candidate == active))
            .map(|index| {
                if reverse {
                    order[(index + order.len() - 1) % order.len()]
                } else {
                    order[(index + 1) % order.len()]
                }
            })
            .unwrap_or_else(|| {
                if reverse {
                    *order.last().unwrap_or(&order[0])
                } else {
                    order[0]
                }
            });

        self.set_active_tile(Some(next_tile));
        self.settings_dialog.target_tile = Some(next_tile);
        self.set_focus_next_frame(next_tile);
        true
    }

    fn handle_terminal_focus_shortcuts(&mut self, ctx: &egui::Context) {
        if self.auth_dialog.is_some()
            || self.host_key_dialog.is_some()
            || self.rename_popup.is_some()
            || self.transfer_delete_dialog.is_some()
            || self.upload_conflict_dialog.is_some()
        {
            return;
        }

        let reverse = ctx.input_mut(|i| {
            i.consume_key(egui::Modifiers::CTRL | egui::Modifiers::SHIFT, egui::Key::Tab)
        });
        if reverse {
            let _ = self.cycle_active_terminal_focus(true);
            return;
        }

        if ctx.input_mut(|i| i.consume_key(egui::Modifiers::CTRL, egui::Key::Tab)) {
            let _ = self.cycle_active_terminal_focus(false);
        }
    }

    fn add_new_pane_to_tabs(
        &mut self,
        tabs_container_id: TileId,
        base_pane_id: Option<TileId>,
    ) -> Option<TileId> {
        let shared_session =
            base_pane_id.and_then(|id| self.shared_terminal_session_for_pane(id));

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

        let pane_id = self.create_pane(
            settings,
            color,
            profile_name,
            scrollback_len,
            shared_session.is_none(),
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

        self.set_active_tile(Some(pane_id));
        self.settings_dialog.target_tile = Some(pane_id);
        if let Some((worker_tx, connection_group_id)) = shared_session {
            if let Some(tab) = self.terminal_pane_mut(pane_id) {
                tab.connection_group_id = connection_group_id;
            }
            if !self.attach_terminal_to_existing_session(pane_id, worker_tx) {
                if let Some(tab) = self.terminal_pane_mut(pane_id) {
                    tab.connection_group_id = tab.id;
                    tab.start_connect();
                }
            }
        }
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

        if self.settings_dialog.open {
            let viewport_id = settings_viewport_id();
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(true));
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Visible(false));
        }
        if self.downloads_window_open {
            let viewport_id = transfers_viewport_id();
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(true));
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Visible(false));
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
        ctx.send_viewport_cmd(egui::ViewportCommand::Resizable(true));
        ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
        if self.settings_dialog.open {
            let viewport_id = settings_viewport_id();
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(false));
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Visible(true));
        }
        if self.downloads_window_open {
            let viewport_id = transfers_viewport_id();
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Minimized(false));
            ctx.send_viewport_cmd_to(viewport_id, egui::ViewportCommand::Visible(true));
        }
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
                            ctx.send_viewport_cmd(egui::ViewportCommand::Resizable(true));
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
            true,
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

        self.set_active_tile(Some(pane_id));
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
            self.set_active_tile(self.first_pane_id());
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
        let shared_session = self.shared_terminal_session_for_pane(pane_id);
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

        let tabs_container_id = self.tree.tiles.parent_of(pane_id)?;
        let parent_of_tabs = self.tree.tiles.parent_of(tabs_container_id);

        let new_pane_id = self.create_pane(
            settings,
            color,
            profile_name,
            scrollback_len,
            shared_session.is_none(),
        );
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

        self.set_active_tile(Some(new_pane_id));
        self.settings_dialog.target_tile = Some(new_pane_id);
        if let Some((worker_tx, connection_group_id)) = shared_session {
            if let Some(tab) = self.terminal_pane_mut(new_pane_id) {
                tab.connection_group_id = connection_group_id;
            }
            if !self.attach_terminal_to_existing_session(new_pane_id, worker_tx) {
                if let Some(tab) = self.terminal_pane_mut(new_pane_id) {
                    tab.connection_group_id = tab.id;
                    tab.start_connect();
                }
            }
        }
        Some(new_pane_id)
    }
}

#[cfg(test)]
mod title_index_tests {
    use super::AppState;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn lowest_unused_terminal_title_index_reuses_gaps() {
        assert_eq!(AppState::lowest_unused_terminal_title_index([2, 3]), 1);
        assert_eq!(AppState::lowest_unused_terminal_title_index([1, 3]), 2);
        assert_eq!(AppState::lowest_unused_terminal_title_index([1, 2, 3]), 4);
        assert_eq!(AppState::lowest_unused_terminal_title_index([0, 1, 1, 4]), 2);
    }

    #[test]
    fn delete_local_download_artifacts_removes_partial_temp_file() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rusty-download-delete-{stamp}"));
        fs::create_dir_all(&dir).expect("create temp dir");

        let local_path = dir.join("report.txt");
        let temp_path = AppState::local_transfer_temp_path(&local_path.display().to_string());
        fs::write(&temp_path, b"partial").expect("write temp file");

        AppState::delete_local_download_artifacts(&local_path.display().to_string());

        assert!(!local_path.exists());
        assert!(!temp_path.exists());

        let _ = fs::remove_dir_all(&dir);
    }
}
