impl eframe::App for AppState {
    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        // Use an opaque clear to avoid VM OpenGL compositing artifacts that can blur text.
        self.theme.bg.to_normalized_gamma_f32()
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // VM OpenGL paths can become blurry at fractional DPI scales (e.g. 125%).
        // Enforce integer pixels-per-point every frame.
        let ppp = ctx.pixels_per_point();
        let snapped = ppp.round().max(1.0);
        if (ppp - snapped).abs() > 0.01 {
            ctx.set_pixels_per_point(snapped);
        }

        // Allow tray callbacks to wake the app even while the viewport is hidden.
        crate::tray::set_wake_ctx(ctx.clone());

        if !self.style_initialized {
            egui_extras::install_image_loaders(ctx);
            self.apply_global_style(ctx);
            self.style_initialized = true;
        }

        self.maybe_restore_window(ctx);

        // Tray integration (created lazily when enabled).
        // If tray minimize was turned off while hidden, bring the window back.
        if !self.config.minimize_to_tray && self.hidden_to_tray {
            self.show_from_tray(ctx);
            self.tray = None;
        }
        self.ensure_tray_icon();
        self.handle_tray_events(ctx);

        self.update_cursor_blink();

        self.ensure_tree_non_empty();

        let mut saw_terminal_activity = false;
        let mut live_session_count: usize = 0;
        for tile_id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(tile_id) {
                if tab.connected || tab.connecting {
                    live_session_count += 1;
                }
                if let Some(rows) = tab.pending_scrollback {
                    if let Some(tx) = tab.worker_tx.as_ref() {
                        let _ = tx.send(WorkerMessage::SetScrollback(rows));
                    }
                }
                if tab.poll_messages() {
                    saw_terminal_activity = true;
                }
            }
        }
        if saw_terminal_activity {
            self.last_terminal_activity = Instant::now();
        }
        self.route_sftp_events();
        self.poll_download_manager_events();
        self.sync_file_panes_with_sources();
        self.poll_update_check_result();
        self.start_update_check_if_due();

        let any_live_session = live_session_count > 0;
        let any_active_download = self.has_active_downloads();
        let ui_modal_open = self.auth_dialog.is_some()
            || self.host_key_dialog.is_some()
            || self.rename_popup.is_some()
            || self.downloads_window_open
            || self.settings_dialog.open
            || self.transfer_delete_dialog.is_some();
        let recent_terminal_activity = any_live_session
            && self.last_terminal_activity.elapsed() <= Duration::from_millis(250);

        let activity_repaint_ms = if live_session_count >= 3 {
            24
        } else if live_session_count >= 2 {
            20
        } else {
            16
        };
        let mut repaint_ms = if self.hidden_to_tray {
            200
        } else if ui_modal_open || any_active_download {
            16
        } else if recent_terminal_activity {
            activity_repaint_ms
        } else if any_live_session {
            33
        } else {
            80
        };
        if any_live_session {
            repaint_ms = repaint_ms.min(self.ms_until_cursor_blink_toggle());
        }
        ctx.request_repaint_after(Duration::from_millis(repaint_ms));

        // Copy flash: after a successful copy, briefly flash the selection then clear it.
        let now = Instant::now();
        for tile_id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(tile_id) {
                if let Some(until) = tab.copy_flash_until {
                    if now >= until {
                        tab.copy_flash_until = None;
                        tab.selection = None;
                        tab.abs_selection = None;
                    }
                }
            }
        }

        // If any SSH worker needs host-key trust confirmation, pop that modal first.
        if self.host_key_dialog.is_none() {
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
                let pending = {
                    let Some(tab) = self.pane_mut(id) else {
                        continue;
                    };
                    tab.pending_host_key.take()
                };
                if let Some(prompt) = pending {
                    self.host_key_dialog = Some(HostKeyDialog {
                        tile_id: id,
                        prompt,
                    });
                    break;
                }
            }
        }

        // If any SSH worker is asking for keyboard-interactive input (password, OTP, etc),
        // pop a modal dialog to collect it.
        if self.auth_dialog.is_none() && self.host_key_dialog.is_none() {
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
                let pending = {
                    let Some(tab) = self.pane_mut(id) else {
                        continue;
                    };
                    tab.pending_auth
                        .take()
                        .map(|p| (p, tab.profile_name.clone()))
                };
                if let Some((p, profile_name)) = pending {
                    let n = p.prompts.len();
                    let remember_key_passphrase = profile_name
                        .as_deref()
                        .and_then(|name| config::find_profile_index(&self.config, name))
                        .and_then(|i| self.config.profiles.get(i))
                        .map(|p| p.remember_key_passphrase)
                        .unwrap_or(false);
                    self.auth_dialog = Some(AuthDialog {
                        tile_id: id,
                        profile_name,
                        instructions: p.instructions,
                        prompts: p.prompts,
                        responses: vec![String::new(); n],
                        just_opened: true,
                        remember_key_passphrase,
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
                let drag_resp = ui.interact(
                    bar_rect,
                    Id::new("rusty_title_drag"),
                    Sense::click_and_drag(),
                );
                if drag_resp.drag_started() {
                    ctx.send_viewport_cmd(egui::ViewportCommand::StartDrag);
                }
                if drag_resp.double_clicked() {
                    let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                }

                let btn_fill = adjust_color(theme.top_bg, 0.10);

                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(APP_TITLE_TEXT)
                            .strong()
                            .color(theme.accent)
                            .size(16.0),
                    );

                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        let close_icon =
                            egui::Image::new(egui::include_image!("../../assets/x.png"))
                                .tint(theme.fg);
                        if title_bar_image_button(
                            ui,
                            close_icon,
                            Vec2::splat(12.0),
                            btn_fill,
                            theme.top_border,
                        )
                        .clicked()
                        {
                            global_actions.push(TilesAction::Exit);
                        }

                        let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                        let maximize_icon =
                            egui::Image::new(egui::include_image!("../../assets/square.png"))
                                .tint(theme.fg);
                        if title_bar_image_button(
                            ui,
                            maximize_icon,
                            Vec2::splat(12.0),
                            btn_fill,
                            theme.top_border,
                        )
                        .clicked()
                        {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                        }

                        // Minimize button (taskbar or tray, depending on settings).
                        let minimize_icon =
                            egui::Image::new(egui::include_image!("../../assets/minus.png"))
                                .tint(theme.fg);
                        if title_bar_image_button(
                            ui,
                            minimize_icon,
                            Vec2::new(14.0, 14.0),
                            btn_fill,
                            theme.top_border,
                        )
                        .clicked()
                        {
                            if self.config.minimize_to_tray {
                                self.minimize_to_tray_requested = true;
                            } else {
                                ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                            }
                        }

                        let target = self.cog_target_tile();

                        let settings_icon =
                            egui::Image::new(egui::include_image!("../../assets/settings.png"))
                                .tint(theme.fg);
                        let settings_resp = title_bar_image_button(
                            ui,
                            settings_icon,
                            Vec2::splat(14.0),
                            btn_fill,
                            theme.top_border,
                        )
                        .on_hover_text("Open Settings");
                        if settings_resp.clicked() {
                            if let Some(tile_id) = target {
                                global_actions.push(TilesAction::OpenSettings(tile_id));
                            }
                        }

                        // Global downloads manager opener.
                        let download_icon =
                            egui::Image::new(egui::include_image!("../../assets/download.png"))
                                .tint(theme.fg);
                        if title_bar_image_button(
                            ui,
                            download_icon,
                            Vec2::splat(14.0),
                            btn_fill,
                            theme.top_border,
                        )
                        .on_hover_text("Open Transfers Manager")
                        .clicked()
                        {
                            self.open_downloads_window();
                        }

                    });
                });
            });

        let mut behavior = SshTilesBehavior::new(
            theme,
            term_theme,
            cursor_visible,
            term_font_size,
            !self.hidden_to_tray,
            self.config.focus_shade,
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
                    self.layout_dirty = true;
                }
                TilesAction::NewTabWithSettings {
                    tabs_container_id,
                    settings,
                    color,
                    profile_name,
                } => {
                    let _ = self.add_new_pane_to_tabs_with_settings(
                        tabs_container_id,
                        settings,
                        color,
                        profile_name,
                    );
                    self.layout_dirty = true;
                }
                TilesAction::TabActivated(tile_id) => {
                    if matches!(self.tree.tiles.get(tile_id), Some(Tile::Pane(_))) {
                        self.active_tile = Some(tile_id);
                        if self.terminal_pane(tile_id).is_some() {
                            self.settings_dialog.target_tile = Some(tile_id);
                            self.set_focus_next_frame(tile_id);
                        }
                        self.layout_dirty = true;
                    }
                }
                TilesAction::Connect(tile_id) => {
                    let (missing_settings, connected_or_connecting) = self
                        .terminal_pane(tile_id)
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
                    } else if let Some(tab) = self.terminal_pane_mut(tile_id) {
                        tab.start_connect();
                        tab.focus_terminal_next_frame = true;
                    }
                    self.active_tile = Some(tile_id);
                    self.settings_dialog.target_tile = Some(tile_id);
                }
                TilesAction::ToggleConnect(tile_id) => {
                    let needs_settings = self
                        .terminal_pane(tile_id)
                        .map(|t| {
                            t.settings.host.trim().is_empty()
                                || t.settings.username.trim().is_empty()
                        })
                        .unwrap_or(true);
                    if needs_settings {
                        self.open_settings_dialog_for_tile(tile_id);
                    } else if let Some(tab) = self.terminal_pane_mut(tile_id) {
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
                TilesAction::OpenFileManager(tile_id) => {
                    let _ = self.open_file_manager_for_terminal(tile_id);
                    self.layout_dirty = true;
                }
                TilesAction::Rename(tile_id) => {
                    if let Some(tab) = self.pane(tile_id) {
                        let initial = tab.user_title.clone().unwrap_or_else(|| tab.title.clone());
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
                        self.layout_dirty = true;
                    }
                }
                TilesAction::Split { pane_id, dir } => {
                    let _ = self.split_pane(pane_id, dir);
                    self.layout_dirty = true;
                }
                TilesAction::FileRefresh { pane_id, path } => {
                    self.request_file_list(pane_id, path);
                }
                TilesAction::FileUp(pane_id) => {
                    self.request_file_up(pane_id);
                }
                TilesAction::FileMkdir { pane_id, dir_name } => {
                    self.request_file_mkdir(pane_id, dir_name);
                    let path = self
                        .file_pane(pane_id)
                        .map(|f| f.cwd.clone())
                        .unwrap_or_else(|| ".".to_string());
                    self.request_file_list(pane_id, path);
                }
                TilesAction::FileRename {
                    pane_id,
                    from_name,
                    to_name,
                } => {
                    self.request_file_rename(pane_id, from_name, to_name);
                    let path = self
                        .file_pane(pane_id)
                        .map(|f| f.cwd.clone())
                        .unwrap_or_else(|| ".".to_string());
                    self.request_file_list(pane_id, path);
                }
                TilesAction::FileDelete {
                    pane_id,
                    name,
                    is_dir,
                } => {
                    self.request_file_delete(pane_id, name, is_dir);
                    let path = self
                        .file_pane(pane_id)
                        .map(|f| f.cwd.clone())
                        .unwrap_or_else(|| ".".to_string());
                    self.request_file_list(pane_id, path);
                }
                TilesAction::FileUpload { pane_id } => {
                    let _ = self.start_upload_for_file(pane_id);
                }
                TilesAction::FileDownload { pane_id, name } => {
                    self.start_download_for_file(pane_id, name);
                }
                TilesAction::Close(tile_id) => {
                    self.close_pane(tile_id);
                    self.layout_dirty = true;
                }
                TilesAction::Exit => {
                    self.layout_dirty = true;
                    self.maybe_save_session_layout(ctx);
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
                self.layout_dirty = true;
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
        self.draw_host_key_dialog(ctx);
        self.draw_auth_dialog(ctx);
        self.draw_downloads_manager_window(ctx);

        self.maybe_save_session_layout(ctx);

        self.clipboard = clipboard;
    }
}
