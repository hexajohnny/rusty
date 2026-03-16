impl eframe::App for AppState {
    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        // Use an opaque clear to avoid VM OpenGL compositing artifacts that can blur text.
        self.theme.bg.to_normalized_gamma_f32()
    }

    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.ui_profile_frame_index = self.ui_profile_frame_index.saturating_add(1);
        let frame_index = self.ui_profile_frame_index;
        let frame_started = Instant::now();
        let profile_enabled = crate::logger::ui_profile_enabled();

        // Some VM + wgpu-gl combinations render text more sharply when egui stays on an
        // integer pixels-per-point. Do not force this on pure glow, where it can blur text.
        if self.snap_fractional_dpi {
            let ppp = ctx.pixels_per_point();
            let snapped = ppp.round().max(1.0);
            if (ppp - snapped).abs() > 0.01 {
                ctx.set_pixels_per_point(snapped);
            }
        }

        // Allow tray callbacks to wake the app even while the viewport is hidden.
        crate::tray::set_wake_ctx(ctx.clone());

        self.ensure_global_style(ctx);

        self.maybe_restore_window(ctx);
        crate::tray::ensure_native_main_hit_test();

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

        let mut clipboard = self.clipboard.take();
        let message_poll_started = Instant::now();
        let mut saw_terminal_activity = false;
        let mut live_session_count: usize = 0;
        for tile_id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(tile_id) {
                if tab.connected || tab.connecting {
                    live_session_count += 1;
                }
                if let Some(rows) = tab.pending_scrollback {
                    if let Some(tx) = tab.worker_tx.as_ref() {
                        let _ = tx.send(WorkerMessage::SetScrollback {
                            client_id: tab.id,
                            rows,
                        });
                    }
                }
                if tab.poll_messages(ctx, &mut clipboard) {
                    saw_terminal_activity = true;
                }
            }
        }
        if saw_terminal_activity {
            self.last_terminal_activity = Instant::now();
        }
        self.sync_shared_terminal_groups();
        self.route_sftp_events();
        self.poll_download_manager_events();
        self.sync_file_panes_with_sources();
        self.poll_update_check_result();
        let message_poll_dt = message_poll_started.elapsed();

        let any_live_session = live_session_count > 0;
        let any_active_download = self.has_active_downloads();
        let now = Instant::now();
        let mut next_copy_flash_ms: Option<u64> = None;
        for tile_id in self.pane_ids() {
            if let Some(tab) = self.pane_mut(tile_id) {
                if let Some(until) = tab.copy_flash_until {
                    if now >= until {
                        tab.copy_flash_until = None;
                        tab.selection = None;
                        tab.abs_selection = None;
                    } else {
                        let remaining_ms = until
                            .saturating_duration_since(now)
                            .as_millis()
                            .clamp(1, u64::MAX as u128) as u64;
                        next_copy_flash_ms = Some(
                            next_copy_flash_ms
                                .map(|current| current.min(remaining_ms))
                                .unwrap_or(remaining_ms),
                        );
                    }
                }
            }
        }

        let mut repaint_ms = if any_live_session && !self.low_power_renderer {
            Some(self.ms_until_cursor_blink_toggle())
        } else {
            None
        };
        if let Some(copy_flash_ms) = next_copy_flash_ms {
            repaint_ms = Some(
                repaint_ms
                    .map(|current| current.min(copy_flash_ms))
                    .unwrap_or(copy_flash_ms),
            );
        }
        if !self.hidden_to_tray && (any_live_session || any_active_download) {
            let fallback_ms = if self.low_power_renderer { 15_000 } else { 5_000 };
            repaint_ms = Some(
                repaint_ms
                    .map(|current| current.min(fallback_ms))
                    .unwrap_or(fallback_ms),
            );
        }
        if let Some(repaint_ms) = repaint_ms {
            ctx.request_repaint_after(Duration::from_millis(repaint_ms));
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

        self.handle_terminal_focus_shortcuts(ctx);

        let theme = self.theme;
        self.term_theme = TermTheme::from_config(&self.config.terminal_colors);
        let term_theme = self.term_theme;
        let cursor_visible = self.cursor_visible;
        let mut term_font_size = self.config.terminal_font_size;
        if !term_font_size.is_finite() || term_font_size <= 0.0 {
            term_font_size = TERM_FONT_SIZE_DEFAULT;
        }
        term_font_size = term_font_size.clamp(TERM_FONT_SIZE_MIN, TERM_FONT_SIZE_MAX);

        let chrome_started = Instant::now();
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

                let bar_rect = Rect::from_min_size(
                    ui.cursor().min,
                    Vec2::new(ui.available_width(), TITLE_BAR_H),
                );
                let drag_resp = ui.interact(
                    bar_rect,
                    Id::new("rusty_title_drag"),
                    Sense::click_and_drag(),
                );

                let btn_fill = adjust_color(theme.top_bg, 0.10);
                let mut title_controls_hot = false;

                ui.allocate_ui_at_rect(bar_rect, |ui| {
                    ui.with_layout(egui::Layout::left_to_right(Align::Center), |ui| {
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
                            let close_resp = title_bar_image_button(
                                ui,
                                close_icon,
                                Vec2::splat(12.0),
                                btn_fill,
                                theme.top_border,
                            );
                            title_controls_hot |= close_resp.hovered();
                            if close_resp.clicked() {
                                global_actions.push(TilesAction::Exit);
                            }

                            let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                            let maximize_icon =
                                egui::Image::new(egui::include_image!("../../assets/square.png"))
                                    .tint(theme.fg);
                            let maximize_resp = title_bar_image_button(
                                ui,
                                maximize_icon,
                                Vec2::splat(12.0),
                                btn_fill,
                                theme.top_border,
                            );
                            title_controls_hot |= maximize_resp.hovered();
                            if maximize_resp.clicked() {
                                ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                            }

                            // Minimize button (taskbar or tray, depending on settings).
                            let minimize_icon =
                                egui::Image::new(egui::include_image!("../../assets/minus.png"))
                                    .tint(theme.fg);
                            let minimize_resp = title_bar_image_button(
                                ui,
                                minimize_icon,
                                Vec2::new(14.0, 14.0),
                                btn_fill,
                                theme.top_border,
                            );
                            title_controls_hot |= minimize_resp.hovered();
                            if minimize_resp.clicked() {
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
                            title_controls_hot |= settings_resp.hovered();
                            if settings_resp.clicked() {
                                if let Some(tile_id) = target {
                                    global_actions.push(TilesAction::OpenSettings(tile_id));
                                }
                            }

                            // Global downloads manager opener.
                            let download_icon =
                                egui::Image::new(egui::include_image!("../../assets/download.png"))
                                    .tint(theme.fg);
                            let downloads_resp = title_bar_image_button(
                                ui,
                                download_icon,
                                Vec2::splat(14.0),
                                btn_fill,
                                theme.top_border,
                            )
                            .on_hover_text("Open Transfers Manager");
                            title_controls_hot |= downloads_resp.hovered();
                            if downloads_resp.clicked() {
                                self.open_downloads_window();
                            }
                        });
                    });
                });
                ui.advance_cursor_after_rect(bar_rect);

                let pressed_on_title =
                    drag_resp.hovered() && ctx.input(|i| i.pointer.primary_pressed());
                if drag_resp.drag_started() || (pressed_on_title && !title_controls_hot) {
                    begin_window_drag(ctx);
                }
                if drag_resp.double_clicked() && !title_controls_hot {
                    let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                    ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                }
            });

        if self.startup_notice.is_some() {
            let mut dismiss_notice = false;
            let notice_text = self.startup_notice.clone().unwrap_or_default();
            egui::TopBottomPanel::top("rusty_startup_notice")
                .resizable(false)
                .frame(
                    egui::Frame::none()
                        .fill(adjust_color(theme.top_bg, 0.10))
                        .stroke(Stroke::new(1.0, theme.top_border))
                        .inner_margin(egui::Margin::symmetric(TITLE_PAD_X, 6.0)),
                )
                .show(ctx, |ui| {
                    ui.visuals_mut().override_text_color = Some(theme.fg);
                    ui.horizontal_wrapped(|ui| {
                        ui.label(
                            egui::RichText::new("Startup Notice")
                                .strong()
                                .color(theme.accent),
                        );
                        ui.label(egui::RichText::new(notice_text).color(theme.fg));
                        ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                            if ui.button("Dismiss").clicked() {
                                dismiss_notice = true;
                            }
                        });
                    });
                });
            if dismiss_notice {
                self.startup_notice = None;
            }
        }
        let chrome_dt = chrome_started.elapsed();

        let profiles_build_started = Instant::now();
        let profiles = self
            .config
            .profiles
            .iter()
            .map(|p| (p.name.clone(), config::write_profile_settings(p)))
            .collect();
        let profiles_build_dt = profiles_build_started.elapsed();

        let mut behavior = SshTilesBehavior::new(SshTilesBehaviorInit {
            theme,
            term_theme,
            cursor_visible,
            term_font_size,
            allow_resize: !self.hidden_to_tray,
            focus_shade: self.config.focus_shade,
            profiles,
            clipboard: &mut clipboard,
            active_tile: self.active_tile,
        });

        let tree_ui_started = Instant::now();
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
        let tree_ui_dt = tree_ui_started.elapsed();

        self.active_tile = behavior.active_tile;

        // Apply behavior actions after the tree has been drawn.
        let mut actions = global_actions;
        actions.extend(std::mem::take(&mut behavior.actions));
        drop(behavior);
        self.clipboard = clipboard;

        let mut action_labels: Vec<&'static str> = Vec::new();
        let actions_started = Instant::now();
        for action in actions {
            if action_labels.len() < 8 {
                action_labels.push(match &action {
                    TilesAction::NewTab { .. } => "new_tab",
                    TilesAction::NewTabWithSettings { .. } => "new_tab_profile",
                    TilesAction::TabActivated(_) => "tab_activated",
                    TilesAction::Connect(_) => "connect",
                    TilesAction::ToggleConnect(_) => "toggle_connect",
                    TilesAction::OpenFileManager(_) => "open_file_manager",
                    TilesAction::OpenSettings(_) => "open_settings",
                    TilesAction::Rename(_) => "rename",
                    TilesAction::SetColor { .. } => "set_color",
                    TilesAction::Split { .. } => "split",
                    TilesAction::FileRefresh { .. } => "file_refresh",
                    TilesAction::FileUp(_) => "file_up",
                    TilesAction::FileMkdir { .. } => "file_mkdir",
                    TilesAction::FileRename { .. } => "file_rename",
                    TilesAction::FileDelete { .. } => "file_delete",
                    TilesAction::FileUploadFiles { .. } => "file_upload_files",
                    TilesAction::FileUploadFolder { .. } => "file_upload_folder",
                    TilesAction::FileUploadPaths { .. } => "file_upload_paths",
                    TilesAction::FileDownload { .. } => "file_download",
                    TilesAction::FileDownloadSelected { .. } => "file_download_selected",
                    TilesAction::FileCopy { .. } => "file_copy",
                    TilesAction::FileMove { .. } => "file_move",
                    TilesAction::FileSetPermissions { .. } => "file_set_permissions",
                    TilesAction::FileSetOwnership { .. } => "file_set_ownership",
                    TilesAction::Close(_) => "close",
                    TilesAction::Exit => "exit",
                });
            }
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
                        self.set_active_tile(Some(tile_id));
                        if self.terminal_pane(tile_id).is_some() {
                            self.settings_dialog.target_tile = Some(tile_id);
                            self.set_focus_next_frame(tile_id);
                        }
                    }
                }
                TilesAction::Connect(tile_id) => {
                    if let Some(connection_group_id) = self
                        .terminal_pane(tile_id)
                        .map(|tab| tab.connection_group_id)
                    {
                        self.clear_transient_prompts_for_connection_group(connection_group_id);
                    } else {
                        self.clear_transient_prompts_for_tile(tile_id);
                    }
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
                    } else {
                        let _ = self.reconnect_terminal_group(tile_id);
                    }
                    self.set_active_tile(Some(tile_id));
                    self.settings_dialog.target_tile = Some(tile_id);
                }
                TilesAction::ToggleConnect(tile_id) => {
                    if let Some(connection_group_id) = self
                        .terminal_pane(tile_id)
                        .map(|tab| tab.connection_group_id)
                    {
                        self.clear_transient_prompts_for_connection_group(connection_group_id);
                    } else {
                        self.clear_transient_prompts_for_tile(tile_id);
                    }
                    let needs_settings = self
                        .terminal_pane(tile_id)
                        .map(|t| {
                            t.settings.host.trim().is_empty()
                                || t.settings.username.trim().is_empty()
                        })
                        .unwrap_or(true);
                    if needs_settings {
                        self.open_settings_dialog_for_tile(tile_id);
                    } else {
                        let live_connection = self
                            .terminal_pane(tile_id)
                            .map(|tab| tab.connecting || tab.connected)
                            .unwrap_or(false);
                        if live_connection {
                            if let Some(tab) = self.terminal_pane_mut(tile_id) {
                                tab.disconnect();
                                tab.focus_terminal_next_frame = true;
                            }
                        } else {
                            let _ = self.reconnect_terminal_group(tile_id);
                        }
                    }
                    self.set_active_tile(Some(tile_id));
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
                }
                TilesAction::FileRename {
                    pane_id,
                    from_name,
                    to_name,
                } => {
                    self.request_file_rename(pane_id, from_name, to_name);
                }
                TilesAction::FileDelete {
                    pane_id,
                    names,
                } => {
                    self.request_file_delete(pane_id, names);
                }
                TilesAction::FileCopy {
                    pane_id,
                    names,
                    destination_dir,
                } => {
                    self.request_file_copy(pane_id, names, destination_dir);
                }
                TilesAction::FileMove {
                    pane_id,
                    names,
                    destination_dir,
                } => {
                    self.request_file_move(pane_id, names, destination_dir);
                }
                TilesAction::FileSetPermissions {
                    pane_id,
                    names,
                    mode,
                } => {
                    self.request_file_set_permissions(pane_id, names, mode);
                }
                TilesAction::FileSetOwnership {
                    pane_id,
                    names,
                    owner,
                    group,
                } => {
                    self.request_file_set_ownership(pane_id, names, owner, group);
                }
                TilesAction::FileUploadFiles { pane_id } => {
                    let _ = self.start_upload_from_files_picker(pane_id);
                }
                TilesAction::FileUploadFolder { pane_id } => {
                    let _ = self.start_upload_from_folder_picker(pane_id);
                }
                TilesAction::FileUploadPaths { pane_id, paths } => {
                    let _ = self.start_upload_paths(pane_id, paths);
                }
                TilesAction::FileDownload { pane_id, name } => {
                    let _ = self.start_download_for_file(pane_id, name);
                }
                TilesAction::FileDownloadSelected { pane_id } => {
                    let _ = self.start_download_for_selected(pane_id);
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
        let actions_dt = actions_started.elapsed();

        // Ensure the active tile is still valid after actions (e.g. closing tabs).
        if let Some(active) = self.active_tile {
            if !matches!(self.tree.tiles.get(active), Some(Tile::Pane(_))) {
                self.set_active_tile(self.first_pane_id());
            }
        }

        let rename_popup_started = Instant::now();
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
        let rename_popup_dt = rename_popup_started.elapsed();

        if self.minimize_to_tray_requested {
            self.minimize_to_tray_requested = false;
            self.hide_to_tray(ctx);
        }

        let resize_started = Instant::now();
        handle_window_resize(ctx);
        let resize_dt = resize_started.elapsed();

        let dialogs_started = Instant::now();
        self.draw_settings_dialog(ctx);
        self.draw_host_key_dialog(ctx);
        self.draw_auth_dialog(ctx);
        self.draw_downloads_manager_window(ctx);
        let dialogs_dt = dialogs_started.elapsed();

        let save_started = Instant::now();
        self.maybe_save_session_layout(ctx);
        let save_dt = save_started.elapsed();

        let frame_dt = frame_started.elapsed();
        if profile_enabled {
            let hot = self.ui_profile_hot_frames > 0;
            let log_due = hot
                || frame_dt >= Duration::from_millis(16)
                || message_poll_dt >= Duration::from_millis(8)
                || chrome_dt >= Duration::from_millis(8)
                || profiles_build_dt >= Duration::from_millis(4)
                || tree_ui_dt >= Duration::from_millis(8)
                || actions_dt >= Duration::from_millis(8)
                || rename_popup_dt >= Duration::from_millis(8)
                || resize_dt >= Duration::from_millis(8)
                || dialogs_dt >= Duration::from_millis(8)
                || save_dt >= Duration::from_millis(8);

            if log_due {
                let pane_count = self.pane_ids().len();
                let action_summary = if action_labels.is_empty() {
                    "none".to_string()
                } else {
                    action_labels.join(",")
                };
                crate::logger::log_ui_profile(&format!(
                    "frame={} total_ms={:.2} poll_ms={:.2} chrome_ms={:.2} profiles_ms={:.2} tree_ms={:.2} actions_ms={:.2} rename_ms={:.2} resize_ms={:.2} dialogs_ms={:.2} save_ms={:.2} panes={} profiles={} live_sessions={} downloads={} active_tile={:?} hidden_to_tray={} settings_open={} downloads_open={} hot={} actions={}",
                    frame_index,
                    frame_dt.as_secs_f64() * 1000.0,
                    message_poll_dt.as_secs_f64() * 1000.0,
                    chrome_dt.as_secs_f64() * 1000.0,
                    profiles_build_dt.as_secs_f64() * 1000.0,
                    tree_ui_dt.as_secs_f64() * 1000.0,
                    actions_dt.as_secs_f64() * 1000.0,
                    rename_popup_dt.as_secs_f64() * 1000.0,
                    resize_dt.as_secs_f64() * 1000.0,
                    dialogs_dt.as_secs_f64() * 1000.0,
                    save_dt.as_secs_f64() * 1000.0,
                    pane_count,
                    self.config.profiles.len(),
                    live_session_count,
                    any_active_download,
                    self.active_tile,
                    self.hidden_to_tray,
                    self.settings_dialog.open,
                    self.downloads_window_open,
                    hot,
                    action_summary,
                ));
            }

            self.ui_profile_hot_frames = self.ui_profile_hot_frames.saturating_sub(1);
        }
    }
}
