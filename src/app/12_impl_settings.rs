impl AppState {
    fn ms_until_cursor_blink_toggle(&self) -> u64 {
        if self.low_power_renderer {
            return u64::MAX;
        }
        let blink_period = Duration::from_millis(530);
        let elapsed = self.last_cursor_blink.elapsed();
        if elapsed >= blink_period {
            return 1;
        }
        let remaining = blink_period.saturating_sub(elapsed);
        remaining.as_millis().clamp(1, u64::MAX as u128) as u64
    }

    fn update_cursor_blink(&mut self) {
        if self.low_power_renderer {
            self.cursor_visible = true;
            return;
        }
        if self.last_cursor_blink.elapsed() >= Duration::from_millis(530) {
            self.cursor_visible = !self.cursor_visible;
            self.last_cursor_blink = Instant::now();
        }
    }

    fn clipboard_text(clipboard: &mut Option<Clipboard>) -> Option<String> {
        let cb = clipboard.as_mut()?;
        let text = cb.get_text().ok()?;
        if text.is_empty() {
            return None;
        }
        Some(text)
    }

    fn open_settings_dialog_for_tile(&mut self, tile_id: TileId) {
        let Some((settings, tab_profile_name)) = self
            .pane(tile_id)
            .map(|t| (t.settings.clone(), t.profile_name.clone()))
        else {
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

        // Prefer the tab's explicit profile link when present.
        let explicit_profile = tab_profile_name
            .as_deref()
            .and_then(|name| config::find_profile_index(&self.config, name))
            .and_then(|idx| self.config.profiles.get(idx).cloned().map(|p| (idx, p)));

        // Fallback: best-effort match by endpoint identity.
        let endpoint_profile = self
            .config
            .profiles
            .iter()
            .enumerate()
            .find(|(_, p)| {
                p.settings.host.trim() == settings.host.trim()
                    && p.settings.port == settings.port
                    && p.settings.username.trim() == settings.username.trim()
            })
            .map(|(idx, p)| (idx, p.clone()));

        if let Some((idx, p)) = explicit_profile.or(endpoint_profile) {
            self.settings_dialog.selected_profile = Some(idx);
            self.settings_dialog.profile_name = p.name.clone();
            self.settings_dialog.remember_password = p.remember_password;
            self.settings_dialog.remember_key_passphrase = p.remember_key_passphrase;
            self.settings_dialog.draft = config::write_profile_settings(&p);
        } else {
            self.settings_dialog.selected_profile = None;
            self.settings_dialog.profile_name.clear();
            self.settings_dialog.remember_password = false;
            self.settings_dialog.remember_key_passphrase = false;
        }
    }

    fn load_profile_into_dialog(&mut self, idx: usize) {
        let Some(p) = self.config.profiles.get(idx).cloned() else {
            return;
        };
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
            self.settings_dialog.selected_profile =
                Some(self.config.profiles.len().saturating_sub(1));
        }

        self.config_saver.request_save(self.config.clone());

        let updated_settings = self.settings_dialog.draft.clone();
        let target_tile = self.settings_dialog.target_tile;
        let pane_ids = self.pane_ids();
        for tile_id in pane_ids {
            if let Some(tab) = self.pane_mut(tile_id) {
                let linked = tab
                    .profile_name
                    .as_deref()
                    .map(|n| n.eq_ignore_ascii_case(&name))
                    .unwrap_or(false);
                let is_target = Some(tile_id) == target_tile;
                if linked || is_target {
                    // Keep linked tabs (and the settings target tab) aligned with profile edits.
                    tab.profile_name = Some(name.clone());
                    tab.settings = updated_settings.clone();
                    tab.title = SshTab::title_for(tab.id, &tab.settings);
                }
            }
        }
    }

    fn delete_selected_profile(&mut self) {
        let Some(idx) = self.settings_dialog.selected_profile else {
            return;
        };
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

        self.config_saver.request_save(self.config.clone());
    }

    fn draw_settings_page_autostart(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        let selected_text = self.config.default_profile.as_deref().unwrap_or("None");
        ui.label(
            egui::RichText::new(format!("Default profile: {selected_text}"))
                .color(theme.muted)
                .size(12.0),
        );
        ui.label(
            egui::RichText::new("Set this in Profiles and Account.")
                .color(theme.muted)
                .size(12.0),
        );

        ui.add_space(10.0);

        let before = self.config.autostart;
        ui.checkbox(&mut self.config.autostart, "Autostart on launch");
        if self.config.autostart != before {
            self.config_saver.request_save(self.config.clone());
        }

        ui.add_space(4.0);
        let note = if !self.config.autostart {
            "When enabled, Rusty connects on launch using the default profile from Profiles and Account."
        } else if self.config.default_profile.is_none() {
            "Set a default profile in Profiles and Account to make autostart work."
        } else if self.config.save_session_layout {
            "Autostart does not run while \"Save Session on Exit\" is enabled in Behavior (saved sessions are restored instead)."
        } else {
            "If the default profile does not store a password, you'll be prompted at startup."
        };
        let note_color = if self.config.autostart
            && (self.config.default_profile.is_none() || self.config.save_session_layout)
        {
            Color32::from_rgb(220, 170, 90)
        } else {
            theme.muted
        };
        ui.label(egui::RichText::new(note).color(note_color));
    }

    fn draw_settings_page_behavior(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;

        let before = self.config.minimize_to_tray;
        ui.checkbox(&mut self.config.minimize_to_tray, "Minimize to tray");
        if self.config.minimize_to_tray != before {
            if !self.config.minimize_to_tray && self.hidden_to_tray {
                // If we are currently hidden and the user disables tray minimize, bring the window back.
                // (This will be a no-op if the backend ignores it.)
                // Note: we don't have access to `ctx` here, so we just clear state and let update handle it.
                self.hidden_to_tray = false;
            }
            self.config_saver.request_save(self.config.clone());
        }
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new(
                "When enabled, the minimize button hides Rusty to the system tray.",
            )
            .color(theme.muted),
        );

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(10.0);

        let before = self.config.save_session_layout;
        ui.checkbox(&mut self.config.save_session_layout, "Save session layout");
        if self.config.save_session_layout != before {
            self.layout_dirty = true;
            // Save immediately so next startup uses the new preference.
            self.config_saver.request_save(self.config.clone());
        }
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new("Remembers splits/tabs and window position/size between launches.")
                .color(theme.muted),
        );
        ui.label(
            egui::RichText::new("(NOT RECOMMENDED FOR MFA SETUPS)").color(theme.muted),
        );
    }

    fn draw_settings_page_appearance(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        let before_focus_shade = self.config.focus_shade;
        ui.checkbox(
            &mut self.config.focus_shade,
            "Focus shade (dim inactive terminals)",
        );
        if self.config.focus_shade != before_focus_shade {
            self.config_saver.request_save(self.config.clone());
        }
        ui.label(
            egui::RichText::new("Applies a 20% gray overlay to non-active terminal panes.")
                .color(theme.muted)
                .size(12.0),
        );

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(10.0);

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
        // Note: on the release-frame `resp.changed()` may be false, so key off `drag_stopped`.
        if resp.drag_stopped()
            || (resp.changed()
                && !resp.dragged()
                && (self.config.terminal_font_size - before).abs() > f32::EPSILON)
        {
            self.config_saver.request_save(self.config.clone());
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
                .range(0..=200_000),
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
            self.config_saver.request_save(self.config.clone());
        }
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new(
                "Applies on new connections (reconnect existing tabs to take effect).",
            )
            .color(theme.muted),
        );
    }

    fn draw_settings_page_ui_theme(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        let ctx = ui.ctx().clone();
        let mode_file = default_theme_file_name(self.config.ui_theme_mode).to_string();
        let available_theme_files = available_theme_file_names();

        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Theme presets").strong());
            if ui.button("Refresh list").clicked() {
                ui.ctx().request_repaint();
            }
        });
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new("Loads .thm UI themes from ./theme and applies them instantly.")
                .color(theme.muted)
                .size(12.0),
        );

        let mut selected_theme_to_apply: Option<Option<String>> = None;
        let list_height = ui.available_height().max(150.0);
        egui::ScrollArea::vertical()
            .id_salt("settings_ui_theme_list_scroll")
            .auto_shrink([false, false])
            .max_height(list_height)
            .show(ui, |ui| {
                let default_title = format!("Mode default ({mode_file})");
                let (default_preview, default_source) =
                    load_ui_theme(self.config.ui_theme_mode, None);
                let default_label = if default_source.is_some() {
                    "Mode default"
                } else {
                    "Built-in fallback"
                };
                let default_selected = self.config.ui_theme_file.is_none();
                let resp = Self::draw_ui_theme_preview_card(
                    ui,
                    theme,
                    default_preview,
                    &default_title,
                    default_label,
                    default_selected,
                );
                if resp.clicked() {
                    selected_theme_to_apply = Some(None);
                }

                if !available_theme_files.is_empty() {
                    ui.add_space(4.0);
                }

                for file in &available_theme_files {
                    let selected = self
                        .config
                        .ui_theme_file
                        .as_deref()
                        .map(|s| s.eq_ignore_ascii_case(file))
                        .unwrap_or(false);
                    let (preview, source) = load_ui_theme(self.config.ui_theme_mode, Some(file));
                    let source_is_file = source
                        .as_ref()
                        .and_then(|p| p.file_name())
                        .and_then(|f| f.to_str())
                        .map(|f| f.eq_ignore_ascii_case(file))
                        .unwrap_or(false);
                    let source_label = if source_is_file {
                        "Custom .thm"
                    } else {
                        "Fallback"
                    };

                    let resp = Self::draw_ui_theme_preview_card(
                        ui,
                        theme,
                        preview,
                        file,
                        source_label,
                        selected,
                    );
                    if resp.clicked() {
                        selected_theme_to_apply = Some(Some(file.clone()));
                    }
                    ui.add_space(4.0);
                }

                if available_theme_files.is_empty() {
                    ui.add_space(8.0);
                    ui.label(
                        egui::RichText::new("No .thm files found in UI theme directories.")
                            .color(theme.muted)
                            .size(12.0),
                    );
                    for dir in theme_dir_paths() {
                        ui.label(
                            egui::RichText::new(format!("Searched: {}", dir.display()))
                                .color(theme.muted)
                                .size(11.0),
                        );
                    }
                }
            });

        if let Some(theme_file) = selected_theme_to_apply {
            if self.apply_ui_theme_file_selection(theme_file, &ctx) {
                ui.ctx().request_repaint();
            }
        }
    }

    fn apply_ui_theme_file_selection(
        &mut self,
        selected_theme_file: Option<String>,
        ctx: &egui::Context,
    ) -> bool {
        let normalized_selected = normalize_theme_file_name(selected_theme_file.as_deref());
        let changed = match (
            self.config.ui_theme_file.as_deref(),
            normalized_selected.as_deref(),
        ) {
            (Some(a), Some(b)) => !a.eq_ignore_ascii_case(b),
            (None, None) => false,
            _ => true,
        };
        if !changed {
            return false;
        }

        self.config.ui_theme_file = normalized_selected;
        let (new_theme, source) = load_ui_theme(
            self.config.ui_theme_mode,
            self.config.ui_theme_file.as_deref(),
        );
        self.theme = new_theme;
        self.theme_source = source;
        self.apply_global_style(ctx);
        self.style_initialized = true;
        self.style_pixels_per_point_bits = ctx.pixels_per_point().to_bits();
        self.config_saver.request_save(self.config.clone());
        true
    }

    fn draw_settings_page_updates(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;

        ui.label(
            egui::RichText::new(format!("Current version: v{}", env!("CARGO_PKG_VERSION")))
                .strong(),
        );
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new(
                "Checks GitHub releases and opens the release page when a newer version is found.",
            )
            .color(theme.muted),
        );
        ui.add_space(10.0);

        if ui
            .add_enabled(
                !self.update_check_in_progress,
                egui::Button::new("Check for updates now"),
            )
            .clicked()
        {
            self.start_update_check_now_open_if_newer();
        }

        if self.update_check_in_progress {
            ui.add_space(6.0);
            ui.label(egui::RichText::new("Checking for updates...").color(theme.muted));
        }

        if let Some(version) = self.update_available_version.as_deref() {
            let shown = if version.starts_with('v') {
                version.to_string()
            } else {
                format!("v{version}")
            };
            ui.add_space(8.0);
            ui.label(
                egui::RichText::new(format!("New release detected: {shown}"))
                    .color(theme.accent)
                    .strong(),
            );
            if ui.button("Open latest release page").clicked() {
                self.open_update_release_page();
            }
        }

        if let Some(status) = self.update_manual_status.as_deref() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new(status).color(theme.muted));
        }

        ui.add_space(12.0);
        ui.separator();
        ui.add_space(8.0);
        ui.label(
            egui::RichText::new("Checks are manual-only and run when you click the button above.")
                .color(theme.muted)
                .size(12.0),
        );
    }

    fn draw_settings_page_terminal_colors(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;

        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Theme presets").strong());
            if ui.button("Reload from term/").clicked() {
                self.refresh_terminal_theme_registry();
            }
        });
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new(
                "Loads WezTerm-compatible TOML schemes from ./term and applies them instantly.",
            )
            .color(theme.muted)
            .size(12.0),
        );

        let mut selected_theme_to_apply: Option<String> = None;
        let loaded_themes = self.terminal_theme_registry.themes().to_vec();
        if self.terminal_theme_registry.is_empty() {
            ui.add_space(6.0);
            ui.label(
                egui::RichText::new(
                    "No valid terminal themes found in term/. Using current/fallback terminal colors.",
                )
                .color(theme.muted)
                .size(12.0),
            );
            for dir in self.terminal_theme_registry.search_dirs() {
                ui.label(
                    egui::RichText::new(format!("Searched: {}", dir.display()))
                        .color(theme.muted)
                        .size(11.0),
                );
            }
        } else {
            for term_theme in &loaded_themes {
                let selected = self
                    .config
                    .selected_terminal_theme
                    .as_deref()
                    .map(|s| s.eq_ignore_ascii_case(&term_theme.id))
                    .unwrap_or(false);
                let resp = Self::draw_terminal_theme_preview_card(ui, theme, term_theme, selected);
                if resp.clicked() {
                    selected_theme_to_apply = Some(term_theme.id.clone());
                }
                ui.add_space(4.0);
            }
        }

        if let Some(theme_id) = selected_theme_to_apply {
            if self.apply_terminal_theme_selection(&theme_id, true) {
                ui.ctx().request_repaint();
            }
        }

        if let Some(selected) = self.config.selected_terminal_theme.as_deref() {
            if let Some(term_theme) = self.terminal_theme_registry.find_by_id_or_name(selected) {
                ui.add_space(6.0);
                ui.label(
                    egui::RichText::new(format!(
                        "Selected theme: {} ({})",
                        term_theme.name, term_theme.id
                    ))
                    .color(theme.muted)
                    .size(12.0),
                );
            }
        }
    }

    fn draw_ui_theme_preview_card(
        ui: &mut egui::Ui,
        app_theme: UiTheme,
        preview_theme: UiTheme,
        title: &str,
        source_label: &str,
        selected: bool,
    ) -> Response {
        let card_width = ui.available_width().max(120.0);
        let fill = if selected {
            adjust_color(app_theme.top_bg, 0.16)
        } else {
            adjust_color(app_theme.top_bg, 0.08)
        };
        let stroke = if selected {
            Stroke::new(1.0, app_theme.accent)
        } else {
            Stroke::new(1.0, app_theme.top_border)
        };

        let inner = egui::Frame::NONE
            .fill(fill)
            .stroke(stroke)
            .corner_radius(egui::CornerRadius::same(6))
            .inner_margin(egui::Margin::same(8))
            .show(ui, |ui| {
                ui.set_min_width((card_width - 16.0).max(96.0));
                ui.horizontal(|ui| {
                    let title = if selected {
                        egui::RichText::new(title)
                            .strong()
                            .size(13.0)
                            .color(app_theme.accent)
                    } else {
                        egui::RichText::new(title)
                            .strong()
                            .size(13.0)
                            .color(app_theme.fg)
                    };
                    ui.label(title);
                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        ui.label(
                            egui::RichText::new(source_label)
                                .size(11.0)
                                .color(app_theme.muted),
                        );
                    });
                });

                ui.add_space(3.0);
                egui::Frame::NONE
                    .fill(preview_theme.bg)
                    .stroke(Stroke::new(1.0, preview_theme.top_border))
                    .corner_radius(egui::CornerRadius::same(5))
                    .inner_margin(egui::Margin::same(6))
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            let (accent_dot, _) =
                                ui.allocate_exact_size(Vec2::new(8.0, 8.0), Sense::hover());
                            ui.painter()
                                .rect_filled(accent_dot, 2.0, preview_theme.accent);
                            ui.label(
                                egui::RichText::new("Rusty Settings")
                                    .size(11.0)
                                    .strong()
                                    .color(preview_theme.fg),
                            );
                            ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                                ui.label(
                                    egui::RichText::new("Preview")
                                        .size(10.0)
                                        .color(preview_theme.muted),
                                );
                            });
                        });

                        ui.add_space(3.0);
                        let preview_w = ui.available_width().max(28.0);
                        let row_h = 10.0;

                        let (top_row, _) =
                            ui.allocate_exact_size(Vec2::new(preview_w, row_h), Sense::hover());
                        ui.painter().rect_filled(top_row, 2.0, preview_theme.top_bg);
                        let top_accent = Rect::from_min_max(
                            Pos2::new(top_row.left() + 2.0, top_row.top() + 2.0),
                            Pos2::new(top_row.left() + 24.0, top_row.bottom() - 2.0),
                        );
                        ui.painter()
                            .rect_filled(top_accent, 1.0, preview_theme.accent);

                        let (mid_row, _) =
                            ui.allocate_exact_size(Vec2::new(preview_w, row_h), Sense::hover());
                        ui.painter().rect_filled(
                            mid_row,
                            2.0,
                            adjust_color(preview_theme.top_bg, 0.06),
                        );
                        let muted_chip = Rect::from_min_max(
                            Pos2::new(mid_row.left() + 2.0, mid_row.top() + 2.0),
                            Pos2::new(mid_row.left() + 20.0, mid_row.bottom() - 2.0),
                        );
                        ui.painter()
                            .rect_filled(muted_chip, 1.0, preview_theme.muted);
                        let fg_chip = Rect::from_min_max(
                            Pos2::new(mid_row.left() + 24.0, mid_row.top() + 2.0),
                            Pos2::new(mid_row.left() + 52.0, mid_row.bottom() - 2.0),
                        );
                        ui.painter().rect_filled(fg_chip, 1.0, preview_theme.fg);
                    });
            });

        inner.response.interact(Sense::click())
    }

    fn rgb_to_color32(color: config::RgbColor) -> Color32 {
        Color32::from_rgb(color.r, color.g, color.b)
    }

    fn draw_terminal_theme_preview_card(
        ui: &mut egui::Ui,
        app_theme: UiTheme,
        term_theme: &crate::terminal_themes::TerminalTheme,
        selected: bool,
    ) -> Response {
        let card_width = ui.available_width().max(120.0);
        let fill = if selected {
            adjust_color(app_theme.top_bg, 0.16)
        } else {
            adjust_color(app_theme.top_bg, 0.08)
        };
        let stroke = if selected {
            Stroke::new(1.0, app_theme.accent)
        } else {
            Stroke::new(1.0, app_theme.top_border)
        };

        let inner = egui::Frame::NONE
            .fill(fill)
            .stroke(stroke)
            .corner_radius(egui::CornerRadius::same(6))
            .inner_margin(egui::Margin::same(8))
            .show(ui, |ui| {
                ui.set_min_width((card_width - 16.0).max(96.0));
                ui.horizontal(|ui| {
                    let title = if selected {
                        egui::RichText::new(&term_theme.name)
                            .strong()
                            .size(13.0)
                            .color(app_theme.accent)
                    } else {
                        egui::RichText::new(&term_theme.name)
                            .strong()
                            .size(13.0)
                            .color(app_theme.fg)
                    };
                    ui.label(title);
                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        ui.label(
                            egui::RichText::new(term_theme.kind_label())
                                .size(11.0)
                                .color(app_theme.muted),
                        );
                    });
                });

                if let Some(comment) = term_theme
                    .comment
                    .as_deref()
                    .map(str::trim)
                    .filter(|c| !c.is_empty() && !c.eq_ignore_ascii_case(&term_theme.name))
                {
                    ui.label(
                        egui::RichText::new(comment)
                            .size(10.0)
                            .color(app_theme.muted),
                    )
                    .on_hover_text(comment);
                }

                ui.add_space(3.0);
                let preview_bg = Self::rgb_to_color32(term_theme.background);
                let preview_fg = Self::rgb_to_color32(term_theme.foreground);
                let preview_border =
                    adjust_color(preview_bg, if term_theme.light { -0.18 } else { 0.25 });
                egui::Frame::NONE
                    .fill(preview_bg)
                    .stroke(Stroke::new(1.0, preview_border))
                    .corner_radius(egui::CornerRadius::same(5))
                    .inner_margin(egui::Margin::same(6))
                    .show(ui, |ui| {
                        ui.visuals_mut().override_text_color = Some(preview_fg);
                        let palette = &term_theme.palette16;
                        ui.label(
                            egui::RichText::new("user@host:~$ ls -la")
                                .monospace()
                                .size(11.0)
                                .color(preview_fg),
                        );
                        ui.label(
                            egui::RichText::new("error: permission denied")
                                .monospace()
                                .size(11.0)
                                .color(Self::rgb_to_color32(palette[9])),
                        );
                        ui.add_space(2.0);
                        let strip_w = ui.available_width().max(16.0);
                        let (strip, _) =
                            ui.allocate_exact_size(Vec2::new(strip_w, 8.0), Sense::hover());
                        let sw = strip.width() / 16.0;
                        for (i, color) in palette.iter().enumerate() {
                            let x0 = strip.left() + sw * i as f32;
                            let x1 = if i == 15 {
                                strip.right()
                            } else {
                                strip.left() + sw * (i as f32 + 1.0)
                            };
                            let rect = Rect::from_min_max(
                                Pos2::new(x0, strip.top()),
                                Pos2::new(x1, strip.bottom()),
                            );
                            ui.painter()
                                .rect_filled(rect, 0.0, Self::rgb_to_color32(*color));
                        }
                    });
            });

        inner.response.interact(Sense::click())
    }

    fn draw_settings_page_profiles_and_account(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        ui.spacing_mut().item_spacing = Vec2::new(8.0, 6.0);
        ui.label(egui::RichText::new("Profiles").strong());
        egui::ScrollArea::vertical()
            .id_salt("settings_profiles_list_scroll")
            .max_height(120.0)
            .show(ui, |ui| {
                let mut load_idx: Option<usize> = None;
                let mut delete_idx: Option<usize> = None;
                let mut set_default_idx: Option<usize> = None;
                let mut clear_default = false;
                for (i, p) in self.config.profiles.iter().enumerate() {
                    let selected = self.settings_dialog.selected_profile == Some(i);
                    let label = config::profile_display_name(p, &self.config);
                    let text_color = if selected {
                        Color32::from_rgb(20, 20, 20)
                    } else {
                        theme.fg
                    };
                    let is_default = self
                        .config
                        .default_profile
                        .as_deref()
                        .map(|d| d.eq_ignore_ascii_case(&p.name))
                        .unwrap_or(false);
                    let resp = ui.add(egui::Button::selectable(
                        selected,
                        egui::RichText::new(label).color(text_color),
                    ));
                    if resp.clicked() {
                        load_idx = Some(i);
                    }
                    resp.context_menu(|ui: &mut egui::Ui| {
                        if is_default {
                            if ui.button("Clear Default Profile").clicked() {
                                clear_default = true;
                                ui.close();
                            }
                        } else if ui.button("Set As Default Profile").clicked() {
                            set_default_idx = Some(i);
                            ui.close();
                        }
                        ui.separator();
                        if ui.button("Delete Profile").clicked() {
                            delete_idx = Some(i);
                            ui.close();
                        }
                    });
                }
                if self.config.profiles.is_empty() {
                    ui.label(egui::RichText::new("No profiles yet.").color(theme.muted));
                }

                if clear_default {
                    self.config.default_profile = None;
                    self.config_saver.request_save(self.config.clone());
                }

                if let Some(i) = set_default_idx {
                    if let Some(profile) = self.config.profiles.get(i) {
                        self.config.default_profile = Some(profile.name.clone());
                        self.config_saver.request_save(self.config.clone());
                    }
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

            let selected_profile_name = self
                .settings_dialog
                .selected_profile
                .and_then(|i| self.config.profiles.get(i))
                .map(|p| p.name.clone());
            if ui
                .add_enabled(
                    selected_profile_name.is_some(),
                    egui::Button::new("Make Default"),
                )
                .clicked()
            {
                self.config.default_profile = selected_profile_name;
                self.config_saver.request_save(self.config.clone());
            }
        });

        ui.horizontal(|ui| {
            ui.label("Profile name");
            let resp = ui.add(
                egui::TextEdit::singleline(&mut self.settings_dialog.profile_name)
                    .hint_text("e.g. prod-1")
                    .desired_width((ui.available_width() - 96.0).max(120.0)),
            );
            if self.settings_dialog.just_opened {
                resp.request_focus();
                self.settings_dialog.just_opened = false;
            }
        });
        if self.settings_dialog.just_opened {
            self.settings_dialog.just_opened = false;
        }

        ui.separator();
        ui.label(egui::RichText::new("Connection").strong());
        egui::Grid::new("settings_profile_connection_grid")
            .num_columns(2)
            .spacing(Vec2::new(10.0, 6.0))
            .show(ui, |ui| {
                ui.label("Host");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings_dialog.draft.host)
                        .hint_text("example.com")
                        .desired_width(ui.available_width()),
                );
                ui.end_row();

                ui.label("Port");
                ui.add(
                    egui::DragValue::new(&mut self.settings_dialog.draft.port)
                        .speed(1.0)
                        .range(1..=65535),
                );
                ui.end_row();

                ui.label("User");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings_dialog.draft.username)
                        .desired_width(ui.available_width()),
                );
                ui.end_row();
            });

        ui.label(egui::RichText::new("Advanced authentication").strong());
        egui::Grid::new("settings_profile_advanced_grid")
            .num_columns(2)
            .spacing(Vec2::new(10.0, 6.0))
            .show(ui, |ui| {
                ui.label("Password (optional)");
                ui.add(
                    egui::TextEdit::singleline(&mut self.settings_dialog.draft.password)
                        .password(true)
                        .desired_width(ui.available_width()),
                );
                ui.end_row();

                ui.label("Remember password");
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.settings_dialog.remember_password, "Enable");
                    ui.label(egui::RichText::new("(i)").color(theme.muted))
                        .on_hover_text("Stored encrypted in local config using Windows DPAPI.");
                });
                ui.end_row();

                ui.label("Private key (optional)");
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(
                            &mut self.settings_dialog.draft.private_key_path,
                        )
                        .hint_text("C:\\\\Users\\\\you\\\\.ssh\\\\id_ed25519")
                        .desired_width((ui.available_width() - 92.0).max(120.0)),
                    );
                    if ui.button("Browse...").clicked() {
                        let mut dlg = rfd::FileDialog::new();
                        if let Some(profile_dir) = user_profile_dir() {
                            dlg = dlg.set_directory(profile_dir);
                        }
                        if let Some(path) = dlg.pick_file() {
                            self.settings_dialog.draft.private_key_path =
                                path.display().to_string();
                        }
                    }
                });
                ui.end_row();
            });

        // Status (only show failures to keep noise down).
        let status_tile = self
            .settings_dialog
            .target_tile
            .or(self.active_tile)
            .or_else(|| self.first_pane_id());
        if let Some(tile_id) = status_tile {
            if let Some(tab) = self.pane(tile_id) {
                if tab.last_status_kind.is_error() && !tab.last_status.trim().is_empty() {
                    ui.separator();
                    ui.label(
                        egui::RichText::new(&tab.last_status)
                            .color(issue_kind_color(theme, tab.last_status_kind)),
                    );
                }
            }
        }

        ui.add_space(4.0);

        let can_save = !self.settings_dialog.profile_name.trim().is_empty();
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
                    self.clear_transient_prompts_for_tile(tile_id);
                    if let Some(tab) = self.pane_mut(tile_id) {
                        tab.profile_name = profile_name;
                        tab.settings = draft;
                        tab.disconnect();
                        tab.start_connect();
                        tab.focus_terminal_next_frame = true;
                    }
                    self.set_active_tile(Some(tile_id));
                }
                self.settings_dialog.open = false;
            }

            if ui
                .add_enabled(can_save, egui::Button::new("Save"))
                .clicked()
            {
                self.upsert_profile_from_dialog();
            }
        });
    }

    fn draw_settings_contents(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        theme: UiTheme,
        embedded: bool,
        _section_frame: &egui::Frame,
    ) {
        ui.visuals_mut().override_text_color = Some(theme.fg);
        ui.spacing_mut().item_spacing = Vec2::new(8.0, 10.0);

        let btn_fill = adjust_color(theme.top_bg, 0.10);
        let controls_enabled = !embedded;
        egui::Frame::NONE
            .fill(adjust_color(theme.top_bg, 0.08))
            .stroke(Stroke::new(1.0, theme.top_border))
            .corner_radius(egui::CornerRadius::same(8))
            .inner_margin(egui::Margin::symmetric(TITLE_PAD_X.round() as i8, 2))
            .show(ui, |ui| {
                let bar_rect = Rect::from_min_size(
                    ui.cursor().min,
                    Vec2::new(ui.available_width(), TITLE_BAR_H),
                );
                let drag_resp = ui.interact(
                    bar_rect,
                    Id::new("rusty_settings_title_drag"),
                    Sense::click_and_drag(),
                );
                let mut title_controls_hot = false;

                ui.scope_builder(
                    egui::UiBuilder::new()
                        .max_rect(bar_rect)
                        .layout(egui::Layout::left_to_right(Align::Center)),
                    |ui| {
                    ui.label(
                        egui::RichText::new("Rusty Settings")
                            .strong()
                            .size(16.0)
                            .color(theme.accent),
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
                            self.settings_dialog.open = false;
                        }

                        if controls_enabled {
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
                                ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
                            }
                        }
                    });
                    },
                );
                ui.advance_cursor_after_rect(bar_rect);

                if controls_enabled {
                    let pressed_on_title =
                        drag_resp.hovered() && ctx.input(|i| i.pointer.primary_pressed());
                    if drag_resp.drag_started() || (pressed_on_title && !title_controls_hot) {
                        begin_window_drag(ctx);
                    }
                    if drag_resp.double_clicked() && !title_controls_hot {
                        let is_max = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
                        ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(!is_max));
                    }
                }
            });
        ui.add_space(8.0);

        // Settings layout that scales: left section list (listbox-style) + right content pane.
        let avail_h = ui.available_height();
        let panel_rounding = egui::CornerRadius::same(10);
        let panel_stroke = Stroke::new(1.0, theme.top_border);

        ui.horizontal(|ui| {
            let nav_w = 220.0;
            let gap = 12.0;

            let (nav_rect, _) = ui.allocate_exact_size(Vec2::new(nav_w, avail_h), Sense::hover());
            ui.painter()
                .rect_filled(nav_rect, panel_rounding, adjust_color(theme.top_bg, 0.10));
            ui.painter()
                .rect_stroke(nav_rect, panel_rounding, panel_stroke, egui::StrokeKind::Inside);
            let nav_inner = nav_rect.shrink(12.0);
            let mut nav_ui = ui.new_child(
                egui::UiBuilder::new()
                    .max_rect(nav_inner)
                    .layout(egui::Layout::top_down(Align::Min)),
            );
            nav_ui.spacing_mut().item_spacing = Vec2::new(6.0, 6.0);
            nav_ui.label(
                egui::RichText::new("Sections")
                    .color(theme.muted)
                    .size(12.0),
            );
            nav_ui.add_space(6.0);

            egui::ScrollArea::vertical()
                .id_salt("settings_nav_scroll")
                .auto_shrink([false, false])
                .show(&mut nav_ui, |ui| {
                    let item_h = 34.0;
                    let rounding = egui::CornerRadius::same(10);
                    let font_id = FontId::proportional(16.0);

                    let mut item = |ui: &mut egui::Ui, page: SettingsPage| {
                        let selected = self.settings_dialog.page == page;
                        let text = egui::WidgetText::from(page.label());
                        let galley = text.into_galley(
                            ui,
                            Some(egui::TextWrapMode::Extend),
                            f32::INFINITY,
                            font_id.clone(),
                        );

                        let (rect, resp) = ui.allocate_exact_size(
                            Vec2::new(ui.available_width(), item_h),
                            Sense::click(),
                        );
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
                                ui.painter().rect_stroke(
                                    rect,
                                    rounding,
                                    stroke,
                                    egui::StrokeKind::Inside,
                                );
                            }

                            if selected {
                                let bar_w = 4.0;
                                let bar = Rect::from_min_max(
                                    rect.min,
                                    Pos2::new(rect.min.x + bar_w, rect.max.y),
                                );
                                let bar_rounding = egui::CornerRadius {
                                    nw: rounding.nw,
                                    sw: rounding.sw,
                                    ne: 0,
                                    se: 0,
                                };
                                ui.painter().rect_filled(bar, bar_rounding, theme.accent);
                            }

                            let text_pos = Pos2::new(
                                rect.left() + 14.0,
                                rect.center().y - galley.size().y * 0.5,
                            );
                            ui.painter().galley(text_pos, galley, text_color);
                        }

                        if resp.clicked() {
                            self.settings_dialog.page = page;
                        }
                    };

                    item(ui, SettingsPage::Autostart);
                    item(ui, SettingsPage::Behavior);
                    item(ui, SettingsPage::Appearance);
                    item(ui, SettingsPage::Updates);
                    item(ui, SettingsPage::UiTheme);
                    item(ui, SettingsPage::TerminalColors);
                    item(ui, SettingsPage::ProfilesAndAccount);
                });

            ui.add_space(gap);

            let content_w = ui.available_width().max(10.0);
            let (content_rect, _) =
                ui.allocate_exact_size(Vec2::new(content_w, avail_h), Sense::hover());
            ui.painter().rect_filled(
                content_rect,
                panel_rounding,
                adjust_color(theme.top_bg, 0.10),
            );
            ui.painter()
                .rect_stroke(
                    content_rect,
                    panel_rounding,
                    panel_stroke,
                    egui::StrokeKind::Inside,
                );
            let content_inner = content_rect.shrink(12.0);
            let mut content_ui = ui.new_child(
                egui::UiBuilder::new()
                    .max_rect(content_inner)
                    .layout(egui::Layout::top_down(Align::Min)),
            );

            egui::ScrollArea::vertical()
                .id_salt("settings_content_scroll")
                .auto_shrink([false, false])
                .show(&mut content_ui, |ui| match self.settings_dialog.page {
                    SettingsPage::Autostart => self.draw_settings_page_autostart(ui),
                    SettingsPage::Behavior => self.draw_settings_page_behavior(ui),
                    SettingsPage::Appearance => self.draw_settings_page_appearance(ui),
                    SettingsPage::Updates => self.draw_settings_page_updates(ui),
                    SettingsPage::UiTheme => self.draw_settings_page_ui_theme(ui),
                    SettingsPage::TerminalColors => self.draw_settings_page_terminal_colors(ui),
                    SettingsPage::ProfilesAndAccount => {
                        self.draw_settings_page_profiles_and_account(ui)
                    }
                });
        });
    }

    fn draw_settings_dialog(&mut self, ctx: &egui::Context) {
        if !self.settings_dialog.open {
            return;
        }

        let viewport_id = settings_viewport_id();
        let force_front = self.settings_dialog.just_opened;
        let mut builder = egui::ViewportBuilder::default()
            .with_title("Rusty Settings")
            .with_inner_size(Vec2::new(640.0, 576.0))
            .with_min_inner_size(Vec2::new(420.0, 416.0))
            .with_decorations(false)
            .with_resizable(true);
        if force_front {
            builder = builder.with_active(true);
        }

        ctx.show_viewport_immediate(viewport_id, builder, |ctx, class| {
            if force_front && !matches!(class, egui::ViewportClass::Embedded) {
                ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            }
            if ctx.input(|i| i.viewport().close_requested()) {
                self.settings_dialog.open = false;
            }
            if !self.settings_dialog.open {
                return;
            }

            let theme = self.theme;
            if !matches!(class, egui::ViewportClass::Embedded) {
                paint_window_chrome(ctx, theme);
                handle_window_resize(ctx);
            }
            let outer_frame = egui::Frame::NONE
                .fill(adjust_color(theme.top_bg, 0.06))
                .stroke(Stroke::new(1.0, theme.top_border))
                .inner_margin(egui::Margin {
                    left: 14,
                    right: 14,
                    top: 4,
                    bottom: 14,
                });
            let section_frame = egui::Frame::NONE
                .fill(adjust_color(theme.top_bg, 0.10))
                .stroke(Stroke::new(1.0, theme.top_border))
                .corner_radius(egui::CornerRadius::same(10))
                .inner_margin(egui::Margin::same(12));

            match class {
                egui::ViewportClass::Embedded => {
                    let mut open = true;
                    egui::Window::new("Settings")
                        .collapsible(false)
                        .resizable(true)
                        .open(&mut open)
                        .frame(outer_frame)
                        .show(ctx, |ui| {
                            self.draw_settings_contents(ui, ctx, theme, true, &section_frame);
                        });
                    if !open {
                        self.settings_dialog.open = false;
                    }
                }
                _ => {
                    egui::CentralPanel::default()
                        .frame(outer_frame)
                        .show(ctx, |ui| {
                            self.draw_settings_contents(ui, ctx, theme, false, &section_frame);
                        });
                }
            }
        });
    }

    fn draw_host_key_dialog(&mut self, ctx: &egui::Context) {
        let Some(dialog) = self.host_key_dialog.take() else {
            return;
        };

        // If the tab was closed while waiting on user input, drop this prompt.
        let Some(tab) = self.pane(dialog.tile_id) else {
            return;
        };
        if !tab.connecting && !tab.connected {
            return;
        }

        let screen_rect = ctx.content_rect();
        let overlay_id =
            egui::LayerId::new(egui::Order::Middle, Id::new("host_key_modal_bg"));
        let painter = ctx.layer_painter(overlay_id);
        painter.rect_filled(
            screen_rect,
            0.0,
            Color32::from_rgba_unmultiplied(0, 0, 0, 160),
        );

        let mut decision: Option<ssh::HostKeyDecision> = None;
        let mut open = true;

        let win_w = (screen_rect.width() * 0.90).clamp(420.0, 640.0);
        let win_h = (screen_rect.height() * 0.65).clamp(300.0, 420.0);
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

        let frame = egui::Frame::NONE
            .fill(adjust_color(self.theme.top_bg, 0.06))
            .stroke(Stroke::new(1.0, self.theme.top_border))
            .corner_radius(egui::CornerRadius::same(12))
            .shadow(egui::epaint::Shadow {
                offset: [0, 8],
                blur: 32,
                spread: 0,
                color: Color32::from_black_alpha(96),
            })
            .inner_margin(egui::Margin::same(12));

        egui::Window::new("Host Key Verification")
            .collapsible(false)
            .resizable(false)
            .title_bar(false)
            .order(egui::Order::Foreground)
            .fixed_rect(win_rect)
            .frame(frame)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.visuals_mut().override_text_color = Some(self.theme.fg);
                ui.spacing_mut().item_spacing = Vec2::new(8.0, 8.0);

                ui.horizontal(|ui| {
                    let title = if dialog.prompt.changed_line.is_some() {
                        "Changed Host Key"
                    } else {
                        "Unknown Host Key"
                    };
                    ui.label(
                        egui::RichText::new(title)
                            .strong()
                            .size(18.0)
                            .color(self.theme.accent),
                    );
                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        if ui.button("Reject").clicked() {
                            decision = Some(ssh::HostKeyDecision::Reject);
                        }
                    });
                });
                ui.separator();

                ui.label(
                    egui::RichText::new(format!(
                        "Host: {}:{}",
                        dialog.prompt.host, dialog.prompt.port
                    ))
                    .strong(),
                );
                ui.label(
                    egui::RichText::new(format!("Algorithm: {}", dialog.prompt.algorithm))
                        .color(self.theme.muted),
                );
                if let Some(line) = dialog.prompt.changed_line {
                    ui.label(
                        egui::RichText::new(format!(
                            "Saved host key mismatch detected on known_hosts line {}.",
                            line
                        ))
                        .color(Color32::from_rgb(220, 170, 90)),
                    );
                }
                ui.add_space(4.0);

                ui.label(egui::RichText::new("SHA256 fingerprint").strong());
                egui::ScrollArea::vertical()
                    .id_salt(("hostkey_fp_scroll", dialog.tile_id))
                    .max_height(56.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.monospace(&dialog.prompt.fingerprint);
                    });
                ui.add_space(4.0);

                ui.label(
                    egui::RichText::new(if let Some(line) = dialog.prompt.changed_line {
                        format!(
                            "If you deliberately replace the saved key, Rusty will remove line {} in:\n{}\nand then save this new host key.",
                            line, dialog.prompt.known_hosts_path
                        )
                    } else {
                        format!(
                            "If trusted, this key will be pinned to:\n{}",
                            dialog.prompt.known_hosts_path
                        )
                    })
                    .color(self.theme.muted),
                );
                ui.add_space(10.0);

                ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                    let action_label = if dialog.prompt.changed_line.is_some() {
                        "Replace Saved Key"
                    } else {
                        "Trust & Save"
                    };
                    let trust = ui.add(egui::Button::new(action_label));
                    if trust.clicked() {
                        decision = Some(if dialog.prompt.changed_line.is_some() {
                            ssh::HostKeyDecision::ReplaceAndSave
                        } else {
                            ssh::HostKeyDecision::TrustAndSave
                        });
                    }
                });
            });

        if !open && decision.is_none() {
            decision = Some(ssh::HostKeyDecision::Reject);
        }

        if let Some(decision) = decision {
            if let Some(tab) = self.pane_mut(dialog.tile_id) {
                if let Some(tx) = tab.host_key_tx.as_ref() {
                    let _ = tx.send(decision);
                }
                if decision == ssh::HostKeyDecision::Reject {
                    if let Some(tx) = tab.worker_tx.as_ref() {
                        let _ = tx.send(WorkerMessage::Disconnect);
                    }
                }
            }
        } else {
            self.host_key_dialog = Some(dialog);
        }
    }

    fn draw_auth_dialog(&mut self, ctx: &egui::Context) {
        let Some(mut auth) = self.auth_dialog.take() else {
            return;
        };
        auth.responses.resize(auth.prompts.len(), String::new());

        // If the tab went away (closed) or no longer exists, drop the prompt.
        let Some(tab) = self.pane(auth.tile_id) else {
            return;
        };
        if !tab.connecting && !tab.connected {
            return;
        }

        // Modal dim background.
        let screen_rect = ctx.content_rect();
        // Paint the dim overlay above panels but below windows.
        let overlay_id = egui::LayerId::new(egui::Order::Middle, Id::new("auth_modal_bg"));
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
        let frame = egui::Frame::NONE
            .fill(adjust_color(self.theme.top_bg, 0.06))
            .stroke(Stroke::new(1.0, self.theme.top_border))
            .corner_radius(egui::CornerRadius::same(12))
            .shadow(egui::epaint::Shadow {
                offset: [0, 8],
                blur: 32,
                spread: 0,
                color: Color32::from_black_alpha(96),
            })
            .inner_margin(egui::Margin::same(12));

        egui::Window::new("Authentication")
            .collapsible(false)
            .resizable(false)
            .title_bar(false)
            .order(egui::Order::Foreground)
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

                egui::ScrollArea::vertical()
                    .id_salt(("auth_prompts_scroll", auth.tile_id))
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let field_w = ui.available_width().min(360.0);
                        for (i, p) in auth.prompts.iter().enumerate() {
                            ui.label(egui::RichText::new(&p.text).strong());
                            let resp = {
                                let value = auth.responses.get_mut(i).unwrap();
                                egui::TextEdit::singleline(value)
                                    .password(!p.echo)
                                    .desired_width(field_w)
                                    .show(ui)
                                    .response
                            };
                            let response_id = resp.id;
                            let field_index = i;
                            resp.context_menu(|ui| {
                                if ui.button("Paste").clicked() {
                                    if let Some(text) = Self::clipboard_text(&mut self.clipboard) {
                                        if let Some(value) = auth.responses.get_mut(field_index) {
                                            *value = text;
                                        }
                                        ui.ctx().memory_mut(|mem| mem.request_focus(response_id));
                                        ui.ctx().request_repaint();
                                    }
                                    ui.close();
                                }
                            });
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
                        ui.checkbox(
                            &mut auth.remember_key_passphrase,
                            "Remember key passphrase for this profile",
                        );
                    });
                    if !can_remember {
                        ui.label(
                            egui::RichText::new(
                                "Open this session from a saved profile to enable remembering.",
                            )
                            .color(self.theme.muted)
                            .size(12.0),
                        );
                    } else {
                        ui.label(
                            egui::RichText::new("Saved encrypted (Windows DPAPI).")
                                .color(self.theme.muted)
                                .size(12.0),
                        );
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
                key_pw = Some(responses.first().cloned().unwrap_or_default());

                if auth.remember_key_passphrase {
                    if let (Some(profile_name), Some(pw)) =
                        (auth.profile_name.as_deref(), key_pw.as_ref())
                    {
                        if let Some(i) = config::find_profile_index(&self.config, profile_name) {
                            self.config.profiles[i].remember_key_passphrase = true;
                            self.config.profiles[i].settings.key_passphrase = pw.clone();
                            self.config_saver.request_save(self.config.clone());

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
}
