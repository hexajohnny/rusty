impl AppState {
    fn update_cursor_blink(&mut self) {
        if self.last_cursor_blink.elapsed() >= Duration::from_millis(530) {
            self.cursor_visible = !self.cursor_visible;
            self.last_cursor_blink = Instant::now();
        }
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

        self.config_saver.request_save(self.config.clone());
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
                    self.config_saver.request_save(self.config.clone());
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
                        self.config_saver.request_save(self.config.clone());
                    }
                }
            });

        ui.add_space(10.0);

        let before = self.config.autostart;
        ui.checkbox(&mut self.config.autostart, "Autostart on launch");
        if self.config.autostart != before {
            self.config_saver.request_save(self.config.clone());
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
        let ctx = ui.ctx().clone();

        ui.label("UI color scheme");
        let before_mode = self.config.ui_theme_mode;
        let before_theme_file = self.config.ui_theme_file.clone();
        let mut reload_requested = false;
        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.config.ui_theme_mode, config::UiThemeMode::Dark, "Dark");
            ui.selectable_value(&mut self.config.ui_theme_mode, config::UiThemeMode::Light, "Light");
            if ui.button("Reload Theme File").clicked() {
                reload_requested = true;
            }
        });

        ui.add_space(6.0);
        ui.label("Theme file (.thm)");
        let mode_file = default_theme_file_name(self.config.ui_theme_mode).to_string();
        let available_theme_files = available_theme_file_names();
        let selected_theme_text = match self.config.ui_theme_file.as_deref() {
            Some(name) => {
                if available_theme_files
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(name))
                {
                    name.to_string()
                } else {
                    format!("{name} (missing)")
                }
            }
            None => format!("Mode default ({mode_file})"),
        };
        let mut chosen_theme_file = self.config.ui_theme_file.clone();
        egui::ComboBox::from_id_source("ui_theme_file_combo")
            .selected_text(selected_theme_text)
            .width(ui.available_width())
            .show_ui(ui, |ui| {
                let mode_label = format!("Mode default ({mode_file})");
                if ui
                    .selectable_label(chosen_theme_file.is_none(), mode_label)
                    .clicked()
                {
                    chosen_theme_file = None;
                }

                for file in &available_theme_files {
                    let selected = chosen_theme_file
                        .as_deref()
                        .map(|s| s.eq_ignore_ascii_case(file))
                        .unwrap_or(false);
                    if ui.selectable_label(selected, file).clicked() {
                        chosen_theme_file = Some(file.clone());
                    }
                }
            });
        self.config.ui_theme_file = chosen_theme_file;

        let mode_changed = self.config.ui_theme_mode != before_mode;
        let theme_file_changed = self.config.ui_theme_file != before_theme_file;
        if mode_changed || theme_file_changed || reload_requested {
            let (new_theme, source) =
                load_ui_theme(self.config.ui_theme_mode, self.config.ui_theme_file.as_deref());
            self.theme = new_theme;
            self.theme_source = source;
            self.apply_global_style(&ctx);
            self.style_initialized = true;
            if mode_changed || theme_file_changed {
                self.config_saver.request_save(self.config.clone());
            }
        }

        if let Some(path) = self.theme_source.as_ref() {
            ui.label(
                egui::RichText::new(format!("Loaded from {}", path.display()))
                    .color(theme.muted)
                    .size(12.0),
            );
        } else {
            let msg = if let Some(file) = self.config.ui_theme_file.as_deref() {
                format!("Using built-in fallback (theme file '{file}' not found or invalid).")
            } else {
                format!(
                    "Using built-in fallback (missing ./theme/{mode_file} near the executable)."
                )
            };
            ui.label(egui::RichText::new(msg).color(theme.muted).size(12.0));
        }

        ui.add_space(8.0);
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
        // Note: on the release-frame `resp.changed()` may be false, so key off `drag_released`.
        if resp.drag_released() {
            self.config_saver.request_save(self.config.clone());
        } else if resp.changed() && !resp.dragged() && (self.config.terminal_font_size - before).abs() > f32::EPSILON {
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
            self.config_saver.request_save(self.config.clone());
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
            self.config_saver.request_save(self.config.clone());
        }
        ui.add_space(4.0);
        ui.label(
            egui::RichText::new("When enabled, the minimize button hides Rusty to the system tray.")
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
            self.config_saver.request_save(self.config.clone());
        }
    }

    fn draw_settings_page_profiles_and_account(&mut self, ui: &mut egui::Ui) {
        let theme = self.theme;
        ui.label(egui::RichText::new("Profiles").strong());
        egui::ScrollArea::vertical()
            .id_source("settings_profiles_list_scroll")
            .max_height(140.0)
            .show(ui, |ui| {
            let mut load_idx: Option<usize> = None;
            let mut delete_idx: Option<usize> = None;
            let mut set_default_idx: Option<usize> = None;
            let mut clear_default = false;
            for (i, p) in self.config.profiles.iter().enumerate() {
                let selected = self.settings_dialog.selected_profile == Some(i);
                let label = config::profile_display_name(p, &self.config);
                let text_color = if selected { Color32::from_rgb(20, 20, 20) } else { theme.fg };
                let is_default = self
                    .config
                    .default_profile
                    .as_deref()
                    .map(|d| d.eq_ignore_ascii_case(&p.name))
                    .unwrap_or(false);
                let resp = ui.add(egui::SelectableLabel::new(
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
                            ui.close_menu();
                        }
                    } else if ui.button("Set As Default Profile").clicked() {
                        set_default_idx = Some(i);
                        ui.close_menu();
                    }
                    ui.separator();
                    if ui.button("Delete Profile").clicked() {
                        delete_idx = Some(i);
                        ui.close_menu();
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

            egui::ScrollArea::vertical()
                .id_source("settings_nav_scroll")
                .auto_shrink([false, false])
                .show(&mut nav_ui, |ui| {
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
                .id_source("settings_content_scroll")
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

                egui::ScrollArea::vertical()
                    .id_source(("auth_prompts_scroll", auth.tile_id))
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
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
