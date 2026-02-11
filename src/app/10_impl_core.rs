impl AppState {
    pub fn new() -> Self {
        let _ = std::fs::create_dir_all("logs");
        let mut config = config::load();
        let config_saver = AsyncConfigSaver::new();

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
                if let Ok(restored) = Self::restore_session_tree(&json, config.terminal_scrollback_lines) {
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

        let (tree, _first_tile_id, active_tile) = if let Some((t, pane_id)) = restored {
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

        Self {
            theme: UiTheme::default(),
            term_theme: TermTheme::from_config(&config.terminal_colors),
            config,
            config_saver,
            settings_dialog,
            tree,
            active_tile,
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
            layout_dirty: false,
            last_layout_save: Instant::now(),
            restored_window: false,
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

    fn restore_session_tree(
        json: &str,
        default_scrollback_len: usize,
    ) -> anyhow::Result<(u64, Tree<SshTab>, Option<TileId>)> {
        let restored: PersistedSession = serde_json::from_str(json)?;
        let Some(root) = restored.tree.root else {
            return Err(anyhow::anyhow!("Saved layout had no root"));
        };

        let mut tiles: Tiles<SshTab> = Tiles::default();
        let mut max_session_id = 0u64;
        let mut to_autoconnect: Vec<TileId> = Vec::new();

        for (tile_id, tile) in restored.tree.tiles.iter() {
            match tile {
                Tile::Pane(p) => {
                    max_session_id = max_session_id.max(p.id);
                    let scrollback_len = if p.scrollback_len == 0 {
                        default_scrollback_len
                    } else {
                        p.scrollback_len
                    };
                    let mut tab = SshTab::new(
                        p.id,
                        p.settings.clone(),
                        p.profile_name.clone(),
                        scrollback_len,
                        format!("logs\\tab-{}.log", p.id),
                    );
                    tab.user_title = p.user_title.clone();
                    tab.color = p.color;
                    tab.title = SshTab::title_for(p.id, &tab.settings);
                    tab.focus_terminal_next_frame = false;
                    tiles.insert(*tile_id, Tile::Pane(tab));
                    if p.autoconnect {
                        to_autoconnect.push(*tile_id);
                    }
                }
                Tile::Container(c) => {
                    tiles.insert(*tile_id, Tile::Container(c.clone()));
                }
            }
        }

        let mut tree = Tree::new("ssh_tree", root, tiles);

        for id in to_autoconnect {
            if let Some(Tile::Pane(tab)) = tree.tiles.get_mut(id) {
                if !tab.settings.host.trim().is_empty() && !tab.settings.username.trim().is_empty() {
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
                    let p = PersistedTab {
                        id: tab.id,
                        user_title: tab.user_title.clone(),
                        color: tab.color,
                        profile_name: tab.profile_name.clone(),
                        settings: tab.settings.clone(),
                        scrollback_len: tab.scrollback_len,
                        autoconnect: tab.connected || tab.connecting,
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
            if let (Some(outer), Some(inner)) = ctx.input(|i| (i.viewport().outer_rect, i.viewport().inner_rect)) {
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
            crate::tray::set_hidden_to_tray_state(false);
            return;
        }

        if self.tray.is_none() {
            self.tray = crate::tray::create_tray().ok();
            if let Some(tray) = self.tray.as_ref() {
                // Keep this as an idempotent action label.
                tray.show_hide_item.set_text("Show Rusty");
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
        }
    }

    fn handle_tray_events(&mut self, ctx: &egui::Context) {
        while let Ok(ev) = self.tray_events.try_recv() {
            match ev {
                crate::tray::TrayAppEvent::Menu(menu_id) => {
                    let Some(tray) = self.tray.as_ref() else { continue };
                    if menu_id == tray.show_hide_id {
                        let was_hidden = self.hidden_to_tray;
                        self.hidden_to_tray = crate::tray::hidden_to_tray_state();
                        tray.show_hide_item.set_text("Show Rusty");
                        if !self.hidden_to_tray && was_hidden {
                            for id in self.pane_ids() {
                                if let Some(tab) = self.pane_mut(id) {
                                    tab.last_sent_size = None;
                                }
                            }
                        }
                    } else if menu_id == tray.exit_id {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        std::process::exit(0);
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

}
