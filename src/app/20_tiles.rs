impl Drop for AppState {
    fn drop(&mut self) {
        // Best-effort final persistence on shutdown.
        self.config_saver.request_save(self.config.clone());
        self.config_saver.flush(Duration::from_secs(2));
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
    focus_shade: bool,
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
        focus_shade: bool,
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
            focus_shade,
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
            self.focus_shade,
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

            let text_color = match tiles.get(tile_id) {
                Some(Tile::Pane(pane)) => pane
                    .color
                    .map(contrast_text_color)
                    .unwrap_or_else(|| contrast_text_color(fill)),
                _ => contrast_text_color(fill),
            };
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

        let pane_connection = match tiles.get(tile_id) {
            Some(Tile::Pane(pane)) => Some((pane.connecting, pane.connected)),
            _ => None,
        };

        if pane_connection.is_some() {
            response.context_menu(|ui: &mut egui::Ui| {
                if let Some((connecting, connected)) = pane_connection {
                    if connecting || connected {
                        if ui.button("Disconnect").clicked() {
                            self.actions.push(TilesAction::ToggleConnect(tile_id));
                            ui.close_menu();
                        }
                    } else if ui.button("Connect").clicked() {
                        self.actions.push(TilesAction::Connect(tile_id));
                        ui.close_menu();
                    }
                    ui.separator();
                }

                if ui.button("Rename Tab").clicked() {
                    self.actions.push(TilesAction::Rename(tile_id));
                    ui.close_menu();
                }

                let hover_delay_sec = 0.18_f64;
                let now = ui.input(|i| i.time);
                let hover_since_id = ui.make_persistent_id(("tab_color_hover_since", tile_id));
                let open_id = ui.make_persistent_id(("tab_color_open", tile_id));

                let mut hover_since = ui.data_mut(|d| d.get_temp::<f64>(hover_since_id));
                let mut color_menu_open = ui
                    .data_mut(|d| d.get_temp::<bool>(open_id))
                    .unwrap_or(false);
                let mut picked_color: Option<Option<Color32>> = None;

                let color_btn = ui.button("Change Tab Color >");
                if color_btn.hovered() {
                    if hover_since.is_none() {
                        hover_since = Some(now);
                    }
                    if now - hover_since.unwrap_or(now) >= hover_delay_sec {
                        color_menu_open = true;
                    }
                } else {
                    hover_since = None;
                }

                if color_btn.clicked() {
                    color_menu_open = true;
                }

                let mut color_panel_hovered = false;
                if color_menu_open {
                    let panel = ui.scope(|ui| {
                        ui.add_space(2.0);
                        egui::Frame::none()
                            .fill(adjust_color(self.theme.top_bg, 0.08))
                            .stroke(Stroke::new(1.0, self.theme.top_border))
                            .rounding(egui::Rounding::same(6.0))
                            .inner_margin(egui::Margin::same(6.0))
                            .show(ui, |ui| {
                                if ui.button("Default").clicked() {
                                    picked_color = Some(None);
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
                                        picked_color = Some(Some(color));
                                    }
                                }
                            });
                    });
                    color_panel_hovered = ui
                        .input(|i| i.pointer.hover_pos())
                        .map(|p| panel.response.rect.contains(p))
                        .unwrap_or(false);
                }

                if !color_btn.hovered() && !color_panel_hovered {
                    color_menu_open = false;
                }

                ui.data_mut(|d| {
                    if let Some(t) = hover_since {
                        d.insert_temp(hover_since_id, t);
                    } else {
                        d.remove::<f64>(hover_since_id);
                    }
                    if color_menu_open {
                        d.insert_temp(open_id, true);
                    } else {
                        d.remove::<bool>(open_id);
                    }
                });

                if let Some(color) = picked_color {
                    self.actions.push(TilesAction::SetColor {
                        pane_id: tile_id,
                        color,
                    });
                    ui.data_mut(|d| d.remove::<bool>(open_id));
                    ui.close_menu();
                }

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

