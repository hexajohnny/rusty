impl AppState {
    fn join_remote_path(base: &str, name: &str) -> String {
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

    fn file_size_label(size: u64, is_dir: bool) -> String {
        if is_dir {
            "-".to_string()
        } else if size >= 1_000_000_000 {
            format!("{:.1} GB", size as f64 / 1_000_000_000.0)
        } else if size >= 1_000_000 {
            format!("{:.1} MB", size as f64 / 1_000_000.0)
        } else if size >= 1_000 {
            format!("{:.1} KB", size as f64 / 1_000.0)
        } else {
            format!("{size} B")
        }
    }

    fn file_modified_label(modified_unix: Option<u64>) -> String {
        let Some(unix) = modified_unix else {
            return "-".to_string();
        };
        let Ok(unix_i64) = i64::try_from(unix) else {
            return format!("{unix}");
        };
        let Ok(utc_dt) = time::OffsetDateTime::from_unix_timestamp(unix_i64) else {
            return format!("{unix}");
        };
        let local_dt = match time::UtcOffset::current_local_offset() {
            Ok(offset) => utc_dt.to_offset(offset),
            Err(_) => utc_dt,
        };
        local_dt
            .format(&time::format_description::well_known::Rfc2822)
            .unwrap_or_else(|_| format!("{unix}"))
    }

    fn file_manager_view(
        ui: &mut egui::Ui,
        pane: &mut SshTab,
        theme: UiTheme,
        tile_id: TileId,
    ) -> Vec<TilesAction> {
        let mut actions: Vec<TilesAction> = Vec::new();
        let Some(file) = pane.file_browser_mut() else {
            return actions;
        };

        let avail = ui.available_size();
        let (rect, _) = ui.allocate_exact_size(avail, Sense::hover());
        ui.painter()
            .rect_filled(rect, 0.0, adjust_color(theme.top_bg, 0.05));
        let mut content = ui.child_ui(rect.shrink(8.0), egui::Layout::top_down(Align::Min));

        content.horizontal(|ui| {
            let status_color = if file.source_connected {
                Color32::from_rgb(95, 200, 115)
            } else {
                Color32::from_rgb(220, 120, 120)
            };
            ui.label(
                egui::RichText::new(if file.source_connected {
                    "Connected"
                } else {
                    "Not connected"
                })
                .color(status_color),
            );
            if !file.status.trim().is_empty() {
                ui.separator();
                ui.label(egui::RichText::new(&file.status).color(theme.muted));
            }
        });

        content.add_space(6.0);
        content.horizontal(|ui| {
            ui.label("Path");
            let path_resp = ui.text_edit_singleline(&mut file.path_input);
            let go = ui
                .add_enabled(!file.busy, egui::Button::new("Go"))
                .on_hover_text("List this remote path");
            let refresh = ui
                .add_enabled(!file.busy, egui::Button::new("Refresh"))
                .on_hover_text("Reload current directory");

            if go.clicked()
                || (path_resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
            {
                let path = file.path_input.trim().to_string();
                actions.push(TilesAction::FileRefresh {
                    pane_id: tile_id,
                    path,
                });
            }
            if refresh.clicked() {
                let path = file.cwd.clone();
                actions.push(TilesAction::FileRefresh {
                    pane_id: tile_id,
                    path,
                });
            }
        });

        content.add_space(4.0);
        let selected_name = file.selected_name.clone();
        let selected_entry = selected_name
            .as_ref()
            .and_then(|name| file.entries.iter().find(|e| e.file_name == name.as_str()));
        let selected_downloadable = selected_entry.map(|e| !e.is_dir).unwrap_or(false);
        let selected_is_dir = selected_entry.map(|e| e.is_dir).unwrap_or(false);
        let selected_any = selected_name.is_some();
        content.horizontal(|ui| {
            if ui
                .add_enabled(!file.busy, egui::Button::new("Upload"))
                .on_hover_text("Upload a local file to the current remote directory")
                .clicked()
            {
                actions.push(TilesAction::FileUpload { pane_id: tile_id });
            }
            if ui
                .add_enabled(
                    !file.busy && selected_downloadable,
                    egui::Button::new("Download"),
                )
                .clicked()
            {
                if let Some(name) = file.selected_name.clone() {
                    actions.push(TilesAction::FileDownload {
                        pane_id: tile_id,
                        name,
                    });
                }
            }

            if ui
                .add_enabled(!file.busy && selected_any, egui::Button::new("Rename"))
                .clicked()
            {
                if let Some(name) = selected_name.as_ref() {
                    file.rename_from = Some(name.clone());
                    file.rename_to = name.clone();
                    file.rename_dialog_open = true;
                }
            }

            if ui
                .add_enabled(!file.busy && selected_any, egui::Button::new("Delete"))
                .clicked()
            {
                if let Some(name) = selected_name.as_ref() {
                    actions.push(TilesAction::FileDelete {
                        pane_id: tile_id,
                        name: name.clone(),
                        is_dir: selected_is_dir,
                    });
                }
            }

            if ui
                .add_enabled(!file.busy, egui::Button::new("Mkdir"))
                .clicked()
            {
                file.mkdir_dialog_open = true;
            }
        });

        content.add_space(6.0);
        content.separator();
        content.add_space(6.0);
        content.horizontal(|ui| {
            let count = file.entries.len();
            let suffix = if count == 1 { "" } else { "s" };
            ui.label(egui::RichText::new(format!("{count} item{suffix}")).color(theme.muted));
        });
        content.add_space(6.0);

        egui::ScrollArea::vertical()
            .id_source(("file_entries", tile_id))
            .auto_shrink([false, false])
            .show(&mut content, |ui| {
                let mut entries = file.entries.clone();
                entries.sort_by(|a, b| {
                    b.is_dir.cmp(&a.is_dir).then_with(|| {
                        a.file_name
                            .to_ascii_lowercase()
                            .cmp(&b.file_name.to_ascii_lowercase())
                    })
                });
                let folder_icon = egui::Image::new(egui::include_image!("../../assets/folder.png"));
                let file_icon = egui::Image::new(egui::include_image!("../../assets/file.png"));
                let card_size = Vec2::new(132.0, 126.0);
                let icon_size = Vec2::splat(44.0);

                ui.horizontal_wrapped(|ui| {
                    ui.spacing_mut().item_spacing = Vec2::new(10.0, 10.0);

                    // Always show the parent directory shortcut first.
                    let (up_rect, up_base_resp) = ui.allocate_exact_size(card_size, Sense::click());
                    let up_resp =
                        up_base_resp.on_hover_text("..\nType: Folder\nGo to parent directory");
                    let up_fill = if up_resp.hovered() {
                        adjust_color(theme.top_bg, 0.12)
                    } else {
                        adjust_color(theme.top_bg, 0.06)
                    };
                    ui.painter().rect_filled(up_rect, 10.0, up_fill);
                    ui.painter()
                        .rect_stroke(up_rect, 10.0, Stroke::new(1.0, theme.top_border));
                    let mut up_ui = ui.child_ui(
                        up_rect.shrink2(Vec2::new(8.0, 8.0)),
                        egui::Layout::top_down(Align::Center),
                    );
                    up_ui.vertical_centered(|ui| {
                        let icon = folder_icon.clone().tint(adjust_color(theme.accent, -0.05));
                        let (icon_rect, _) = ui.allocate_exact_size(icon_size, Sense::hover());
                        icon.paint_at(ui, icon_rect);
                        ui.add_space(6.0);
                        ui.label(egui::RichText::new("..").color(theme.fg).strong());
                        ui.label(
                            egui::RichText::new("Parent folder")
                                .color(theme.muted)
                                .size(11.0),
                        );
                    });
                    if up_resp.clicked() || up_resp.double_clicked() {
                        file.selected_name = None;
                        actions.push(TilesAction::FileUp(tile_id));
                    }
                    up_resp.context_menu(|ui| {
                        if ui.button("Open parent").clicked() {
                            actions.push(TilesAction::FileUp(tile_id));
                            ui.close_menu();
                        }
                    });

                    for entry in entries {
                        let selected = file
                            .selected_name
                            .as_deref()
                            .map(|s| s == entry.file_name.as_str())
                            .unwrap_or(false);

                        let (rect, base_resp) = ui.allocate_exact_size(card_size, Sense::click());
                        let hover_text = format!(
                            "{}\nType: {}\nSize: {}\nModified: {}",
                            entry.file_name,
                            if entry.is_dir { "Folder" } else { "File" },
                            Self::file_size_label(entry.size, entry.is_dir),
                            Self::file_modified_label(entry.modified_unix),
                        );
                        let resp = base_resp.on_hover_text(hover_text);

                        let fill = if selected {
                            adjust_color(theme.top_bg, 0.18)
                        } else if resp.hovered() {
                            adjust_color(theme.top_bg, 0.12)
                        } else {
                            adjust_color(theme.top_bg, 0.06)
                        };
                        let stroke = if selected {
                            Stroke::new(1.0, theme.accent)
                        } else {
                            Stroke::new(1.0, theme.top_border)
                        };
                        ui.painter().rect_filled(rect, 10.0, fill);
                        ui.painter().rect_stroke(rect, 10.0, stroke);

                        let mut card_ui = ui.child_ui(
                            rect.shrink2(Vec2::new(8.0, 8.0)),
                            egui::Layout::top_down(Align::Center),
                        );
                        card_ui.vertical_centered(|ui| {
                            let icon_tint = if entry.is_dir {
                                adjust_color(theme.accent, -0.05)
                            } else {
                                theme.fg
                            };
                            let icon = if entry.is_dir {
                                folder_icon.clone().tint(icon_tint)
                            } else {
                                file_icon.clone().tint(icon_tint)
                            };
                            let (icon_rect, _) = ui.allocate_exact_size(icon_size, Sense::hover());
                            icon.paint_at(ui, icon_rect);

                            ui.add_space(6.0);
                            let max_chars = 18usize;
                            let display_name = if entry.file_name.chars().count() > max_chars {
                                let truncated: String =
                                    entry.file_name.chars().take(max_chars).collect();
                                format!("{truncated}...")
                            } else {
                                entry.file_name.clone()
                            };
                            ui.label(egui::RichText::new(display_name).color(theme.fg).strong());
                            let meta = if entry.is_dir {
                                "Folder".to_string()
                            } else {
                                Self::file_size_label(entry.size, false)
                            };
                            ui.label(egui::RichText::new(meta).color(theme.muted).size(11.0));
                        });

                        if resp.clicked() {
                            file.selected_name = Some(entry.file_name.clone());
                        }
                        if resp.double_clicked() {
                            if entry.is_dir {
                                let path = Self::join_remote_path(&file.cwd, &entry.file_name);
                                actions.push(TilesAction::FileRefresh {
                                    pane_id: tile_id,
                                    path,
                                });
                            } else {
                                actions.push(TilesAction::FileDownload {
                                    pane_id: tile_id,
                                    name: entry.file_name.clone(),
                                });
                            }
                        }
                        resp.context_menu(|ui| {
                            if entry.is_dir {
                                if ui.button("Open").clicked() {
                                    let path = Self::join_remote_path(&file.cwd, &entry.file_name);
                                    actions.push(TilesAction::FileRefresh {
                                        pane_id: tile_id,
                                        path,
                                    });
                                    ui.close_menu();
                                }
                            } else if ui.button("Download").clicked() {
                                actions.push(TilesAction::FileDownload {
                                    pane_id: tile_id,
                                    name: entry.file_name.clone(),
                                });
                                ui.close_menu();
                            }
                        });
                    }
                });
            });

        if file.rename_dialog_open {
            let mut open = true;
            let mut confirm = false;
            let mut cancel = false;
            egui::Window::new("Rename Entry")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ui.ctx(), |ui| {
                    ui.label("New name:");
                    let edit = ui.text_edit_singleline(&mut file.rename_to);
                    if edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        confirm = true;
                    }
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                        if ui
                            .add_enabled(
                                !file.rename_to.trim().is_empty(),
                                egui::Button::new("Rename"),
                            )
                            .clicked()
                        {
                            confirm = true;
                        }
                    });
                });

            if !open || cancel {
                file.rename_dialog_open = false;
            } else if confirm {
                if let Some(from_name) = file.rename_from.take() {
                    let to_name = file.rename_to.trim().to_string();
                    if !to_name.is_empty() && to_name != from_name {
                        actions.push(TilesAction::FileRename {
                            pane_id: tile_id,
                            from_name,
                            to_name,
                        });
                    }
                }
                file.rename_dialog_open = false;
            }
        }

        if file.mkdir_dialog_open {
            let mut open = true;
            let mut confirm = false;
            let mut cancel = false;
            egui::Window::new("Create Folder")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ui.ctx(), |ui| {
                    ui.label("Folder name:");
                    let edit = ui.text_edit_singleline(&mut file.mkdir_name);
                    if edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        confirm = true;
                    }
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                        if ui
                            .add_enabled(
                                !file.mkdir_name.trim().is_empty(),
                                egui::Button::new("Create"),
                            )
                            .clicked()
                        {
                            confirm = true;
                        }
                    });
                });

            if !open || cancel {
                file.mkdir_dialog_open = false;
            } else if confirm {
                let dir_name = file.mkdir_name.trim().to_string();
                if !dir_name.is_empty() {
                    actions.push(TilesAction::FileMkdir {
                        pane_id: tile_id,
                        dir_name,
                    });
                    file.mkdir_name.clear();
                }
                file.mkdir_dialog_open = false;
            }
        }

        actions
    }
}
