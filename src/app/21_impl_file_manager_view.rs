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

    fn file_permissions_label(permissions: Option<u32>) -> String {
        permissions
            .map(|mode| format!("{:04o}", mode & 0o7777))
            .unwrap_or_else(|| "-".to_string())
    }

    fn file_owner_segment(name: Option<&str>, id: Option<u32>, label: &str) -> String {
        match (name.filter(|value| !value.trim().is_empty()), id) {
            (Some(name), Some(id)) => format!("{name} ({id})"),
            (Some(name), None) => name.to_string(),
            (None, Some(id)) => id.to_string(),
            (None, None) => label.to_string(),
        }
    }

    fn file_ownership_label(entry: &ssh::SftpEntry) -> String {
        format!(
            "{}:{}",
            Self::file_owner_segment(entry.user.as_deref(), entry.uid, "owner -"),
            Self::file_owner_segment(entry.group.as_deref(), entry.gid, "group -"),
        )
    }

    fn parse_permission_mode(raw: &str) -> Option<u32> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let raw = raw
            .strip_prefix("0o")
            .or_else(|| raw.strip_prefix("0O"))
            .or_else(|| raw.strip_prefix('0'))
            .filter(|trimmed| !trimmed.is_empty())
            .unwrap_or(raw);
        (!raw.is_empty() && raw.len() <= 4 && raw.chars().all(|ch| matches!(ch, '0'..='7')))
            .then(|| u32::from_str_radix(raw, 8).ok())
            .flatten()
            .filter(|mode| *mode <= 0o7777)
    }

    fn normalize_optional_remote_name(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
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
        let mut content = ui.new_child(
            egui::UiBuilder::new()
                .max_rect(rect.shrink(8.0))
                .layout(egui::Layout::top_down(Align::Min)),
        );
        let rounded_button =
            |label: &str| egui::Button::new(label).corner_radius(egui::CornerRadius::same(6));

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
                ui.label(
                    egui::RichText::new(&file.status).color(issue_kind_color(theme, file.status_kind)),
                );
            }
        });

        content.add_space(6.0);
        content.horizontal(|ui| {
            ui.label("Path");
            let path_resp = ui.text_edit_singleline(&mut file.path_input);
            let go = ui
                .add_enabled(!file.busy, rounded_button("Go"))
                .on_hover_text("List this remote path");
            let refresh = ui
                .add_enabled(!file.busy, rounded_button("Refresh"))
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
        let selected_names = file.selected_names_in_entry_order();
        let selected_count = selected_names.len();
        let selected_any = file.has_selection();
        let single_selected_name = file.single_selected_name();
        content.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(!file.busy, rounded_button("Upload Files"))
                .on_hover_text("Pick one or more local files to upload")
                .clicked()
            {
                actions.push(TilesAction::FileUploadFiles { pane_id: tile_id });
            }
            if ui
                .add_enabled(!file.busy, rounded_button("Upload Folder"))
                .on_hover_text("Pick a local folder and upload it recursively")
                .clicked()
            {
                actions.push(TilesAction::FileUploadFolder { pane_id: tile_id });
            }
            if ui
                .add_enabled(!file.busy && selected_any, rounded_button("Download"))
                .on_hover_text("Download the selected files and folders")
                .clicked()
            {
                actions.push(TilesAction::FileDownloadSelected { pane_id: tile_id });
            }
            if ui
                .add_enabled(!file.busy && selected_any, rounded_button("Copy"))
                .on_hover_text("Copy the selected items to another remote folder")
                .clicked()
            {
                file.open_batch_destination_dialog(FileBatchDestinationMode::Copy);
            }
            if ui
                .add_enabled(!file.busy && selected_any, rounded_button("Move"))
                .on_hover_text("Move the selected items to another remote folder")
                .clicked()
            {
                file.open_batch_destination_dialog(FileBatchDestinationMode::Move);
            }
            if ui
                .add_enabled(!file.busy && selected_count == 1, rounded_button("Rename"))
                .clicked()
            {
                if let Some(name) = single_selected_name.as_ref() {
                    file.rename_from = Some(name.clone());
                    file.rename_to = name.clone();
                    file.rename_dialog_open = true;
                }
            }
            if ui
                .add_enabled(!file.busy && selected_any, rounded_button("Delete"))
                .clicked()
            {
                file.open_delete_confirm(selected_names.clone());
            }
            if ui
                .add_enabled(!file.busy, rounded_button("Mkdir"))
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
            if selected_any {
                ui.separator();
                let selected_suffix = if selected_count == 1 { "" } else { "s" };
                ui.label(
                    egui::RichText::new(format!("{selected_count} selected item{selected_suffix}"))
                        .color(theme.accent),
                );
            }
        });
        content.add_space(6.0);

        egui::ScrollArea::vertical()
            .id_salt(("file_entries", tile_id))
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
                let card_size = Vec2::new(102.0, 92.0);
                let icon_size = Vec2::splat(32.0);
                let ordered_names: Vec<String> =
                    entries.iter().map(|entry| entry.file_name.clone()).collect();

                ui.horizontal_wrapped(|ui| {
                    ui.spacing_mut().item_spacing = Vec2::new(6.0, 6.0);

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
                    ui.painter().rect_stroke(
                        up_rect,
                        10.0,
                        Stroke::new(1.0, theme.top_border),
                        egui::StrokeKind::Inside,
                    );
                    let mut up_ui = ui.new_child(
                        egui::UiBuilder::new()
                            .max_rect(up_rect.shrink2(Vec2::new(4.0, 4.0)))
                            .layout(egui::Layout::top_down(Align::Center)),
                    );
                    up_ui.vertical_centered(|ui| {
                        let icon = folder_icon.clone().tint(adjust_color(theme.accent, -0.05));
                        let (icon_rect, _) = ui.allocate_exact_size(icon_size, Sense::hover());
                        icon.paint_at(ui, icon_rect);
                        ui.add_space(3.0);
                        ui.label(egui::RichText::new("..").color(theme.fg).strong());
                            ui.label(
                                egui::RichText::new("Parent folder")
                                    .color(theme.muted)
                                    .size(10.0),
                        );
                    });
                    if up_resp.clicked() || up_resp.double_clicked() {
                        file.clear_selection();
                        actions.push(TilesAction::FileUp(tile_id));
                    }
                    up_resp.context_menu(|ui| {
                        if ui.button("Open parent").clicked() {
                            actions.push(TilesAction::FileUp(tile_id));
                            ui.close();
                        }
                    });

                    for entry in entries {
                        let selected = file.selected_names.contains(&entry.file_name);

                        let (rect, base_resp) = ui.allocate_exact_size(card_size, Sense::click());
                        let hover_text = format!(
                            "{}\nType: {}\nSize: {}\nModified: {}\nOwnership: {}\nPermissions: {}",
                            entry.file_name,
                            if entry.is_dir { "Folder" } else { "File" },
                            Self::file_size_label(entry.size, entry.is_dir),
                            Self::file_modified_label(entry.modified_unix),
                            Self::file_ownership_label(&entry),
                            Self::file_permissions_label(entry.permissions),
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
                        ui.painter()
                            .rect_stroke(rect, 10.0, stroke, egui::StrokeKind::Inside);

                        let mut card_ui = ui.new_child(
                            egui::UiBuilder::new()
                                .max_rect(rect.shrink2(Vec2::new(4.0, 4.0)))
                                .layout(egui::Layout::top_down(Align::Center)),
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

                            ui.add_space(3.0);
                            let max_chars = 14usize;
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
                            ui.label(egui::RichText::new(meta).color(theme.muted).size(10.0));
                        });

                        if resp.clicked() {
                            let modifiers = ui.input(|i| i.modifiers);
                            file.apply_selection_click(
                                &entry.file_name,
                                &ordered_names,
                                modifiers.command || modifiers.ctrl,
                                modifiers.shift,
                            );
                        }
                        if resp.secondary_clicked() && !selected {
                            file.set_single_selection(entry.file_name.clone());
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
                            if entry.is_dir && ui.button("Open").clicked() {
                                let path = Self::join_remote_path(&file.cwd, &entry.file_name);
                                actions.push(TilesAction::FileRefresh {
                                    pane_id: tile_id,
                                    path,
                                });
                                ui.close();
                            }
                            if ui.button("Download").clicked() {
                                actions.push(TilesAction::FileDownload {
                                    pane_id: tile_id,
                                    name: entry.file_name.clone(),
                                });
                                ui.close();
                            }

                            ui.separator();
                            if ui.button("Copy to...").clicked() {
                                file.open_batch_destination_dialog(FileBatchDestinationMode::Copy);
                                ui.close();
                            }
                            if ui.button("Move to...").clicked() {
                                file.open_batch_destination_dialog(FileBatchDestinationMode::Move);
                                ui.close();
                            }
                            if ui
                                .add_enabled(file.selected_count() == 1, egui::Button::new("Rename"))
                                .clicked()
                            {
                                if let Some(name) = file.single_selected_name() {
                                    file.rename_from = Some(name.clone());
                                    file.rename_to = name;
                                }
                                file.rename_dialog_open = true;
                                ui.close();
                            }
                            if ui.button("Delete").clicked() {
                                file.open_delete_confirm(file.selected_names_in_entry_order());
                                ui.close();
                            }
                            ui.separator();
                            if ui.button("Change Permissions...").clicked() {
                                file.open_permissions_dialog(file.selected_names_in_entry_order());
                                ui.close();
                            }
                            if ui.button("Change Ownership...").clicked() {
                                file.open_ownership_dialog(file.selected_names_in_entry_order());
                                ui.close();
                            }
                        });
                    }
                });
            });

        let drop_hover_paths: Vec<PathBuf> = ui
            .ctx()
            .input(|i| i.raw.hovered_files.iter().filter_map(|file| file.path.clone()).collect());
        let dropped_paths: Vec<PathBuf> = ui
            .ctx()
            .input(|i| i.raw.dropped_files.iter().filter_map(|file| file.path.clone()).collect());
        let pointer_inside = ui
            .ctx()
            .input(|i| i.pointer.hover_pos().or_else(|| i.pointer.interact_pos()))
            .map(|pos| rect.contains(pos))
            .unwrap_or(false);
        if !file.busy && pointer_inside && !drop_hover_paths.is_empty() {
            ui.painter().rect_filled(
                rect.shrink(4.0),
                10.0,
                Color32::from_rgba_premultiplied(
                    theme.accent.r(),
                    theme.accent.g(),
                    theme.accent.b(),
                    32,
                ),
            );
            ui.painter().rect_stroke(
                rect.shrink(4.0),
                10.0,
                Stroke::new(2.0, theme.accent),
                egui::StrokeKind::Inside,
            );
            ui.painter().text(
                rect.center(),
                egui::Align2::CENTER_CENTER,
                "Drop files or folders to upload",
                FontId::proportional(16.0),
                theme.fg,
            );
        }
        if !file.busy && pointer_inside && !dropped_paths.is_empty() {
            actions.push(TilesAction::FileUploadPaths {
                pane_id: tile_id,
                paths: dropped_paths,
            });
        }

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

        if let Some(delete_confirm) = file.delete_confirm.clone() {
            let mut open = true;
            let mut confirm = false;
            let mut cancel = false;
            let delete_count = delete_confirm.names.len();
            let delete_suffix = if delete_count == 1 { "" } else { "s" };
            let delete_has_dirs = file.entries.iter().any(|entry| {
                entry.is_dir && delete_confirm.names.iter().any(|name| name == &entry.file_name)
            });
            egui::Window::new("Confirm Delete")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ui.ctx(), |ui| {
                    ui.label(format!(
                        "Delete {delete_count} selected item{delete_suffix} from the remote server?"
                    ));
                    ui.add_space(4.0);
                    if delete_count == 1 {
                        ui.label(format!("Name: {}", delete_confirm.names[0]));
                    } else {
                        ui.label(format!("First item: {}", delete_confirm.names[0]));
                        ui.label(format!("Total selected: {delete_count}"));
                    }
                    let warning = if delete_has_dirs {
                        "Folders will be deleted recursively. This cannot be undone."
                    } else {
                        "This cannot be undone."
                    };
                    ui.add_space(4.0);
                    ui.label(egui::RichText::new(warning).color(theme.muted));
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                        if ui.button("Delete").clicked() {
                            confirm = true;
                        }
                    });
                });

            if !open || cancel {
                file.delete_confirm = None;
            } else if confirm {
                actions.push(TilesAction::FileDelete {
                    pane_id: tile_id,
                    names: delete_confirm.names,
                });
                file.delete_confirm = None;
            }
        }

        if let Some(mut permissions_dialog) = file.permissions_dialog.clone() {
            let mut open = true;
            let mut confirm = false;
            let mut cancel = false;
            let mut parsed_mode = Self::parse_permission_mode(&permissions_dialog.mode);
            let selected_count = permissions_dialog.names.len();
            let suffix = if selected_count == 1 { "" } else { "s" };
            egui::Window::new("Change Permissions")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ui.ctx(), |ui| {
                    ui.label(format!(
                        "Apply a new mode to {selected_count} selected item{suffix}:"
                    ));
                    if selected_count == 1 {
                        ui.label(format!("Name: {}", permissions_dialog.names[0]));
                    }
                    ui.add_space(4.0);
                    ui.label("Mode (octal):");
                    let edit = ui.text_edit_singleline(&mut permissions_dialog.mode);
                    parsed_mode = Self::parse_permission_mode(&permissions_dialog.mode);
                    if edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        confirm = true;
                    }
                    ui.label(
                        egui::RichText::new("Use octal values like 755, 0644, or 1777.")
                            .color(theme.muted),
                    );
                    if !permissions_dialog.mode.trim().is_empty() && parsed_mode.is_none() {
                        ui.label(
                            egui::RichText::new("Enter a valid octal mode.")
                                .color(issue_kind_color(theme, ssh::IssueKind::Configuration)),
                        );
                    }
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                        if ui
                            .add_enabled(parsed_mode.is_some(), egui::Button::new("Apply"))
                            .clicked()
                        {
                            confirm = true;
                        }
                    });
                });

            if !open || cancel {
                file.permissions_dialog = None;
            } else if confirm {
                if let Some(mode) = Self::parse_permission_mode(&permissions_dialog.mode) {
                    actions.push(TilesAction::FileSetPermissions {
                        pane_id: tile_id,
                        names: permissions_dialog.names,
                        mode,
                    });
                    file.permissions_dialog = None;
                } else {
                    file.permissions_dialog = Some(permissions_dialog);
                }
            } else {
                file.permissions_dialog = Some(permissions_dialog);
            }
        }

        if let Some(mut ownership_dialog) = file.ownership_dialog.clone() {
            let mut open = true;
            let mut confirm = false;
            let mut cancel = false;
            let mut has_change =
                !ownership_dialog.owner.trim().is_empty() || !ownership_dialog.group.trim().is_empty();
            let selected_count = ownership_dialog.names.len();
            let suffix = if selected_count == 1 { "" } else { "s" };
            egui::Window::new("Change Ownership")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ui.ctx(), |ui| {
                    ui.label(format!(
                        "Apply new ownership to {selected_count} selected item{suffix}:"
                    ));
                    if selected_count == 1 {
                        ui.label(format!("Name: {}", ownership_dialog.names[0]));
                    }
                    ui.add_space(4.0);
                    ui.label("Owner:");
                    ui.text_edit_singleline(&mut ownership_dialog.owner);
                    ui.label("Group:");
                    let edit = ui.text_edit_singleline(&mut ownership_dialog.group);
                    has_change =
                        !ownership_dialog.owner.trim().is_empty()
                            || !ownership_dialog.group.trim().is_empty();
                    if edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        confirm = true;
                    }
                    ui.label(
                        egui::RichText::new(
                            "Use remote user/group names when available. Leave either field blank to keep it unchanged.",
                        )
                            .color(theme.muted),
                    );
                    if !has_change {
                        ui.label(
                            egui::RichText::new("Enter an owner, a group, or both.")
                                .color(issue_kind_color(theme, ssh::IssueKind::Configuration)),
                        );
                    }
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                        if ui
                            .add_enabled(has_change, egui::Button::new("Apply"))
                            .clicked()
                        {
                            confirm = true;
                        }
                    });
                });

            if !open || cancel {
                file.ownership_dialog = None;
            } else if confirm {
                let owner = Self::normalize_optional_remote_name(&ownership_dialog.owner);
                let group = Self::normalize_optional_remote_name(&ownership_dialog.group);
                if owner.is_some() || group.is_some() {
                    actions.push(TilesAction::FileSetOwnership {
                        pane_id: tile_id,
                        names: ownership_dialog.names,
                        owner,
                        group,
                    });
                    file.ownership_dialog = None;
                } else {
                    file.ownership_dialog = Some(ownership_dialog);
                }
            } else {
                file.ownership_dialog = Some(ownership_dialog);
            }
        }

        if let Some(mode) = file.batch_destination_mode {
            let mut open = true;
            let mut confirm = false;
            let mut cancel = false;
            let title = match mode {
                FileBatchDestinationMode::Copy => "Copy Items",
                FileBatchDestinationMode::Move => "Move Items",
            };
            let confirm_label = match mode {
                FileBatchDestinationMode::Copy => "Copy",
                FileBatchDestinationMode::Move => "Move",
            };
            egui::Window::new(title)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .open(&mut open)
                .show(ui.ctx(), |ui| {
                    let selected_count = file.selected_count();
                    let suffix = if selected_count == 1 { "" } else { "s" };
                    ui.label(format!(
                        "Target remote folder for {selected_count} selected item{suffix}:"
                    ));
                    let edit = ui.text_edit_singleline(&mut file.batch_target_dir);
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
                                !file.batch_target_dir.trim().is_empty() && file.has_selection(),
                                egui::Button::new(confirm_label),
                            )
                            .clicked()
                        {
                            confirm = true;
                        }
                    });
                });

            if !open || cancel {
                file.batch_destination_mode = None;
            } else if confirm {
                let destination_dir = file.batch_target_dir.trim().to_string();
                let names = file.selected_names_in_entry_order();
                if !destination_dir.is_empty() && !names.is_empty() {
                    actions.push(match mode {
                        FileBatchDestinationMode::Copy => TilesAction::FileCopy {
                            pane_id: tile_id,
                            names,
                            destination_dir,
                        },
                        FileBatchDestinationMode::Move => TilesAction::FileMove {
                            pane_id: tile_id,
                            names,
                            destination_dir,
                        },
                    });
                }
                file.batch_destination_mode = None;
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
