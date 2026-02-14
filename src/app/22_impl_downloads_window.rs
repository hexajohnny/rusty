impl AppState {
    fn draw_transfer_delete_dialog(&mut self, ctx: &egui::Context) {
        let Some(request_id) = self.transfer_delete_dialog.as_ref().map(|d| d.request_id) else {
            return;
        };

        let Some(job) = self
            .download_jobs
            .iter()
            .find(|j| j.request_id == request_id)
            .cloned()
        else {
            self.transfer_delete_dialog = None;
            return;
        };

        let mut open = true;
        let mut confirm = false;
        let mut cancel = false;
        egui::Window::new("Confirm Delete")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
            .open(&mut open)
            .show(ctx, |ui| {
                let is_running = matches!(job.state, DownloadState::Queued | DownloadState::Running);
                ui.label(match job.direction {
                    TransferDirection::Download => {
                        "Delete local file and remove this transfer from history?"
                    }
                    TransferDirection::Upload => {
                        "Remove this transfer from history?"
                    }
                });
                ui.add_space(4.0);
                ui.label(format!("Remote: {}", job.remote_path));
                ui.label(format!("Local: {}", job.local_path));
                if job.direction == TransferDirection::Download {
                    ui.label(
                        egui::RichText::new("The local file will be deleted if it exists.")
                            .color(self.theme.muted),
                    );
                } else {
                    ui.label(
                        egui::RichText::new("The local file will not be deleted.")
                            .color(self.theme.muted),
                    );
                }
                if is_running {
                    ui.label(
                        egui::RichText::new(
                            "This transfer is active and will be canceled before removal.",
                        )
                        .color(self.theme.muted),
                    );
                }

                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                    let confirm_label = if job.direction == TransferDirection::Download {
                        "Delete + Remove"
                    } else {
                        "Remove"
                    };
                    if ui.button(confirm_label).clicked() {
                        confirm = true;
                    }
                });
            });

        if !open || cancel {
            self.transfer_delete_dialog = None;
        } else if confirm {
            self.remove_download_job(request_id);
            self.transfer_delete_dialog = None;
        }
    }

    fn draw_transfer_progress_bar(
        &self,
        ui: &mut egui::Ui,
        frac: Option<f32>,
        text: &str,
        rtl_fill: bool,
    ) {
        let frac = frac.unwrap_or(0.0).clamp(0.0, 1.0);
        let bar_height = (ui.spacing().interact_size.y - 4.0).max(14.0);
        let (rect, _) =
            ui.allocate_exact_size(Vec2::new(ui.available_width(), bar_height), Sense::hover());
        let rounding = egui::Rounding::same(4.0);

        ui.painter()
            .rect_filled(rect, rounding, adjust_color(self.theme.top_bg, 0.04));
        ui.painter()
            .rect_stroke(rect, rounding, Stroke::new(1.0, self.theme.top_border));

        if frac > 0.0 {
            let fill_w = rect.width() * frac;
            let fill_rect = if rtl_fill {
                Rect::from_min_max(
                    Pos2::new(rect.right() - fill_w, rect.top()),
                    Pos2::new(rect.right(), rect.bottom()),
                )
            } else {
                Rect::from_min_max(
                    Pos2::new(rect.left(), rect.top()),
                    Pos2::new(rect.left() + fill_w, rect.bottom()),
                )
            };
            ui.painter()
                .rect_filled(fill_rect, rounding, self.theme.accent);
        }

        let label = if frac > 0.0 {
            format!("{text} ({:.0}%)", frac * 100.0)
        } else {
            text.to_string()
        };
        ui.painter().text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            label,
            FontId::proportional(11.0),
            self.theme.fg,
        );
    }

    fn draw_downloads_manager_contents(&mut self, ui: &mut egui::Ui) {
        ui.visuals_mut().override_text_color = Some(self.theme.fg);
        if self.download_jobs.is_empty() {
            ui.label(egui::RichText::new("No transfers yet.").color(self.theme.muted));
            return;
        }

        egui::ScrollArea::vertical()
            .id_source("downloads_manager_scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let mut cancel_ids: Vec<u64> = Vec::new();
                let mut dismiss_ids: Vec<u64> = Vec::new();
                let mut retry_ids: Vec<u64> = Vec::new();
                let mut open_folder_ids: Vec<u64> = Vec::new();
                let mut remove_confirm_ids: Vec<u64> = Vec::new();
                for job in self.download_jobs.iter().rev() {
                    let direction_text = match job.direction {
                        TransferDirection::Download => "Download",
                        TransferDirection::Upload => "Upload",
                    };
                    let state_text = match job.state {
                        DownloadState::Queued => "Queued",
                        DownloadState::Running => "Running",
                        DownloadState::Paused => "Paused",
                        DownloadState::Finished => "Finished",
                        DownloadState::Failed => "Failed",
                        DownloadState::Canceled => "Canceled",
                    };
                    let state_color = match job.state {
                        DownloadState::Queued => self.theme.muted,
                        DownloadState::Running => self.theme.accent,
                        DownloadState::Paused => Color32::from_rgb(210, 165, 85),
                        DownloadState::Finished => Color32::from_rgb(95, 200, 115),
                        DownloadState::Failed => Color32::from_rgb(220, 120, 120),
                        DownloadState::Canceled => Color32::from_rgb(180, 180, 120),
                    };
                    let action_fill = adjust_color(self.theme.top_bg, 0.10);
                    let action_border = self.theme.top_border;

                    ui.group(|ui| {
                        ui.spacing_mut().item_spacing = Vec2::new(6.0, 3.0);
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(state_text)
                                    .strong()
                                    .size(12.0)
                                    .color(state_color),
                            );
                            ui.label(
                                egui::RichText::new(direction_text)
                                    .color(self.theme.muted)
                                    .size(10.0),
                            );
                            ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                                let cancel_icon =
                                    egui::Image::new(egui::include_image!("../../assets/x.png"))
                                        .tint(self.theme.fg);
                                let cancel_hover_text =
                                    if matches!(job.state, DownloadState::Queued | DownloadState::Running) {
                                        match job.direction {
                                            TransferDirection::Download => "Cancel download",
                                            TransferDirection::Upload => "Cancel upload",
                                        }
                                    } else {
                                        "Remove from transfer history"
                                    };
                                if title_bar_image_button(
                                    ui,
                                    cancel_icon,
                                    Vec2::new(12.0, 12.0),
                                    action_fill,
                                    action_border,
                                )
                                .on_hover_text(cancel_hover_text)
                                .clicked()
                                {
                                    if matches!(job.state, DownloadState::Queued | DownloadState::Running) {
                                        cancel_ids.push(job.request_id);
                                    } else {
                                        dismiss_ids.push(job.request_id);
                                    }
                                }

                                let folder_icon =
                                    egui::Image::new(egui::include_image!("../../assets/folder.png"))
                                        .tint(self.theme.fg);
                                if title_bar_image_button(
                                    ui,
                                    folder_icon,
                                    Vec2::new(14.0, 14.0),
                                    action_fill,
                                    action_border,
                                )
                                .on_hover_text("Open containing folder")
                                .clicked()
                                {
                                    open_folder_ids.push(job.request_id);
                                }

                                if matches!(
                                    job.state,
                                    DownloadState::Failed | DownloadState::Canceled | DownloadState::Paused
                                ) {
                                    let retry_icon =
                                        egui::Image::new(egui::include_image!("../../assets/retry.png"))
                                            .tint(self.theme.fg);
                                    let retry_hover_text = match job.direction {
                                        TransferDirection::Download => "Retry and resume partial download",
                                        TransferDirection::Upload => "Retry upload",
                                    };
                                    if title_bar_image_button(
                                        ui,
                                        retry_icon,
                                        Vec2::new(14.0, 14.0),
                                        action_fill,
                                        action_border,
                                    )
                                    .on_hover_text(retry_hover_text)
                                    .clicked()
                                    {
                                        retry_ids.push(job.request_id);
                                    }
                                }

                                let trash_icon =
                                    egui::Image::new(egui::include_image!("../../assets/trash.png"))
                                        .tint(self.theme.fg);
                                if title_bar_image_button(
                                    ui,
                                    trash_icon,
                                    Vec2::new(13.0, 13.0),
                                    action_fill,
                                    action_border,
                                )
                                .on_hover_text(match job.direction {
                                    TransferDirection::Download => {
                                        "Delete local file and remove from history"
                                    }
                                    TransferDirection::Upload => {
                                        "Remove from history (does not delete local file)"
                                    }
                                })
                                .clicked()
                                {
                                    remove_confirm_ids.push(job.request_id);
                                }
                            });
                        });

                        ui.horizontal_wrapped(|ui| {
                            ui.label(
                                egui::RichText::new(format!("Remote: {}", job.remote_path))
                                    .size(11.0)
                                    .color(self.theme.fg),
                            );
                            ui.separator();
                            ui.label(
                                egui::RichText::new(format!("Local: {}", job.local_path))
                                    .size(11.0)
                                    .color(self.theme.muted),
                            );
                        });

                        let progress_text = if let Some(total) = job.total_bytes {
                            format!("{}/{} bytes", job.downloaded_bytes, total)
                        } else {
                            format!("{} bytes", job.downloaded_bytes)
                        };
                        if let Some(total) = job.total_bytes {
                            let frac = if total == 0 {
                                0.0
                            } else {
                                (job.downloaded_bytes as f32 / total as f32).clamp(0.0, 1.0)
                            };
                            self.draw_transfer_progress_bar(
                                ui,
                                Some(frac),
                                &progress_text,
                                job.direction == TransferDirection::Upload,
                            );
                        } else {
                            self.draw_transfer_progress_bar(
                                ui,
                                None,
                                &progress_text,
                                job.direction == TransferDirection::Upload,
                            );
                        }

                        let speed_text = if job.speed_bps > 0.0 {
                            if job.speed_bps >= 1_000_000.0 {
                                format!("{:.2} MB/s", job.speed_bps / 1_000_000.0)
                            } else if job.speed_bps >= 1_000.0 {
                                format!("{:.1} KB/s", job.speed_bps / 1_000.0)
                            } else {
                                format!("{:.0} B/s", job.speed_bps)
                            }
                        } else {
                            "-".to_string()
                        };
                        ui.horizontal_wrapped(|ui| {
                            ui.label(
                                egui::RichText::new(format!("Speed: {speed_text}"))
                                    .size(11.0)
                                    .color(self.theme.muted),
                            );
                            if !job.message.trim().is_empty() {
                                ui.separator();
                                ui.label(
                                    egui::RichText::new(&job.message)
                                        .size(11.0)
                                        .color(self.theme.muted),
                                );
                            }
                        });
                    });
                    ui.add_space(3.0);
                }

                if !cancel_ids.is_empty() {
                    for request_id in cancel_ids {
                        self.cancel_download_job(request_id);
                    }
                }
                if !dismiss_ids.is_empty() {
                    for request_id in dismiss_ids {
                        self.dismiss_transfer_job(request_id);
                    }
                }
                if !retry_ids.is_empty() {
                    for request_id in retry_ids {
                        self.retry_download_job(request_id);
                    }
                }
                if !open_folder_ids.is_empty() {
                    for request_id in open_folder_ids {
                        self.open_download_job_folder(request_id);
                    }
                }
                if !remove_confirm_ids.is_empty() {
                    for request_id in remove_confirm_ids {
                        self.transfer_delete_dialog = Some(TransferDeleteDialog { request_id });
                    }
                }
            });
    }

    fn draw_downloads_manager_window(&mut self, ctx: &egui::Context) {
        if !self.downloads_window_open {
            self.transfer_delete_dialog = None;
            self.downloads_window_just_opened = false;
            return;
        }

        let viewport_id = egui::ViewportId::from_hash_of("rusty_transfers_viewport");
        let force_front = self.downloads_window_just_opened;
        let mut builder = egui::ViewportBuilder::default()
            .with_title("Rusty Transfers")
            .with_inner_size(Vec2::new(900.0, 560.0))
            .with_min_inner_size(Vec2::new(640.0, 380.0))
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
                self.downloads_window_open = false;
                self.transfer_delete_dialog = None;
            }
            if !self.downloads_window_open {
                return;
            }

            let outer_frame = egui::Frame::none()
                .fill(adjust_color(self.theme.top_bg, 0.06))
                .stroke(Stroke::new(1.0, self.theme.top_border))
                .inner_margin(egui::Margin::same(10.0));
            match class {
                egui::ViewportClass::Embedded => {
                    let mut open = true;
                    egui::Window::new("Transfers")
                        .open(&mut open)
                        .default_size(Vec2::new(820.0, 520.0))
                        .resizable(true)
                        .frame(outer_frame)
                        .show(ctx, |ui| {
                            self.draw_downloads_manager_contents(ui);
                        });
                    if !open {
                        self.downloads_window_open = false;
                        self.transfer_delete_dialog = None;
                    }
                }
                _ => {
                    egui::CentralPanel::default()
                        .frame(outer_frame)
                        .show(ctx, |ui| {
                            self.draw_downloads_manager_contents(ui);
                        });
                }
            }
            self.draw_transfer_delete_dialog(ctx);
        });
        self.downloads_window_just_opened = false;
    }
}
