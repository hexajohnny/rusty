struct TerminalViewOptions {
    is_active: bool,
    theme: UiTheme,
    term_theme: TermTheme,
    cursor_visible: bool,
    term_font_size: f32,
    allow_resize: bool,
    focus_shade: bool,
}

impl AppState {
    fn terminal_view(
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        tab: &mut SshTab,
        options: TerminalViewOptions,
    ) {
        let avail = ui.available_size();
        let (rect, _) = ui.allocate_exact_size(avail, Sense::hover());
        let term_id = Id::new(("terminal_view", tab.id));
        let response = ui.interact(rect, term_id, Sense::click_and_drag());
        tab.last_view_rect = Some(rect);

        // Keep terminal focus locked to the terminal for common terminal keys (arrows/tab/escape).
        // Without this, egui may move focus to other widgets (e.g. the settings cog) on arrow keys.
        ui.memory_mut(|mem| {
            mem.set_focus_lock_filter(
                term_id,
                EventFilter {
                    tab: true,
                    horizontal_arrows: true,
                    vertical_arrows: true,
                    escape: true,
                },
            );
        });

        if response.clicked() {
            response.request_focus();
        }
        if tab.focus_terminal_next_frame {
            response.request_focus();
            tab.focus_terminal_next_frame = false;
        }

        let painter = ui.painter().with_clip_rect(rect);
        let rounding = egui::Rounding::ZERO;
        painter.rect_filled(rect, rounding, options.term_theme.bg);

        // Compute visible rows/cols and keep the remote PTY in sync.
        let ppp = ctx.pixels_per_point().max(1.0);
        let snapped_term_font_size = ((options.term_font_size * ppp).round() / ppp).max(8.0);
        let font_id = FontId::monospace(snapped_term_font_size);
        let (cell_w, cell_h) = Self::cell_metrics(ctx, &font_id);

        let inner_size = rect.size() - Vec2::new(TERM_PAD_X * 2.0, TERM_PAD_Y * 2.0);
        let cols = ((inner_size.x / cell_w).floor().max(1.0)) as u16;
        let rows = ((inner_size.y / cell_h).floor().max(1.0)) as u16;
        let width_px = (inner_size.x * ctx.pixels_per_point()).round().max(1.0) as u32;
        let height_px = (inner_size.y * ctx.pixels_per_point()).round().max(1.0) as u32;

        if options.allow_resize && tab.connected {
            if let Some(tx) = tab.worker_tx.as_ref() {
                // Avoid resizing to a "degenerate" 1x1 while minimized/hidden or during transient layouts.
                // Keeping the last good PTY size prevents the screen from effectively going blank.
                if inner_size.x >= cell_w && inner_size.y >= cell_h {
                    let new_size = (rows, cols, width_px, height_px);
                    if tab.last_sent_size != Some(new_size) {
                        tab.pending_resize = Some(new_size);
                    }
                }

                // During drag-resize (window edges or tile splitters), don't spam intermediate sizes.
                // Send only when the user releases the mouse, which prevents "cut off" screens when
                // expanding back out.
                let dragging = ctx.input(|i| i.pointer.any_down());
                if !dragging {
                    if let Some((rows, cols, width_px, height_px)) = tab.pending_resize.take() {
                        tab.last_sent_size = Some((rows, cols, width_px, height_px));
                        let _ = tx.send(WorkerMessage::Resize {
                            rows,
                            cols,
                            width_px,
                            height_px,
                        });
                    }
                }
            }
        } else {
            tab.pending_resize = None;
        }

        let origin = rect.min + Vec2::new(TERM_PAD_X, TERM_PAD_Y);
        // Snap to pixels so our overlays (cursor/selection) line up with the text tessellation.
        let ppp = ctx.pixels_per_point();
        let origin = Pos2::new((origin.x * ppp).round() / ppp, (origin.y * ppp).round() / ppp);

        if tab.connected {
            let job = Self::screen_to_layout_job(&tab.screen, font_id, &options.term_theme);
            let galley = ui.fonts(|fonts| fonts.layout_job(job));
            painter.galley(origin, galley.clone(), Color32::WHITE);
            let draw_sel = if let Some(sel) = tab.abs_selection {
                Self::visible_selection_from_abs(tab, sel)
            } else {
                tab.selection
            };
            if let Some(sel) = draw_sel {
                // Draw selection *after* the galley so it stays visible even when ANSI background
                // colors are present.
                Self::draw_selection_galley(
                    &painter,
                    tab,
                    origin,
                    &galley,
                    &options.term_theme,
                    sel,
                );
            }
            Self::draw_cursor_galley(
                &painter,
                tab,
                origin,
                &galley,
                options.cursor_visible,
                options.term_theme.cursor,
                ppp,
            );

            Self::handle_terminal_io(
                TerminalIoContext {
                    ctx,
                    clipboard,
                    ui,
                    term_rect: rect,
                    origin,
                    cell_w,
                    cell_h,
                    galley: Some(&galley),
                    response: &response,
                },
                tab,
            );
        } else {
            // Minimal empty state.
            let text = if tab.connecting {
                "Connecting..."
            } else if !tab.last_status.trim().is_empty() {
                tab.last_status.trim()
            } else {
                "Not connected"
            };
            painter.text(
                origin,
                egui::Align2::LEFT_TOP,
                text,
                FontId::proportional(14.0),
                options.theme.muted,
            );

            Self::handle_terminal_io(
                TerminalIoContext {
                    ctx,
                    clipboard,
                    ui,
                    term_rect: rect,
                    origin,
                    cell_w,
                    cell_h,
                    galley: None,
                    response: &response,
                },
                tab,
            );
        }

        // Hover-only scrollback bar (right side).
        let hovering_term = ui
            .input(|i| i.pointer.hover_pos())
            .map(|pos| rect.contains(pos))
            .unwrap_or(false);
        if (hovering_term || tab.scrollbar_dragging) && tab.scrollback_max > 0 {
            let visible_rows = tab.screen.size().0;
            Self::draw_scrollback_bar(
                &painter,
                rect,
                visible_rows,
                tab.screen.scrollback(),
                tab.scrollback_max,
                options.theme,
            );
        }

        if options.focus_shade && !options.is_active && !response.has_focus() {
            painter.rect_filled(
                rect,
                rounding,
                Color32::from_rgba_unmultiplied(128, 128, 128, 51),
            );
        }

        // Active-pane affordance: subtle border/glow so it's obvious which terminal is "current".
        if options.is_active || response.has_focus() {
            let c = options.theme.accent;
            let (a_stroke, a_glow) = if response.has_focus() { (180u8, 70u8) } else { (110u8, 38u8) };
            let stroke = Stroke::new(1.0, Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a_stroke));
            let glow = Stroke::new(3.0, Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a_glow));
            let r0 = rect.shrink(1.0);
            let r1 = rect.shrink(3.0);
            painter.rect_stroke(r1, rounding, glow);
            painter.rect_stroke(r0, rounding, stroke);
        }
    }
}
