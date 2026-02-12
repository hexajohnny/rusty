impl AppState {
    fn visible_cell_to_abs(tab: &SshTab, row: u16, col: u16) -> (i64, u16) {
        let top_abs = (tab.scrollback_max as i64 - tab.screen.scrollback() as i64).max(0);
        let abs_row = top_abs + row as i64;
        (abs_row, col)
    }

    fn cell_metrics(ctx: &egui::Context, font_id: &FontId) -> (f32, f32) {
        let ppp = ctx.pixels_per_point();
        ctx.fonts(|fonts| {
            // Derive metrics from actual layout to better match pixel snapping in the text renderer.
            let sample = "WWWWWWWWWWWWWWWW";
            let galley = fonts.layout_no_wrap(sample.to_string(), font_id.clone(), Color32::WHITE);
            let mut w = (galley.size().x / sample.len() as f32).max(1.0);
            let mut h = galley.size().y.max(1.0);

            // Snap to physical pixels to keep the grid stable and align overlays.
            let w_px = (w * ppp).round().max(1.0);
            let h_px = (h * ppp).round().max(1.0);
            w = w_px / ppp;
            h = h_px / ppp;
            (w, h)
        })
    }

    fn vt_color_to_color32(c: vt100::Color, default: Color32, term_theme: &TermTheme) -> Color32 {
        match c {
            vt100::Color::Default => default,
            vt100::Color::Rgb(r, g, b) => Color32::from_rgb(r, g, b),
            vt100::Color::Idx(i) => xterm_256_color(i, &term_theme.palette16),
        }
    }

    fn cell_style(cell: &vt100::Cell, term_theme: &TermTheme) -> TermStyle {
        let mut fg = Self::vt_color_to_color32(cell.fgcolor(), term_theme.fg, term_theme);
        let mut bg = Self::vt_color_to_color32(cell.bgcolor(), term_theme.bg, term_theme);

        // Common terminal behavior: bold maps to bright variants for the first 8 colors.
        if cell.bold() {
            if let vt100::Color::Idx(i) = cell.fgcolor() {
                if i < 8 {
                    fg = xterm_256_color(i + 8, &term_theme.palette16);
                }
            }
        }

        let inverse = cell.inverse();
        if inverse {
            std::mem::swap(&mut fg, &mut bg);
        }

        // Some TUIs rely on SGR 2 (faint/dim) for secondary text. The vt100 state carries this,
        // but egui has no "dim" attribute, so we simulate it by blending the foreground toward
        // the cell background.
        if cell.dim() {
            fg = lerp_color(fg, bg, term_theme.dim_blend);
        }

        TermStyle {
            fg,
            bg,
            italic: cell.italic(),
            underline: cell.underline(),
            inverse,
        }
    }

    fn screen_to_layout_job(
        screen: &vt100::Screen,
        font_id: FontId,
        term_theme: &TermTheme,
    ) -> LayoutJob {
        let (rows, cols) = screen.size();
        let mut job = LayoutJob::default();
        // NOTE: TextWrapping::max_rows == 0 means "render nothing". Keep the defaults,
        // only ensuring we don't wrap within rows (newlines still break rows).
        job.wrap.max_width = f32::INFINITY;
        job.wrap.max_rows = usize::MAX;
        job.wrap.break_anywhere = false;

        let mut current_style: Option<TermStyle> = None;
        let mut run = String::new();

        for row in 0..rows {
            for col in 0..cols {
                let cell = match screen.cell(row, col) {
                    Some(c) => c,
                    None => continue,
                };

                let style = Self::cell_style(cell, term_theme);
                if current_style.map(|s| s != style).unwrap_or(true) {
                    if let Some(s) = current_style.take() {
                        if !run.is_empty() {
                            job.append(&run, 0.0, s.to_text_format(font_id.clone()));
                            run.clear();
                        }
                        current_style = Some(style);
                    } else {
                        current_style = Some(style);
                    }
                }

                // Wide characters occupy two cells. Render the continuation cell as a space
                // to preserve monospace alignment.
                if cell.is_wide_continuation() {
                    run.push(' ');
                } else if cell.has_contents() {
                    run.push_str(&cell.contents());
                } else {
                    run.push(' ');
                }
            }

            if row + 1 < rows {
                run.push('\n');
            }
        }

        if let Some(s) = current_style {
            if !run.is_empty() {
                job.append(&run, 0.0, s.to_text_format(font_id));
            }
        }

        job
    }

    fn send_bytes(tab: &mut SshTab, bytes: Vec<u8>) {
        if let Some(tx) = tab.worker_tx.as_ref() {
            let _ = tx.send(WorkerMessage::Input(bytes));
        }
    }

    fn set_scrollback(tab: &mut SshTab, rows: usize) {
        let target = rows.min(tab.scrollback_max);
        tab.screen.set_scrollback(target);
        tab.pending_scrollback = Some(target);
    }

    fn send_paste_text(tab: &mut SshTab, s: &str) {
        if s.is_empty() {
            return;
        }
        let mut bytes = Vec::new();
        if tab.screen.bracketed_paste() {
            bytes.extend_from_slice(b"\x1b[200~");
            bytes.extend_from_slice(s.as_bytes());
            bytes.extend_from_slice(b"\x1b[201~");
        } else {
            bytes.extend_from_slice(s.as_bytes());
        }
        Self::send_bytes(tab, bytes);
    }

    fn copy_text_to_clipboard(ctx: &egui::Context, clipboard: &mut Option<Clipboard>, text: String) {
        ctx.output_mut(|o| o.copied_text = text.clone());
        if let Some(cb) = clipboard.as_mut() {
            let _ = cb.set_text(text);
        }
    }

    fn copy_selection_with_flash(
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        tab: &mut SshTab,
        text: String,
    ) {
        if text.is_empty() {
            return;
        }
        Self::copy_text_to_clipboard(ctx, clipboard, text);
        tab.copy_flash_until = Some(Instant::now() + Duration::from_millis(150));
    }

    fn key_to_ctrl_byte(key: egui::Key) -> Option<u8> {
        use egui::Key::*;
        let c = match key {
            A => b'a',
            B => b'b',
            C => b'c',
            D => b'd',
            E => b'e',
            F => b'f',
            G => b'g',
            H => b'h',
            I => b'i',
            J => b'j',
            K => b'k',
            L => b'l',
            M => b'm',
            N => b'n',
            O => b'o',
            P => b'p',
            Q => b'q',
            R => b'r',
            S => b's',
            T => b't',
            U => b'u',
            V => b'v',
            W => b'w',
            X => b'x',
            Y => b'y',
            Z => b'z',
            _ => return None,
        };
        Some(c & 0x1f)
    }

    fn send_key(tab: &mut SshTab, key: egui::Key, mods: egui::Modifiers) {
        // Terminal-style copy shortcut that doesn't collide with SIGINT.
        if mods.ctrl && mods.shift && key == egui::Key::C {
            return;
        }

        if mods.ctrl {
            if let Some(b) = Self::key_to_ctrl_byte(key) {
                Self::send_bytes(tab, vec![b]);
                return;
            }
        }

        let app_cursor = tab.screen.application_cursor();
        let bytes: Option<&'static [u8]> = match key {
            egui::Key::Enter => Some(b"\r"),
            egui::Key::Tab => Some(b"\t"),
            egui::Key::Backspace => Some(&[0x7f]),
            egui::Key::Escape => Some(&[0x1b]),
            egui::Key::ArrowUp => Some(if app_cursor { b"\x1bOA" } else { b"\x1b[A" }),
            egui::Key::ArrowDown => Some(if app_cursor { b"\x1bOB" } else { b"\x1b[B" }),
            egui::Key::ArrowRight => Some(if app_cursor { b"\x1bOC" } else { b"\x1b[C" }),
            egui::Key::ArrowLeft => Some(if app_cursor { b"\x1bOD" } else { b"\x1b[D" }),
            egui::Key::Home => Some(if app_cursor { b"\x1bOH" } else { b"\x1b[H" }),
            egui::Key::End => Some(if app_cursor { b"\x1bOF" } else { b"\x1b[F" }),
            egui::Key::PageUp => Some(b"\x1b[5~"),
            egui::Key::PageDown => Some(b"\x1b[6~"),
            egui::Key::Insert => Some(b"\x1b[2~"),
            egui::Key::Delete => Some(b"\x1b[3~"),
            _ => None,
        };

        if let Some(b) = bytes {
            Self::send_bytes(tab, b.to_vec());
        }
    }

    fn mouse_event_bytes(
        encoding: vt100::MouseProtocolEncoding,
        mode: vt100::MouseProtocolMode,
        pressed: bool,
        button: egui::PointerButton,
        col_1: u16,
        row_1: u16,
    ) -> Option<Vec<u8>> {
        if mode == vt100::MouseProtocolMode::None {
            return None;
        }

        let btn_code = match button {
            egui::PointerButton::Primary => 0u8,
            egui::PointerButton::Middle => 1u8,
            egui::PointerButton::Secondary => 2u8,
            _ => return None,
        };

        // In press-only mode, ignore releases.
        if mode == vt100::MouseProtocolMode::Press && !pressed {
            return None;
        }

        match encoding {
            vt100::MouseProtocolEncoding::Sgr => {
                let suffix = if pressed { b'M' } else { b'm' };
                let s = format!("\x1b[<{};{};{}{}", btn_code, col_1, row_1, suffix as char);
                Some(s.into_bytes())
            }
            _ => {
                // Default encoding: CSI M Cb Cx Cy. Release is encoded with Cb=3.
                let cb = 32u8 + if pressed { btn_code } else { 3u8 };
                let cx = 32u8.saturating_add(col_1.min(223) as u8);
                let cy = 32u8.saturating_add(row_1.min(223) as u8);
                Some(vec![0x1b, b'[', b'M', cb, cx, cy])
            }
        }
    }

    fn handle_terminal_io(
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        ui: &mut egui::Ui,
        tab: &mut SshTab,
        term_rect: Rect,
        origin: Pos2,
        cell_w: f32,
        cell_h: f32,
        galley: Option<&egui::Galley>,
        response: &egui::Response,
    ) {
        let events = ui.input(|i| i.events.clone());
        let global_mods = ui.input(|i| i.modifiers);
        let has_copy_event = events.iter().any(|e| matches!(e, egui::Event::Copy));
        let has_paste_event = events.iter().any(|e| matches!(e, egui::Event::Paste(_)));
        let has_text_event = events
            .iter()
            .any(|e| matches!(e, egui::Event::Text(t) if !t.is_empty()));

        let (screen_rows, screen_cols) = tab.screen.size();
        let remote_mouse_enabled =
            tab.connected && tab.screen.mouse_protocol_mode() != vt100::MouseProtocolMode::None;
        let local_select_enabled = !remote_mouse_enabled || global_mods.shift;
        let allow_remote_mouse = remote_mouse_enabled && !global_mods.shift;

        // Selection and remote mouse clicks:
        // - If the remote enabled mouse reporting, clicking interacts with the remote.
        // - Click-drag selects text locally (so you can still copy output).
        //
        // We intentionally avoid relying on `egui::Event::PointerMoved` here, because pointer move
        // events are less reliable across nested UI layouts. Instead we use pointer state and the
        // widget `Response` to keep selection responsive.
        let pointer_pos = response
            .interact_pointer_pos()
            .or_else(|| ui.input(|i| i.pointer.latest_pos()));
        let primary_pressed = ui.input(|i| i.pointer.primary_pressed());
        let primary_down = ui.input(|i| i.pointer.primary_down());
        let primary_released = ui.input(|i| i.pointer.primary_released());
        let hovering_term = pointer_pos.map(|pos| term_rect.contains(pos)).unwrap_or(false) || response.hovered();

        // Scrollbar interaction (hover-only; click-drag to scroll).
        // We keep this independent from "remote mouse mode" so you can always scroll locally.
        // Keep the hit area wider and slightly inset from the right window edge so resize grips
        // do not steal pointer hover/clicks.
        let scrollbar_w = 22.0;
        let scrollbar_edge_inset = 8.0;
        let scrollbar_right = (term_rect.right() - scrollbar_edge_inset).max(term_rect.left());
        let scrollbar_left = (scrollbar_right - scrollbar_w).max(term_rect.left());
        let scrollbar_rect = Rect::from_min_max(
            Pos2::new(scrollbar_left, term_rect.top()),
            Pos2::new(scrollbar_right, term_rect.bottom()),
        );
        let scrollbar_id = Id::new(("terminal_scrollbar", tab.id));
        let scrollbar_response = ui.interact(scrollbar_rect, scrollbar_id, Sense::click_and_drag());
        let hovering_scrollbar = pointer_pos
            .map(|p| scrollbar_rect.contains(p))
            .unwrap_or(false)
            || scrollbar_response.hovered();
        if (scrollbar_response.drag_started() || (primary_pressed && hovering_scrollbar))
            && tab.connected
            && tab.scrollback_max > 0
        {
            tab.scrollbar_dragging = true;
            response.request_focus();
        }
        let was_scrollbar_dragging = tab.scrollbar_dragging;
        if primary_released {
            tab.scrollbar_dragging = false;
        }
        if tab.scrollbar_dragging && primary_down && tab.connected && tab.scrollback_max > 0 {
            if let Some(pos) = pointer_pos {
                // Map pointer Y to scrollback offset.
                let visible_rows = tab.screen.size().0 as f32;
                let total_rows = visible_rows + tab.scrollback_max as f32;
                let track_h = term_rect.height().max(1.0);
                let min_thumb_h = 18.0_f32.min(track_h);
                let handle_h = (track_h * (visible_rows / total_rows)).clamp(min_thumb_h, track_h);
                let track_min = term_rect.top();
                let track_max = term_rect.bottom() - handle_h;
                let y = pos.y.clamp(track_min, track_max.max(track_min));
                let t = if track_max > track_min {
                    (y - track_min) / (track_max - track_min)
                } else {
                    0.0
                };
                // t=0 => top (max scrollback), t=1 => bottom (0 scrollback)
                let max = tab.scrollback_max as f32;
                let desired = ((1.0 - t) * max).round().clamp(0.0, max) as usize;
                if desired != tab.screen.scrollback() {
                    Self::set_scrollback(tab, desired);
                }
            }
        }

        // Local scrollback (mouse wheel / trackpad). This is independent of any remote app state.
        if hovering_term && tab.connected {
            // Prefer per-frame wheel events. `smooth_scroll_delta` introduces inertial drift and
            // can keep scrolling on repaint ticks even after wheel input has stopped.
            let mut dy = 0.0f32;
            for ev in events.iter() {
                if let egui::Event::Scroll(delta) = ev {
                    dy += delta.y;
                }
            }
            if dy.abs() <= 0.001 {
                dy = ui.input(|i| i.raw_scroll_delta.y);
            }

            if dy.abs() > 0.001 {
                // Accumulate into rows and apply integer deltas.
                let step = cell_h.max(1.0);
                tab.scroll_wheel_accum += dy / step;
                let rows_delta = (tab.scroll_wheel_accum.trunc() as i64).clamp(-256, 256) as i32;
                if rows_delta != 0 {
                    tab.scroll_wheel_accum -= rows_delta as f32;
                    let cur = tab.screen.scrollback() as i64;
                    let max = tab.scrollback_max as i64;
                    let delta = rows_delta as i64;
                    let next = (cur + delta).clamp(0, max) as usize;
                    if next != tab.screen.scrollback() {
                        Self::set_scrollback(tab, next);
                    }
                }
            }
        }

        // Clamp to the text grid area (not the outer padding) so selections still work if
        // you start dragging inside the padding.
        let grid_min = origin;
        let mut grid_max = Pos2::new(term_rect.right() - TERM_PAD_X, term_rect.bottom() - TERM_PAD_Y);
        grid_max.x = grid_max.x.max(grid_min.x + 1.0);
        grid_max.y = grid_max.y.max(grid_min.y + 1.0);
        let clamp_pos_to_grid = |p: Pos2| -> Pos2 {
            let x = p.x.clamp(grid_min.x, grid_max.x - 0.001);
            let y = p.y.clamp(grid_min.y, grid_max.y - 0.001);
            Pos2::new(x, y)
        };

        if primary_pressed && !hovering_scrollbar {
            if let Some(pos) = pointer_pos {
                if term_rect.contains(pos) {
                    let pos = clamp_pos_to_grid(pos);

                    if local_select_enabled {
                        tab.pending_remote_click = None;
                        if let Some((row, col)) = Self::pos_to_cell(
                            pos,
                            origin,
                            cell_w,
                            cell_h,
                            &tab.screen,
                            galley,
                            screen_rows,
                            screen_cols,
                        ) {
                            tab.selection = Some(TermSelection {
                                anchor: (row, col),
                                cursor: (row, col),
                                dragging: true,
                            });
                            let abs = Self::visible_cell_to_abs(tab, row, col);
                            tab.abs_selection = Some(TermAbsSelection {
                                anchor: abs,
                                cursor: abs,
                                dragging: true,
                            });
                        }
                    } else if allow_remote_mouse {
                        // Remote mouse is enabled. Treat this as a remote click unless the user drags,
                        // in which case we switch into local selection mode.
                        tab.selection = None;
                        tab.abs_selection = None;
                        if let Some((row, col)) = Self::pos_to_cell(
                            pos,
                            origin,
                            cell_w,
                            cell_h,
                            &tab.screen,
                            galley,
                            screen_rows,
                            screen_cols,
                        ) {
                            tab.pending_remote_click = Some(PendingRemoteClick {
                                start_pos: pos,
                                start_cell: (row, col),
                            });
                        } else {
                            tab.pending_remote_click = None;
                        }
                    }
                } else {
                    // Clicking outside clears selection and any pending click.
                    tab.selection = None;
                    tab.abs_selection = None;
                    tab.pending_remote_click = None;
                }
            }
        }

        if primary_down && !tab.scrollbar_dragging {
            if let Some(raw_pos) = pointer_pos {
                let pos = clamp_pos_to_grid(raw_pos);
                if let Some(sel) = tab.selection.as_mut() {
                    if sel.dragging {
                        if let Some((row, col)) = Self::pos_to_cell(
                            pos,
                            origin,
                            cell_w,
                            cell_h,
                            &tab.screen,
                            galley,
                            screen_rows,
                            screen_cols,
                        ) {
                            sel.cursor = (row, col);
                            let abs_cursor = Self::visible_cell_to_abs(tab, row, col);
                            if let Some(abs_sel) = tab.abs_selection.as_mut() {
                                abs_sel.cursor = abs_cursor;
                                abs_sel.dragging = true;
                            }
                        }
                    }
                } else if allow_remote_mouse {
                    // When remote mouse is enabled, a small drag switches the gesture into local selection mode.
                    if let Some(pending) = tab.pending_remote_click {
                        let d = pos - pending.start_pos;
                        if d.length_sq() >= 6.0 * 6.0 {
                            if let Some((row, col)) = Self::pos_to_cell(
                                pos,
                                origin,
                                cell_w,
                                cell_h,
                                &tab.screen,
                                galley,
                                screen_rows,
                                screen_cols,
                            ) {
                                tab.selection = Some(TermSelection {
                                    anchor: pending.start_cell,
                                    cursor: (row, col),
                                    dragging: true,
                                });
                                let abs_anchor =
                                    Self::visible_cell_to_abs(tab, pending.start_cell.0, pending.start_cell.1);
                                let abs_cursor = Self::visible_cell_to_abs(tab, row, col);
                                tab.abs_selection = Some(TermAbsSelection {
                                    anchor: abs_anchor,
                                    cursor: abs_cursor,
                                    dragging: true,
                                });
                                tab.pending_remote_click = None;
                            }
                        }
                    }
                }

                // Auto-scroll while selecting beyond the viewport edge.
                if tab.connected && tab.scrollback_max > 0 {
                    let selection_dragging = tab.selection.map(|s| s.dragging).unwrap_or(false);
                    if selection_dragging {
                        let dist_top = (term_rect.top() - raw_pos.y).max(0.0);
                        let dist_bottom = (raw_pos.y - term_rect.bottom()).max(0.0);
                        let mut step: i32 = 0;
                        if dist_top > 0.0 {
                            let boost = (dist_top / (cell_h * 2.5)).floor() as i32;
                            step = (1 + boost).clamp(1, 4);
                        } else if dist_bottom > 0.0 {
                            let boost = (dist_bottom / (cell_h * 2.5)).floor() as i32;
                            step = -(1 + boost).clamp(1, 4);
                        }

                        if step != 0
                            && tab.last_selection_autoscroll.elapsed() >= Duration::from_millis(35)
                        {
                            tab.last_selection_autoscroll = Instant::now();
                            let cur = tab.screen.scrollback() as i64;
                            let max = tab.scrollback_max as i64;
                            let next = (cur + step as i64).clamp(0, max) as usize;
                            if next != tab.screen.scrollback() {
                                let mut cursor_after: Option<(u16, u16)> = None;
                                if let Some(sel) = tab.selection.as_mut() {
                                    if step > 0 {
                                        sel.cursor.0 = 0;
                                    } else if screen_rows > 0 {
                                        sel.cursor.0 = screen_rows.saturating_sub(1);
                                    }
                                    cursor_after = Some(sel.cursor);
                                }

                                Self::set_scrollback(tab, next);

                                if let Some((row, col)) = cursor_after {
                                    let abs_cursor = Self::visible_cell_to_abs(tab, row, col);
                                    if let Some(abs_sel) = tab.abs_selection.as_mut() {
                                        abs_sel.cursor = abs_cursor;
                                        abs_sel.dragging = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if primary_released && !was_scrollbar_dragging {
            // End local selection if active.
            if let Some(sel) = tab.selection.as_mut() {
                if sel.dragging {
                    sel.dragging = false;
                }
                if sel.is_empty() {
                    tab.selection = None;
                }
                if let Some(abs_sel) = tab.abs_selection.as_mut() {
                    abs_sel.dragging = false;
                }
                if tab.abs_selection.map(|s| s.is_empty()).unwrap_or(false) {
                    tab.abs_selection = None;
                }
                // Local selection consumes the gesture: do not send remote click.
                tab.pending_remote_click = None;
            } else if allow_remote_mouse {
                // Dispatch remote click if it was not turned into a local selection.
                if let Some(pending) = tab.pending_remote_click.take() {
                    let mode = tab.screen.mouse_protocol_mode();
                    let encoding = tab.screen.mouse_protocol_encoding();

                    let release_cell = pointer_pos
                        .map(clamp_pos_to_grid)
                        .and_then(|pos| {
                            Self::pos_to_cell(
                                pos,
                                origin,
                                cell_w,
                                cell_h,
                                &tab.screen,
                                galley,
                                screen_rows,
                                screen_cols,
                            )
                        })
                        .unwrap_or(pending.start_cell);

                    // xterm mouse protocol is 1-based coordinates.
                    let (sr, sc) = pending.start_cell;
                    let (rr, rc) = release_cell;
                    let sc_1 = sc.saturating_add(1);
                    let sr_1 = sr.saturating_add(1);
                    let rc_1 = rc.saturating_add(1);
                    let rr_1 = rr.saturating_add(1);

                    if let Some(bytes) = Self::mouse_event_bytes(
                        encoding,
                        mode,
                        true,
                        egui::PointerButton::Primary,
                        sc_1,
                        sr_1,
                    ) {
                        Self::send_bytes(tab, bytes);
                    }
                    if let Some(bytes) = Self::mouse_event_bytes(
                        encoding,
                        mode,
                        false,
                        egui::PointerButton::Primary,
                        rc_1,
                        rr_1,
                    ) {
                        Self::send_bytes(tab, bytes);
                    }
                }
            } else {
                tab.pending_remote_click = None;
            }
        }

        // Keyboard input only when our terminal region has focus.
        if response.has_focus() && tab.connected {
            for ev in events.iter() {
                match ev {
                    egui::Event::Copy => {
                        if tab.selection.is_some() || tab.abs_selection.is_some() {
                            let text = if let Some(abs_sel) = tab.abs_selection {
                                Self::selection_text_abs(&tab.screen, tab.scrollback_max, abs_sel)
                            } else if let Some(sel) = tab.selection {
                                Self::selection_text(&tab.screen, sel)
                            } else {
                                String::new()
                            };
                            if !text.is_empty() {
                                Self::copy_selection_with_flash(ctx, clipboard, tab, text);
                            }
                        } else {
                            // Treat Ctrl+C as SIGINT when nothing is selected.
                            Self::send_bytes(tab, vec![0x03]);
                        }
                    }
                    egui::Event::Text(t) => {
                        if !t.is_empty() {
                            Self::send_bytes(tab, t.as_bytes().to_vec());
                        }
                    }
                    egui::Event::Paste(s) => {
                        Self::send_paste_text(tab, s);
                    }
                    egui::Event::Key {
                        key,
                        pressed: true,
                        modifiers,
                        ..
                    } => {
                        // Copy selection to clipboard (terminal-style shortcut).
                        if modifiers.ctrl && modifiers.shift && *key == egui::Key::C {
                            let text = if let Some(abs_sel) = tab.abs_selection {
                                Self::selection_text_abs(&tab.screen, tab.scrollback_max, abs_sel)
                            } else if let Some(sel) = tab.selection {
                                Self::selection_text(&tab.screen, sel)
                            } else {
                                tab.screen.contents()
                            };
                            if !text.is_empty() {
                                Self::copy_text_to_clipboard(ctx, clipboard, text);
                            }
                            continue;
                        }

                        // If there is a selection, Ctrl+C should copy (like Windows Terminal)
                        // instead of sending SIGINT to the remote.
                        if modifiers.ctrl && !modifiers.shift && *key == egui::Key::C {
                            // Some platforms report Ctrl+C as `Event::Copy` instead of `Event::Key`.
                            // Let `Event::Copy` handle it to avoid double actions.
                            if has_copy_event {
                                continue;
                            }
                            if tab.selection.is_some() || tab.abs_selection.is_some() {
                                let text = if let Some(abs_sel) = tab.abs_selection {
                                    Self::selection_text_abs(&tab.screen, tab.scrollback_max, abs_sel)
                                } else if let Some(sel) = tab.selection {
                                    Self::selection_text(&tab.screen, sel)
                                } else {
                                    String::new()
                                };
                                if !text.is_empty() {
                                    Self::copy_selection_with_flash(ctx, clipboard, tab, text);
                                }
                            } else {
                                // No local selection: behave like a real terminal (SIGINT).
                                Self::send_bytes(tab, vec![0x03]);
                            }
                            continue;
                        }

                        // Paste shortcut. Prefer the platform integration's Paste event, but
                        // fall back to reading the OS clipboard directly if needed.
                        if (modifiers.ctrl && *key == egui::Key::V) || (modifiers.ctrl && modifiers.shift && *key == egui::Key::V) {
                            if has_paste_event || has_text_event {
                                continue;
                            }
                            if let Some(cb) = clipboard.as_mut() {
                                if let Ok(s) = cb.get_text() {
                                    Self::send_paste_text(tab, &s);
                                }
                            }
                            continue;
                        }

                        Self::send_key(tab, *key, *modifiers);
                    }
                    _ => {}
                }
            }
        }

    }

    fn pos_to_cell(
        pos: Pos2,
        origin: Pos2,
        cell_w: f32,
        cell_h: f32,
        screen: &vt100::Screen,
        galley: Option<&egui::Galley>,
        rows: u16,
        cols: u16,
    ) -> Option<(u16, u16)> {
        if let Some(g) = galley {
            if let Some((r, c)) = Self::pos_to_cell_galley(pos, origin, screen, g, rows, cols) {
                return Some((r, c));
            }
        }

        if rows == 0 || cols == 0 {
            return None;
        }

        let col = ((pos.x - origin.x) / cell_w).floor() as i32;
        let row = ((pos.y - origin.y) / cell_h).floor() as i32;
        if col < 0 || row < 0 {
            return None;
        }

        let col = (col as u16).min(cols.saturating_sub(1));
        let row = (row as u16).min(rows.saturating_sub(1));
        Some((row, col))
    }

    fn pos_to_cell_galley(
        pos: Pos2,
        origin: Pos2,
        screen: &vt100::Screen,
        galley: &egui::Galley,
        rows: u16,
        cols: u16,
    ) -> Option<(u16, u16)> {
        if rows == 0 || cols == 0 {
            return None;
        }

        let x = pos.x - origin.x;
        let y = pos.y - origin.y;
        if x < 0.0 || y < 0.0 {
            return None;
        }

        let max_rows = rows as usize;
        let usable_rows = galley.rows.len().min(max_rows);
        if usable_rows == 0 {
            return None;
        }

        let mut row_idx: Option<usize> = None;
        for (i, row) in galley.rows.iter().take(usable_rows).enumerate() {
            if y >= row.rect.top() && y < row.rect.bottom() {
                row_idx = Some(i);
                break;
            }
        }

        let row_idx = row_idx.unwrap_or_else(|| {
            if y >= galley.rows[usable_rows - 1].rect.bottom() {
                usable_rows - 1
            } else {
                0
            }
        });

        let row_g = &galley.rows[row_idx];
        let char_idx = row_g.char_at(x);

        let row_u16 = row_idx as u16;
        let map = Self::row_col_to_char_index_map(screen, row_u16);
        let mut col_idx = Self::char_index_to_col(&map, char_idx);
        if col_idx as u16 >= cols {
            col_idx = cols.saturating_sub(1) as usize;
        }
        Some((row_u16, col_idx as u16))
    }

    fn row_col_to_char_index_map(screen: &vt100::Screen, row: u16) -> Vec<usize> {
        let (_rows, cols) = screen.size();
        let cols_usize = cols as usize;
        let mut out = Vec::with_capacity(cols_usize.saturating_add(1));

        let mut idx = 0usize;
        out.push(0);
        for col in 0..cols {
            let add = match screen.cell(row, col) {
                Some(cell) => {
                    if cell.is_wide_continuation() {
                        1usize
                    } else if cell.has_contents() {
                        cell.contents().chars().count().max(1)
                    } else {
                        1usize
                    }
                }
                None => 1usize,
            };
            idx = idx.saturating_add(add);
            out.push(idx);
        }
        out
    }

    fn col_to_char_index(map: &[usize], col: u16) -> usize {
        let i = col as usize;
        if i < map.len() {
            map[i]
        } else {
            *map.last().unwrap_or(&0)
        }
    }

    fn char_index_to_col(map: &[usize], char_idx: usize) -> usize {
        if map.len() <= 1 {
            return 0;
        }

        let mut col = match map.binary_search(&char_idx) {
            Ok(i) => i,
            Err(next) => next.saturating_sub(1),
        };
        // `map.len() == cols + 1`, so clamp to the last visible column.
        if col >= map.len().saturating_sub(1) {
            col = map.len().saturating_sub(2);
        }
        col
    }

    fn selection_text(screen: &vt100::Screen, sel: TermSelection) -> String {
        let (rows, cols) = screen.size();
        if rows == 0 || cols == 0 {
            return String::new();
        }

        let ((mut sr, mut sc), (mut er, mut ec)) = sel.normalized();
        sr = sr.min(rows.saturating_sub(1));
        er = er.min(rows.saturating_sub(1));
        sc = sc.min(cols.saturating_sub(1));
        ec = ec.min(cols.saturating_sub(1));

        let mut out = String::new();
        for row in sr..=er {
            let start_col = if row == sr { sc } else { 0 };
            let end_col = if row == er { ec } else { cols.saturating_sub(1) };
            if start_col > end_col {
                continue;
            }

            let mut line = String::new();
            for col in start_col..=end_col {
                if let Some(cell) = screen.cell(row, col) {
                    if cell.is_wide_continuation() {
                        continue;
                    }
                    if cell.has_contents() {
                        line.push_str(&cell.contents());
                    } else {
                        line.push(' ');
                    }
                } else {
                    line.push(' ');
                }
            }

            let trimmed = line.trim_end_matches(' ');
            out.push_str(trimmed);
            if row != er {
                out.push('\n');
            }
        }

        out
    }

    fn row_segment_text(screen: &vt100::Screen, row: u16, start_col: u16, end_col: u16) -> String {
        if start_col > end_col {
            return String::new();
        }

        let mut line = String::new();
        for col in start_col..=end_col {
            if let Some(cell) = screen.cell(row, col) {
                if cell.is_wide_continuation() {
                    continue;
                }
                if cell.has_contents() {
                    line.push_str(&cell.contents());
                } else {
                    line.push(' ');
                }
            } else {
                line.push(' ');
            }
        }
        line
    }

    fn selection_text_abs(
        screen: &vt100::Screen,
        max_scrollback: usize,
        sel: TermAbsSelection,
    ) -> String {
        let (rows, cols) = screen.size();
        if rows == 0 || cols == 0 {
            return String::new();
        }

        let max_abs_row = max_scrollback as i64 + rows as i64 - 1;
        if max_abs_row < 0 {
            return String::new();
        }

        let (mut sr, mut sc) = sel.anchor;
        let (mut er, mut ec) = sel.cursor;
        if (sr, sc) > (er, ec) {
            std::mem::swap(&mut sr, &mut er);
            std::mem::swap(&mut sc, &mut ec);
        }

        sr = sr.clamp(0, max_abs_row);
        er = er.clamp(0, max_abs_row);
        sc = sc.min(cols.saturating_sub(1));
        ec = ec.min(cols.saturating_sub(1));

        let mut out = String::new();
        let mut scn = screen.clone();
        for abs_row in sr..=er {
            let start_col = if abs_row == sr { sc } else { 0 };
            let end_col = if abs_row == er { ec } else { cols.saturating_sub(1) };
            if start_col > end_col {
                continue;
            }

            // Map absolute row -> viewport by setting an appropriate scrollback offset.
            let desired_scrollback = (max_scrollback as i64 - abs_row).max(0) as usize;
            scn.set_scrollback(desired_scrollback);
            let top_abs = max_scrollback as i64 - scn.scrollback() as i64;
            let view_row = (abs_row - top_abs).clamp(0, rows as i64 - 1) as u16;

            let line = Self::row_segment_text(&scn, view_row, start_col, end_col);
            out.push_str(line.trim_end_matches(' '));
            if abs_row != er {
                out.push('\n');
            }
        }

        out
    }

    fn visible_selection_from_abs(tab: &SshTab, sel: TermAbsSelection) -> Option<TermSelection> {
        let (rows, cols) = tab.screen.size();
        if rows == 0 || cols == 0 {
            return None;
        }

        let mut a = sel.anchor;
        let mut b = sel.cursor;
        if a > b {
            std::mem::swap(&mut a, &mut b);
        }

        let top_abs = (tab.scrollback_max as i64 - tab.screen.scrollback() as i64).max(0);
        let bottom_abs = top_abs + rows as i64 - 1;
        if b.0 < top_abs || a.0 > bottom_abs {
            return None;
        }

        let start_abs_row = a.0.max(top_abs);
        let end_abs_row = b.0.min(bottom_abs);
        let start_row = (start_abs_row - top_abs) as u16;
        let end_row = (end_abs_row - top_abs) as u16;

        let start_col = if a.0 < top_abs { 0 } else { a.1 }.min(cols.saturating_sub(1));
        let end_col = if b.0 > bottom_abs {
            cols.saturating_sub(1)
        } else {
            b.1
        }
        .min(cols.saturating_sub(1));

        Some(TermSelection {
            anchor: (start_row, start_col),
            cursor: (end_row, end_col),
            dragging: sel.dragging,
        })
    }

    fn draw_selection_galley(
        painter: &egui::Painter,
        tab: &SshTab,
        origin: Pos2,
        galley: &egui::Galley,
        sel: TermSelection,
    ) {
        // Slightly stronger than a typical text selection so it's visible over dense ANSI color output.
        let selection_bg = if tab.copy_flash_until.is_some() {
            // Flash brighter on copy, then selection disappears (handled in the AppState update loop).
            Color32::from_rgba_unmultiplied(255, 184, 108, 190)
        } else {
            Color32::from_rgba_unmultiplied(255, 184, 108, 96)
        };
        let (rows, cols) = tab.screen.size();
        if rows == 0 || cols == 0 {
            return;
        }

        let ((mut sr, mut sc), (mut er, mut ec)) = sel.normalized();
        sr = sr.min(rows.saturating_sub(1));
        er = er.min(rows.saturating_sub(1));
        sc = sc.min(cols.saturating_sub(1));
        ec = ec.min(cols.saturating_sub(1));

        let usable_rows = galley.rows.len().min(rows as usize);
        if usable_rows == 0 {
            return;
        }

        for row in sr..=er {
            let row_idx = row as usize;
            if row_idx >= usable_rows {
                break;
            }
            let row_g = &galley.rows[row_idx];
            let map = Self::row_col_to_char_index_map(&tab.screen, row);

            let start_col = if row == sr { sc } else { 0 };
            let end_col = if row == er { ec } else { cols.saturating_sub(1) };
            if start_col > end_col {
                continue;
            }

            let start_i = Self::col_to_char_index(&map, start_col);
            let end_i = Self::col_to_char_index(&map, end_col.saturating_add(1));
            let x0 = origin.x + row_g.x_offset(start_i);
            let x1 = origin.x + row_g.x_offset(end_i);
            let y0 = origin.y + row_g.rect.top();
            let y1 = origin.y + row_g.rect.bottom();
            let rect = Rect::from_min_max(Pos2::new(x0, y0), Pos2::new(x1, y1));
            painter.rect_filled(rect, 0.0, selection_bg);
        }
    }

    fn draw_cursor_galley(
        painter: &egui::Painter,
        tab: &SshTab,
        origin: Pos2,
        galley: &egui::Galley,
        cursor_visible: bool,
        cursor_color: Color32,
        ppp: f32,
    ) {
        // Hide the cursor while viewing scrollback, like a normal terminal.
        if tab.screen.hide_cursor() || !cursor_visible || tab.screen.scrollback() > 0 {
            return;
        }

        let (rows, cols) = tab.screen.size();
        if rows == 0 || cols == 0 {
            return;
        }

        let (mut row, mut col) = tab.screen.cursor_position();
        row = row.min(rows.saturating_sub(1));
        col = col.min(cols.saturating_sub(1));

        let row_idx = row as usize;
        if row_idx >= galley.rows.len() {
            return;
        }
        let row_g = &galley.rows[row_idx];
        let map = Self::row_col_to_char_index_map(&tab.screen, row);

        let start_i = Self::col_to_char_index(&map, col);
        let end_i = Self::col_to_char_index(&map, col.saturating_add(1));
        let x0 = origin.x + row_g.x_offset(start_i);
        let x1 = origin.x + row_g.x_offset(end_i);
        let w = (x1 - x0).max(2.0 / ppp.max(1.0));

        let thickness = (2.0 * ppp).round().max(1.0) / ppp.max(1.0);
        let y1 = origin.y + row_g.rect.bottom();
        let rect = Rect::from_min_size(Pos2::new(x0, y1 - thickness), Vec2::new(w, thickness));
        painter.rect_filled(rect, 0.0, cursor_color);
    }

    fn draw_scrollback_bar(
        painter: &egui::Painter,
        rect: Rect,
        visible_rows: u16,
        scrollback: usize,
        max_scrollback: usize,
        theme: UiTheme,
    ) {
        if max_scrollback == 0 || rect.width() < 20.0 || rect.height() < 20.0 {
            return;
        }

        let with_alpha = |c: Color32, a: u8| Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a);

        let bar_w = 12.0;
        let edge_inset = 8.0;
        let pad = 2.0;
        let track_right = (rect.right() - edge_inset - pad).max(rect.left());
        let track_left = (track_right - bar_w).max(rect.left());
        let track = Rect::from_min_max(
            Pos2::new(track_left, rect.top() + pad),
            Pos2::new(track_right, rect.bottom() - pad),
        );

        let total_rows = visible_rows as f32 + max_scrollback as f32;
        let visible = (visible_rows as f32).max(1.0);
        let ratio = (visible / total_rows.max(visible)).clamp(0.05, 1.0);
        let mut thumb_h = (track.height() * ratio).round().max(14.0);
        thumb_h = thumb_h.min(track.height());

        let t = (scrollback as f32 / max_scrollback as f32).clamp(0.0, 1.0);
        let y = track.bottom() - thumb_h - t * (track.height() - thumb_h);
        let thumb = Rect::from_min_size(Pos2::new(track.left(), y), Vec2::new(track.width(), thumb_h));

        painter.rect_filled(track, 3.0, with_alpha(theme.top_border, 70));
        painter.rect_filled(thumb, 3.0, with_alpha(theme.accent, 150));
    }

}
