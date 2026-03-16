struct TerminalIoContext<'a> {
    ctx: &'a egui::Context,
    clipboard: &'a mut Option<Clipboard>,
    ui: &'a mut egui::Ui,
    term_rect: Rect,
    origin: Pos2,
    cell_w: f32,
    cell_h: f32,
    galley: Option<&'a egui::Galley>,
    response: &'a egui::Response,
}

struct CellLookup<'a> {
    origin: Pos2,
    cell_w: f32,
    cell_h: f32,
    galley: Option<&'a egui::Galley>,
    rows: u16,
    cols: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteMouseAction {
    Press(egui::PointerButton),
    Release(egui::PointerButton),
    Move(Option<egui::PointerButton>),
    WheelUp,
    WheelDown,
    WheelLeft,
    WheelRight,
}

impl AppState {
    fn visible_cell_to_abs(tab: &SshTab, row: u16, col: u16) -> (i64, u16) {
        let top_abs = (tab.scrollback_max as i64 - tab.screen.scrollback() as i64).max(0);
        let abs_row = top_abs + row as i64;
        (abs_row, col)
    }

    fn cell_metrics(tab: &mut SshTab, ctx: &egui::Context, font_id: &FontId) -> (f32, f32) {
        let font_size_bits = font_id.size.to_bits();
        let pixels_per_point_bits = ctx.pixels_per_point().to_bits();
        if let Some(cache) = tab.cell_metrics_cache.as_ref() {
            if cache.font_size_bits == font_size_bits
                && cache.pixels_per_point_bits == pixels_per_point_bits
            {
                return (cache.cell_w, cache.cell_h);
            }
        }

        let ppp = ctx.pixels_per_point();
        let (cell_w, cell_h) = ctx.fonts(|fonts| {
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
        });
        tab.cell_metrics_cache = Some(CellMetricsCache {
            font_size_bits,
            pixels_per_point_bits,
            cell_w,
            cell_h,
        });
        (cell_w, cell_h)
    }

    fn terminal_galley(
        ui: &egui::Ui,
        tab: &mut SshTab,
        font_id: &FontId,
        term_theme: &TermTheme,
    ) -> Arc<egui::Galley> {
        let font_size_bits = font_id.size.to_bits();
        let pixels_per_point_bits = ui.ctx().pixels_per_point().to_bits();
        if let Some(cache) = tab.render_cache.as_ref() {
            if cache.font_size_bits == font_size_bits
                && cache.pixels_per_point_bits == pixels_per_point_bits
                && cache.term_theme == *term_theme
            {
                return cache.galley.clone();
            }
        }

        let job = Self::screen_to_layout_job(&tab.screen, font_id.clone(), term_theme);
        let galley = ui.fonts(|fonts| fonts.layout_job(job));
        tab.render_cache = Some(TerminalRenderCache {
            font_size_bits,
            pixels_per_point_bits,
            term_theme: *term_theme,
            galley: galley.clone(),
        });
        galley
    }

    fn vt_color_to_color32(c: crate::terminal_emulator::Color, default: Color32, term_theme: &TermTheme) -> Color32 {
        match c {
            crate::terminal_emulator::Color::Default => default,
            crate::terminal_emulator::Color::Rgb(r, g, b) => Color32::from_rgb(r, g, b),
            crate::terminal_emulator::Color::Idx(i) => xterm_256_color(i, &term_theme.palette16),
        }
    }

    fn cell_style(cell: &crate::terminal_emulator::Cell, term_theme: &TermTheme) -> TermStyle {
        let mut fg = Self::vt_color_to_color32(cell.fgcolor(), term_theme.fg, term_theme);
        let mut bg = Self::vt_color_to_color32(cell.bgcolor(), term_theme.bg, term_theme);

        // Common terminal behavior: bold maps to bright variants for the first 8 colors.
        if cell.bold() {
            if let crate::terminal_emulator::Color::Idx(i) = cell.fgcolor() {
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
        screen: &crate::terminal_emulator::Screen,
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
            let _ = tx.send(WorkerMessage::Input {
                client_id: tab.id,
                data: bytes,
            });
        }
    }

    fn set_scrollback(tab: &mut SshTab, rows: usize) {
        let target = rows.min(tab.scrollback_max);
        tab.screen.set_scrollback(target);
        tab.invalidate_terminal_render_cache();
        tab.pending_scrollback = Some(target);
    }

    fn mouse_wheel_delta_in_terminal_cells(
        delta: Vec2,
        unit: egui::MouseWheelUnit,
        cell_w: f32,
        cell_h: f32,
        page_cols: f32,
        page_rows: f32,
    ) -> Vec2 {
        match unit {
            egui::MouseWheelUnit::Point => egui::vec2(
                delta.x / cell_w.max(1.0),
                delta.y / cell_h.max(1.0),
            ),
            egui::MouseWheelUnit::Line => delta,
            egui::MouseWheelUnit::Page => egui::vec2(
                delta.x * page_cols.max(1.0),
                delta.y * page_rows.max(1.0),
            ),
        }
    }

    fn accumulated_mouse_wheel_delta_in_terminal_cells(
        events: &[egui::Event],
        cell_w: f32,
        cell_h: f32,
        page_cols: f32,
        page_rows: f32,
    ) -> Vec2 {
        let mut delta = Vec2::ZERO;
        for event in events {
            if let egui::Event::MouseWheel {
                unit,
                delta: wheel_delta,
                ..
            } = event
            {
                delta += Self::mouse_wheel_delta_in_terminal_cells(
                    *wheel_delta,
                    *unit,
                    cell_w,
                    cell_h,
                    page_cols,
                    page_rows,
                );
            }
        }
        delta
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

    fn paste_from_clipboard(tab: &mut SshTab, clipboard: &mut Option<Clipboard>) {
        let Some(cb) = clipboard.as_mut() else {
            return;
        };
        if let Ok(s) = cb.get_text() {
            Self::send_paste_text(tab, &s);
        }
    }

    fn selected_text(tab: &SshTab) -> String {
        if let Some(abs_sel) = tab.abs_selection {
            Self::selection_text_abs(&tab.screen, tab.scrollback_max, abs_sel)
        } else if let Some(sel) = tab.selection {
            Self::selection_text(&tab.screen, sel)
        } else {
            String::new()
        }
    }

    fn clear_remote_mouse_state(tab: &mut SshTab) {
        tab.active_remote_mouse = None;
        tab.remote_hover_pos = None;
        tab.remote_scroll_accum = Vec2::ZERO;
    }

    fn select_all(tab: &mut SshTab) {
        let (rows, cols) = tab.screen.size();
        if rows == 0 || cols == 0 {
            return;
        }

        let last_row = rows.saturating_sub(1);
        let last_col = cols.saturating_sub(1);
        let last_abs_row = tab.scrollback_max as i64 + last_row as i64;

        tab.selection = Some(TermSelection {
            anchor: (0, 0),
            cursor: (last_row, last_col),
            dragging: false,
        });
        tab.abs_selection = Some(TermAbsSelection {
            anchor: (0, 0),
            cursor: (last_abs_row, last_col),
            dragging: false,
        });
        Self::clear_remote_mouse_state(tab);
    }

    fn show_terminal_context_menu(
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        clipboard: &mut Option<Clipboard>,
        tab: &mut SshTab,
    ) {
        let selected_text = Self::selected_text(tab);
        let can_copy = !selected_text.is_empty();
        let (rows, cols) = tab.screen.size();
        let can_select_all = rows > 0 && cols > 0;

        if ui.add_enabled(can_copy, egui::Button::new("Copy")).clicked() {
            Self::copy_selection_with_flash(ctx, clipboard, tab, selected_text);
            ui.close_menu();
        }

        if ui.add_enabled(tab.connected, egui::Button::new("Paste")).clicked() {
            Self::paste_from_clipboard(tab, clipboard);
            Self::clear_remote_mouse_state(tab);
            ui.close_menu();
        }

        ui.separator();

        if ui
            .add_enabled(can_select_all, egui::Button::new("Select All"))
            .clicked()
        {
            Self::select_all(tab);
            ui.close_menu();
        }
    }

    fn copy_text_to_clipboard(ctx: &egui::Context, clipboard: &mut Option<Clipboard>, text: String) {
        ctx.copy_text(text.clone());
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
        match key {
            A => Some(b'a' & 0x1f),
            B => Some(b'b' & 0x1f),
            C => Some(b'c' & 0x1f),
            D => Some(b'd' & 0x1f),
            E => Some(b'e' & 0x1f),
            F => Some(b'f' & 0x1f),
            G => Some(b'g' & 0x1f),
            H => Some(b'h' & 0x1f),
            I => Some(b'i' & 0x1f),
            J => Some(b'j' & 0x1f),
            K => Some(b'k' & 0x1f),
            L => Some(b'l' & 0x1f),
            M => Some(b'm' & 0x1f),
            N => Some(b'n' & 0x1f),
            O => Some(b'o' & 0x1f),
            P => Some(b'p' & 0x1f),
            Q => Some(b'q' & 0x1f),
            R => Some(b'r' & 0x1f),
            S => Some(b's' & 0x1f),
            T => Some(b't' & 0x1f),
            U => Some(b'u' & 0x1f),
            V => Some(b'v' & 0x1f),
            W => Some(b'w' & 0x1f),
            X => Some(b'x' & 0x1f),
            Y => Some(b'y' & 0x1f),
            Z => Some(b'z' & 0x1f),
            OpenBracket => Some(0x1b),
            Backslash => Some(0x1c),
            CloseBracket => Some(0x1d),
            Num6 => Some(0x1e),
            Slash => Some(0x1f),
            Space => Some(0x00),
            _ => None,
        }
    }

    fn xterm_modifier_param(mods: egui::Modifiers) -> Option<u8> {
        let mut value = 1u8;
        if mods.shift {
            value = value.saturating_add(1);
        }
        if mods.alt {
            value = value.saturating_add(2);
        }
        if mods.ctrl {
            value = value.saturating_add(4);
        }
        if value == 1 {
            None
        } else {
            Some(value)
        }
    }

    fn with_escape_prefix(bytes: Vec<u8>) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len().saturating_add(1));
        out.push(0x1b);
        out.extend_from_slice(&bytes);
        out
    }

    fn csi_final(final_byte: u8) -> Vec<u8> {
        vec![0x1b, b'[', final_byte]
    }

    fn ss3_final(final_byte: u8) -> Vec<u8> {
        vec![0x1b, b'O', final_byte]
    }

    fn csi_with_modifier(final_byte: u8, modifier: u8) -> Vec<u8> {
        format!("\x1b[1;{}{final}", modifier, final = final_byte as char).into_bytes()
    }

    fn csi_tilde(code: u8) -> Vec<u8> {
        format!("\x1b[{code}~").into_bytes()
    }

    fn csi_tilde_with_modifier(code: u8, modifier: u8) -> Vec<u8> {
        format!("\x1b[{code};{modifier}~").into_bytes()
    }

    fn function_key_bytes(key: egui::Key, modifier: Option<u8>) -> Option<Vec<u8>> {
        use egui::Key::*;

        match key {
            F1 => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'P', m),
                None => Self::ss3_final(b'P'),
            }),
            F2 => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'Q', m),
                None => Self::ss3_final(b'Q'),
            }),
            F3 => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'R', m),
                None => Self::ss3_final(b'R'),
            }),
            F4 => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'S', m),
                None => Self::ss3_final(b'S'),
            }),
            F5 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(15, m),
                None => Self::csi_tilde(15),
            }),
            F6 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(17, m),
                None => Self::csi_tilde(17),
            }),
            F7 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(18, m),
                None => Self::csi_tilde(18),
            }),
            F8 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(19, m),
                None => Self::csi_tilde(19),
            }),
            F9 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(20, m),
                None => Self::csi_tilde(20),
            }),
            F10 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(21, m),
                None => Self::csi_tilde(21),
            }),
            F11 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(23, m),
                None => Self::csi_tilde(23),
            }),
            F12 => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(24, m),
                None => Self::csi_tilde(24),
            }),
            _ => None,
        }
    }

    fn key_event_bytes(
        key: egui::Key,
        mods: egui::Modifiers,
        app_cursor: bool,
    ) -> Option<Vec<u8>> {
        // Terminal-style copy shortcut that doesn't collide with SIGINT.
        if mods.ctrl && mods.shift && key == egui::Key::C {
            return None;
        }

        // Keep AltGr-style text entry out of the control-byte path.
        if mods.ctrl && !mods.alt {
            if let Some(b) = Self::key_to_ctrl_byte(key) {
                return Some(vec![b]);
            }
        }

        let modifier = Self::xterm_modifier_param(mods);
        let mut bytes = match key {
            egui::Key::Enter => Some(vec![b'\r']),
            egui::Key::Tab => {
                if mods.shift {
                    Some(match modifier {
                        Some(m) => format!("\x1b[1;{m}Z").into_bytes(),
                        None => b"\x1b[Z".to_vec(),
                    })
                } else {
                    Some(vec![b'\t'])
                }
            }
            egui::Key::Backspace => Some(vec![0x7f]),
            egui::Key::Escape => Some(vec![0x1b]),
            egui::Key::ArrowUp => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'A', m),
                None if app_cursor => Self::ss3_final(b'A'),
                None => Self::csi_final(b'A'),
            }),
            egui::Key::ArrowDown => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'B', m),
                None if app_cursor => Self::ss3_final(b'B'),
                None => Self::csi_final(b'B'),
            }),
            egui::Key::ArrowRight => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'C', m),
                None if app_cursor => Self::ss3_final(b'C'),
                None => Self::csi_final(b'C'),
            }),
            egui::Key::ArrowLeft => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'D', m),
                None if app_cursor => Self::ss3_final(b'D'),
                None => Self::csi_final(b'D'),
            }),
            egui::Key::Home => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'H', m),
                None if app_cursor => Self::ss3_final(b'H'),
                None => Self::csi_final(b'H'),
            }),
            egui::Key::End => Some(match modifier {
                Some(m) => Self::csi_with_modifier(b'F', m),
                None if app_cursor => Self::ss3_final(b'F'),
                None => Self::csi_final(b'F'),
            }),
            egui::Key::PageUp => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(5, m),
                None => Self::csi_tilde(5),
            }),
            egui::Key::PageDown => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(6, m),
                None => Self::csi_tilde(6),
            }),
            egui::Key::Insert => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(2, m),
                None => Self::csi_tilde(2),
            }),
            egui::Key::Delete => Some(match modifier {
                Some(m) => Self::csi_tilde_with_modifier(3, m),
                None => Self::csi_tilde(3),
            }),
            _ => Self::function_key_bytes(key, modifier),
        }?;

        if mods.alt
            && (matches!(
                key,
                egui::Key::Enter | egui::Key::Backspace | egui::Key::Escape
            ) || (key == egui::Key::Tab && !mods.shift))
        {
            bytes = Self::with_escape_prefix(bytes);
        }

        Some(bytes)
    }

    fn text_event_bytes(text: &str, mods: egui::Modifiers) -> Option<Vec<u8>> {
        if text.is_empty() {
            return None;
        }
        if mods.command || (mods.ctrl && !mods.alt) {
            return None;
        }

        let mut bytes = text.as_bytes().to_vec();
        if mods.alt && !mods.ctrl {
            bytes = Self::with_escape_prefix(bytes);
        }
        Some(bytes)
    }

    fn send_key(tab: &mut SshTab, key: egui::Key, mods: egui::Modifiers) {
        if let Some(bytes) = Self::key_event_bytes(key, mods, tab.screen.application_cursor()) {
            Self::send_bytes(tab, bytes);
        }
    }

    fn mouse_modifier_bits(mods: egui::Modifiers) -> u8 {
        let mut bits = 0u8;
        if mods.shift {
            bits = bits.saturating_add(4);
        }
        if mods.alt {
            bits = bits.saturating_add(8);
        }
        if mods.ctrl {
            bits = bits.saturating_add(16);
        }
        bits
    }

    fn mouse_button_code(button: egui::PointerButton) -> Option<u8> {
        match button {
            egui::PointerButton::Primary => Some(0),
            egui::PointerButton::Middle => Some(1),
            egui::PointerButton::Secondary => Some(2),
            _ => None,
        }
    }

    fn append_encoded_mouse_coord(coord_1: u16, utf8: bool, out: &mut Vec<u8>) -> bool {
        let value = u32::from(coord_1).saturating_add(32);
        if utf8 {
            if value >= 0x800 {
                return false;
            }
            let mut buf = [0; 2];
            let Some(ch) = char::from_u32(value) else {
                return false;
            };
            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            true
        } else if value < 0x100 {
            out.push(value as u8);
            true
        } else {
            false
        }
    }

    fn mouse_report_position(
        pos: Pos2,
        lookup: &CellLookup<'_>,
        screen: &crate::terminal_emulator::Screen,
        encoding: crate::terminal_emulator::MouseProtocolEncoding,
        pixels_per_point: f32,
    ) -> Option<RemoteMouseReportPosition> {
        match encoding {
            crate::terminal_emulator::MouseProtocolEncoding::SgrPixels => {
                let rel_x = ((pos.x - lookup.origin.x).max(0.0) * pixels_per_point).floor();
                let rel_y = ((pos.y - lookup.origin.y).max(0.0) * pixels_per_point).floor();
                let x_1 = (rel_x.max(0.0) as u32)
                    .saturating_add(1)
                    .min(u16::MAX as u32) as u16;
                let y_1 = (rel_y.max(0.0) as u32)
                    .saturating_add(1)
                    .min(u16::MAX as u32) as u16;
                Some(RemoteMouseReportPosition { x_1, y_1 })
            }
            _ => Self::pos_to_cell(pos, lookup, screen).map(|(row, col)| RemoteMouseReportPosition {
                x_1: col.saturating_add(1),
                y_1: row.saturating_add(1),
            }),
        }
    }

    fn mouse_event_bytes(
        encoding: crate::terminal_emulator::MouseProtocolEncoding,
        action: RemoteMouseAction,
        pos: RemoteMouseReportPosition,
        mods: egui::Modifiers,
    ) -> Option<Vec<u8>> {
        let modifier_bits = Self::mouse_modifier_bits(mods);
        let (cb, sgr_suffix) = match action {
            RemoteMouseAction::Press(button) => {
                (Self::mouse_button_code(button)?.saturating_add(modifier_bits), b'M')
            }
            RemoteMouseAction::Release(button) => match encoding {
                crate::terminal_emulator::MouseProtocolEncoding::Sgr
                | crate::terminal_emulator::MouseProtocolEncoding::SgrPixels => {
                    (Self::mouse_button_code(button)?.saturating_add(modifier_bits), b'm')
                }
                _ => (3, b'M'),
            },
            RemoteMouseAction::Move(button) => {
                let base = match button {
                    Some(button) => Self::mouse_button_code(button)?,
                    None => 3,
                };
                (32u8.saturating_add(base).saturating_add(modifier_bits), b'M')
            }
            RemoteMouseAction::WheelUp => (64u8.saturating_add(modifier_bits), b'M'),
            RemoteMouseAction::WheelDown => (65u8.saturating_add(modifier_bits), b'M'),
            RemoteMouseAction::WheelLeft => (66u8.saturating_add(modifier_bits), b'M'),
            RemoteMouseAction::WheelRight => (67u8.saturating_add(modifier_bits), b'M'),
        };

        match encoding {
            crate::terminal_emulator::MouseProtocolEncoding::Sgr
            | crate::terminal_emulator::MouseProtocolEncoding::SgrPixels => {
                let s = format!("\x1b[<{};{};{}{}", cb, pos.x_1, pos.y_1, sgr_suffix as char);
                Some(s.into_bytes())
            }
            crate::terminal_emulator::MouseProtocolEncoding::Urxvt => {
                let b = 32u16.saturating_add(cb as u16);
                let s = format!("\x1b[{b};{};{}M", pos.x_1, pos.y_1);
                Some(s.into_bytes())
            }
            crate::terminal_emulator::MouseProtocolEncoding::Utf8
            | crate::terminal_emulator::MouseProtocolEncoding::Default => {
                let mut out = vec![0x1b, b'[', b'M', 32u8.saturating_add(cb)];
                let utf8 = matches!(
                    encoding,
                    crate::terminal_emulator::MouseProtocolEncoding::Utf8
                );
                if !Self::append_encoded_mouse_coord(pos.x_1, utf8, &mut out)
                    || !Self::append_encoded_mouse_coord(pos.y_1, utf8, &mut out)
                {
                    return None;
                }
                Some(out)
            }
        }
    }

    fn send_remote_mouse_action(
        tab: &mut SshTab,
        action: RemoteMouseAction,
        pos: RemoteMouseReportPosition,
        mods: egui::Modifiers,
    ) {
        let encoding = tab.screen.mouse_protocol_encoding();
        if let Some(bytes) = Self::mouse_event_bytes(encoding, action, pos, mods) {
            Self::send_bytes(tab, bytes);
        }
    }

    fn handle_terminal_io(io: TerminalIoContext<'_>, tab: &mut SshTab) {
        let TerminalIoContext {
            ctx,
            clipboard,
            ui,
            term_rect,
            origin,
            cell_w,
            cell_h,
            galley,
            response,
        } = io;
        let events = ui.input(|i| i.events.clone());
        let global_mods = ui.input(|i| i.modifiers);
        let has_copy_event = events.iter().any(|e| matches!(e, egui::Event::Copy));
        let has_cut_event = events.iter().any(|e| matches!(e, egui::Event::Cut));
        let has_paste_event = events.iter().any(|e| matches!(e, egui::Event::Paste(_)));
        let has_text_event = events
            .iter()
            .any(|e| matches!(e, egui::Event::Text(t) if !t.is_empty()));

        let (screen_rows, screen_cols) = tab.screen.size();
        let cell_lookup = CellLookup {
            origin,
            cell_w,
            cell_h,
            galley,
            rows: screen_rows,
            cols: screen_cols,
        };
        let remote_mouse_enabled =
            tab.connected && tab.screen.mouse_protocol_mode() != crate::terminal_emulator::MouseProtocolMode::None;
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
        let primary_pressed = ui.input(|i| i.pointer.button_pressed(egui::PointerButton::Primary));
        let primary_down = ui.input(|i| i.pointer.button_down(egui::PointerButton::Primary));
        let primary_released =
            ui.input(|i| i.pointer.button_released(egui::PointerButton::Primary));
        let middle_pressed = ui.input(|i| i.pointer.button_pressed(egui::PointerButton::Middle));
        let middle_down = ui.input(|i| i.pointer.button_down(egui::PointerButton::Middle));
        let middle_released =
            ui.input(|i| i.pointer.button_released(egui::PointerButton::Middle));
        let secondary_pressed =
            ui.input(|i| i.pointer.button_pressed(egui::PointerButton::Secondary));
        let secondary_down = ui.input(|i| i.pointer.button_down(egui::PointerButton::Secondary));
        let secondary_released =
            ui.input(|i| i.pointer.button_released(egui::PointerButton::Secondary));
        let hovering_term = pointer_pos.map(|pos| term_rect.contains(pos)).unwrap_or(false) || response.hovered();
        let context_menu_open = ctx.memory(|mem| mem.any_popup_open());
        let pixels_per_point = ctx.pixels_per_point().max(1.0);
        let mouse_mode = tab.screen.mouse_protocol_mode();
        let mouse_encoding = tab.screen.mouse_protocol_encoding();
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

        if !allow_remote_mouse && tab.active_remote_mouse.is_none() {
            tab.remote_hover_pos = None;
            tab.remote_scroll_accum = Vec2::ZERO;
        }

        if !allow_remote_mouse && response.middle_clicked() && tab.connected {
            Self::paste_from_clipboard(tab, clipboard);
            response.request_focus();
            Self::clear_remote_mouse_state(tab);
        }

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

        let wheel_delta = Self::accumulated_mouse_wheel_delta_in_terminal_cells(
            &events,
            cell_w,
            cell_h,
            screen_cols as f32,
            screen_rows as f32,
        );

        // Local scrollback or remote wheel reporting.
        if hovering_term && tab.connected {
            if allow_remote_mouse {
                tab.remote_scroll_accum += wheel_delta;

                let x_steps = (tab.remote_scroll_accum.x.trunc() as i64).clamp(-32, 32) as i32;
                if x_steps != 0 {
                    tab.remote_scroll_accum.x -= x_steps as f32;
                    let action = if x_steps > 0 {
                        RemoteMouseAction::WheelRight
                    } else {
                        RemoteMouseAction::WheelLeft
                    };
                    if let Some(report_pos) = pointer_pos
                        .filter(|pos| term_rect.contains(*pos))
                        .map(clamp_pos_to_grid)
                        .and_then(|pos| {
                            Self::mouse_report_position(
                                pos,
                                &cell_lookup,
                                &tab.screen,
                                mouse_encoding,
                                pixels_per_point,
                            )
                        })
                    {
                        for _ in 0..x_steps.unsigned_abs() {
                            Self::send_remote_mouse_action(tab, action, report_pos, global_mods);
                        }
                    }
                }

                let y_steps = (tab.remote_scroll_accum.y.trunc() as i64).clamp(-32, 32) as i32;
                if y_steps != 0 {
                    tab.remote_scroll_accum.y -= y_steps as f32;
                    let action = if y_steps > 0 {
                        RemoteMouseAction::WheelUp
                    } else {
                        RemoteMouseAction::WheelDown
                    };
                    if let Some(report_pos) = pointer_pos
                        .filter(|pos| term_rect.contains(*pos))
                        .map(clamp_pos_to_grid)
                        .and_then(|pos| {
                            Self::mouse_report_position(
                                pos,
                                &cell_lookup,
                                &tab.screen,
                                mouse_encoding,
                                pixels_per_point,
                            )
                        })
                    {
                        for _ in 0..y_steps.unsigned_abs() {
                            Self::send_remote_mouse_action(tab, action, report_pos, global_mods);
                        }
                    }
                }
            } else {
                // Prefer per-frame wheel events. `smooth_scroll_delta` introduces inertial drift and
                // can keep scrolling on repaint ticks even after wheel input has stopped.
                let mut dy = wheel_delta.y;
                if dy.abs() <= 0.001 {
                    let raw_delta = ui.input(|i| i.raw_scroll_delta);
                    dy = Self::mouse_wheel_delta_in_terminal_cells(
                        raw_delta,
                        egui::MouseWheelUnit::Point,
                        cell_w,
                        cell_h,
                        screen_cols as f32,
                        screen_rows as f32,
                    )
                    .y;
                }

                if dy.abs() > 0.001 {
                    // Accumulate into rows and apply integer deltas.
                    tab.scroll_wheel_accum += dy;
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
        }

        if primary_pressed && !hovering_scrollbar && !context_menu_open {
            if let Some(pos) = pointer_pos {
                if term_rect.contains(pos) {
                    let pos = clamp_pos_to_grid(pos);

                    if local_select_enabled {
                        Self::clear_remote_mouse_state(tab);
                        if let Some((row, col)) = Self::pos_to_cell(pos, &cell_lookup, &tab.screen)
                        {
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
                    }
                } else {
                    // Clicking outside clears any local selection.
                    tab.selection = None;
                    tab.abs_selection = None;
                }
            }
        }

        if primary_down && !tab.scrollbar_dragging {
            if let Some(raw_pos) = pointer_pos {
                let pos = clamp_pos_to_grid(raw_pos);
                if let Some(sel) = tab.selection.as_mut() {
                    if sel.dragging {
                        if let Some((row, col)) = Self::pos_to_cell(pos, &cell_lookup, &tab.screen)
                        {
                            sel.cursor = (row, col);
                            let abs_cursor = Self::visible_cell_to_abs(tab, row, col);
                            if let Some(abs_sel) = tab.abs_selection.as_mut() {
                                abs_sel.cursor = abs_cursor;
                                abs_sel.dragging = true;
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
            }
        }

        if allow_remote_mouse && !hovering_scrollbar && !context_menu_open {
            for (button, pressed) in [
                (egui::PointerButton::Primary, primary_pressed),
                (egui::PointerButton::Middle, middle_pressed),
                (egui::PointerButton::Secondary, secondary_pressed),
            ] {
                if !pressed {
                    continue;
                }
                let Some(pos) = pointer_pos.filter(|pos| term_rect.contains(*pos)) else {
                    continue;
                };
                let pos = clamp_pos_to_grid(pos);
                let Some(report_pos) = Self::mouse_report_position(
                    pos,
                    &cell_lookup,
                    &tab.screen,
                    mouse_encoding,
                    pixels_per_point,
                ) else {
                    continue;
                };

                tab.selection = None;
                tab.abs_selection = None;
                tab.active_remote_mouse = Some(ActiveRemoteMouse {
                    button,
                    pos: report_pos,
                });
                tab.remote_hover_pos = None;
                Self::send_remote_mouse_action(tab, RemoteMouseAction::Press(button), report_pos, global_mods);
                response.request_focus();
            }
        }

        if let Some(active) = tab.active_remote_mouse {
            let still_down = match active.button {
                egui::PointerButton::Primary => primary_down,
                egui::PointerButton::Middle => middle_down,
                egui::PointerButton::Secondary => secondary_down,
                _ => false,
            };
            if still_down
                && !tab.scrollbar_dragging
                && matches!(
                    mouse_mode,
                    crate::terminal_emulator::MouseProtocolMode::Drag
                        | crate::terminal_emulator::MouseProtocolMode::Move
                )
            {
                if let Some(raw_pos) = pointer_pos {
                    let pos = clamp_pos_to_grid(raw_pos);
                    if let Some(report_pos) = Self::mouse_report_position(
                        pos,
                        &cell_lookup,
                        &tab.screen,
                        mouse_encoding,
                        pixels_per_point,
                    ) {
                        if report_pos != active.pos {
                            Self::send_remote_mouse_action(
                                tab,
                                RemoteMouseAction::Move(Some(active.button)),
                                report_pos,
                                global_mods,
                            );
                            tab.active_remote_mouse = Some(ActiveRemoteMouse {
                                button: active.button,
                                pos: report_pos,
                            });
                        }
                    }
                }
            }
        } else if allow_remote_mouse
            && mouse_mode == crate::terminal_emulator::MouseProtocolMode::Move
            && hovering_term
            && !context_menu_open
        {
            if let Some(raw_pos) = pointer_pos {
                let pos = clamp_pos_to_grid(raw_pos);
                if let Some(report_pos) = Self::mouse_report_position(
                    pos,
                    &cell_lookup,
                    &tab.screen,
                    mouse_encoding,
                    pixels_per_point,
                ) {
                    if tab.remote_hover_pos != Some(report_pos) {
                        Self::send_remote_mouse_action(
                            tab,
                            RemoteMouseAction::Move(None),
                            report_pos,
                            global_mods,
                        );
                        tab.remote_hover_pos = Some(report_pos);
                    }
                }
            }
        } else if !hovering_term || !allow_remote_mouse {
            tab.remote_hover_pos = None;
        }

        if let Some(active) = tab.active_remote_mouse {
            let released = match active.button {
                egui::PointerButton::Primary => primary_released,
                egui::PointerButton::Middle => middle_released,
                egui::PointerButton::Secondary => secondary_released,
                _ => false,
            };
            if released {
                let report_pos = pointer_pos
                    .map(clamp_pos_to_grid)
                    .and_then(|pos| {
                        Self::mouse_report_position(
                            pos,
                            &cell_lookup,
                            &tab.screen,
                            mouse_encoding,
                            pixels_per_point,
                        )
                    })
                    .unwrap_or(active.pos);
                Self::send_remote_mouse_action(
                    tab,
                    RemoteMouseAction::Release(active.button),
                    report_pos,
                    global_mods,
                );
                tab.active_remote_mouse = None;
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
                    egui::Event::Cut => {
                        // Some platforms surface Ctrl+X as a high-level cut command instead of a
                        // raw key event. In a terminal, that should still reach the remote app.
                        Self::send_bytes(tab, vec![0x18]);
                    }
                    egui::Event::Text(t) => {
                        if let Some(bytes) = Self::text_event_bytes(t, global_mods) {
                            Self::send_bytes(tab, bytes);
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
                        if modifiers.ctrl && *key == egui::Key::V {
                            if has_paste_event || has_text_event {
                                continue;
                            }
                            Self::paste_from_clipboard(tab, clipboard);
                            continue;
                        }

                        if modifiers.ctrl && !modifiers.shift && *key == egui::Key::X && has_cut_event
                        {
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
        lookup: &CellLookup<'_>,
        screen: &crate::terminal_emulator::Screen,
    ) -> Option<(u16, u16)> {
        if let Some(g) = lookup.galley {
            if let Some((r, c)) =
                Self::pos_to_cell_galley(pos, lookup.origin, screen, g, lookup.rows, lookup.cols)
            {
                return Some((r, c));
            }
        }

        if lookup.rows == 0 || lookup.cols == 0 {
            return None;
        }

        let col = ((pos.x - lookup.origin.x) / lookup.cell_w).floor() as i32;
        let row = ((pos.y - lookup.origin.y) / lookup.cell_h).floor() as i32;
        if col < 0 || row < 0 {
            return None;
        }

        let col = (col as u16).min(lookup.cols.saturating_sub(1));
        let row = (row as u16).min(lookup.rows.saturating_sub(1));
        Some((row, col))
    }

    fn pos_to_cell_galley(
        pos: Pos2,
        origin: Pos2,
        screen: &crate::terminal_emulator::Screen,
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

    fn row_col_to_char_index_map(screen: &crate::terminal_emulator::Screen, row: u16) -> Vec<usize> {
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

    fn selection_text(screen: &crate::terminal_emulator::Screen, sel: TermSelection) -> String {
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

    fn row_segment_text(screen: &crate::terminal_emulator::Screen, row: u16, start_col: u16, end_col: u16) -> String {
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
        screen: &crate::terminal_emulator::Screen,
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
        term_theme: &TermTheme,
        sel: TermSelection,
    ) {
        // Flash brighter on copy, then selection disappears (handled in the AppState update loop).
        let with_alpha =
            |c: Color32, a: u8| Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), a);
        let selection_bg = if tab.copy_flash_until.is_some() {
            with_alpha(adjust_color(term_theme.selection_bg, 0.18), 210)
        } else {
            with_alpha(term_theme.selection_bg, 120)
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
            painter.rect_stroke(
                rect,
                0.0,
                Stroke::new(1.0, with_alpha(term_theme.selection_fg, 70)),
            );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctrl_x_maps_to_terminal_control_byte() {
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::X), Some(0x18));
    }

    #[test]
    fn ctrl_punctuation_maps_to_common_terminal_control_bytes() {
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::OpenBracket), Some(0x1b));
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::Backslash), Some(0x1c));
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::CloseBracket), Some(0x1d));
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::Num6), Some(0x1e));
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::Slash), Some(0x1f));
        assert_eq!(AppState::key_to_ctrl_byte(egui::Key::Space), Some(0x00));
    }

    #[test]
    fn function_keys_encode_to_xterm_sequences() {
        assert_eq!(
            AppState::key_event_bytes(egui::Key::F1, egui::Modifiers::NONE, false),
            Some(b"\x1bOP".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::F5, egui::Modifiers::NONE, false),
            Some(b"\x1b[15~".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::F12, egui::Modifiers::SHIFT, false),
            Some(b"\x1b[24;2~".to_vec())
        );
    }

    #[test]
    fn mouse_events_encode_sgr_releases_and_urxvt_wheel() {
        let pos = RemoteMouseReportPosition { x_1: 7, y_1: 3 };
        assert_eq!(
            AppState::mouse_event_bytes(
                crate::terminal_emulator::MouseProtocolEncoding::Sgr,
                RemoteMouseAction::Release(egui::PointerButton::Secondary),
                pos,
                egui::Modifiers::NONE,
            ),
            Some(b"\x1b[<2;7;3m".to_vec())
        );

        assert_eq!(
            AppState::mouse_event_bytes(
                crate::terminal_emulator::MouseProtocolEncoding::Urxvt,
                RemoteMouseAction::WheelUp,
                RemoteMouseReportPosition { x_1: 10, y_1: 4 },
                egui::Modifiers::CTRL,
            ),
            Some(b"\x1b[112;10;4M".to_vec())
        );
    }

    #[test]
    fn wheel_line_units_map_directly_to_terminal_rows() {
        let delta = AppState::mouse_wheel_delta_in_terminal_cells(
            egui::vec2(0.0, 3.0),
            egui::MouseWheelUnit::Line,
            9.0,
            18.0,
            80.0,
            24.0,
        );
        assert_eq!(delta, egui::vec2(0.0, 3.0));
    }

    #[test]
    fn wheel_point_and_page_units_convert_to_terminal_cells() {
        let point_delta = AppState::mouse_wheel_delta_in_terminal_cells(
            egui::vec2(18.0, 36.0),
            egui::MouseWheelUnit::Point,
            9.0,
            18.0,
            80.0,
            24.0,
        );
        assert_eq!(point_delta, egui::vec2(2.0, 2.0));

        let page_delta = AppState::mouse_wheel_delta_in_terminal_cells(
            egui::vec2(1.0, -1.0),
            egui::MouseWheelUnit::Page,
            9.0,
            18.0,
            80.0,
            24.0,
        );
        assert_eq!(page_delta, egui::vec2(80.0, -24.0));
    }

    #[test]
    fn arrows_and_navigation_keys_use_modifier_parameters() {
        assert_eq!(
            AppState::key_event_bytes(egui::Key::ArrowUp, egui::Modifiers::ALT, false),
            Some(b"\x1b[1;3A".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::ArrowLeft, egui::Modifiers::CTRL, false),
            Some(b"\x1b[1;5D".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::Home, egui::Modifiers::SHIFT, false),
            Some(b"\x1b[1;2H".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::PageDown, egui::Modifiers::ALT, false),
            Some(b"\x1b[6;3~".to_vec())
        );
    }

    #[test]
    fn shift_tab_encodes_backtab() {
        assert_eq!(
            AppState::key_event_bytes(egui::Key::Tab, egui::Modifiers::SHIFT, false),
            Some(b"\x1b[1;2Z".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(
                egui::Key::Tab,
                egui::Modifiers::SHIFT | egui::Modifiers::ALT,
                false,
            ),
            Some(b"\x1b[1;4Z".to_vec())
        );
    }

    #[test]
    fn alt_prefixed_text_and_keys_use_escape_prefix() {
        assert_eq!(
            AppState::text_event_bytes("b", egui::Modifiers::ALT),
            Some(b"\x1bb".to_vec())
        );
        assert_eq!(
            AppState::text_event_bytes("B", egui::Modifiers::ALT | egui::Modifiers::SHIFT),
            Some(b"\x1bB".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::Backspace, egui::Modifiers::ALT, false),
            Some(vec![0x1b, 0x7f])
        );
    }

    #[test]
    fn ctrl_alt_text_is_left_for_altgr_style_input() {
        assert_eq!(
            AppState::text_event_bytes("@", egui::Modifiers::CTRL | egui::Modifiers::ALT),
            Some(b"@".to_vec())
        );
        assert_eq!(
            AppState::key_event_bytes(egui::Key::Q, egui::Modifiers::CTRL | egui::Modifiers::ALT, false),
            None
        );
    }
}
