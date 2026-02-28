use std::io::{self, Write};
use std::sync::Arc;

use wezterm_term::color::{ColorAttribute, ColorPalette};
use wezterm_term::{
    CellAttributes as WezCellAttributes, Intensity, Line, Terminal, TerminalConfiguration,
    TerminalSize, Underline,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Color {
    Default,
    Rgb(u8, u8, u8),
    Idx(u8),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MouseProtocolEncoding {
    Default,
    Utf8,
    Sgr,
    Urxvt,
    SgrPixels,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MouseProtocolMode {
    None,
    Press,
    Drag,
    Move,
}

#[derive(Clone, Debug)]
pub struct Cell {
    fg: Color,
    bg: Color,
    bold: bool,
    dim: bool,
    inverse: bool,
    italic: bool,
    underline: bool,
    text: String,
    has_contents: bool,
    is_wide_continuation: bool,
}

impl Cell {
    fn blank() -> Self {
        Self {
            fg: Color::Default,
            bg: Color::Default,
            bold: false,
            dim: false,
            inverse: false,
            italic: false,
            underline: false,
            text: String::new(),
            has_contents: false,
            is_wide_continuation: false,
        }
    }

    fn wide_continuation_from(base: &Self) -> Self {
        Self {
            fg: base.fg,
            bg: base.bg,
            bold: base.bold,
            dim: base.dim,
            inverse: base.inverse,
            italic: base.italic,
            underline: base.underline,
            text: String::new(),
            has_contents: false,
            is_wide_continuation: true,
        }
    }

    fn from_cell(text: &str, attrs: &WezCellAttributes) -> Self {
        let intensity = attrs.intensity();
        let is_blank = text == " " || text.is_empty();
        Self {
            fg: map_color(attrs.foreground()),
            bg: map_color(attrs.background()),
            bold: intensity == Intensity::Bold,
            dim: intensity == Intensity::Half,
            inverse: attrs.reverse(),
            italic: attrs.italic(),
            underline: attrs.underline() != Underline::None,
            text: if is_blank {
                String::new()
            } else {
                text.to_string()
            },
            has_contents: !is_blank,
            is_wide_continuation: false,
        }
    }

    pub fn fgcolor(&self) -> Color {
        self.fg
    }

    pub fn bgcolor(&self) -> Color {
        self.bg
    }

    pub fn bold(&self) -> bool {
        self.bold
    }

    pub fn dim(&self) -> bool {
        self.dim
    }

    pub fn inverse(&self) -> bool {
        self.inverse
    }

    pub fn italic(&self) -> bool {
        self.italic
    }

    pub fn underline(&self) -> bool {
        self.underline
    }

    pub fn is_wide_continuation(&self) -> bool {
        self.is_wide_continuation
    }

    pub fn has_contents(&self) -> bool {
        self.has_contents
    }

    pub fn contents(&self) -> String {
        self.text.clone()
    }
}

#[derive(Clone, Debug)]
pub struct Screen {
    rows: u16,
    cols: u16,
    scrollback: usize,
    scrollback_max: usize,
    lines: Vec<Line>,
    visible_cells: Vec<Cell>,
    title: String,
    bracketed_paste: bool,
    application_cursor: bool,
    mouse_mode: MouseProtocolMode,
    mouse_encoding: MouseProtocolEncoding,
    cursor_row: u16,
    cursor_col: u16,
    hide_cursor: bool,
}

impl Screen {
    fn from_snapshot(snapshot: ScreenSnapshot) -> Self {
        let mut screen = Self {
            rows: snapshot.rows,
            cols: snapshot.cols,
            scrollback: snapshot.scrollback,
            scrollback_max: snapshot.scrollback_max,
            lines: snapshot.lines,
            visible_cells: Vec::new(),
            title: snapshot.title,
            bracketed_paste: snapshot.bracketed_paste,
            application_cursor: snapshot.application_cursor,
            mouse_mode: snapshot.mouse_mode,
            mouse_encoding: snapshot.mouse_encoding,
            cursor_row: snapshot.cursor_row,
            cursor_col: snapshot.cursor_col,
            hide_cursor: snapshot.hide_cursor,
        };
        screen.rebuild_visible_cells();
        screen
    }

    fn rebuild_visible_cells(&mut self) {
        let rows = self.rows as usize;
        let cols = self.cols as usize;
        self.visible_cells = vec![Cell::blank(); rows.saturating_mul(cols)];

        let top_abs = self.scrollback_max.saturating_sub(self.scrollback);
        for row in 0..rows {
            let Some(line) = self.lines.get(top_abs.saturating_add(row)) else {
                continue;
            };
            let row_slice_start = row.saturating_mul(cols);
            let row_slice_end = row_slice_start.saturating_add(cols);
            let row_slice = &mut self.visible_cells[row_slice_start..row_slice_end];

            for cell_ref in line.visible_cells() {
                let col = cell_ref.cell_index();
                if col >= cols {
                    continue;
                }
                let base = Cell::from_cell(cell_ref.str(), cell_ref.attrs());
                row_slice[col] = base.clone();

                let width = cell_ref.width().max(1);
                for off in 1..width {
                    let c = col + off;
                    if c >= cols {
                        break;
                    }
                    row_slice[c] = Cell::wide_continuation_from(&base);
                }
            }
        }
    }

    pub fn size(&self) -> (u16, u16) {
        (self.rows, self.cols)
    }

    pub fn cell(&self, row: u16, col: u16) -> Option<&Cell> {
        if row >= self.rows || col >= self.cols {
            return None;
        }
        let idx = row as usize * self.cols as usize + col as usize;
        self.visible_cells.get(idx)
    }

    pub fn set_scrollback(&mut self, rows: usize) {
        self.scrollback = rows.min(self.scrollback_max);
        self.rebuild_visible_cells();
    }

    pub fn scrollback(&self) -> usize {
        self.scrollback
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn bracketed_paste(&self) -> bool {
        self.bracketed_paste
    }

    pub fn application_cursor(&self) -> bool {
        self.application_cursor
    }

    pub fn mouse_protocol_mode(&self) -> MouseProtocolMode {
        self.mouse_mode
    }

    pub fn mouse_protocol_encoding(&self) -> MouseProtocolEncoding {
        self.mouse_encoding
    }

    pub fn hide_cursor(&self) -> bool {
        self.hide_cursor
    }

    pub fn cursor_position(&self) -> (u16, u16) {
        (self.cursor_row, self.cursor_col)
    }

    pub fn contents(&self) -> String {
        let mut out = String::new();
        for row in 0..self.rows {
            let mut line = String::new();
            for col in 0..self.cols {
                if let Some(cell) = self.cell(row, col) {
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
            out.push_str(line.trim_end_matches(' '));
            if row + 1 < self.rows {
                out.push('\n');
            }
        }
        out
    }
}

pub struct Parser {
    terminal: Terminal,
    screen: Screen,
    scrollback: usize,
    mode_state: ModeState,
    seq_filter: SeqFilter,
    disable_alt_screen: bool,
}

impl Parser {
    pub fn new(rows: u16, cols: u16, scrollback_len: usize) -> Self {
        let rows = rows.max(1) as usize;
        let cols = cols.max(1) as usize;
        let scrollback_len = scrollback_len.min(200_000);
        let term_size = TerminalSize {
            rows,
            cols,
            pixel_width: cols.saturating_mul(8),
            pixel_height: rows.saturating_mul(16),
            dpi: 0,
        };
        let mut parser = Self {
            terminal: Terminal::new(
                term_size,
                Arc::new(ParserConfig {
                    scrollback: scrollback_len,
                }),
                "Rusty",
                env!("CARGO_PKG_VERSION"),
                Box::new(NullWriter),
            ),
            screen: Screen::from_snapshot(ScreenSnapshot::empty(rows as u16, cols as u16)),
            scrollback: 0,
            mode_state: ModeState::default(),
            seq_filter: SeqFilter::default(),
            // Preserve inline history while running TUIs by neutralizing alt-screen toggles.
            disable_alt_screen: true,
        };
        parser.refresh_screen();
        parser
    }

    pub fn process(&mut self, bytes: &[u8]) {
        let filtered = self
            .seq_filter
            .transform(bytes, &mut self.mode_state, self.disable_alt_screen);
        if !filtered.is_empty() {
            self.terminal.advance_bytes(filtered);
        }
        self.refresh_screen();
    }

    pub fn screen(&self) -> &Screen {
        &self.screen
    }

    pub fn set_size(&mut self, rows: u16, cols: u16) {
        let rows = rows.max(1) as usize;
        let cols = cols.max(1) as usize;
        let size = self.terminal.get_size();
        self.terminal.resize(TerminalSize {
            rows,
            cols,
            pixel_width: size.pixel_width,
            pixel_height: size.pixel_height,
            dpi: size.dpi,
        });
        self.refresh_screen();
    }

    pub fn set_scrollback(&mut self, rows: usize) {
        self.scrollback = rows;
        self.refresh_screen();
    }

    fn refresh_screen(&mut self) {
        let size = self.terminal.get_size();
        let rows = size.rows.max(1);
        let cols = size.cols.max(1);
        let term_screen = self.terminal.screen();
        let total_rows = term_screen.scrollback_rows();
        let scrollback_max = total_rows.saturating_sub(rows);
        self.scrollback = self.scrollback.min(scrollback_max);

        let cursor = self.terminal.cursor_pos();
        let cursor_row = (cursor.y.clamp(0, rows.saturating_sub(1) as i64)) as u16;
        let cursor_col = cursor.x.min(cols.saturating_sub(1)) as u16;
        let hide_cursor = format!("{:?}", cursor.visibility) == "Hidden";

        let snapshot = ScreenSnapshot {
            rows: rows as u16,
            cols: cols as u16,
            scrollback: self.scrollback,
            scrollback_max,
            lines: term_screen.lines_in_phys_range(0..total_rows),
            title: self.terminal.get_title().to_string(),
            bracketed_paste: self.terminal.bracketed_paste_enabled(),
            application_cursor: self.mode_state.application_cursor,
            mouse_mode: self.mode_state.mouse_mode(),
            mouse_encoding: self.mode_state.mouse_encoding,
            cursor_row,
            cursor_col,
            hide_cursor,
        };
        self.screen = Screen::from_snapshot(snapshot);
    }
}

#[derive(Debug)]
struct ParserConfig {
    scrollback: usize,
}

impl TerminalConfiguration for ParserConfig {
    fn scrollback_size(&self) -> usize {
        self.scrollback
    }

    fn color_palette(&self) -> ColorPalette {
        ColorPalette::default()
    }
}

#[derive(Debug)]
struct NullWriter;

impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ScreenSnapshot {
    rows: u16,
    cols: u16,
    scrollback: usize,
    scrollback_max: usize,
    lines: Vec<Line>,
    title: String,
    bracketed_paste: bool,
    application_cursor: bool,
    mouse_mode: MouseProtocolMode,
    mouse_encoding: MouseProtocolEncoding,
    cursor_row: u16,
    cursor_col: u16,
    hide_cursor: bool,
}

impl ScreenSnapshot {
    fn empty(rows: u16, cols: u16) -> Self {
        Self {
            rows,
            cols,
            scrollback: 0,
            scrollback_max: 0,
            lines: Vec::new(),
            title: String::new(),
            bracketed_paste: false,
            application_cursor: false,
            mouse_mode: MouseProtocolMode::None,
            mouse_encoding: MouseProtocolEncoding::Default,
            cursor_row: 0,
            cursor_col: 0,
            hide_cursor: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ModeState {
    application_cursor: bool,
    mouse_press: bool,
    mouse_drag: bool,
    mouse_move: bool,
    mouse_encoding: MouseProtocolEncoding,
}

impl Default for ModeState {
    fn default() -> Self {
        Self {
            application_cursor: false,
            mouse_press: false,
            mouse_drag: false,
            mouse_move: false,
            mouse_encoding: MouseProtocolEncoding::Default,
        }
    }
}

impl ModeState {
    fn mouse_mode(self) -> MouseProtocolMode {
        if self.mouse_move {
            MouseProtocolMode::Move
        } else if self.mouse_drag {
            MouseProtocolMode::Drag
        } else if self.mouse_press {
            MouseProtocolMode::Press
        } else {
            MouseProtocolMode::None
        }
    }

    fn apply_private_mode(&mut self, mode: u16, enabled: bool) {
        match mode {
            1 => self.application_cursor = enabled,
            9 | 1000 => self.mouse_press = enabled,
            1002 => self.mouse_drag = enabled,
            1003 => self.mouse_move = enabled,
            1005 => {
                if enabled {
                    self.mouse_encoding = MouseProtocolEncoding::Utf8;
                } else if self.mouse_encoding == MouseProtocolEncoding::Utf8 {
                    self.mouse_encoding = MouseProtocolEncoding::Default;
                }
            }
            1006 => {
                if enabled {
                    self.mouse_encoding = MouseProtocolEncoding::Sgr;
                } else if self.mouse_encoding == MouseProtocolEncoding::Sgr {
                    self.mouse_encoding = MouseProtocolEncoding::Default;
                }
            }
            1015 => {
                if enabled {
                    self.mouse_encoding = MouseProtocolEncoding::Urxvt;
                } else if self.mouse_encoding == MouseProtocolEncoding::Urxvt {
                    self.mouse_encoding = MouseProtocolEncoding::Default;
                }
            }
            1016 => {
                if enabled {
                    self.mouse_encoding = MouseProtocolEncoding::SgrPixels;
                } else if self.mouse_encoding == MouseProtocolEncoding::SgrPixels {
                    self.mouse_encoding = MouseProtocolEncoding::Default;
                }
            }
            _ => {}
        }
    }
}

#[derive(Default, Debug)]
struct SeqFilter {
    pending: Vec<u8>,
}

impl SeqFilter {
    fn transform(
        &mut self,
        bytes: &[u8],
        mode_state: &mut ModeState,
        disable_alt_screen: bool,
    ) -> Vec<u8> {
        let mut input = Vec::with_capacity(self.pending.len() + bytes.len());
        input.extend_from_slice(&self.pending);
        input.extend_from_slice(bytes);
        self.pending.clear();

        let mut out = Vec::with_capacity(input.len());
        let mut i = 0usize;
        while i < input.len() {
            if input[i] != 0x1b {
                out.push(input[i]);
                i += 1;
                continue;
            }

            if i + 1 >= input.len() {
                self.pending.extend_from_slice(&input[i..]);
                break;
            }

            if input[i + 1] != b'[' {
                out.push(input[i]);
                i += 1;
                continue;
            }

            let seq_start = i;
            let mut j = i + 2;
            let mut complete = false;
            while j < input.len() {
                let b = input[j];
                if (0x40..=0x7e).contains(&b) {
                    let seq = &input[seq_start..=j];
                    if let Some(transformed) =
                        transform_csi_sequence(seq, mode_state, disable_alt_screen)
                    {
                        out.extend_from_slice(&transformed);
                    }
                    i = j + 1;
                    complete = true;
                    break;
                }
                if j.saturating_sub(seq_start) > 128 {
                    out.extend_from_slice(&input[seq_start..=j]);
                    i = j + 1;
                    complete = true;
                    break;
                }
                j += 1;
            }

            if !complete {
                self.pending.extend_from_slice(&input[seq_start..]);
                break;
            }
        }

        out
    }
}

fn transform_csi_sequence(
    seq: &[u8],
    mode_state: &mut ModeState,
    disable_alt_screen: bool,
) -> Option<Vec<u8>> {
    if seq.len() < 3 || seq[0] != 0x1b || seq[1] != b'[' {
        return Some(seq.to_vec());
    }

    let final_byte = *seq.last().unwrap_or(&0);
    let params = &seq[2..seq.len().saturating_sub(1)];
    if matches!(final_byte, b'h' | b'l') {
        if let Some(modes) = parse_private_modes(params) {
            let enabled = final_byte == b'h';
            for mode in &modes {
                mode_state.apply_private_mode(*mode, enabled);
            }

            if disable_alt_screen {
                let filtered: Vec<u16> = modes
                    .into_iter()
                    .filter(|m| *m != 47 && *m != 1047 && *m != 1049)
                    .collect();
                if filtered.is_empty() {
                    return None;
                }
                let mut out = Vec::new();
                out.extend_from_slice(b"\x1b[?");
                for (idx, mode) in filtered.iter().enumerate() {
                    if idx > 0 {
                        out.push(b';');
                    }
                    out.extend_from_slice(mode.to_string().as_bytes());
                }
                out.push(final_byte);
                return Some(out);
            }
        }
    }

    Some(seq.to_vec())
}

fn parse_private_modes(params: &[u8]) -> Option<Vec<u16>> {
    if params.first().copied() != Some(b'?') {
        return None;
    }
    let body = std::str::from_utf8(&params[1..]).ok()?;
    if body.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for part in body.split(';') {
        let mode = part.parse::<u16>().ok()?;
        out.push(mode);
    }
    Some(out)
}

fn map_color(attr: ColorAttribute) -> Color {
    match attr {
        ColorAttribute::Default => Color::Default,
        ColorAttribute::PaletteIndex(idx) => Color::Idx(idx),
        ColorAttribute::TrueColorWithPaletteFallback(rgb, _) => {
            let (r, g, b, _) = rgb.as_rgba_u8();
            Color::Rgb(r, g, b)
        }
        ColorAttribute::TrueColorWithDefaultFallback(rgb) => {
            let (r, g, b, _) = rgb.as_rgba_u8();
            Color::Rgb(r, g, b)
        }
    }
}
