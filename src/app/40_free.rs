fn paint_window_chrome(ctx: &egui::Context, theme: UiTheme) {
    let rect = ctx.screen_rect();
    let window_rounding = egui::Rounding::ZERO;
    // Use the shared background layer so our fills are always behind panels/widgets.
    let painter_bg = ctx.layer_painter(egui::LayerId::background());
    // Draw the border/lines above all UI so minimal padding doesn't hide them.
    let painter_fg = ctx.layer_painter(egui::LayerId::new(
        egui::Order::Foreground,
        Id::new("window_chrome_fg"),
    ));

    painter_bg.rect_filled(rect, window_rounding, theme.bg);

    let bar_rect = Rect::from_min_size(rect.min, Vec2::new(rect.width(), TITLE_BAR_H));
    painter_bg.rect_filled(bar_rect, window_rounding, theme.top_bg);

    painter_fg.rect_stroke(
        rect.shrink(0.5),
        window_rounding,
        Stroke::new(1.0, theme.top_border),
    );
}

fn handle_window_resize(ctx: &egui::Context) {
    let maximized = ctx.input(|i| i.viewport().maximized.unwrap_or(false));
    let fullscreen = ctx.input(|i| i.viewport().fullscreen.unwrap_or(false));
    if maximized || fullscreen {
        return;
    }

    let rect = ctx.screen_rect();
    let Some(pos) = ctx.input(|i| i.pointer.latest_pos()) else {
        return;
    };

    let left = pos.x <= rect.left() + RESIZE_MARGIN;
    let right = pos.x >= rect.right() - RESIZE_MARGIN;
    let top = pos.y <= rect.top() + RESIZE_MARGIN;
    let bottom = pos.y >= rect.bottom() - RESIZE_MARGIN;

    let dir = match (left, right, top, bottom) {
        (true, _, true, _) => Some(egui::ResizeDirection::NorthWest),
        (_, true, true, _) => Some(egui::ResizeDirection::NorthEast),
        (true, _, _, true) => Some(egui::ResizeDirection::SouthWest),
        (_, true, _, true) => Some(egui::ResizeDirection::SouthEast),
        (true, _, _, _) => Some(egui::ResizeDirection::West),
        (_, true, _, _) => Some(egui::ResizeDirection::East),
        (_, _, true, _) => Some(egui::ResizeDirection::North),
        (_, _, _, true) => Some(egui::ResizeDirection::South),
        _ => None,
    };

    let Some(dir) = dir else { return };

    let icon = match dir {
        egui::ResizeDirection::East | egui::ResizeDirection::West => {
            egui::CursorIcon::ResizeHorizontal
        }
        egui::ResizeDirection::North | egui::ResizeDirection::South => {
            egui::CursorIcon::ResizeVertical
        }
        egui::ResizeDirection::NorthEast | egui::ResizeDirection::SouthWest => {
            egui::CursorIcon::ResizeNeSw
        }
        egui::ResizeDirection::NorthWest | egui::ResizeDirection::SouthEast => {
            egui::CursorIcon::ResizeNwSe
        }
    };
    ctx.output_mut(|o| o.cursor_icon = icon);

    if ctx.input(|i| i.pointer.primary_pressed()) {
        ctx.send_viewport_cmd(egui::ViewportCommand::BeginResize(dir));
    }
}

fn xterm_256_color(idx: u8, base: &[Color32; 16]) -> Color32 {
    match idx {
        0..=15 => base[idx as usize],
        16..=231 => {
            let i = idx - 16;
            let r = i / 36;
            let g = (i % 36) / 6;
            let b = i % 6;
            let conv = |v: u8| -> u8 {
                match v {
                    0 => 0,
                    1 => 95,
                    2 => 135,
                    3 => 175,
                    4 => 215,
                    _ => 255,
                }
            };
            Color32::from_rgb(conv(r), conv(g), conv(b))
        }
        232..=255 => {
            let v = 8u8.saturating_add((idx - 232).saturating_mul(10));
            Color32::from_rgb(v, v, v)
        }
    }
}

const TAB_COLOR_PRESETS: [(&str, Color32); 12] = [
    ("Ash", Color32::from_rgb(56, 62, 72)),
    ("Steel", Color32::from_rgb(72, 92, 116)),
    ("Cobalt", Color32::from_rgb(39, 80, 158)),
    ("Ocean", Color32::from_rgb(0, 118, 150)),
    ("Mint", Color32::from_rgb(35, 160, 132)),
    ("Forest", Color32::from_rgb(0, 132, 90)),
    ("Lime", Color32::from_rgb(120, 200, 72)),
    ("Sand", Color32::from_rgb(214, 168, 76)),
    ("Sunset", Color32::from_rgb(223, 99, 72)),
    ("Ember", Color32::from_rgb(190, 54, 54)),
    ("Rose", Color32::from_rgb(198, 64, 131)),
    ("Purple", Color32::from_rgb(120, 78, 191)),
];

fn contrast_text_color(bg: Color32) -> Color32 {
    // Fast gamma-space luma is good enough for picking black/white text.
    let lum = (0.299 * bg.r() as f32 + 0.587 * bg.g() as f32 + 0.114 * bg.b() as f32) / 255.0;
    if lum > 0.62 {
        Color32::from_rgb(18, 18, 18)
    } else {
        Color32::from_rgb(245, 245, 245)
    }
}

fn lerp_color(a: Color32, b: Color32, t: f32) -> Color32 {
    let (ar, ag, ab, aa) = a.to_tuple();
    let (br, bg, bb, _ba) = b.to_tuple();
    let t = t.clamp(0.0, 1.0);
    let lerp = |x: u8, y: u8| -> u8 {
        (x as f32 + (y as f32 - x as f32) * t)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    Color32::from_rgba_premultiplied(lerp(ar, br), lerp(ag, bg), lerp(ab, bb), aa)
}

fn adjust_color(c: Color32, delta: f32) -> Color32 {
    let (r, g, b, a) = c.to_tuple();
    let t = delta.abs().clamp(0.0, 1.0);
    let (tr, tg, tb) = if delta >= 0.0 {
        (255u8, 255u8, 255u8)
    } else {
        (0u8, 0u8, 0u8)
    };
    let lerp = |x: u8, y: u8| -> u8 {
        (x as f32 + (y as f32 - x as f32) * t)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    Color32::from_rgba_premultiplied(lerp(r, tr), lerp(g, tg), lerp(b, tb), a)
}

fn title_bar_icon_button<'a>(
    button: egui::Button<'a>,
    fill: Color32,
    border: Color32,
) -> egui::Button<'a> {
    button
        .fill(fill)
        .stroke(Stroke::new(1.0, border))
        .rounding(egui::Rounding::same(8.0))
        .min_size(Vec2::new(30.0, 24.0))
}

fn title_bar_image_button(
    ui: &mut egui::Ui,
    icon: egui::Image<'_>,
    icon_size: Vec2,
    fill: Color32,
    border: Color32,
) -> Response {
    let response = ui.add(title_bar_icon_button(egui::Button::new(""), fill, border));
    if ui.is_rect_visible(response.rect) {
        let icon_rect = Rect::from_center_size(response.rect.center(), icon_size);
        icon.paint_at(ui, icon_rect);
    }
    response
}
