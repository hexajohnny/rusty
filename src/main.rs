#![cfg_attr(windows, windows_subsystem = "windows")]

mod app;
mod async_config;
mod config;
mod crypto;
mod logger;
mod model;
mod ssh;
mod tray;

fn main() -> eframe::Result<()> {
    let mut native_options = eframe::NativeOptions::default();
    // Hide native window chrome; we draw our own title bar + close button in egui.
    let icon = eframe::icon_data::from_png_bytes(include_bytes!("../assets/icon.png")).ok();
    let mut viewport = native_options
        .viewport
        .with_decorations(false)
        .with_resizable(true)
        .with_transparent(true);
    if let Some(icon) = icon {
        viewport = viewport.with_icon(std::sync::Arc::new(icon));
    }
    native_options.viewport = viewport;
    eframe::run_native(
        concat!("Rusty SSH - v", env!("CARGO_PKG_VERSION")),
        native_options,
        Box::new(|_cc| Box::new(app::AppState::new())),
    )
}
