#![cfg_attr(windows, windows_subsystem = "windows")]

mod app;
mod config;
mod crypto;
mod logger;
mod model;
mod ssh;
mod tray;

fn main() -> eframe::Result<()> {
    let mut native_options = eframe::NativeOptions::default();
    // Hide native window chrome; we draw our own title bar + close button in egui.
    native_options.viewport = native_options
        .viewport
        .with_decorations(false)
        .with_resizable(true)
        .with_transparent(true);
    eframe::run_native(
        concat!("Rusty SSH - v", env!("CARGO_PKG_VERSION")),
        native_options,
        Box::new(|_cc| Box::new(app::AppState::new())),
    )
}
