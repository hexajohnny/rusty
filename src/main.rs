#![cfg_attr(windows, windows_subsystem = "windows")]

mod app;
mod async_config;
mod config;
mod crypto;
mod logger;
mod model;
mod ssh;
mod tray;
mod terminal_emulator;

fn main() -> eframe::Result<()> {
    if std::env::var_os("WGPU_BACKEND").is_none() {
        std::env::set_var("WGPU_BACKEND", "dx12");
    }
    if std::env::var_os("WGPU_DX12_COMPILER").is_none() {
        std::env::set_var("WGPU_DX12_COMPILER", "fxc");
    }
    run_with_renderer(eframe::Renderer::Wgpu)
}

fn run_with_renderer(renderer: eframe::Renderer) -> eframe::Result<()> {
    let mut native_options = eframe::NativeOptions::default();
    native_options.renderer = renderer;
    native_options.wgpu_options.supported_backends = wgpu::Backends::DX12;
    // Hide native window chrome; we draw our own title bar + close button in egui.
    let icon = eframe::icon_data::from_png_bytes(include_bytes!("../assets/icon.png")).ok();
    let mut viewport = native_options
        .viewport
        .with_decorations(false)
        .with_resizable(true)
        .with_transparent(false);
    if let Some(icon) = icon {
        viewport = viewport.with_icon(std::sync::Arc::new(icon));
    }
    native_options.viewport = viewport;
    eframe::run_native(
        concat!("Rusty - v", env!("CARGO_PKG_VERSION")),
        native_options,
        Box::new(|_cc| Box::new(app::AppState::new())),
    )
}
