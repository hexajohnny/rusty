#![cfg_attr(windows, windows_subsystem = "windows")]

mod app;
mod async_config;
mod config;
mod crypto;
mod logger;
mod model;
mod ssh;
mod terminal_emulator;
mod terminal_themes;
mod tray;

const RENDERER_LOG_PATH: &str = "logs\\renderer-startup.log";
const RUSTY_LOW_POWER_RENDERER_ENV: &str = "RUSTY_LOW_POWER_RENDERER";
const RUSTY_RENDERER_KIND_ENV: &str = "RUSTY_RENDERER_KIND";
const RUSTY_WGPU_BACKEND_ENV: &str = "RUSTY_WGPU_BACKEND";

#[derive(Clone, Copy, Debug)]
struct WgpuBackendAttempt {
    backend: wgpu::Backends,
    label: &'static str,
    score: i32,
    available: bool,
}

fn main() -> eframe::Result<()> {
    let _ = std::fs::create_dir_all("logs");
    logger::log_line(
        RENDERER_LOG_PATH,
        &format!("=== startup pid={} ===", std::process::id()),
    );

    if std::env::var_os("WGPU_DX12_COMPILER").is_none() {
        std::env::set_var("WGPU_DX12_COMPILER", "fxc");
    }

    if let Some(backend_override) = std::env::var_os("WGPU_BACKEND") {
        logger::log_line(
            RENDERER_LOG_PATH,
            &format!(
                "Honoring WGPU_BACKEND override: {}",
                backend_override.to_string_lossy()
            ),
        );
        return run_with_renderer(eframe::Renderer::Wgpu, None, "wgpu(env)");
    }

    let mut last_err: Option<eframe::Error> = None;
    for attempt in guided_wgpu_backend_attempts() {
        if !attempt.available {
            logger::log_line(
                RENDERER_LOG_PATH,
                &format!(
                    "Skipping wgpu backend {} because the probe found no adapters",
                    attempt.label
                ),
            );
            continue;
        }

        logger::log_line(
            RENDERER_LOG_PATH,
            &format!(
                "Trying wgpu backend {} (score={})",
                attempt.label, attempt.score
            ),
        );

        match run_with_renderer(eframe::Renderer::Wgpu, Some(attempt.backend), attempt.label) {
            Ok(()) => return Ok(()),
            Err(err) => {
                logger::log_line(
                    RENDERER_LOG_PATH,
                    &format!("wgpu backend {} failed: {err}", attempt.label),
                );
                last_err = Some(err);
            }
        }
    }

    Err(last_err.expect("renderer attempts should produce an error"))
}

fn run_with_renderer(
    renderer: eframe::Renderer,
    backend_override: Option<wgpu::Backends>,
    attempt_label: &'static str,
) -> eframe::Result<()> {
    let mut native_options = eframe::NativeOptions {
        renderer,
        ..Default::default()
    };
    if let Some(backends) = backend_override {
        native_options.wgpu_options.supported_backends = backends;
    }
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
        Box::new(move |cc| {
            log_renderer_selection(renderer, attempt_label, cc);
            Box::new(app::AppState::new())
        }),
    )
}

fn guided_wgpu_backend_attempts() -> Vec<WgpuBackendAttempt> {
    platform_backend_candidates()
        .into_iter()
        .map(|(backend, label)| probe_backend_attempt(backend, label))
        .collect()
}

fn platform_backend_candidates() -> Vec<(wgpu::Backends, &'static str)> {
    #[cfg(target_os = "windows")]
    {
        vec![
            (wgpu::Backends::VULKAN, "vulkan"),
            (wgpu::Backends::DX12, "dx12"),
        ]
    }

    #[cfg(target_os = "macos")]
    {
        vec![(wgpu::Backends::VULKAN, "vulkan")]
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        vec![(wgpu::Backends::VULKAN, "vulkan")]
    }
}

fn probe_backend_attempt(backends: wgpu::Backends, label: &'static str) -> WgpuBackendAttempt {
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
        backends,
        ..Default::default()
    });
    let adapters = instance.enumerate_adapters(backends);
    if adapters.is_empty() {
        logger::log_line(
            RENDERER_LOG_PATH,
            &format!("Probe {backends:?}: no adapters"),
        );
        return WgpuBackendAttempt {
            backend: backends,
            label,
            score: i32::MIN / 4,
            available: false,
        };
    }

    let mut best_score = i32::MIN / 4;
    let mut summaries = Vec::new();
    for adapter in adapters {
        let info = adapter.get_info();
        let score = adapter_score(&info);
        if score > best_score {
            best_score = score;
        }
        summaries.push(format!(
            "{}:{}:{:?}:score={score}",
            info.backend.to_str(),
            info.name,
            info.device_type,
        ));
    }
    logger::log_line(
        RENDERER_LOG_PATH,
        &format!("Probe {backends:?}: {}", summaries.join(" | ")),
    );
    WgpuBackendAttempt {
        backend: backends,
        label,
        score: best_score,
        available: true,
    }
}

fn adapter_score(info: &wgpu::AdapterInfo) -> i32 {
    let software_penalty = if is_software_adapter_name(&info.name) {
        -250
    } else {
        0
    };
    let device_score = match info.device_type {
        wgpu::DeviceType::DiscreteGpu => 500,
        wgpu::DeviceType::IntegratedGpu => 400,
        wgpu::DeviceType::VirtualGpu => 220,
        wgpu::DeviceType::Other => 150,
        wgpu::DeviceType::Cpu => 25,
    };
    let backend_score = match info.device_type {
        wgpu::DeviceType::DiscreteGpu => match info.backend {
            wgpu::Backend::Vulkan => 80,
            wgpu::Backend::Dx12 => 70,
            wgpu::Backend::Metal => 70,
            wgpu::Backend::Gl => 20,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Empty => -1000,
        },
        wgpu::DeviceType::IntegratedGpu => match info.backend {
            wgpu::Backend::Vulkan => 70,
            wgpu::Backend::Dx12 => 60,
            wgpu::Backend::Metal => 60,
            wgpu::Backend::Gl => 25,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Empty => -1000,
        },
        wgpu::DeviceType::VirtualGpu => match info.backend {
            wgpu::Backend::Gl => 95,
            wgpu::Backend::Vulkan => 40,
            wgpu::Backend::Metal => 35,
            wgpu::Backend::Dx12 => -10,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Empty => -1000,
        },
        wgpu::DeviceType::Cpu => match info.backend {
            wgpu::Backend::Gl => 65,
            wgpu::Backend::Vulkan => 15,
            wgpu::Backend::Metal => 10,
            wgpu::Backend::Dx12 => -60,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Empty => -1000,
        },
        wgpu::DeviceType::Other => match info.backend {
            wgpu::Backend::Vulkan => 55,
            wgpu::Backend::Dx12 => 45,
            wgpu::Backend::Metal => 45,
            wgpu::Backend::Gl => 35,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Empty => -1000,
        },
    };
    device_score + backend_score + software_penalty
}

fn is_software_adapter_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.contains("basic render")
        || name.contains("swiftshader")
        || name.contains("llvmpipe")
        || name.contains("softpipe")
        || name.contains("software")
        || name.contains("svga3d")
        || name.contains("vmware")
        || name.contains("virtualbox")
        || name.contains("virgl")
        || name.contains("parallels")
}

fn is_low_power_adapter(info: &wgpu::AdapterInfo) -> bool {
    matches!(
        info.device_type,
        wgpu::DeviceType::VirtualGpu | wgpu::DeviceType::Cpu
    ) || is_software_adapter_name(&info.name)
}

fn log_renderer_selection(
    renderer: eframe::Renderer,
    attempt_label: &'static str,
    cc: &eframe::CreationContext<'_>,
) {
    if let Some(render_state) = cc.wgpu_render_state.as_ref() {
        let info = render_state.adapter.get_info();
        let low_power = is_low_power_adapter(&info);
        std::env::set_var(
            RUSTY_LOW_POWER_RENDERER_ENV,
            if low_power { "1" } else { "0" },
        );
        std::env::set_var(RUSTY_RENDERER_KIND_ENV, "wgpu");
        std::env::set_var(RUSTY_WGPU_BACKEND_ENV, info.backend.to_str());
        logger::log_line(
            RENDERER_LOG_PATH,
            &format!(
                "Renderer {:?} ({attempt_label}) selected adapter backend={} type={:?} name={} low_power={low_power}",
                renderer,
                info.backend.to_str(),
                info.device_type,
                info.name
            ),
        );
        return;
    }
    std::env::set_var(RUSTY_LOW_POWER_RENDERER_ENV, "0");
    std::env::set_var(RUSTY_RENDERER_KIND_ENV, "wgpu");
    std::env::remove_var(RUSTY_WGPU_BACKEND_ENV);
    logger::log_line(
        RENDERER_LOG_PATH,
        &format!("Renderer {:?} ({attempt_label}) initialized", renderer),
    );
}
