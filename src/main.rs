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
    low_power_hint: bool,
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

    for attempt in guided_wgpu_backend_attempts() {
        logger::log_line(
            RENDERER_LOG_PATH,
            &format!(
                "Trying wgpu backend {} (score={} low_power_hint={})",
                attempt.label, attempt.score, attempt.low_power_hint
            ),
        );

        let tried_glow_first = attempt.backend == wgpu::Backends::GL && attempt.low_power_hint;
        if tried_glow_first {
            logger::log_line(
                RENDERER_LOG_PATH,
                "Low-power GL adapter detected; trying glow before wgpu-gl.",
            );
            match run_with_renderer(eframe::Renderer::Glow, None, "glow(gl-preferred-low-power)") {
                Ok(()) => return Ok(()),
                Err(glow_err) => {
                    logger::log_line(
                        RENDERER_LOG_PATH,
                        &format!("Preferred Glow GL path failed: {glow_err}"),
                    );
                }
            }
        }

        match run_with_renderer(eframe::Renderer::Wgpu, Some(attempt.backend), attempt.label) {
            Ok(()) => return Ok(()),
            Err(err) => {
                logger::log_line(
                    RENDERER_LOG_PATH,
                    &format!("wgpu backend {} failed: {err}", attempt.label),
                );
                if attempt.backend == wgpu::Backends::GL && !tried_glow_first {
                    logger::log_line(
                        RENDERER_LOG_PATH,
                        "wgpu-gl failed; trying glow before advancing to the next backend.",
                    );
                    match run_with_renderer(eframe::Renderer::Glow, None, "glow(gl-fallback)") {
                        Ok(()) => return Ok(()),
                        Err(glow_err) => {
                            logger::log_line(
                                RENDERER_LOG_PATH,
                                &format!("Glow GL fallback failed: {glow_err}"),
                            );
                        }
                    }
                }
            }
        }
    }

    logger::log_line(
        RENDERER_LOG_PATH,
        "All guided wgpu backend attempts failed; trying glow fallback.",
    );
    run_with_renderer(eframe::Renderer::Glow, None, "glow").map_err(|err| {
        logger::log_line(RENDERER_LOG_PATH, &format!("Glow fallback failed: {err}"));
        err
    })
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
        native_options.wgpu_options.wgpu_setup = egui_wgpu::WgpuSetupCreateNew {
            instance_descriptor: wgpu::InstanceDescriptor {
                backends,
                ..Default::default()
            },
            ..Default::default()
        }
        .into();
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
            Ok(Box::new(app::AppState::new()))
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
            (wgpu::Backends::GL, "gl"),
        ]
    }

    #[cfg(target_os = "macos")]
    {
        vec![(wgpu::Backends::METAL, "metal"), (wgpu::Backends::GL, "gl")]
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        vec![
            (wgpu::Backends::VULKAN, "vulkan"),
            (wgpu::Backends::GL, "gl"),
        ]
    }
}

fn probe_backend_attempt(backends: wgpu::Backends, label: &'static str) -> WgpuBackendAttempt {
    let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
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
            low_power_hint: true,
        };
    }

    let mut best_score = i32::MIN / 4;
    let mut best_low_power_hint = true;
    let mut summaries = Vec::new();
    for adapter in adapters {
        let info = adapter.get_info();
        let score = adapter_score(&info);
        let low_power_hint = is_low_power_adapter(&info);
        if score > best_score {
            best_score = score;
            best_low_power_hint = low_power_hint;
        }
        summaries.push(format!(
            "{}:{}:{:?}:score={score}:low_power={low_power_hint}",
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
        low_power_hint: best_low_power_hint,
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
            wgpu::Backend::Noop => -1000,
        },
        wgpu::DeviceType::IntegratedGpu => match info.backend {
            wgpu::Backend::Vulkan => 70,
            wgpu::Backend::Dx12 => 60,
            wgpu::Backend::Metal => 60,
            wgpu::Backend::Gl => 25,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Noop => -1000,
        },
        wgpu::DeviceType::VirtualGpu => match info.backend {
            wgpu::Backend::Gl => 95,
            wgpu::Backend::Vulkan => 40,
            wgpu::Backend::Metal => 35,
            wgpu::Backend::Dx12 => -10,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Noop => -1000,
        },
        wgpu::DeviceType::Cpu => match info.backend {
            wgpu::Backend::Gl => 65,
            wgpu::Backend::Vulkan => 15,
            wgpu::Backend::Metal => 10,
            wgpu::Backend::Dx12 => -60,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Noop => -1000,
        },
        wgpu::DeviceType::Other => match info.backend {
            wgpu::Backend::Vulkan => 55,
            wgpu::Backend::Dx12 => 45,
            wgpu::Backend::Metal => 45,
            wgpu::Backend::Gl => 35,
            wgpu::Backend::BrowserWebGpu => 5,
            wgpu::Backend::Noop => -1000,
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

fn is_low_power_gl_renderer(vendor: &str, renderer_name: &str) -> bool {
    is_software_adapter_name(&format!("{vendor} {renderer_name}"))
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

    if let Some(gl) = cc.gl.as_ref() {
        use eframe::glow::HasContext as _;
        let version = unsafe { gl.get_parameter_string(eframe::glow::VERSION) };
        let renderer_name = unsafe { gl.get_parameter_string(eframe::glow::RENDERER) };
        let vendor = unsafe { gl.get_parameter_string(eframe::glow::VENDOR) };
        let low_power = is_low_power_gl_renderer(&vendor, &renderer_name);
        std::env::set_var(
            RUSTY_LOW_POWER_RENDERER_ENV,
            if low_power { "1" } else { "0" },
        );
        std::env::set_var(RUSTY_RENDERER_KIND_ENV, "glow");
        std::env::remove_var(RUSTY_WGPU_BACKEND_ENV);
        logger::log_line(
            RENDERER_LOG_PATH,
            &format!(
                "Glow context ({attempt_label}) vendor={vendor} renderer={renderer_name} version={version}"
            ),
        );
        logger::log_line(
            RENDERER_LOG_PATH,
            &format!(
                "Renderer {:?} ({attempt_label}) initialized low_power={low_power}",
                renderer
            ),
        );
        return;
    }

    let low_power = matches!(renderer, eframe::Renderer::Glow);
    std::env::set_var(
        RUSTY_LOW_POWER_RENDERER_ENV,
        if low_power { "1" } else { "0" },
    );
    std::env::set_var(
        RUSTY_RENDERER_KIND_ENV,
        match renderer {
            eframe::Renderer::Glow => "glow",
            eframe::Renderer::Wgpu => "wgpu",
        },
    );
    std::env::remove_var(RUSTY_WGPU_BACKEND_ENV);
    logger::log_line(
        RENDERER_LOG_PATH,
        &format!(
            "Renderer {:?} ({attempt_label}) initialized low_power={low_power}",
            renderer
        ),
    );
}
