use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

const UI_PROFILE_LOG_PATH: &str = "logs\\ui-profile.log";

static UI_PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();

pub fn log_line<P: AsRef<Path>>(path: P, line: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

pub fn ui_profile_enabled() -> bool {
    *UI_PROFILE_ENABLED.get_or_init(|| {
        std::env::var_os("RUSTY_PROFILE_UI")
            .map(|value| value != "0")
            .unwrap_or(false)
    })
}

pub fn log_ui_profile(line: &str) {
    if ui_profile_enabled() {
        log_line(UI_PROFILE_LOG_PATH, line);
    }
}
