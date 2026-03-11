use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::model::ConnectionSettings;
use crate::{crypto, logger};

const CFG_MAGIC_PREFIX: &str = "RUSTYCFG1:";
const CONFIG_SAVE_RETRY_COUNT: usize = 3;
const CONFIG_SAVE_RETRY_BASE_DELAY_MS: u64 = 120;

fn default_terminal_font_size() -> f32 {
    14.0
}

fn default_terminal_scrollback_lines() -> usize {
    5000
}

#[derive(Clone, Debug)]
pub struct ConfigLoadOutcome {
    pub config: AppConfig,
    pub notice: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum UiThemeMode {
    #[default]
    Dark,
    Light,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SavedWindow {
    pub outer_pos: [f32; 2],
    pub inner_size: [f32; 2],
    #[serde(default)]
    pub maximized: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RgbColor {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl RgbColor {
    pub const fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }
}

fn default_dim_blend() -> f32 {
    0.38
}

fn default_terminal_bg() -> RgbColor {
    RgbColor::new(0, 0, 0)
}

fn default_terminal_fg() -> RgbColor {
    RgbColor::new(220, 220, 220)
}

fn default_terminal_cursor() -> RgbColor {
    default_terminal_fg()
}

fn default_terminal_selection_bg() -> RgbColor {
    RgbColor::new(255, 184, 108)
}

fn default_terminal_selection_fg() -> RgbColor {
    RgbColor::new(20, 20, 20)
}

fn default_terminal_palette16() -> [RgbColor; 16] {
    [
        RgbColor::new(0, 0, 0),       // 0 black
        RgbColor::new(205, 49, 49),   // 1 red
        RgbColor::new(13, 188, 121),  // 2 green
        RgbColor::new(229, 229, 16),  // 3 yellow
        RgbColor::new(36, 114, 200),  // 4 blue
        RgbColor::new(188, 63, 188),  // 5 magenta
        RgbColor::new(17, 168, 205),  // 6 cyan
        RgbColor::new(229, 229, 229), // 7 white
        RgbColor::new(102, 102, 102), // 8 bright black (gray)
        RgbColor::new(241, 76, 76),   // 9 bright red
        RgbColor::new(35, 209, 139),  // 10 bright green
        RgbColor::new(245, 245, 67),  // 11 bright yellow
        RgbColor::new(59, 142, 234),  // 12 bright blue
        RgbColor::new(214, 112, 214), // 13 bright magenta
        RgbColor::new(41, 184, 219),  // 14 bright cyan
        RgbColor::new(255, 255, 255), // 15 bright white
    ]
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TerminalColorsConfig {
    #[serde(default = "default_terminal_bg")]
    pub bg: RgbColor,
    #[serde(default = "default_terminal_fg")]
    pub fg: RgbColor,
    #[serde(default = "default_terminal_cursor")]
    pub cursor: RgbColor,
    #[serde(default = "default_terminal_selection_bg")]
    pub selection_bg: RgbColor,
    #[serde(default = "default_terminal_selection_fg")]
    pub selection_fg: RgbColor,
    #[serde(default = "default_terminal_palette16")]
    pub palette16: [RgbColor; 16],
    #[serde(default = "default_dim_blend")]
    pub dim_blend: f32,
}

impl Default for TerminalColorsConfig {
    fn default() -> Self {
        Self {
            bg: default_terminal_bg(),
            fg: default_terminal_fg(),
            cursor: default_terminal_cursor(),
            selection_bg: default_terminal_selection_bg(),
            selection_fg: default_terminal_selection_fg(),
            palette16: default_terminal_palette16(),
            dim_blend: default_dim_blend(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirectionConfig {
    Download,
    Upload,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransferStateConfig {
    Queued,
    Running,
    Paused,
    Finished,
    Failed,
    Canceled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferHistoryEntry {
    pub request_id: u64,
    pub direction: TransferDirectionConfig,
    pub settings: ConnectionSettings,
    pub remote_path: String,
    pub local_path: String,
    pub transferred_bytes: u64,
    pub total_bytes: Option<u64>,
    pub speed_bps: f64,
    pub state: TransferStateConfig,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub profiles: Vec<ConnectionProfile>,
    pub default_profile: Option<String>,
    pub autostart: bool,
    #[serde(default)]
    pub ui_theme_mode: UiThemeMode,
    #[serde(default)]
    pub ui_theme_file: Option<String>,
    #[serde(default)]
    pub minimize_to_tray: bool,
    #[serde(default)]
    pub focus_shade: bool,
    #[serde(default = "default_terminal_font_size")]
    pub terminal_font_size: f32,
    #[serde(default = "default_terminal_scrollback_lines")]
    pub terminal_scrollback_lines: usize,
    #[serde(default)]
    pub save_session_layout: bool,
    #[serde(default)]
    pub saved_session_layout_json: Option<String>,
    #[serde(default)]
    pub saved_window: Option<SavedWindow>,
    #[serde(default)]
    pub terminal_colors: TerminalColorsConfig,
    #[serde(default)]
    pub selected_terminal_theme: Option<String>,
    #[serde(default)]
    pub transfer_history: Vec<TransferHistoryEntry>,
    #[serde(default)]
    pub update_available_version: Option<String>,
    #[serde(default)]
    pub update_available_url: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            profiles: Vec::new(),
            default_profile: None,
            autostart: false,
            ui_theme_mode: UiThemeMode::Dark,
            ui_theme_file: None,
            minimize_to_tray: false,
            focus_shade: false,
            terminal_font_size: default_terminal_font_size(),
            terminal_scrollback_lines: default_terminal_scrollback_lines(),
            save_session_layout: false,
            saved_session_layout_json: None,
            saved_window: None,
            terminal_colors: TerminalColorsConfig::default(),
            selected_terminal_theme: None,
            transfer_history: Vec::new(),
            update_available_version: None,
            update_available_url: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub name: String,
    pub settings: ConnectionSettings,
    pub remember_password: bool,
    #[serde(default)]
    pub remember_key_passphrase: bool,
}

fn config_dir() -> Option<PathBuf> {
    // Prefer a stable per-user location.
    // Example: %APPDATA%\Rusty\config.json
    std::env::var_os("APPDATA").map(|p| PathBuf::from(p).join("Rusty"))
}

fn legacy_config_dir() -> Option<PathBuf> {
    // Legacy app-data path from previous builds.
    std::env::var_os("APPDATA").map(|p| {
        let mut legacy_name = String::from("Rusty");
        legacy_name.push_str("SSH");
        PathBuf::from(p).join(legacy_name)
    })
}

pub fn config_path() -> PathBuf {
    if let Some(dir) = config_dir() {
        return dir.join("config.json");
    }
    PathBuf::from("config.json")
}

fn corrupt_backup_path(path: &Path) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("config");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("json");
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    for attempt in 0..1000u32 {
        let suffix = if attempt == 0 {
            format!("{stem}.corrupt-{stamp}")
        } else {
            format!("{stem}.corrupt-{stamp}-{attempt}")
        };
        let file_name = if ext.is_empty() {
            suffix
        } else {
            format!("{suffix}.{ext}")
        };
        let candidate = path.with_file_name(file_name);
        if !candidate.exists() {
            return candidate;
        }
    }

    path.with_file_name(format!("{stem}.corrupt-backup"))
}

fn preserve_unreadable_config(path: &Path, bytes: &[u8], reason: &str) -> Option<PathBuf> {
    let backup = corrupt_backup_path(path);
    if let Some(parent) = backup.parent() {
        let _ = fs::create_dir_all(parent);
    }

    match fs::rename(path, &backup) {
        Ok(()) => Some(backup),
        Err(rename_err) => {
            logger::log_line(
                "logs\\config.log",
                &format!(
                    "Config backup rename failed for {} -> {}: {rename_err}",
                    path.display(),
                    backup.display()
                ),
            );
            match fs::write(&backup, bytes) {
                Ok(()) => Some(backup),
                Err(write_err) => {
                    logger::log_line(
                        "logs\\config.log",
                        &format!(
                            "Config backup write failed for {} after load error ({reason}): {write_err}",
                            backup.display()
                        ),
                    );
                    None
                }
            }
        }
    }
}

fn unreadable_config_outcome(path: &Path, bytes: &[u8], reason: &str) -> ConfigLoadOutcome {
    logger::log_line(
        "logs\\config.log",
        &format!("Config load failed for {}: {reason}", path.display()),
    );

    let backup = preserve_unreadable_config(path, bytes, reason);
    let notice = match backup {
        Some(backup) => format!(
            "Rusty could not read the saved config at {}. It was preserved as {} and defaults were loaded.",
            path.display(),
            backup.display()
        ),
        None => format!(
            "Rusty could not read the saved config at {}. Defaults were loaded; see logs\\\\config.log.",
            path.display()
        ),
    };

    ConfigLoadOutcome {
        config: AppConfig::default(),
        notice: Some(notice),
    }
}

fn sanitized_for_plaintext_fallback(cfg: &AppConfig) -> AppConfig {
    let mut sanitized = cfg.clone();

    for profile in &mut sanitized.profiles {
        profile.remember_password = false;
        profile.remember_key_passphrase = false;
        profile.settings.password.clear();
        profile.settings.key_passphrase.clear();
    }

    for transfer in &mut sanitized.transfer_history {
        transfer.settings.password.clear();
        transfer.settings.key_passphrase.clear();
    }

    // Session layout snapshots may embed serialized connection settings with secrets.
    sanitized.saved_session_layout_json = None;
    sanitized
}

pub fn load() -> ConfigLoadOutcome {
    let path = config_path();
    let (bytes, loaded_from_path) = if let Ok(bytes) = fs::read(&path) {
        (bytes, path.clone())
    } else {
        let legacy = legacy_config_dir()
            .map(|dir| dir.join("config.json"))
            .filter(|p| p != &path);
        match legacy {
            Some(legacy_path) => match fs::read(&legacy_path) {
                Ok(bytes) => (bytes, legacy_path),
                Err(_) => {
                    return ConfigLoadOutcome {
                        config: AppConfig::default(),
                        notice: None,
                    };
                }
            },
            None => {
                return ConfigLoadOutcome {
                    config: AppConfig::default(),
                    notice: None,
                };
            }
        }
    };

    let cfg = if bytes.starts_with(CFG_MAGIC_PREFIX.as_bytes()) {
        let b64 = &bytes[CFG_MAGIC_PREFIX.len()..];
        let cipher = match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(cipher) => cipher,
            Err(err) => {
                return unreadable_config_outcome(
                    &loaded_from_path,
                    &bytes,
                    &format!("base64 decode failed: {err}"),
                );
            }
        };
        let plain = match crypto::decrypt_for_current_user(&cipher) {
            Ok(plain) => plain,
            Err(err) => {
                return unreadable_config_outcome(
                    &loaded_from_path,
                    &bytes,
                    &format!("decrypt failed: {err}"),
                );
            }
        };
        match serde_json::from_slice::<AppConfig>(&plain) {
            Ok(cfg) => cfg,
            Err(err) => {
                return unreadable_config_outcome(
                    &loaded_from_path,
                    &bytes,
                    &format!("JSON parse failed after decrypt: {err}"),
                );
            }
        }
    } else {
        match serde_json::from_slice::<AppConfig>(&bytes) {
            Ok(cfg) => cfg,
            Err(err) => {
                return unreadable_config_outcome(
                    &loaded_from_path,
                    &bytes,
                    &format!("JSON parse failed: {err}"),
                );
            }
        }
    };

    // Best-effort migration: rewrite plaintext configs encrypted, and migrate from the legacy
    // directory to the current one.
    if !bytes.starts_with(CFG_MAGIC_PREFIX.as_bytes()) || loaded_from_path != path {
        save(&cfg);
    }

    ConfigLoadOutcome {
        config: cfg,
        notice: None,
    }
}

pub fn save(cfg: &AppConfig) {
    let path = config_path();
    if let Some(parent) = path.parent() {
        if let Err(err) = fs::create_dir_all(parent) {
            logger::log_line(
                "logs\\config.log",
                &format!(
                    "Config save failed to create directory {}: {err}",
                    parent.display()
                ),
            );
            return;
        }
    }

    let Ok(json) = serde_json::to_vec_pretty(cfg) else {
        logger::log_line(
            "logs\\config.log",
            "Config save failed to serialize JSON payload",
        );
        return;
    };

    let payload = match crypto::encrypt_for_current_user(&json) {
        Ok(cipher) => {
            let b64 = base64::engine::general_purpose::STANDARD.encode(cipher);
            let mut out = Vec::with_capacity(CFG_MAGIC_PREFIX.len() + b64.len());
            out.extend_from_slice(CFG_MAGIC_PREFIX.as_bytes());
            out.extend_from_slice(b64.as_bytes());
            out
        }
        Err(err) => {
            // Never fall back to writing credentials in plaintext.
            let sanitized = sanitized_for_plaintext_fallback(cfg);
            let Ok(sanitized_json) = serde_json::to_vec_pretty(&sanitized) else {
                logger::log_line(
                    "logs\\config.log",
                    "Config save failed to serialize sanitized JSON payload",
                );
                return;
            };
            logger::log_line(
                "logs\\config.log",
                &format!(
                    "Config encryption failed: {err}. Writing sanitized plaintext config without stored secrets."
                ),
            );
            sanitized_json
        }
    };

    // Best-effort atomic write with bounded retries for transient file lock races.
    let tmp = path.with_extension("json.tmp");
    for attempt in 0..=CONFIG_SAVE_RETRY_COUNT {
        match write_config_atomic(&path, &tmp, &payload) {
            Ok(()) => return,
            Err(err) => {
                let attempt_num = attempt + 1;
                let total_attempts = CONFIG_SAVE_RETRY_COUNT + 1;
                logger::log_line(
                    "logs\\config.log",
                    &format!(
                        "Config save attempt {attempt_num}/{total_attempts} failed for {}: {err}",
                        path.display()
                    ),
                );
                if attempt < CONFIG_SAVE_RETRY_COUNT {
                    let delay_ms = CONFIG_SAVE_RETRY_BASE_DELAY_MS * (attempt as u64 + 1);
                    std::thread::sleep(Duration::from_millis(delay_ms));
                } else {
                    logger::log_line(
                        "logs\\config.log",
                        "Config save paused after retry limit; next save request will retry.",
                    );
                }
            }
        }
    }
}

fn write_config_atomic(path: &Path, tmp: &Path, payload: &[u8]) -> std::io::Result<()> {
    fs::write(tmp, payload)?;

    if fs::rename(tmp, path).is_ok() {
        return Ok(());
    }

    // If rename fails (e.g. cross-device), fall back to copy+remove.
    let bytes = fs::read(tmp)?;
    fs::write(path, bytes)?;
    match fs::remove_file(tmp) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

pub fn find_profile_index(cfg: &AppConfig, name: &str) -> Option<usize> {
    cfg.profiles
        .iter()
        .position(|p| p.name.eq_ignore_ascii_case(name))
}

pub fn profile_display_name(p: &ConnectionProfile, cfg: &AppConfig) -> String {
    let mut s = p.name.clone();
    if cfg
        .default_profile
        .as_deref()
        .map(|d| d.eq_ignore_ascii_case(&p.name))
        .unwrap_or(false)
    {
        s.push_str(" (default)");
    }
    s
}

pub fn sanitized_profile_name(name: &str) -> String {
    name.trim().to_string()
}

pub fn write_profile_settings(profile: &ConnectionProfile) -> ConnectionSettings {
    let mut s = profile.settings.clone();
    if !profile.remember_password {
        s.password.clear();
    }
    if !profile.remember_key_passphrase {
        s.key_passphrase.clear();
    }
    s
}

pub fn read_profile_from_settings(
    name: String,
    settings: &ConnectionSettings,
    remember_password: bool,
    remember_key_passphrase: bool,
) -> ConnectionProfile {
    let mut s = settings.clone();
    if !remember_password {
        s.password.clear();
    }
    if !remember_key_passphrase {
        s.key_passphrase.clear();
    }
    ConnectionProfile {
        name,
        settings: s,
        remember_password,
        remember_key_passphrase,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_fallback_strips_secrets() {
        let mut cfg = AppConfig {
            saved_session_layout_json: Some("{\"embedded\":\"secret\"}".to_string()),
            ..AppConfig::default()
        };
        cfg.profiles.push(ConnectionProfile {
            name: "prod".to_string(),
            settings: ConnectionSettings {
                host: "example.com".to_string(),
                port: 22,
                username: "alice".to_string(),
                password: "pw".to_string(),
                private_key_path: "id_ed25519".to_string(),
                key_passphrase: "keypw".to_string(),
            },
            remember_password: true,
            remember_key_passphrase: true,
        });
        cfg.transfer_history.push(TransferHistoryEntry {
            request_id: 1,
            direction: TransferDirectionConfig::Download,
            settings: ConnectionSettings {
                host: "example.com".to_string(),
                port: 22,
                username: "alice".to_string(),
                password: "pw".to_string(),
                private_key_path: "id_ed25519".to_string(),
                key_passphrase: "keypw".to_string(),
            },
            remote_path: "/tmp/file".to_string(),
            local_path: "file".to_string(),
            transferred_bytes: 0,
            total_bytes: None,
            speed_bps: 0.0,
            state: TransferStateConfig::Queued,
            message: String::new(),
        });

        let sanitized = sanitized_for_plaintext_fallback(&cfg);
        assert_eq!(sanitized.profiles[0].settings.password, "");
        assert_eq!(sanitized.profiles[0].settings.key_passphrase, "");
        assert!(!sanitized.profiles[0].remember_password);
        assert!(!sanitized.profiles[0].remember_key_passphrase);
        assert_eq!(sanitized.transfer_history[0].settings.password, "");
        assert_eq!(sanitized.transfer_history[0].settings.key_passphrase, "");
        assert!(sanitized.saved_session_layout_json.is_none());
    }
}
