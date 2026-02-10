use std::fs;
use std::path::PathBuf;

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::model::ConnectionSettings;
use crate::{crypto, logger};

const CFG_MAGIC_PREFIX: &str = "RUSTYCFG1:";

fn default_terminal_font_size() -> f32 {
    16.0
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct RgbColor {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl RgbColor {
    pub const fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }

    pub fn to_array(self) -> [u8; 3] {
        [self.r, self.g, self.b]
    }

    pub fn from_array(v: [u8; 3]) -> Self {
        Self {
            r: v[0],
            g: v[1],
            b: v[2],
        }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalColorsConfig {
    #[serde(default = "default_terminal_bg")]
    pub bg: RgbColor,
    #[serde(default = "default_terminal_fg")]
    pub fg: RgbColor,
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
            palette16: default_terminal_palette16(),
            dim_blend: default_dim_blend(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub profiles: Vec<ConnectionProfile>,
    pub default_profile: Option<String>,
    pub autostart: bool,
    #[serde(default)]
    pub minimize_to_tray: bool,
    #[serde(default = "default_terminal_font_size")]
    pub terminal_font_size: f32,
    #[serde(default)]
    pub terminal_colors: TerminalColorsConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            profiles: Vec::new(),
            default_profile: None,
            autostart: false,
            minimize_to_tray: false,
            terminal_font_size: default_terminal_font_size(),
            terminal_colors: TerminalColorsConfig::default(),
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
    // Example: %APPDATA%\RustySSH\config.json
    std::env::var_os("APPDATA").map(|p| PathBuf::from(p).join("RustySSH"))
}

pub fn config_path() -> PathBuf {
    if let Some(dir) = config_dir() {
        return dir.join("config.json");
    }
    PathBuf::from("config.json")
}

pub fn load() -> AppConfig {
    let path = config_path();
    let Ok(bytes) = fs::read(&path) else {
        return AppConfig::default();
    };

    let parsed = if bytes.starts_with(CFG_MAGIC_PREFIX.as_bytes()) {
        let b64 = &bytes[CFG_MAGIC_PREFIX.len()..];
        let Ok(cipher) = base64::engine::general_purpose::STANDARD.decode(b64) else {
            return AppConfig::default();
        };
        let Ok(plain) = crypto::decrypt_for_current_user(&cipher) else {
            return AppConfig::default();
        };
        serde_json::from_slice::<AppConfig>(&plain).ok()
    } else {
        serde_json::from_slice::<AppConfig>(&bytes).ok()
    };

    let Some(cfg) = parsed else { return AppConfig::default() };

    // Best-effort migration: if config was plaintext, rewrite it encrypted.
    if !bytes.starts_with(CFG_MAGIC_PREFIX.as_bytes()) {
        save(&cfg);
    }

    cfg
}

pub fn save(cfg: &AppConfig) {
    let path = config_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let Ok(json) = serde_json::to_vec_pretty(cfg) else {
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
            // If encryption fails for some reason, avoid breaking the app; log and fall back.
            logger::log_line("logs\\config.log", &format!("Config encryption failed: {err}"));
            json
        }
    };

    // Best-effort atomic write.
    let tmp = path.with_extension("json.tmp");
    if fs::write(&tmp, payload).is_ok() {
        let _ = fs::rename(&tmp, &path).or_else(|_| {
            // If rename fails (e.g. cross-device), fall back.
            fs::write(&path, fs::read(&tmp).unwrap_or_default()).and_then(|_| fs::remove_file(&tmp))
        });
    }
}

pub fn find_profile_index(cfg: &AppConfig, name: &str) -> Option<usize> {
    cfg.profiles.iter().position(|p| p.name.eq_ignore_ascii_case(name))
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
