use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::config::{self, RgbColor};
use toml::value::Table;
use wezterm_term::color::SrgbaTuple;

const THEME_LOG_PATH: &str = "logs\\terminal-themes.log";
type PalettePair = ([RgbColor; 8], [RgbColor; 8]);

#[derive(Clone, Debug)]
pub struct TerminalTheme {
    pub id: String,
    pub name: String,
    pub comment: Option<String>,
    pub light: bool,
    pub background: RgbColor,
    pub foreground: RgbColor,
    pub cursor: RgbColor,
    pub selection_bg: RgbColor,
    pub selection_fg: RgbColor,
    pub palette16: [RgbColor; 16],
    pub source_path: PathBuf,
}

impl TerminalTheme {
    pub fn fallback() -> Self {
        let cfg = config::TerminalColorsConfig::default();
        Self {
            id: "rusty-default".to_string(),
            name: "Rusty Default".to_string(),
            comment: Some("Built-in fallback terminal theme".to_string()),
            light: false,
            background: cfg.bg,
            foreground: cfg.fg,
            cursor: cfg.cursor,
            selection_bg: cfg.selection_bg,
            selection_fg: cfg.selection_fg,
            palette16: cfg.palette16,
            source_path: PathBuf::from("<built-in>"),
        }
    }

    pub fn to_terminal_colors_config(&self, dim_blend: f32) -> config::TerminalColorsConfig {
        let clamped_dim_blend = if dim_blend.is_finite() {
            dim_blend.clamp(0.0, 0.90)
        } else {
            0.38
        };
        config::TerminalColorsConfig {
            bg: self.background,
            fg: self.foreground,
            cursor: self.cursor,
            selection_bg: self.selection_bg,
            selection_fg: self.selection_fg,
            palette16: self.palette16,
            dim_blend: clamped_dim_blend,
        }
    }

    pub fn kind_label(&self) -> &'static str {
        if self.light {
            "Light"
        } else {
            "Dark"
        }
    }

    pub fn from_file(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path).map_err(|err| format!("read failed: {err}"))?;
        parse_toml_theme(path, &text)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ThemeRegistry {
    themes: Vec<TerminalTheme>,
    search_dirs: Vec<PathBuf>,
}

impl ThemeRegistry {
    pub fn load() -> Self {
        let dirs = Self::theme_dir_paths();
        let mut logger = |line: &str| crate::logger::log_line(THEME_LOG_PATH, line);
        let registry = Self::load_from_dirs_impl(&dirs, &mut logger);
        if registry.themes.is_empty() {
            logger("No valid terminal themes discovered; using current/fallback terminal colors.");
        } else {
            logger(&format!(
                "Loaded {} terminal theme(s) from {} search path(s).",
                registry.themes.len(),
                registry.search_dirs.len()
            ));
        }
        registry
    }

    fn load_from_dirs_impl(dirs: &[PathBuf], log: &mut impl FnMut(&str)) -> Self {
        let mut themes: Vec<TerminalTheme> = Vec::new();
        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut seen_names: HashSet<String> = HashSet::new();

        for dir in dirs {
            if !dir.is_dir() {
                log(&format!("Theme directory missing: {}", dir.display()));
                continue;
            }

            let mut files: Vec<PathBuf> = match fs::read_dir(dir) {
                Ok(entries) => entries
                    .flatten()
                    .map(|e| e.path())
                    .filter(|path| is_theme_file(path))
                    .collect(),
                Err(err) => {
                    log(&format!(
                        "Failed to enumerate theme directory {}: {err}",
                        dir.display()
                    ));
                    continue;
                }
            };

            files.sort_by(|a, b| {
                let a_name = a
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let b_name = b
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                a_name.cmp(&b_name)
            });

            for path in files {
                match TerminalTheme::from_file(&path) {
                    Ok(theme) => {
                        let id_key = theme.id.to_ascii_lowercase();
                        if !seen_ids.insert(id_key) {
                            log(&format!(
                                "Skipping duplicate terminal theme id '{}' from {}",
                                theme.id,
                                theme.source_path.display()
                            ));
                            continue;
                        }

                        let name_key = theme.name.to_ascii_lowercase();
                        if !seen_names.insert(name_key) {
                            log(&format!(
                                "Skipping duplicate terminal theme name '{}' from {}",
                                theme.name,
                                theme.source_path.display()
                            ));
                            continue;
                        }

                        themes.push(theme);
                    }
                    Err(err) => {
                        log(&format!(
                            "Failed to parse terminal theme {}: {err}",
                            path.display()
                        ));
                    }
                }
            }
        }

        themes.sort_by(|a, b| {
            let an = a.name.to_ascii_lowercase();
            let bn = b.name.to_ascii_lowercase();
            an.cmp(&bn).then_with(|| a.id.cmp(&b.id))
        });

        Self {
            themes,
            search_dirs: dirs.to_vec(),
        }
    }

    fn theme_dir_paths() -> Vec<PathBuf> {
        let mut dirs = Vec::new();
        let mut push_unique = |path: PathBuf| {
            if !dirs.iter().any(|p| p == &path) {
                dirs.push(path);
            }
        };

        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                push_unique(dir.join("term"));
                push_unique(dir.join("dist").join("term"));
            }
        }

        if let Ok(cwd) = std::env::current_dir() {
            push_unique(cwd.join("term"));
            push_unique(cwd.join("dist").join("term"));
        }

        dirs
    }

    pub fn themes(&self) -> &[TerminalTheme] {
        &self.themes
    }

    pub fn search_dirs(&self) -> &[PathBuf] {
        &self.search_dirs
    }

    pub fn is_empty(&self) -> bool {
        self.themes.is_empty()
    }

    pub fn find_by_id_or_name(&self, selected: &str) -> Option<&TerminalTheme> {
        let needle = selected.trim();
        if needle.is_empty() {
            return None;
        }

        self.themes
            .iter()
            .find(|theme| theme.id.eq_ignore_ascii_case(needle))
            .or_else(|| {
                self.themes
                    .iter()
                    .find(|theme| theme.name.eq_ignore_ascii_case(needle))
            })
    }

    pub fn default_theme(&self) -> TerminalTheme {
        self.themes
            .first()
            .cloned()
            .unwrap_or_else(TerminalTheme::fallback)
    }

    #[cfg(test)]
    fn load_for_tests(dirs: &[PathBuf]) -> (Self, Vec<String>) {
        let mut logs = Vec::new();
        let mut collector = |line: &str| logs.push(line.to_string());
        let registry = Self::load_from_dirs_impl(dirs, &mut collector);
        (registry, logs)
    }
}

fn parse_toml_theme(path: &Path, text: &str) -> Result<TerminalTheme, String> {
    let value: toml::Value = text
        .parse::<toml::Value>()
        .map_err(|err| format!("TOML parse error: {err}"))?;
    let root = value
        .as_table()
        .ok_or_else(|| "TOML root must be a table".to_string())?;

    let file_stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| "missing file stem".to_string())?;
    let theme_id = normalize_theme_id(file_stem);
    if theme_id.is_empty() {
        return Err("empty file stem".to_string());
    }

    let colors = root.get("colors").and_then(toml::Value::as_table);
    let metadata = root.get("metadata").and_then(toml::Value::as_table);

    let name = root
        .get("name")
        .and_then(toml::Value::as_str)
        .or_else(|| metadata.and_then(|m| m.get("name").and_then(toml::Value::as_str)))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| file_stem.to_string());

    let comment = root
        .get("comment")
        .and_then(toml::Value::as_str)
        .or_else(|| metadata.and_then(|m| m.get("author").and_then(toml::Value::as_str)))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string);

    let background =
        match read_color_with_keys(root, colors, &["background"], TransparentPolicy::Allow) {
            ColorLookup::Value(c) => c.to_rgb(),
            ColorLookup::Missing => return Err("missing required color 'background'".to_string()),
            ColorLookup::Invalid { key, value } => {
                return Err(format!("invalid color for '{key}': {value}"));
            }
        };
    let foreground = match read_color_with_keys(
        root,
        colors,
        &["foreground", "fg"],
        TransparentPolicy::Allow,
    ) {
        ColorLookup::Value(c) => c.to_rgb(),
        ColorLookup::Missing => return Err("missing required color 'foreground'".to_string()),
        ColorLookup::Invalid { key, value } => {
            return Err(format!("invalid color for '{key}': {value}"));
        }
    };

    let cursor = match read_color_with_keys(
        root,
        colors,
        &["cursor", "cursor_bg", "cursor_border"],
        TransparentPolicy::TreatAsMissing,
    ) {
        ColorLookup::Value(c) => c.to_rgb(),
        ColorLookup::Missing => foreground,
        ColorLookup::Invalid { key, value } => {
            return Err(format!("invalid color for '{key}': {value}"));
        }
    };

    let selection_bg = match read_color_with_keys(
        root,
        colors,
        &["selection_bg"],
        TransparentPolicy::TreatAsMissing,
    ) {
        ColorLookup::Value(c) => c.to_rgb(),
        ColorLookup::Missing => RgbColor::new(0x33, 0x44, 0x66),
        ColorLookup::Invalid { key, value } => {
            return Err(format!("invalid color for '{key}': {value}"));
        }
    };
    let selection_fg = match read_color_with_keys(
        root,
        colors,
        &["selection_fg"],
        TransparentPolicy::TreatAsMissing,
    ) {
        ColorLookup::Value(c) => c.to_rgb(),
        ColorLookup::Missing => foreground,
        ColorLookup::Invalid { key, value } => {
            return Err(format!("invalid color for '{key}': {value}"));
        }
    };

    let palette16 = read_palette16(root, colors)?;

    let light = root
        .get("light")
        .and_then(toml::Value::as_bool)
        .unwrap_or_else(|| infer_light_theme(background));

    Ok(TerminalTheme {
        id: theme_id,
        name,
        comment,
        light,
        background,
        foreground,
        cursor,
        selection_bg,
        selection_fg,
        palette16,
        source_path: path.to_path_buf(),
    })
}

fn read_palette16(root: &Table, colors: Option<&Table>) -> Result<[RgbColor; 16], String> {
    if let Some((ansi, brights)) = read_palette_arrays(root, colors)? {
        let mut out = [RgbColor::new(0, 0, 0); 16];
        out[..8].copy_from_slice(&ansi);
        out[8..].copy_from_slice(&brights);
        apply_indexed_overrides(&mut out, root, colors)?;
        return Ok(out);
    }

    let mut out = [RgbColor::new(0, 0, 0); 16];
    for (idx, key) in ANSI_KEY_ORDER.iter().enumerate() {
        let color = match read_color_with_keys(root, colors, &[*key], TransparentPolicy::Reject) {
            ColorLookup::Value(c) => c.to_rgb(),
            ColorLookup::Missing => {
                return Err(format!("missing required ANSI color '{key}'"));
            }
            ColorLookup::Invalid { key, value } => {
                return Err(format!("invalid color for '{key}': {value}"));
            }
        };
        out[idx] = color;
    }
    apply_indexed_overrides(&mut out, root, colors)?;
    Ok(out)
}

fn read_palette_arrays(
    root: &Table,
    colors: Option<&Table>,
) -> Result<Option<PalettePair>, String> {
    let ansi_vals = get_array(root, colors, "ansi");
    let bright_vals = get_array(root, colors, "brights");

    let (Some(ansi_vals), Some(bright_vals)) = (ansi_vals, bright_vals) else {
        return Ok(None);
    };

    if ansi_vals.len() != 8 {
        return Err(format!(
            "'ansi' must contain exactly 8 colors, got {}",
            ansi_vals.len()
        ));
    }
    if bright_vals.len() != 8 {
        return Err(format!(
            "'brights' must contain exactly 8 colors, got {}",
            bright_vals.len()
        ));
    }

    let mut ansi = [RgbColor::new(0, 0, 0); 8];
    let mut brights = [RgbColor::new(0, 0, 0); 8];

    for (i, val) in ansi_vals.iter().enumerate() {
        let parsed = parse_wezterm_color_value(val)
            .ok_or_else(|| format!("invalid ansi[{i}] color '{}'", value_for_error(val)))?;
        if parsed.is_fully_transparent() {
            return Err(format!(
                "ansi[{i}] cannot be fully transparent ('{}')",
                value_for_error(val)
            ));
        }
        ansi[i] = parsed.to_rgb();
    }
    for (i, val) in bright_vals.iter().enumerate() {
        let parsed = parse_wezterm_color_value(val)
            .ok_or_else(|| format!("invalid brights[{i}] color '{}'", value_for_error(val)))?;
        if parsed.is_fully_transparent() {
            return Err(format!(
                "brights[{i}] cannot be fully transparent ('{}')",
                value_for_error(val)
            ));
        }
        brights[i] = parsed.to_rgb();
    }

    Ok(Some((ansi, brights)))
}

fn get_array<'a>(
    root: &'a Table,
    colors: Option<&'a Table>,
    key: &str,
) -> Option<&'a Vec<toml::Value>> {
    colors
        .and_then(|table| table.get(key))
        .and_then(toml::Value::as_array)
        .or_else(|| root.get(key).and_then(toml::Value::as_array))
}

fn get_value<'a>(root: &'a Table, colors: Option<&'a Table>, key: &str) -> Option<&'a toml::Value> {
    colors
        .and_then(|table| table.get(key))
        .or_else(|| root.get(key))
}

fn get_table<'a>(root: &'a Table, colors: Option<&'a Table>, key: &str) -> Option<&'a Table> {
    get_value(root, colors, key).and_then(toml::Value::as_table)
}

fn apply_indexed_overrides(
    palette: &mut [RgbColor; 16],
    root: &Table,
    colors: Option<&Table>,
) -> Result<(), String> {
    let Some(indexed) = get_table(root, colors, "indexed") else {
        return Ok(());
    };

    for (idx_raw, value) in indexed {
        let index = idx_raw
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("'indexed' key '{idx_raw}' is not a valid numeric index"))?;
        if index >= palette.len() {
            continue;
        }

        let parsed = parse_wezterm_color_value(value).ok_or_else(|| {
            format!(
                "invalid indexed[{index}] color '{}'",
                value_for_error(value)
            )
        })?;
        if parsed.is_fully_transparent() {
            return Err(format!(
                "indexed[{index}] cannot be fully transparent ('{}')",
                value_for_error(value)
            ));
        }

        palette[index] = parsed.to_rgb();
    }

    Ok(())
}

enum ColorLookup {
    Missing,
    Value(ParsedColor),
    Invalid { key: String, value: String },
}

#[derive(Clone, Copy)]
enum TransparentPolicy {
    Allow,
    TreatAsMissing,
    Reject,
}

fn read_color_with_keys(
    root: &Table,
    colors: Option<&Table>,
    keys: &[&str],
    transparent_policy: TransparentPolicy,
) -> ColorLookup {
    for key in keys {
        if let Some(raw) = get_value(root, colors, key) {
            let Some(parsed) = parse_wezterm_color_value(raw) else {
                return ColorLookup::Invalid {
                    key: (*key).to_string(),
                    value: value_for_error(raw),
                };
            };

            if parsed.is_fully_transparent() {
                return match transparent_policy {
                    TransparentPolicy::Allow => ColorLookup::Value(parsed),
                    TransparentPolicy::TreatAsMissing => ColorLookup::Missing,
                    TransparentPolicy::Reject => ColorLookup::Invalid {
                        key: (*key).to_string(),
                        value: format!("{} (fully transparent)", value_for_error(raw)),
                    },
                };
            }

            return ColorLookup::Value(parsed);
        }
    }
    ColorLookup::Missing
}

fn normalize_theme_id(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn infer_light_theme(bg: RgbColor) -> bool {
    let luma = (0.299 * bg.r as f32 + 0.587 * bg.g as f32 + 0.114 * bg.b as f32) / 255.0;
    luma >= 0.60
}

#[derive(Clone, Copy, Debug)]
struct ParsedColor {
    tuple: SrgbaTuple,
}

impl ParsedColor {
    fn to_rgb(self) -> RgbColor {
        srgba_tuple_to_rgb(self.tuple)
    }

    fn is_fully_transparent(self) -> bool {
        self.tuple.3 <= (1.0 / 255.0)
    }
}

fn parse_wezterm_color_value(value: &toml::Value) -> Option<ParsedColor> {
    match value {
        toml::Value::String(raw) => parse_wezterm_color_string(raw),
        toml::Value::Array(items) => parse_wezterm_color_array(items),
        toml::Value::Table(table) => parse_wezterm_color_table(table),
        _ => None,
    }
}

fn parse_wezterm_color_string(value: &str) -> Option<ParsedColor> {
    let tuple = SrgbaTuple::from_str(value.trim()).ok()?;
    Some(ParsedColor { tuple })
}

fn parse_wezterm_color_array(items: &[toml::Value]) -> Option<ParsedColor> {
    if !(items.len() == 3 || items.len() == 4) {
        return None;
    }

    let r = toml_number_to_f32(&items[0])?;
    let g = toml_number_to_f32(&items[1])?;
    let b = toml_number_to_f32(&items[2])?;
    let a = if items.len() == 4 {
        toml_number_to_f32(&items[3])?
    } else {
        1.0
    };

    parsed_color_from_numeric_components(r, g, b, a)
}

fn parse_wezterm_color_table(table: &Table) -> Option<ParsedColor> {
    for key in ["color", "Color", "value", "Value"] {
        if let Some(value) = table.get(key) {
            return parse_wezterm_color_value(value);
        }
    }

    let r = get_numeric_component(table, &["r", "red"])?;
    let g = get_numeric_component(table, &["g", "green"])?;
    let b = get_numeric_component(table, &["b", "blue"])?;
    let a = get_numeric_component(table, &["a", "alpha"]).unwrap_or(1.0);
    parsed_color_from_numeric_components(r, g, b, a)
}

fn get_numeric_component(table: &Table, keys: &[&str]) -> Option<f32> {
    for key in keys {
        if let Some(value) = table.get(*key) {
            return toml_number_to_f32(value);
        }
    }
    None
}

fn toml_number_to_f32(value: &toml::Value) -> Option<f32> {
    match value {
        toml::Value::Integer(v) => Some(*v as f32),
        toml::Value::Float(v) => Some(*v as f32),
        _ => None,
    }
}

fn parsed_color_from_numeric_components(r: f32, g: f32, b: f32, a: f32) -> Option<ParsedColor> {
    if !r.is_finite() || !g.is_finite() || !b.is_finite() || !a.is_finite() {
        return None;
    }

    let uses_255_scale = r > 1.0 || g > 1.0 || b > 1.0;
    let (r, g, b) = if uses_255_scale {
        (r / 255.0, g / 255.0, b / 255.0)
    } else {
        (r, g, b)
    };
    if !(0.0..=1.0).contains(&r) || !(0.0..=1.0).contains(&g) || !(0.0..=1.0).contains(&b) {
        return None;
    }

    let alpha = if a > 1.0 { a / 255.0 } else { a };
    if !(0.0..=1.0).contains(&alpha) {
        return None;
    }

    Some(ParsedColor {
        tuple: SrgbaTuple(r, g, b, alpha),
    })
}

fn value_for_error(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}

fn srgba_tuple_to_rgb(color: SrgbaTuple) -> RgbColor {
    // Preserve WezTerm-compatible parsing syntax while reducing to RGB for Rusty's palette model.
    // Alpha is currently ignored by terminal rendering in this app.
    let (r, g, b, _a) = color.to_srgb_u8();
    RgbColor::new(r, g, b)
}

fn is_theme_file(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .and_then(|s| s.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("toml"))
            .unwrap_or(false)
}

const ANSI_KEY_ORDER: [&str; 16] = [
    "black",
    "red",
    "green",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "white",
    "bright_black",
    "bright_red",
    "bright_green",
    "bright_yellow",
    "bright_blue",
    "bright_magenta",
    "bright_cyan",
    "bright_white",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_wezterm_toml(name: &str) -> String {
        format!(
            "[metadata]\nname = \"{name}\"\nauthor = \"Rusty Tests\"\n\n[colors]\nforeground = \"#D4D4D4\"\nbackground = \"#1E1E1E\"\ncursor_bg = \"#D4D4D4\"\nselection_bg = \"#264F78\"\nselection_fg = \"#FFFFFF\"\nansi = [\"#000000\", \"#CD3131\", \"#0DBC79\", \"#E5E510\", \"#2472C8\", \"#BC3FBC\", \"#11A8CD\", \"#E5E5E5\"]\nbrights = [\"#666666\", \"#F14C4C\", \"#23D18B\", \"#F5F543\", \"#3B8EEA\", \"#D670D6\", \"#29B8DB\", \"#FFFFFF\"]\n"
        )
    }

    fn make_temp_dir(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        path.push(format!("rusty-term-theme-test-{tag}-{pid}-{now}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn parses_valid_wezterm_theme() {
        let dir = make_temp_dir("parse-valid");
        let path = dir.join("tokyo-night.toml");
        fs::write(&path, valid_wezterm_toml("Tokyo Night")).expect("write theme");

        let theme = TerminalTheme::from_file(&path).expect("valid theme should parse");
        assert_eq!(theme.id, "tokyo-night");
        assert_eq!(theme.name, "Tokyo Night");
        assert_eq!(theme.background, RgbColor::new(0x1E, 0x1E, 0x1E));
        assert_eq!(theme.palette16[12], RgbColor::new(0x3B, 0x8E, 0xEA));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_invalid_color() {
        let dir = make_temp_dir("parse-invalid");
        let path = dir.join("broken.toml");
        let mut text = valid_wezterm_toml("Broken");
        text = text.replace("\"#CD3131\"", "\"not-a-valid-color\"");
        fs::write(&path, text).expect("write theme");

        let err = TerminalTheme::from_file(&path).expect_err("invalid color should fail parse");
        assert!(err.contains("invalid") || err.contains("missing"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_extended_wezterm_color_forms_and_indexed_overrides() {
        let dir = make_temp_dir("parse-extended");
        let path = dir.join("extended.toml");
        let text = r##"
[metadata]
name = "Extended"

[colors]
foreground = "rgb:d6/d6/d6"
background = { r = 30, g = 30, b = 30 }
cursor_bg = { color = "hsl:120 100 50" }
selection_bg = { red = 40, green = 80, blue = 120, alpha = 200 }
selection_fg = "none"
ansi = [
    [0, 0, 0],
    [205, 49, 49],
    [13, 188, 121],
    [229, 229, 16],
    [36, 114, 200],
    [188, 63, 188],
    [17, 168, 205],
    [229, 229, 229],
]
brights = [
    [102, 102, 102],
    [241, 76, 76],
    [35, 209, 139],
    [245, 245, 67],
    [59, 142, 234],
    [214, 112, 214],
    [41, 184, 219],
    [255, 255, 255],
]
indexed = { "1" = "#ff0000", "8" = [170, 170, 170], "42" = "#123456" }
"##;
        fs::write(&path, text).expect("write theme");

        let theme = TerminalTheme::from_file(&path).expect("extended theme should parse");
        assert_eq!(theme.foreground, RgbColor::new(0xD6, 0xD6, 0xD6));
        assert_eq!(theme.background, RgbColor::new(30, 30, 30));
        assert_eq!(theme.cursor, RgbColor::new(0x00, 0xFF, 0x00));
        assert_eq!(theme.selection_bg, RgbColor::new(40, 80, 120));
        assert_eq!(theme.selection_fg, theme.foreground);
        assert_eq!(theme.palette16[1], RgbColor::new(0xFF, 0x00, 0x00));
        assert_eq!(theme.palette16[8], RgbColor::new(170, 170, 170));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_non_numeric_indexed_keys() {
        let dir = make_temp_dir("parse-bad-indexed");
        let path = dir.join("bad-indexed.toml");
        let text = r##"
[colors]
foreground = "#D4D4D4"
background = "#1E1E1E"
ansi = ["#000000", "#CD3131", "#0DBC79", "#E5E510", "#2472C8", "#BC3FBC", "#11A8CD", "#E5E5E5"]
brights = ["#666666", "#F14C4C", "#23D18B", "#F5F543", "#3B8EEA", "#D670D6", "#29B8DB", "#FFFFFF"]
indexed = { "link" = "#123456" }
"##;
        fs::write(&path, text).expect("write theme");

        let err = TerminalTheme::from_file(&path).expect_err("invalid indexed key should fail");
        assert!(err.contains("indexed"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn loads_directory_with_mixed_valid_and_invalid_files() {
        let dir = make_temp_dir("load-mixed");
        fs::write(dir.join("good.toml"), valid_wezterm_toml("Good")).expect("write good");
        fs::write(dir.join("bad.toml"), "[colors]\nbackground = \"#000000\"\n").expect("write bad");
        fs::write(dir.join("notes.txt"), "ignored").expect("write txt");

        let (registry, logs) = ThemeRegistry::load_for_tests(std::slice::from_ref(&dir));
        assert_eq!(registry.themes().len(), 1);
        assert_eq!(registry.themes()[0].name, "Good");
        assert!(logs
            .iter()
            .any(|line| line.contains("Failed to parse terminal theme")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn finds_selected_theme_by_id_or_name_and_falls_back_to_none() {
        let dir = make_temp_dir("select-fallback");
        fs::write(
            dir.join("tokyo-night.toml"),
            valid_wezterm_toml("Tokyo Night"),
        )
        .expect("write tokyo");
        fs::write(dir.join("nord.toml"), valid_wezterm_toml("Nord")).expect("write nord");

        let (registry, _) = ThemeRegistry::load_for_tests(std::slice::from_ref(&dir));
        assert!(registry.find_by_id_or_name("tokyo-night").is_some());
        assert!(registry.find_by_id_or_name("Tokyo Night").is_some());
        assert!(registry.find_by_id_or_name("missing-theme").is_none());

        let _ = fs::remove_dir_all(dir);
    }
}
