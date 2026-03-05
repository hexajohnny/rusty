# Scripts

## sync_wezterm_themes.ps1

Downloads WezTerm `.toml` themes from `mbadolato/iTerm2-Color-Schemes` and syncs them into your Rusty install `term` folder.

### Run

From the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_wezterm_themes.ps1
```

### Common options

```powershell
# Force x64 install path
powershell -ExecutionPolicy Bypass -File .\scripts\sync_wezterm_themes.ps1 -Architecture x64

# Force x86 install path
powershell -ExecutionPolicy Bypass -File .\scripts\sync_wezterm_themes.ps1 -Architecture x86

# Sync both installs
powershell -ExecutionPolicy Bypass -File .\scripts\sync_wezterm_themes.ps1 -Architecture both

# Also remove local .toml files that are not in upstream
powershell -ExecutionPolicy Bypass -File .\scripts\sync_wezterm_themes.ps1 -Clean
```

### Notes

- `-Architecture` accepts: `Auto`, `x64`, `x86`, `both` (default: `Auto`).
- The script uses HTTP requests only (no `git`).
- Because it writes under `C:\Program Files\...`, run PowerShell as **Administrator** if needed.
