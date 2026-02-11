# Rusty SSH

Rusty is a Windows desktop SSH client built in Rust with an embedded, tabbed terminal and a dockable layout (via `egui_tiles`).

![Rusty Screenshot](misc/example.png)

## Features

### SSH
- Password authentication.
- Keyboard-interactive authentication (server-driven prompts like password + OTP).
- Private key authentication (key file path per profile) with optional passphrase prompt.
- PTY shell session (`TERM=xterm-256color`).
- stderr merged into stdout for better TUI compatibility.

### Terminal
- Embedded terminal renderer (VT100/xterm-style escape sequences).
- ANSI color support (16-color palette + 256-color modes) with configurable palette.
- Scrollback (configurable in code; current default is 5000 lines).
- Mouse wheel scrolls scrollback.
- Hover-only scroll bar indicator.
- Copy/paste:
  - Click-drag selection to copy (copies to clipboard).
  - Paste into the terminal from clipboard.
- Focus locking for common terminal keys (arrows/tab/escape) so focus does not jump to UI controls.

### Tabs + Docking
- Multiple SSH terminals in tabs.
- `+` button opens a new tab.
- Right-click `+` to create a new tab from a saved profile.
- Right-click a tab:
  - Rename tab
  - Change tab color (presets)
  - Split right / split down (dockable layout)
  - Close tab
- Active terminal pane is highlighted with a subtle border/glow.

### Settings + Profiles
- Settings window with left-side section navigation.
- Profiles:
  - Create, edit, delete profiles.
  - Right-click a profile to delete it.
  - Default profile + optional autostart on launch.
- Appearance:
  - Terminal font size (persists across restarts).
  - “Minimize to tray” option.
- Terminal Colors:
  - Background/foreground
  - 16-color palette (normal + bright)
  - Dim/faint blending strength

### Tray / Window Chrome
- Custom borderless window with rounded corners.
- Custom title bar with minimize / maximize / close.
- Optional minimize-to-tray:
  - Tray icon menu: Show/Hide, Exit
  - Double-click tray icon toggles show/hide.

## Security Notes
- Config is stored per-user and encrypted on Windows using DPAPI (low CPU).
- If you choose to remember secrets (password / key passphrase), they are stored encrypted in the local config.

## Config Location
- `%APPDATA%\\RustySSH\\config.json`

## Build (Windows / MSVC)
```powershell
cargo build --release
```

## Run
```powershell
.\target\release\rusty.exe
```

## Current Limitations (Planned / Not Implemented Yet)
- Host key verification UI and `known_hosts` management.
- SSH agent support (OpenSSH agent/Pageant/1Password agent).
- Port forwarding (`-L/-R/-D`) and SOCKS proxy.
- SFTP/SCP file transfer UI.
- Jump hosts / ProxyCommand.
