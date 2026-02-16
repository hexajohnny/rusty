# Rusty Theme Files (`.thm`)

Rusty UI themes are plain-text files with `key=value` lines.

## File locations

Rusty looks for theme files in this order:

1. Near the executable:
   - `.\theme\`
   - `.\dist\theme\`
2. Current working directory:
   - `.\theme\`
   - `.\dist\theme\`

If a selected theme cannot be loaded, Rusty falls back to built-in defaults.

## Format rules

- One setting per line: `key=value`
- Blank lines are ignored.
- Comment lines start with `#` or `;`.
- Keys are case-insensitive.
- Unknown keys are ignored.
- Invalid color values are ignored (other valid keys still apply).

## Supported keys

- `bg`: Main app background.
- `fg`: Main text color.
- `top_bg`: Top bar / panel background.
- `top_border`: Border color for top-level chrome/panels.
- `accent`: Accent color (highlights, active states, primary emphasis).
- `muted`: Secondary text color.

## Color formats

Each value can be either:

- Hex: `#RRGGBB`
- RGB triplet: `r,g,b` (0-255 each)

Examples:

- `accent=#ffb86c`
- `muted=140,150,160`

## Example theme

```ini
# Example Rusty theme
bg=#0a0c0e
fg=#dcdcdc
top_bg=#121418
top_border=#2d323a
accent=#ffb86c
muted=140,150,160
```

## Notes

- Theme files style the Rusty UI chrome.
- Terminal text/background/palette colors are configured separately in Settings -> Terminal Colors.
