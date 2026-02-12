use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Mutex;
use std::sync::Once;

use crossbeam_channel::{unbounded, Receiver, Sender};
use eframe::egui;
use once_cell::sync::Lazy;

use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder, TrayIconEvent};

#[derive(Debug, Clone)]
pub enum TrayAppEvent {
    Menu(MenuId),
    Tray(TrayIconEvent),
}

static INIT: Once = Once::new();
static CHANNEL: Lazy<(Sender<TrayAppEvent>, Receiver<TrayAppEvent>)> = Lazy::new(unbounded);
static WAKE_CTX: Lazy<Mutex<Option<egui::Context>>> = Lazy::new(|| Mutex::new(None));
static HIDDEN_TO_TRAY: AtomicBool = AtomicBool::new(false);
static MAIN_HWND: AtomicIsize = AtomicIsize::new(0);
static MENU_IDS: Lazy<Mutex<Option<(MenuId, MenuId)>>> = Lazy::new(|| Mutex::new(None));

pub fn set_wake_ctx(ctx: egui::Context) {
    if let Ok(mut guard) = WAKE_CTX.lock() {
        *guard = Some(ctx);
    }
}

fn wake_app() {
    if let Ok(guard) = WAKE_CTX.lock() {
        if let Some(ctx) = guard.as_ref() {
            ctx.request_repaint();
        }
    }
}

pub fn set_hidden_to_tray_state(hidden: bool) {
    HIDDEN_TO_TRAY.store(hidden, Ordering::Relaxed);
}

fn menu_action_for(id: &MenuId) -> Option<&'static str> {
    let Ok(guard) = MENU_IDS.lock() else {
        return None;
    };
    let Some((show_hide_id, exit_id)) = guard.as_ref() else {
        return None;
    };

    if id == show_hide_id {
        Some("show_hide")
    } else if id == exit_id {
        Some("exit")
    } else {
        None
    }
}

fn direct_show_from_tray() {
    if let Ok(guard) = WAKE_CTX.lock() {
        if let Some(ctx) = guard.as_ref() {
            #[cfg(target_os = "windows")]
            {
                let _ = native_show_window();
            }
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            HIDDEN_TO_TRAY.store(false, Ordering::Relaxed);
            ctx.request_repaint();
        }
    }
}

fn direct_exit_from_tray() {
    if let Ok(guard) = WAKE_CTX.lock() {
        if let Some(ctx) = guard.as_ref() {
            // Ensure the viewport is in a closable state on backends that ignore
            // close while fully hidden/minimized.
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            ctx.request_repaint();
        }
    }
}

fn apply_direct_menu_action(action: &str) {
    match action {
        "show_hide" => direct_show_from_tray(),
        "exit" => direct_exit_from_tray(),
        _ => {}
    }
}

#[cfg(target_os = "windows")]
pub fn capture_foreground_hwnd() {
    use windows_sys::Win32::UI::WindowsAndMessaging::GetForegroundWindow;

    unsafe {
        let hwnd = GetForegroundWindow();
        if hwnd != 0 {
            MAIN_HWND.store(hwnd, Ordering::Relaxed);
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub fn capture_foreground_hwnd() {}

#[cfg(target_os = "windows")]
fn find_process_window() -> Option<isize> {
    use windows_sys::Win32::Foundation::{BOOL, HWND, LPARAM};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        EnumWindows, GW_OWNER, GetWindow, GetWindowTextW, GetWindowThreadProcessId,
    };

    struct SearchCtx {
        pid: u32,
        hwnd: isize,
    }

    unsafe extern "system" fn enum_windows_cb(hwnd: HWND, lparam: LPARAM) -> BOOL {
        let ctx = &mut *(lparam as *mut SearchCtx);
        let mut win_pid: u32 = 0;
        GetWindowThreadProcessId(hwnd, &mut win_pid);
        if win_pid == ctx.pid && GetWindow(hwnd, GW_OWNER) == 0 {
            let mut title_buf = [0u16; 256];
            let title_len = GetWindowTextW(hwnd, title_buf.as_mut_ptr(), title_buf.len() as i32);
            if title_len > 0 {
                let title = String::from_utf16_lossy(&title_buf[..title_len as usize]);
                if title.starts_with("Rusty - v") {
                    ctx.hwnd = hwnd;
                    return 0;
                }
            }
        }
        1
    }

    let mut ctx = SearchCtx {
        pid: std::process::id(),
        hwnd: 0,
    };
    unsafe {
        EnumWindows(Some(enum_windows_cb), &mut ctx as *mut SearchCtx as LPARAM);
    }
    (ctx.hwnd != 0).then_some(ctx.hwnd)
}

#[cfg(target_os = "windows")]
fn native_show_window() -> bool {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        SW_RESTORE, SW_SHOW, SetForegroundWindow, ShowWindowAsync,
    };

    let cached = MAIN_HWND.load(Ordering::Relaxed);
    let hwnd = if cached != 0 {
        cached
    } else if let Some(found) = find_process_window() {
        found
    } else {
        return false;
    };

    unsafe {
        let _ = ShowWindowAsync(hwnd, SW_RESTORE);
        let _ = ShowWindowAsync(hwnd, SW_SHOW);
        let _ = SetForegroundWindow(hwnd);
    }
    true
}

pub fn install_handlers() -> Receiver<TrayAppEvent> {
    INIT.call_once(|| {
        let tx_menu = CHANNEL.0.clone();
        MenuEvent::set_event_handler(Some(move |ev: MenuEvent| {
            if let Some(action) = menu_action_for(&ev.id) {
                apply_direct_menu_action(action);
            }
            let _ = tx_menu.send(TrayAppEvent::Menu(ev.id));
            wake_app();
        }));

        let tx_tray = CHANNEL.0.clone();
        TrayIconEvent::set_event_handler(Some(move |ev: TrayIconEvent| {
            if let TrayIconEvent::Click {
                button: tray_icon::MouseButton::Left,
                button_state: tray_icon::MouseButtonState::Up,
                ..
            } = ev
            {
                // Single left-click should restore/raise immediately.
                direct_show_from_tray();
            }
            let _ = tx_tray.send(TrayAppEvent::Tray(ev));
            wake_app();
        }));
    });

    CHANNEL.1.clone()
}

pub struct TrayState {
    #[allow(dead_code)]
    tray: TrayIcon,
    pub show_hide_item: MenuItem,
    #[allow(dead_code)]
    pub exit_item: MenuItem,
    pub show_hide_id: MenuId,
    pub exit_id: MenuId,
}

impl TrayState {
    pub fn set_visible(&self, visible: bool) {
        let _ = self.tray.set_visible(visible);
    }
}

fn load_tray_icon() -> anyhow::Result<Icon> {
    let bytes = include_bytes!("../assets/icon.png");
    let img = image::load_from_memory(bytes)?.into_rgba8();

    // Windows tray icons are typically rendered at small sizes (16-32px).
    let tray_px: u32 = 32;
    let (w, h) = img.dimensions();
    let img = if w != tray_px || h != tray_px {
        image::imageops::resize(&img, tray_px, tray_px, image::imageops::FilterType::Lanczos3)
    } else {
        img
    };
    let (w, h) = img.dimensions();
    Ok(Icon::from_rgba(img.into_raw(), w, h)?)
}

pub fn create_tray() -> anyhow::Result<TrayState> {
    let menu = Menu::new();
    let show_hide_item = MenuItem::new("Show Rusty", true, None);
    let exit_item = MenuItem::new("Exit", true, None);

    let _ = menu.append(&show_hide_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&exit_item);

    let icon = load_tray_icon()?;
    let tray = TrayIconBuilder::new()
        .with_tooltip("Rusty")
        .with_menu(Box::new(menu))
        .with_icon(icon)
        .build()?;
    // Tray icon should only be visible when the app is hidden/minimized to tray.
    let _ = tray.set_visible(false);

    if let Ok(mut guard) = MENU_IDS.lock() {
        *guard = Some((show_hide_item.id().clone(), exit_item.id().clone()));
    }

    Ok(TrayState {
        tray,
        show_hide_id: show_hide_item.id().clone(),
        exit_id: exit_item.id().clone(),
        show_hide_item,
        exit_item,
    })
}
