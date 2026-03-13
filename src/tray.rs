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
#[cfg(target_os = "windows")]
static MAIN_WNDPROC: AtomicIsize = AtomicIsize::new(0);
#[cfg(target_os = "windows")]
static HIT_TEST_HWND: AtomicIsize = AtomicIsize::new(0);
static MENU_IDS: Lazy<Mutex<Option<(MenuId, MenuId)>>> = Lazy::new(|| Mutex::new(None));

#[cfg(target_os = "windows")]
const CUSTOM_CHROME_TITLE_BAR_HEIGHT: i32 = 28;
#[cfg(target_os = "windows")]
const CUSTOM_CHROME_RESIZE_MARGIN: i32 = 6;
#[cfg(target_os = "windows")]
const CUSTOM_CHROME_BUTTON_STRIP_WIDTH: i32 = 220;

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

pub fn request_app_repaint() {
    wake_app();
}

pub fn set_hidden_to_tray_state(hidden: bool) {
    HIDDEN_TO_TRAY.store(hidden, Ordering::Relaxed);
}

fn menu_action_for(id: &MenuId) -> Option<&'static str> {
    let Ok(guard) = MENU_IDS.lock() else {
        return None;
    };
    let (show_hide_id, exit_id) = guard.as_ref()?;

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
            ctx.send_viewport_cmd(egui::ViewportCommand::Resizable(true));
            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            HIDDEN_TO_TRAY.store(false, Ordering::Relaxed);
            ctx.request_repaint();
        }
    }
}

fn direct_exit_from_tray() {
    // Safety net: some hidden-window states ignore close commands.
    // Force-exit shortly after requesting graceful shutdown so tray Exit is reliable.
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(1200));
        std::process::exit(0);
    });

    #[cfg(target_os = "windows")]
    {
        let _ = native_show_window();
        // Close the native window directly so exit works even while fully hidden to tray.
        if native_close_window() {
            return;
        }
    }

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
        EnumWindows, GetWindow, GetWindowTextW, GetWindowThreadProcessId, GW_OWNER,
    };

    struct SearchCtx {
        pid: u32,
        hwnd: isize,
        fallback_rusty: isize,
        fallback_any: isize,
    }

    unsafe extern "system" fn enum_windows_cb(hwnd: HWND, lparam: LPARAM) -> BOOL {
        let ctx = &mut *(lparam as *mut SearchCtx);
        let mut win_pid: u32 = 0;
        GetWindowThreadProcessId(hwnd, &mut win_pid);
        if win_pid == ctx.pid && GetWindow(hwnd, GW_OWNER) == 0 {
            // Keep the first top-level window as a fallback, even if title is empty/mismatched.
            if ctx.fallback_any == 0 {
                ctx.fallback_any = hwnd;
            }
            let mut title_buf = [0u16; 256];
            let title_len = GetWindowTextW(hwnd, title_buf.as_mut_ptr(), title_buf.len() as i32);
            if title_len > 0 {
                let title = String::from_utf16_lossy(&title_buf[..title_len as usize]);
                if title.starts_with("Rusty - v") {
                    ctx.hwnd = hwnd;
                    return 0;
                }
                if title.starts_with("Rusty") && ctx.fallback_rusty == 0 {
                    ctx.fallback_rusty = hwnd;
                }
            }
        }
        1
    }

    let mut ctx = SearchCtx {
        pid: std::process::id(),
        hwnd: 0,
        fallback_rusty: 0,
        fallback_any: 0,
    };
    unsafe {
        EnumWindows(Some(enum_windows_cb), &mut ctx as *mut SearchCtx as LPARAM);
    }
    if ctx.hwnd != 0 {
        Some(ctx.hwnd)
    } else if ctx.fallback_rusty != 0 {
        Some(ctx.fallback_rusty)
    } else {
        (ctx.fallback_any != 0).then_some(ctx.fallback_any)
    }
}

#[cfg(target_os = "windows")]
fn main_window_hwnd() -> Option<isize> {
    let cached = MAIN_HWND.load(Ordering::Relaxed);
    if cached != 0 {
        return Some(cached);
    }

    let found = find_process_window()?;
    MAIN_HWND.store(found, Ordering::Relaxed);
    Some(found)
}

#[cfg(target_os = "windows")]
fn active_rusty_window_hwnd() -> Option<isize> {
    use windows_sys::Win32::Foundation::POINT;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetAncestor, GetCursorPos, GetForegroundWindow, GetWindowThreadProcessId, WindowFromPoint,
        GA_ROOT,
    };

    let pid_for = |hwnd: isize| -> bool {
        if hwnd == 0 {
            return false;
        }
        let mut pid = 0u32;
        unsafe {
            GetWindowThreadProcessId(hwnd, &mut pid);
        }
        pid == std::process::id()
    };

    let mut pt = POINT { x: 0, y: 0 };
    let cursor_hwnd = unsafe {
        if GetCursorPos(&mut pt) != 0 {
            let hwnd = WindowFromPoint(pt);
            if hwnd != 0 {
                GetAncestor(hwnd, GA_ROOT)
            } else {
                0
            }
        } else {
            0
        }
    };
    if pid_for(cursor_hwnd) {
        return Some(cursor_hwnd);
    }

    let foreground = unsafe { GetForegroundWindow() };
    if pid_for(foreground) {
        return Some(foreground);
    }

    main_window_hwnd()
}

#[cfg(target_os = "windows")]
fn custom_hit_test_result(width: i32, height: i32, x: i32, y: i32, maximized: bool) -> isize {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        HTBOTTOM, HTBOTTOMLEFT, HTBOTTOMRIGHT, HTCAPTION, HTCLIENT, HTLEFT, HTRIGHT, HTTOP,
        HTTOPLEFT, HTTOPRIGHT,
    };

    if width <= 0 || height <= 0 {
        return HTCLIENT as isize;
    }

    if !maximized {
        let left = x < CUSTOM_CHROME_RESIZE_MARGIN;
        let right = x >= width - CUSTOM_CHROME_RESIZE_MARGIN;
        let top = y < CUSTOM_CHROME_RESIZE_MARGIN;
        let bottom = y >= height - CUSTOM_CHROME_RESIZE_MARGIN;
        match (left, right, top, bottom) {
            (true, _, true, _) => return HTTOPLEFT as isize,
            (_, true, true, _) => return HTTOPRIGHT as isize,
            (true, _, _, true) => return HTBOTTOMLEFT as isize,
            (_, true, _, true) => return HTBOTTOMRIGHT as isize,
            (true, _, _, _) => return HTLEFT as isize,
            (_, true, _, _) => return HTRIGHT as isize,
            (_, _, true, _) => return HTTOP as isize,
            (_, _, _, true) => return HTBOTTOM as isize,
            _ => {}
        }
    }

    let caption_limit =
        (width - CUSTOM_CHROME_BUTTON_STRIP_WIDTH).max(CUSTOM_CHROME_RESIZE_MARGIN + 60);
    let within_caption_band =
        (CUSTOM_CHROME_RESIZE_MARGIN..CUSTOM_CHROME_TITLE_BAR_HEIGHT).contains(&y);
    let within_caption_width = x >= CUSTOM_CHROME_RESIZE_MARGIN && x < caption_limit;
    if within_caption_band && within_caption_width {
        HTCAPTION as isize
    } else {
        HTCLIENT as isize
    }
}

#[cfg(target_os = "windows")]
unsafe fn call_main_wndproc(hwnd: isize, msg: u32, wparam: usize, lparam: isize) -> isize {
    use windows_sys::Win32::UI::WindowsAndMessaging::{CallWindowProcW, DefWindowProcW, WNDPROC};

    let prev = MAIN_WNDPROC.load(Ordering::Relaxed);
    if prev != 0 {
        let proc: WNDPROC = std::mem::transmute(prev);
        CallWindowProcW(proc, hwnd, msg, wparam, lparam)
    } else {
        DefWindowProcW(hwnd, msg, wparam, lparam)
    }
}

#[cfg(target_os = "windows")]
unsafe extern "system" fn main_window_subclass_proc(
    hwnd: isize,
    msg: u32,
    wparam: usize,
    lparam: isize,
) -> isize {
    use windows_sys::Win32::Foundation::RECT;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetWindowRect, IsZoomed, WM_NCDESTROY, WM_NCHITTEST,
    };

    if msg == WM_NCHITTEST {
        let default_hit = call_main_wndproc(hwnd, msg, wparam, lparam);
        if default_hit != windows_sys::Win32::UI::WindowsAndMessaging::HTCLIENT as isize {
            return default_hit;
        }

        let mut rect = RECT {
            left: 0,
            top: 0,
            right: 0,
            bottom: 0,
        };
        if GetWindowRect(hwnd, &mut rect) != 0 {
            let x = (lparam as i32 as i16) as i32;
            let y = ((lparam >> 16) as i32 as i16) as i32;
            let local_x = x - rect.left;
            let local_y = y - rect.top;
            let width = rect.right - rect.left;
            let height = rect.bottom - rect.top;
            let maximized = IsZoomed(hwnd) != 0;
            return custom_hit_test_result(width, height, local_x, local_y, maximized);
        }

        return default_hit;
    }

    let result = call_main_wndproc(hwnd, msg, wparam, lparam);
    if msg == WM_NCDESTROY {
        HIT_TEST_HWND.store(0, Ordering::Relaxed);
        MAIN_WNDPROC.store(0, Ordering::Relaxed);
        MAIN_HWND.store(0, Ordering::Relaxed);
    }
    result
}

#[cfg(target_os = "windows")]
pub fn ensure_native_main_hit_test() {
    use windows_sys::Win32::UI::WindowsAndMessaging::{SetWindowLongPtrW, GWLP_WNDPROC};

    let Some(hwnd) = main_window_hwnd() else {
        return;
    };
    if HIT_TEST_HWND.load(Ordering::Relaxed) == hwnd {
        return;
    }

    #[cfg(target_pointer_width = "64")]
    let prev = unsafe {
        SetWindowLongPtrW(
            hwnd,
            GWLP_WNDPROC,
            main_window_subclass_proc as *const () as isize,
        ) as isize
    };

    #[cfg(target_pointer_width = "32")]
    let prev = unsafe {
        SetWindowLongPtrW(
            hwnd,
            GWLP_WNDPROC,
            main_window_subclass_proc as *const () as i32,
        ) as isize
    };
    if prev != 0 {
        MAIN_WNDPROC.store(prev, Ordering::Relaxed);
        HIT_TEST_HWND.store(hwnd, Ordering::Relaxed);
        MAIN_HWND.store(hwnd, Ordering::Relaxed);
    }
}

#[cfg(not(target_os = "windows"))]
pub fn ensure_native_main_hit_test() {}

#[cfg(target_os = "windows")]
fn native_show_window() -> bool {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        SetForegroundWindow, ShowWindowAsync, SW_RESTORE, SW_SHOW,
    };

    let Some(hwnd) = main_window_hwnd() else {
        return false;
    };

    unsafe {
        let _ = ShowWindowAsync(hwnd, SW_RESTORE);
        let _ = ShowWindowAsync(hwnd, SW_SHOW);
        let _ = SetForegroundWindow(hwnd);
    }
    true
}

#[cfg(target_os = "windows")]
fn native_close_window() -> bool {
    use windows_sys::Win32::UI::WindowsAndMessaging::{PostMessageW, WM_CLOSE};

    let Some(hwnd) = main_window_hwnd() else {
        return false;
    };

    unsafe { PostMessageW(hwnd, WM_CLOSE, 0, 0) != 0 }
}

#[cfg(target_os = "windows")]
pub fn begin_native_drag() -> bool {
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::ReleaseCapture;
    use windows_sys::Win32::UI::WindowsAndMessaging::{SendMessageW, HTCAPTION, WM_NCLBUTTONDOWN};

    let Some(hwnd) = active_rusty_window_hwnd() else {
        return false;
    };

    unsafe {
        let _ = ReleaseCapture();
        let _ = SendMessageW(hwnd, WM_NCLBUTTONDOWN, HTCAPTION as usize, 0);
    }
    true
}

#[cfg(not(target_os = "windows"))]
pub fn begin_native_drag() -> bool {
    false
}

#[cfg(target_os = "windows")]
pub fn begin_native_resize(dir: egui::ResizeDirection) -> bool {
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::ReleaseCapture;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        SendMessageW, HTBOTTOM, HTBOTTOMLEFT, HTBOTTOMRIGHT, HTLEFT, HTRIGHT, HTTOP, HTTOPLEFT,
        HTTOPRIGHT, WM_NCLBUTTONDOWN,
    };

    let hit = match dir {
        egui::ResizeDirection::NorthWest => HTTOPLEFT,
        egui::ResizeDirection::North => HTTOP,
        egui::ResizeDirection::NorthEast => HTTOPRIGHT,
        egui::ResizeDirection::East => HTRIGHT,
        egui::ResizeDirection::SouthEast => HTBOTTOMRIGHT,
        egui::ResizeDirection::South => HTBOTTOM,
        egui::ResizeDirection::SouthWest => HTBOTTOMLEFT,
        egui::ResizeDirection::West => HTLEFT,
    };

    let Some(hwnd) = active_rusty_window_hwnd() else {
        return false;
    };

    unsafe {
        let _ = ReleaseCapture();
        let _ = SendMessageW(hwnd, WM_NCLBUTTONDOWN, hit as usize, 0);
    }
    true
}

#[cfg(not(target_os = "windows"))]
pub fn begin_native_resize(_dir: egui::ResizeDirection) -> bool {
    false
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

#[cfg(all(test, target_os = "windows"))]
mod tests {
    use super::custom_hit_test_result;
    use windows_sys::Win32::UI::WindowsAndMessaging::{HTCAPTION, HTCLIENT, HTLEFT, HTTOPLEFT};

    #[test]
    fn caption_hit_test_uses_main_title_band() {
        let hit = custom_hit_test_result(1200, 800, 120, 14, false);
        assert_eq!(hit, HTCAPTION as isize);
    }

    #[test]
    fn button_strip_stays_client_area() {
        let hit = custom_hit_test_result(1200, 800, 1120, 14, false);
        assert_eq!(hit, HTCLIENT as isize);
    }

    #[test]
    fn resize_edges_take_priority_over_caption() {
        let left = custom_hit_test_result(1200, 800, 2, 120, false);
        let top_left = custom_hit_test_result(1200, 800, 2, 2, false);
        assert_eq!(left, HTLEFT as isize);
        assert_eq!(top_left, HTTOPLEFT as isize);
    }
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
        image::imageops::resize(
            &img,
            tray_px,
            tray_px,
            image::imageops::FilterType::Lanczos3,
        )
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
