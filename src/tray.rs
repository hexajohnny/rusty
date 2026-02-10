use std::sync::Once;

use crossbeam_channel::{unbounded, Receiver, Sender};
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

pub fn install_handlers() -> Receiver<TrayAppEvent> {
    INIT.call_once(|| {
        let tx_menu = CHANNEL.0.clone();
        MenuEvent::set_event_handler(Some(move |ev: MenuEvent| {
            let _ = tx_menu.send(TrayAppEvent::Menu(ev.id));
        }));

        let tx_tray = CHANNEL.0.clone();
        TrayIconEvent::set_event_handler(Some(move |ev: TrayIconEvent| {
            let _ = tx_tray.send(TrayAppEvent::Tray(ev));
        }));
    });

    CHANNEL.1.clone()
}

pub struct TrayState {
    #[allow(dead_code)]
    tray: TrayIcon,
    pub show_hide_item: MenuItem,
    pub exit_item: MenuItem,
    pub show_hide_id: MenuId,
    pub exit_id: MenuId,
}

fn load_tray_icon() -> anyhow::Result<Icon> {
    let bytes = include_bytes!("../assets/icon.png");
    let img = image::load_from_memory(bytes)?.into_rgba8();
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
        .with_tooltip("Rusty SSH")
        .with_menu(Box::new(menu))
        .with_icon(icon)
        .build()?;

    Ok(TrayState {
        tray,
        show_hide_id: show_hide_item.id().clone(),
        exit_id: exit_item.id().clone(),
        show_hide_item,
        exit_item,
    })
}

