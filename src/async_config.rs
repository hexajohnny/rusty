use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::config::AppConfig;

/// Background config saver to keep file I/O off the UI thread.
///
/// Design goals:
/// - Coalesce bursts of updates (debounce) to reduce writes.
/// - Never block the UI thread on disk I/O in the common case.
/// - Provide an explicit `flush()` for orderly shutdown.
pub struct AsyncConfigSaver {
    tx: mpsc::Sender<Msg>,
    handle: Option<thread::JoinHandle<()>>,
}

enum Msg {
    Save(AppConfig),
    Flush(mpsc::Sender<()>),
    Shutdown,
}

impl AsyncConfigSaver {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Msg>();
        let handle = thread::Builder::new()
            .name("rusty-config-saver".to_string())
            .spawn(move || saver_thread(rx))
            .ok();
        Self { tx, handle }
    }

    /// Request a save. This is best-effort and returns immediately.
    pub fn request_save(&self, cfg: AppConfig) {
        let _ = self.tx.send(Msg::Save(cfg));
    }

    /// Flush any pending save and wait for completion (bounded by a timeout).
    pub fn flush(&self, timeout: Duration) {
        let (ack_tx, ack_rx) = mpsc::channel::<()>();
        if self.tx.send(Msg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv_timeout(timeout);
        }
    }
}

impl Drop for AsyncConfigSaver {
    fn drop(&mut self) {
        // Best-effort: ask the thread to stop. Any pending save is handled by the thread.
        let _ = self.tx.send(Msg::Shutdown);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn saver_thread(rx: mpsc::Receiver<Msg>) {
    let mut pending: Option<AppConfig> = None;
    loop {
        // Wait for the next message.
        let msg = match rx.recv() {
            Ok(m) => m,
            Err(_) => return,
        };

        match msg {
            Msg::Save(cfg) => {
                pending = Some(cfg);
                // Debounce: gather any more updates for a short time.
                loop {
                    match rx.recv_timeout(Duration::from_millis(250)) {
                        Ok(Msg::Save(cfg)) => pending = Some(cfg),
                        Ok(Msg::Flush(ack)) => {
                            if let Some(cfg) = pending.take() {
                                crate::config::save(&cfg);
                            }
                            let _ = ack.send(());
                        }
                        Ok(Msg::Shutdown) => {
                            if let Some(cfg) = pending.take() {
                                crate::config::save(&cfg);
                            }
                            return;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => break,
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                }

                if let Some(cfg) = pending.take() {
                    crate::config::save(&cfg);
                }
            }
            Msg::Flush(ack) => {
                if let Some(cfg) = pending.take() {
                    crate::config::save(&cfg);
                }
                let _ = ack.send(());
            }
            Msg::Shutdown => {
                if let Some(cfg) = pending.take() {
                    crate::config::save(&cfg);
                }
                return;
            }
        }
    }
}

