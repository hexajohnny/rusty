// Split from a large single file into smaller pieces to keep each `.rs` under 1000 LOC.
// These includes are concatenated in order, so the module behavior is unchanged.

include!("app/00_all.rs");
include!("app/10_impl_core.rs");
include!("app/11_impl_terminal_helpers.rs");
include!("app/12_impl_settings.rs");
include!("app/13_impl_terminal_view.rs");
include!("app/20_tiles.rs");
include!("app/21_impl_file_manager_view.rs");
include!("app/22_impl_downloads_window.rs");
include!("app/30_app_impl.rs");
include!("app/40_free.rs");
