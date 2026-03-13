# Rusty SSH Review

Date: 2026-03-10

Scope: review of the SSH transport, terminal/input handling, SFTP flow, transfer manager, config persistence, and related UI behavior.

## Confirmed Findings


### Medium Priority

- Mouse reporting is only partially implemented.
  The terminal tracks xterm mouse modes, but the UI only sends primary click and release. Right-click is consumed for paste, drag/move reporting is incomplete, and non-SGR encodings are treated as a simplified default path.
  Relevant code: `src/app/11_impl_terminal_helpers.rs:256`, `src/app/11_impl_terminal_helpers.rs:337`, `src/app/11_impl_terminal_helpers.rs:626`


- File-manager tabs also use the full shell and PTY connection path instead of an SFTP-only worker.
  That wastes a shell session, increases coupling, and makes SFTP behavior depend on shell-worker lifecycle.
  Relevant code: `src/app/00_all.rs:736`, `src/app/10_impl_core.rs:1052`, `src/ssh.rs:2099`

- Host key rotation has no remediation path.
  Unknown host keys prompt interactively, but changed keys hard-fail. There is no UI for deliberate replacement after server rekeying.
  Relevant code: `src/ssh.rs:1605`

- Transfers write directly to final target paths.
  Partial downloads can be left behind under the real destination name after failure or cancellation. Uploads also restart from zero rather than using a resumable strategy.
  Relevant code: `src/ssh.rs:856`, `src/ssh.rs:1070`

## Improvements

- Split session types by concern.
  Separate shell, SFTP, and transfer workers while sharing auth and host-key logic. Right now too much behavior is routed through the shell worker.

- Fail closed on secret storage.
  If config encryption fails, either refuse to persist secrets or strip sensitive fields before saving. Do not write plaintext credentials as fallback behavior.

- Preserve damaged config files.
  If config parsing or decrypting fails, rename the bad file to something like `config.corrupt-<timestamp>.json` and surface an error in the UI instead of silently resetting to defaults.

- Make alternate-screen behavior configurable.
  Some users may want inline history preservation, but it should be opt-in. Default terminal behavior should match normal SSH clients.

- Improve transfer durability.
  Download to a temp file, fsync or flush appropriately, then rename into place on success. For uploads, add resume support or make restart semantics explicit in the UI.

- Replace string-based retry detection.
  `is_retryable_transfer_error` currently mixes typed IO checks with message matching. That is brittle and will miss library-specific errors or misclassify future ones.

- Add clearer failure modes in the UI.
  Distinguish authentication failures, host-key failures, permission errors, path errors, and transport disconnects in the status and transfer surfaces.

- Stop hard-locking rendering to DX12/FXC only.
  `src/main.rs` forces DX12 and `fxc`. That may cause avoidable startup failures on some Windows environments where Vulkan or a different DX12 compiler path would work.
  Relevant code: `src/main.rs:15`, `src/main.rs:29`

- Get `cargo clippy --all-targets -- -D warnings` green.
  The code currently fails Clippy on refactor-quality issues including too-many-arguments, question-mark suggestions, duplicated branches, and type-complexity warnings.

- Add meaningful automated tests.
  Current tests only cover terminal-theme parsing. There is no coverage for SSH auth flows, transfer retry behavior, known-host handling, or terminal input encoding.

## Suggested Features

### SSH Core

- Support `~/.ssh/config` import and resolution.
- Support multiple identity files per profile.
- Support `ProxyJump` and bastion hosts.
- Support local, remote, and dynamic port forwarding.
- Support agent forwarding.
- Support per-profile startup command, working directory, and environment variables.
- Support configurable TERM and terminal profile per connection.

### Authentication and Trust

- Add a known-hosts manager UI.
- Add a host-key replacement flow for legitimate key rotation.
- Support agent-only and key-only profile modes explicitly.
- Improve keyboard-interactive UX for multi-step MFA prompts.

### Terminal

- Implement proper alternate-screen behavior.
- Add F-key support and more complete modified key sequences.
- Add Alt/meta key handling.
- Add backtab and modified arrow/home/end sequences.
- Add OSC 52 clipboard support where appropriate.
- Add URL and hyperlink detection.
- Add scrollback search.
- Add optional bell notifications or visual bell.

### SFTP and File Manager

- Support recursive directory upload/download.
- Support directory upload from local folder selection.
- Support multi-select operations.
- Support drag-and-drop upload.
- Support sortable list or table view in addition to cards.
- Support hidden-file toggle.
- Support chmod/chown where server permits it.
- Support symlink display and operations.
- Add conflict policy options for upload/download overwrite behavior.

### Transfers

- Add ETA and per-transfer elapsed time.
- Add checksum verification after transfer.
- Add configurable concurrency limits.
- Add pause/resume semantics that distinguish real resume from restart.
- Add conflict-safe destination naming.
- Add retry policy configuration per profile or globally.

### Session UX

- Reuse authenticated connections between terminal and SFTP panes.
- Add reconnect action for disconnected sessions.
- Add pane cloning.
- Add broadcast input to multiple terminals.
- Add session templates for common hosts or workflows.

## Validation Notes

- `cargo test` passed during review.
- `cargo clippy --all-targets -- -D warnings` failed with 14 warnings promoted to errors.
- No live SSH/manual smoke test was run against a server in this review session.

