use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub fn log_line<P: AsRef<Path>>(path: P, line: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}
