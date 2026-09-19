//! Timestamped transcript output, so a run reads as a sequence of observable
//! events rather than an undifferentiated log.

use std::fmt::Display;
use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();

/// Anchor the transcript clock at process start.
pub fn start() {
    let _ = START.get_or_init(Instant::now);
}

/// Seconds since [`start`], formatted for a fixed-width transcript gutter.
fn elapsed() -> String {
    match START.get() {
        Some(start) => format!("{:>6.1}s", start.elapsed().as_secs_f64()),
        None => "     ?s".to_string(),
    }
}

/// One transcript line, tagged with the emitting player.
pub fn line(tag: &str, message: impl Display) {
    println!("[{}] {:<3} {message}", elapsed(), tag);
}

/// A line about the run itself rather than one player.
pub fn note(message: impl Display) {
    line("---", message);
}
