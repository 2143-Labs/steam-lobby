use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Simple in-memory sliding-window rate limiter keyed by IP.
/// Per-process state only: behind a reverse proxy all clients share the
/// proxy's IP, so the limit applies globally there (acceptable fail-closed
/// behavior for a single-instance service).
pub struct RateLimiter {
    max: usize,
    window: Duration,
    hits: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
}

impl RateLimiter {
    pub fn new(max: usize, window: Duration) -> Self {
        Self {
            max,
            window,
            hits: Mutex::new(HashMap::new()),
        }
    }

    /// Returns true if the call is allowed; records the hit when allowed.
    pub fn check(&self, ip: IpAddr) -> bool {
        let mut hits = self.hits.lock().unwrap();
        let q = hits.entry(ip).or_default();
        while q.front().is_some_and(|t| t.elapsed() > self.window) {
            q.pop_front();
        }
        if q.len() >= self.max {
            return false;
        }
        q.push_back(Instant::now());
        true
    }
}
