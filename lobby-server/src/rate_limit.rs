//! In-memory sliding-window rate limiter keyed by client IP. Per-process
//! state only — behind a reverse proxy all clients share one budget
//! (acceptable fail-closed behavior for a single-instance service).
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::thread;

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, n))
    }

    #[test]
    fn allows_up_to_max_then_blocks() {
        let rl = RateLimiter::new(3, Duration::from_secs(60));
        assert!(rl.check(ip(1)));
        assert!(rl.check(ip(1)));
        assert!(rl.check(ip(1)));
        assert!(
            !rl.check(ip(1)),
            "4th hit within the window must be rejected"
        );
    }

    #[test]
    fn distinct_ips_are_isolated() {
        let rl = RateLimiter::new(1, Duration::from_secs(60));
        assert!(rl.check(ip(1)));
        assert!(rl.check(ip(2)), "a fresh IP has its own budget");
        assert!(!rl.check(ip(1)), "the exhausted IP stays exhausted");
    }

    #[test]
    fn expired_hits_free_slots() {
        let rl = RateLimiter::new(1, Duration::from_millis(50));
        assert!(rl.check(ip(1)));
        assert!(!rl.check(ip(1)));
        thread::sleep(Duration::from_millis(60));
        assert!(rl.check(ip(1)), "after the window, the slot opens again");
    }

    #[test]
    fn zero_max_rejects_everything() {
        let rl = RateLimiter::new(0, Duration::from_secs(60));
        assert!(!rl.check(ip(1)));
    }
}
