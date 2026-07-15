//! Spot price feed: pure state (EMA, windowed returns, staleness) plus the
//! Coinbase WebSocket task that feeds it. Only the pure part is unit-tested;
//! the WS task mirrors transport.rs's reconnect pattern.

use std::collections::VecDeque;

/// Ticks older than this are pruned (must exceed the largest ret() window).
const RETAIN_S: f64 = 120.0;
/// A >2% tick-to-tick jump within this many seconds is a bad print.
const OUTLIER_JUMP: f64 = 0.02;
const OUTLIER_WINDOW_S: f64 = 10.0;

/// Pure rolling state for one spot product. Times are monotonic seconds.
#[derive(Debug)]
pub struct SpotState {
    ticks: VecDeque<(f64, f64)>, // (mono_s, price)
    ema: Option<f64>,
    tau_s: f64,
}

impl SpotState {
    pub fn new(tau_s: f64) -> Self {
        Self { ticks: VecDeque::new(), ema: None, tau_s }
    }

    /// Fold one tick. Returns false when the outlier guard dropped it.
    pub fn on_tick(&mut self, price: f64, now_mono: f64) -> bool {
        if !(price > 0.0) {
            return false;
        }
        if let Some(&(t_last, p_last)) = self.ticks.back() {
            let fast = now_mono - t_last < OUTLIER_WINDOW_S;
            if fast && (price / p_last - 1.0).abs() > OUTLIER_JUMP {
                return false;
            }
            // Irregular-interval EMA: alpha = 1 - exp(-dt/tau)
            let dt = (now_mono - t_last).max(0.0);
            let alpha = 1.0 - (-dt / self.tau_s).exp();
            let ema = self.ema.unwrap_or(p_last);
            self.ema = Some(ema + alpha * (price - ema));
        } else {
            self.ema = Some(price);
        }
        self.ticks.push_back((now_mono, price));
        while self.ticks.front().is_some_and(|&(t, _)| now_mono - t > RETAIN_S) {
            self.ticks.pop_front();
        }
        true
    }

    pub fn latest(&self) -> Option<f64> {
        self.ticks.back().map(|&(_, p)| p)
    }

    pub fn ema(&self) -> Option<f64> {
        self.ema
    }

    /// Return over `window_s`, or None until the buffer spans the window
    /// (callers treat None as "gated" — no history, no quoting).
    pub fn ret(&self, window_s: f64, now_mono: f64) -> Option<f64> {
        let &(_, p_now) = self.ticks.back()?;
        // Newest tick at least window_s old serves as the base
        let &(_, p_base) = self.ticks.iter().rev().find(|&&(t, _)| now_mono - t >= window_s)?;
        Some(p_now / p_base - 1.0)
    }

    pub fn is_stale(&self, now_mono: f64, max_age_s: f64) -> bool {
        self.ticks.back().is_none_or(|&(t, _)| now_mono - t > max_age_s)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.ticks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ema_converges_and_lags_linear_drift() {
        let mut s = SpotState::new(60.0);
        // Constant price: EMA == price
        for i in 0..10 {
            assert!(s.on_tick(100.0, i as f64));
        }
        assert!((s.ema().unwrap() - 100.0).abs() < 1e-9);
        // Linear drift v=1.0 $/s: after t >> tau, spot - ema ≈ v * tau = 60
        let mut s = SpotState::new(60.0);
        for i in 0..600 {
            s.on_tick(1000.0 + i as f64, i as f64);
        }
        let lag = s.latest().unwrap() - s.ema().unwrap();
        assert!((lag - 60.0).abs() < 3.0, "lag {lag} not ~60");
    }

    #[test]
    fn ret_requires_full_window() {
        let mut s = SpotState::new(60.0);
        s.on_tick(100.0, 0.0);
        s.on_tick(101.0, 30.0);
        assert_eq!(s.ret(60.0, 30.0), None); // buffer doesn't span 60s yet
        s.on_tick(102.0, 61.0);
        let r = s.ret(60.0, 61.0).unwrap(); // base = tick at t=0 (age 61 >= 60)
        assert!((r - 0.02).abs() < 1e-9);
    }

    #[test]
    fn staleness() {
        let mut s = SpotState::new(60.0);
        assert!(s.is_stale(0.0, 10.0)); // no ticks ever
        s.on_tick(100.0, 100.0);
        assert!(!s.is_stale(105.0, 10.0));
        assert!(s.is_stale(111.0, 10.0));
    }

    #[test]
    fn outlier_guard_drops_fast_jumps_but_allows_gaps() {
        let mut s = SpotState::new(60.0);
        s.on_tick(100.0, 0.0);
        // >2% jump 1s later: bad print, dropped
        assert!(!s.on_tick(103.0, 1.0));
        assert_eq!(s.latest(), Some(100.0));
        // Same jump but 11s after the last ACCEPTED tick: genuine gap, kept
        assert!(s.on_tick(103.0, 11.0));
        assert_eq!(s.latest(), Some(103.0));
    }

    #[test]
    fn buffer_is_pruned() {
        let mut s = SpotState::new(60.0);
        for i in 0..500 {
            s.on_tick(100.0, i as f64);
        }
        assert!(s.len() <= 122); // 120s retention + endpoints
    }
}
