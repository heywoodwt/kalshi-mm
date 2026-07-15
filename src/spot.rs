//! Spot price feed: pure state (EMA, windowed returns, staleness) plus the
//! Coinbase WebSocket task that feeds it. Only the pure part is unit-tested;
//! the WS task mirrors transport.rs's reconnect pattern.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::transport::WsEvent;

/// Ticks older than this are pruned (must exceed the largest ret() window).
const RETAIN_S: f64 = 120.0;
/// A >2% tick-to-tick jump within this many seconds is a bad print.
const OUTLIER_JUMP: f64 = 0.02;
const OUTLIER_WINDOW_S: f64 = 10.0;
/// Reconnect backoff: initial/max, and how long a connection must live to
/// count as healthy (resets the backoff instead of compounding it).
const BACKOFF_INITIAL_S: f64 = 1.0;
const BACKOFF_MAX_S: f64 = 60.0;
const BACKOFF_RESET_AFTER_S: f64 = 30.0;

/// Pure rolling state for one spot product. Times are monotonic seconds.
#[derive(Debug)]
pub struct SpotState {
    ticks: VecDeque<(f64, f64)>, // (mono_s, price)
    ema: Option<f64>,
    tau_s: f64,
}

impl SpotState {
    pub fn new(tau_s: f64) -> Self {
        assert!(tau_s > 0.0, "spot EMA tau_s must be positive (got {tau_s})");
        Self { ticks: VecDeque::new(), ema: None, tau_s }
    }

    /// Fold one tick. Returns false when the outlier guard dropped it.
    pub fn on_tick(&mut self, price: f64, now_mono: f64) -> bool {
        if !price.is_finite() || price <= 0.0 {
            return false; // rejects NaN/inf/non-positive junk prints
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

pub const COINBASE_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

/// Price from a Coinbase `ticker` channel message, or None for anything else.
pub fn parse_ticker_price(msg: &Value) -> Option<f64> {
    if msg.get("type").and_then(Value::as_str) != Some("ticker") {
        return None;
    }
    match msg.get("price")? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Public (unauthenticated) spot feed. Same lifecycle as WsClient: pump
/// until drop, reconnect with backoff, forever while `running`.
pub struct SpotFeed {
    url: String,
    product_id: String,
}

impl SpotFeed {
    pub fn new(url: &str, product_id: &str) -> Self {
        Self { url: url.to_string(), product_id: product_id.to_string() }
    }

    pub async fn run(&self, tx: mpsc::Sender<WsEvent>, running: Arc<AtomicBool>) {
        let mut delay = BACKOFF_INITIAL_S;
        while running.load(Ordering::Relaxed) {
            let attempt_start = std::time::Instant::now();
            match self.connect_and_pump(&tx, &running).await {
                Ok(()) => warn!("Spot feed disconnected, reconnecting in {delay:.0}s..."),
                Err(e) => error!("Spot feed error: {e}, reconnecting in {delay:.0}s..."),
            }
            // A connection that lived a while was healthy — treat the drop as
            // a fresh blip, not a flapping loop (backoff exists for the latter)
            delay = if attempt_start.elapsed().as_secs_f64() >= BACKOFF_RESET_AFTER_S {
                BACKOFF_INITIAL_S
            } else {
                (delay * 2.0).min(BACKOFF_MAX_S)
            };
            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
        }
    }

    async fn connect_and_pump(
        &self,
        tx: &mpsc::Sender<WsEvent>,
        running: &Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        info!("Connecting to spot feed: {} ({})", self.url, self.product_id);
        let (ws, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("Spot feed connect")?;
        let (mut sink, mut stream) = ws.split();
        let sub = json!({
            "type": "subscribe",
            "product_ids": [self.product_id],
            "channels": ["ticker"],
        });
        sink.send(Message::Text(sub.to_string())).await?;
        info!("✓ Spot feed connected");

        while let Some(msg) = stream.next().await {
            if !running.load(Ordering::Relaxed) {
                return Ok(());
            }
            match msg.context("Spot feed read")? {
                Message::Text(text) => {
                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
                        if let Some(price) = parse_ticker_price(&data) {
                            // Latest-value stream: drop ticks when the shared
                            // channel is saturated instead of blocking the
                            // read loop (a newer tick supersedes anyway)
                            if let Err(mpsc::error::TrySendError::Closed(_)) =
                                tx.try_send(WsEvent::Spot { price })
                            {
                                return Ok(()); // trader gone = shutdown
                            }
                        } else {
                            match data.get("type").and_then(Value::as_str) {
                                Some("subscriptions") => info!("✓ Spot feed subscription confirmed"),
                                Some("error") => error!("Spot feed rejected request: {data}"),
                                _ => {}
                            }
                        }
                    }
                }
                Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                Message::Close(frame) => {
                    debug!("Spot feed close frame: {frame:?}");
                    return Ok(());
                }
                _ => {}
            }
        }
        Ok(())
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
    #[should_panic(expected = "tau_s must be positive")]
    fn zero_tau_rejected() {
        SpotState::new(0.0);
    }

    #[test]
    fn buffer_is_pruned() {
        let mut s = SpotState::new(60.0);
        for i in 0..500 {
            s.on_tick(100.0, i as f64);
        }
        assert!(s.len() <= 122); // 120s retention + endpoints
    }

    #[test]
    fn parses_coinbase_ticker_messages() {
        use serde_json::json;
        let m = json!({"type": "ticker", "product_id": "BTC-USD", "price": "64123.45"});
        assert_eq!(parse_ticker_price(&m), Some(64123.45));
        let m = json!({"type": "ticker", "price": 64123.45}); // numeric price tolerated
        assert_eq!(parse_ticker_price(&m), Some(64123.45));
        assert_eq!(parse_ticker_price(&json!({"type": "subscriptions"})), None);
        assert_eq!(parse_ticker_price(&json!({"type": "ticker", "price": "junk"})), None);
    }
}
