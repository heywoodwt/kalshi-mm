//! Canonical live orderbook + pure quote-safety helpers.
//!
//! Direct port of `rl_bot/live/book.py`. Kalshi delivers book data in three
//! shapes with conflicting orderings (REST snapshots best-LAST, WS snapshots
//! one side per message, WS deltas single-level) — so levels are stored in a
//! BTreeMap keyed by price and the best level is always computed, never
//! index-dependent.
//!
//! Everything is in YES terms:
//!   "yes" side = bids to buy YES  -> best bid = max yes price
//!   "no" side  = bids to buy NO   -> a NO bid at p sells YES at 1-p,
//!                                    so best ask = 1 - max(no price)
//!
//! Prices are u32 MILLIDOLLARS (1 = $0.001, the subpenny tick) — integer
//! keys make the float-noise defenses of the Python version unnecessary at
//! the storage layer. Helper math converts to f64 dollars exactly where the
//! Python does float math, so parity fixtures can demand tight tolerances.

use serde_json::Value;

/// $1.00 in millidollars.
pub const ONE_DOLLAR_MD: u32 = 1000;

/// Convert dollars to millidollar key (rounding kills upstream float noise,
/// same job as the Python `round(price, 4)` key normalization).
pub fn to_md(dollars: f64) -> u32 {
    (dollars * 1000.0).round() as u32
}

/// Millidollars back to f64 dollars (exact: md <= 1000).
pub fn to_dollars(md: u32) -> f64 {
    f64::from(md) / 1000.0
}

/// Round to 4 decimals — mirrors Python's `_KEY_DECIMALS` rounding in the
/// pure helpers (clamp_quotes), where intermediate math is float.
fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

fn round6(x: f64) -> f64 {
    (x * 1_000_000.0).round() / 1_000_000.0
}

/// Per-fill fee rounded UP to the next cent — Kalshi's schedule.
/// fee = ceil(rate * contracts * p * (1-p) * 100) / 100
pub fn ceil_cent_fee(rate: f64, contracts: f64, price: f64) -> f64 {
    (rate * contracts * price * (1.0 - price) * 100.0).ceil() / 100.0
}

/// Which side the taker of a public trade print was on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerSide {
    Yes,
    No,
}

/// One public trade print: (arrival epoch seconds, contracts, taker side).
pub type TradePrint = (f64, i64, TakerSide);

/// Orderbook state for one market, safe against level-ordering bugs.
#[derive(Debug, Default, Clone)]
pub struct Book {
    yes: std::collections::BTreeMap<u32, f64>, // YES bid price -> contracts
    no: std::collections::BTreeMap<u32, f64>,  // NO bid price -> contracts
}

impl Book {
    #[allow(dead_code)] // tests construct via new(); binary uses Default
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse `[[price, qty], ...]` (any order; numbers or numeric strings)
    /// into a level map, dropping empty levels.
    fn parse_levels(levels: &Value) -> std::collections::BTreeMap<u32, f64> {
        let mut out = std::collections::BTreeMap::new();
        let Some(rows) = levels.as_array() else {
            return out;
        };
        for row in rows {
            let Some(pair) = row.as_array() else { continue };
            if pair.len() < 2 {
                continue;
            }
            let (Some(price), Some(qty)) = (json_num(&pair[0]), json_num(&pair[1])) else {
                continue;
            };
            if qty > 0.0 {
                out.insert(to_md(price), qty);
            }
        }
        out
    }

    /// Load a REST or WS snapshot. Replaces only the sides present, so
    /// Kalshi's one-side-per-message WS snapshots merge correctly.
    pub fn load_snapshot(&mut self, payload: &Value) {
        // Key variants across REST and WS API versions, checked in order.
        for key in ["yes_dollars_fp", "yes_dollars", "yes"] {
            if let Some(levels) = payload.get(key) {
                self.yes = Self::parse_levels(levels);
                break;
            }
        }
        for key in ["no_dollars_fp", "no_dollars", "no"] {
            if let Some(levels) = payload.get(key) {
                self.no = Self::parse_levels(levels);
                break;
            }
        }
    }

    /// Apply a WS orderbook_delta: adjust one level's quantity; levels at
    /// qty <= 0 are removed.
    pub fn apply_delta(&mut self, side: &str, price_dollars: f64, delta_qty: f64) {
        let levels = if side == "yes" { &mut self.yes } else { &mut self.no };
        let key = to_md(price_dollars);
        let new_qty = levels.get(&key).copied().unwrap_or(0.0) + delta_qty;
        if new_qty > 0.0 {
            levels.insert(key, new_qty);
        } else {
            levels.remove(&key);
        }
    }

    /// Highest YES bid in millidollars.
    pub fn best_bid_md(&self) -> Option<u32> {
        self.yes.keys().next_back().copied()
    }

    /// Lowest YES ask = $1 - highest NO bid, in millidollars.
    ///
    /// saturating_sub guards malformed data: a NO bid priced above $1.00
    /// implies a YES ask below $0.00, which is not a real ask. Saturating to
    /// 0 makes is_valid() reject the book (0 can't exceed a real bid) — the
    /// same outcome as Python, whose `1.0 - no_price` goes negative and fails
    /// the identical ask > bid check. Plain `-` would underflow: a debug-build
    /// panic (crashing the bot) or a release-build wrap to a huge bogus ask
    /// that poisons mid/spread for the ticker.
    pub fn best_ask_md(&self) -> Option<u32> {
        self.no.keys().next_back().map(|p| ONE_DOLLAR_MD.saturating_sub(*p))
    }

    /// True when both sides exist and the book is not crossed.
    pub fn is_valid(&self) -> bool {
        match (self.best_bid_md(), self.best_ask_md()) {
            (Some(bid), Some(ask)) => ask > bid,
            _ => false,
        }
    }

    /// Best bid in dollars (call only after is_valid()).
    pub fn best_bid(&self) -> Option<f64> {
        self.best_bid_md().map(to_dollars)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.best_ask_md().map(to_dollars)
    }

    /// Midpoint of a valid book in dollars.
    pub fn mid(&self) -> Option<f64> {
        if !self.is_valid() {
            return None;
        }
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }

    #[allow(dead_code)] // engine computes spread inline; kept for parity tooling
    pub fn spread(&self) -> Option<f64> {
        if !self.is_valid() {
            return None;
        }
        Some(self.best_ask()? - self.best_bid()?)
    }

    /// Top n YES bid levels as (price dollars, qty), best first.
    pub fn bid_levels(&self, n: usize) -> Vec<(f64, f64)> {
        self.yes
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &q)| (to_dollars(p), q))
            .collect()
    }

    /// Top n YES ask levels as (price dollars, qty), best (lowest ask)
    /// first. Prices converted from NO bids; sizes are NO-side sizes.
    pub fn ask_levels(&self, n: usize) -> Vec<(f64, f64)> {
        // saturating_sub for the same malformed-data guard as best_ask_md
        // (a NO price > $1.00 can't underflow the ask into a wrap/panic).
        self.no
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &q)| (to_dollars(ONE_DOLLAR_MD.saturating_sub(p)), q))
            .collect()
    }
}

fn json_num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

// --- pure quote-safety helpers (no I/O) --------------------------------------

/// Force quotes to be passive: bid stays under the market ask, ask stays
/// over the market bid. A crossing "quote" executes as taker (spread + 7%
/// fee) — the opposite of market making. Returns None when the clamped
/// quotes would cross each other (market too tight to quote both sides).
pub fn clamp_quotes(
    bid: f64,
    ask: f64,
    best_bid: f64,
    best_ask: f64,
    tick: f64,
) -> Option<(f64, f64)> {
    // Round before comparing — float subtraction noise would otherwise let
    // a bid==ask pair slip through the crossing check (same as Python).
    let bid = round4(bid.min(best_ask - tick));
    let ask = round4(ask.max(best_bid + tick));
    if ask <= bid {
        return None;
    }
    Some((bid, ask))
}

/// True when the quoted spread beats round-trip ceil'd maker fees plus a
/// minimum edge, per contract. Kalshi rounds each fill's fee UP to the next
/// cent, so at size=1 a round trip near 0.50 costs $0.02 regardless of the
/// nominal 1.75% rate — quoting a spread fees consume guarantees losses.
pub fn quote_edge_ok(bid: f64, ask: f64, fee_rate: f64, size: i64, min_edge: f64) -> bool {
    let size_f = size as f64;
    let fee_buy = ceil_cent_fee(fee_rate, size_f, bid);
    let fee_sell = ceil_cent_fee(fee_rate, size_f, ask);
    let captured = round6((ask - bid) * size_f);
    captured > round6(fee_buy + fee_sell + min_edge * size_f)
}

/// Maintain a mid-price history sampled on the training cadence: one entry
/// per 60s window, last entry refreshed in place while the window is open.
/// (Momentum/velocity/vol and the 0.05 vol threshold were all calibrated on
/// 60s windows — per-tick appends ran them ~60x too fast.)
/// Mutates `hist`; returns the (possibly advanced) sample timestamp.
pub fn sample_mid(hist: &mut Vec<f64>, last_sample_ts: f64, now_ts: f64, mid: f64) -> f64 {
    const WINDOW_S: f64 = 60.0;
    const MAX_LEN: usize = 20;
    if hist.is_empty() || now_ts - last_sample_ts >= WINDOW_S {
        hist.push(mid);
        if hist.len() > MAX_LEN {
            let drop = hist.len() - MAX_LEN;
            hist.drain(..drop);
        }
        return now_ts;
    }
    *hist.last_mut().unwrap() = mid; // same window still open — update in place
    last_sample_ts
}

/// Trade-print retention window (seconds) — obs [9]/[16] look back this far.
pub const TRADE_WINDOW_S: f64 = 60.0;

/// Drop trade prints older than the 60s window. Prints arrive time-ordered,
/// so the stale ones are a prefix. Called on every incoming print (which
/// bounds memory even for a market whose book stays one-sided and so never
/// builds an observation) as well as by trade_window_features.
pub fn prune_trades(trades: &mut Vec<TradePrint>, now_s: f64) {
    let cutoff = now_s - TRADE_WINDOW_S;
    let stale = trades.iter().take_while(|t| t.0 < cutoff).count();
    trades.drain(..stale);
}

/// Obs [9] (trade volume) and [16] (flow imbalance) from real trade prints,
/// exactly as training computes them per 60s window:
///   volume_1m = number of PRINTS / 50, capped at 1.0
///   flow      = (buy_vol - sell_vol) / total contracts, where "buy" means
///               taker_side == No (taker bought NO -> the maker bought YES)
///               — matching mm_env's side encoding, not intuition.
pub fn trade_window_features(trades: &mut Vec<TradePrint>, now_s: f64) -> (f64, f64) {
    prune_trades(trades, now_s);
    if trades.is_empty() {
        return (0.0, 0.0);
    }
    let buy_vol: i64 = trades.iter().filter(|t| t.2 == TakerSide::No).map(|t| t.1).sum();
    let sell_vol: i64 = trades.iter().filter(|t| t.2 == TakerSide::Yes).map(|t| t.1).sum();
    let volume_1m = (trades.len() as f64 / 50.0).min(1.0);
    let flow = (buy_vol - sell_vol) as f64 / (buy_vol + sell_vol).max(1) as f64;
    (volume_1m, flow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn best_levels_are_computed_not_positional() {
        let mut b = Book::new();
        // Ascending arrays (REST order: best LAST) must still yield the max
        b.load_snapshot(&json!({
            "yes": [[0.38, 10], [0.39, 30], [0.40, 50]],
            "no":  [[0.56, 5], [0.57, 20], [0.58, 40]],
        }));
        assert_eq!(b.best_bid_md(), Some(400));
        assert_eq!(b.best_ask_md(), Some(420)); // 1 - 0.58
        assert!(b.is_valid());
        // mid is f64 dollar math — same float result Python produces
        assert!((b.mid().unwrap() - 0.41).abs() < 1e-12);
        assert_eq!(b.bid_levels(2), vec![(0.40, 50.0), (0.39, 30.0)]);
        assert_eq!(b.ask_levels(2), vec![(0.42, 40.0), (0.43, 20.0)]);
    }

    #[test]
    fn one_sided_snapshots_merge() {
        let mut b = Book::new();
        b.load_snapshot(&json!({"yes": [[0.40, 50]]}));
        assert!(!b.is_valid()); // no ask side yet
        b.load_snapshot(&json!({"no": [[0.58, 40]]}));
        assert!(b.is_valid()); // yes side survived the no-only snapshot
        assert_eq!(b.best_bid_md(), Some(400));
    }

    #[test]
    fn string_prices_and_fp_keys_parse() {
        let mut b = Book::new();
        b.load_snapshot(&json!({
            "yes_dollars_fp": [["0.401", "12.5"]],
            "no_dollars_fp": [["0.58", "3"]],
        }));
        assert_eq!(b.best_bid_md(), Some(401)); // subpenny level survives
        assert_eq!(b.bid_levels(1), vec![(0.401, 12.5)]);
    }

    #[test]
    fn delta_adds_and_removes_levels() {
        let mut b = Book::new();
        b.load_snapshot(&json!({"yes": [[0.40, 5]], "no": [[0.58, 4]]}));
        b.apply_delta("yes", 0.41, 3.0);
        assert_eq!(b.best_bid_md(), Some(410));
        b.apply_delta("yes", 0.41, -3.0); // removes the level entirely
        assert_eq!(b.best_bid_md(), Some(400));
        b.apply_delta("no", 0.58, -4.0);
        assert!(!b.is_valid());
    }

    #[test]
    fn no_price_above_one_dollar_is_invalid_not_panic() {
        // Malformed data: a NO bid above $1.00 implies a YES ask below $0.00.
        // Must not underflow (debug panic / release wrap to a huge bogus ask);
        // the book is invalid, exactly as Python's negative ask fails ask>bid.
        let mut b = Book::new();
        b.load_snapshot(&json!({"yes": [[0.40, 50]], "no": [[1.05, 10]]}));
        assert_eq!(b.best_ask_md(), Some(0)); // saturated to 0, not wrapped
        assert!(!b.is_valid());
        assert_eq!(b.mid(), None);
        assert_eq!(b.ask_levels(1), vec![(0.0, 10.0)]); // no panic in the depth path
        // A valid NO level behind the bad one does NOT rescue the book: the
        // best (max) NO is still the bad level, same as Python's max(no).
        let mut b2 = Book::new();
        b2.load_snapshot(&json!({"yes": [[0.40, 50]], "no": [[1.05, 10], [0.58, 40]]}));
        assert!(!b2.is_valid());
    }

    #[test]
    fn crossed_book_is_invalid() {
        let mut b = Book::new();
        // bid 0.50, ask 1-0.55=0.45 -> crossed
        b.load_snapshot(&json!({"yes": [[0.50, 1]], "no": [[0.55, 1]]}));
        assert!(!b.is_valid());
    }

    #[test]
    fn clamp_forces_passive_quotes() {
        // Quote crossing the touch is pulled back a tick
        let (bid, ask) = clamp_quotes(0.45, 0.46, 0.40, 0.42, 0.01).unwrap();
        assert_eq!(bid, 0.41); // min(0.45, 0.42-0.01)
        assert_eq!(ask, 0.46); // max(0.46, 0.41)
        // Quoting AT the touch is passive and allowed (matches Python)
        assert_eq!(clamp_quotes(0.41, 0.42, 0.41, 0.42, 0.01), Some((0.41, 0.42)));
        // None requires spread <= 2 ticks with both quotes clamped into each
        // other: aggressive two-sided quote into a 2c-wide market collapses
        assert!(clamp_quotes(0.45, 0.38, 0.40, 0.42, 0.01).is_none());
    }

    #[test]
    fn fee_gate_matches_python_cases() {
        // From tests/test_live_book.py: 3c spread at mid prices beats 2x 1c fees
        assert!(quote_edge_ok(0.49, 0.52, 0.0175, 1, 0.0));
        assert!(!quote_edge_ok(0.49, 0.52, 0.0175, 1, 0.02));
        // Extreme prices still pay ceil'd 1c/side -> 3c spread passes
        assert!(quote_edge_ok(0.06, 0.09, 0.0175, 1, 0.0));
        // 2c spread never beats 2c round-trip fees
        assert!(!quote_edge_ok(0.49, 0.51, 0.0175, 1, 0.0));
    }

    #[test]
    fn ceil_cent_fee_values() {
        assert_eq!(ceil_cent_fee(0.0175, 1.0, 0.40), 0.01); // maker buy @ .40
        assert_eq!(ceil_cent_fee(0.07, 1.0, 0.50), 0.02); // taker @ .50
    }

    #[test]
    fn sample_mid_training_cadence() {
        let mut hist = vec![];
        let t0 = sample_mid(&mut hist, 0.0, 100.0, 0.40);
        assert_eq!((t0, hist.clone()), (100.0, vec![0.40]));
        // Same window: refresh in place, timestamp unchanged
        let t1 = sample_mid(&mut hist, t0, 130.0, 0.42);
        assert_eq!((t1, hist.clone()), (100.0, vec![0.42]));
        // New window: append
        let t2 = sample_mid(&mut hist, t1, 161.0, 0.44);
        assert_eq!((t2, hist.clone()), (161.0, vec![0.42, 0.44]));
        // Bounded at 20
        let mut long = (0..25).map(|i| i as f64).collect::<Vec<_>>();
        sample_mid(&mut long, 0.0, 100.0, 99.0);
        assert_eq!(long.len(), 20);
        assert_eq!(*long.last().unwrap(), 99.0);
    }

    #[test]
    fn trade_window_volume_and_flow() {
        // Taker bought NO -> maker bought YES -> counts as buy flow
        let mut trades: Vec<TradePrint> = vec![
            (40.0, 5, TakerSide::No),   // stale (window is 60s from now=105)
            (100.0, 10, TakerSide::No), // buy 10
            (101.0, 4, TakerSide::Yes), // sell 4
        ];
        let (vol, flow) = trade_window_features(&mut trades, 105.0);
        assert_eq!(trades.len(), 2); // stale print pruned
        assert_eq!(vol, 2.0 / 50.0);
        assert!((flow - (10.0 - 4.0) / 14.0).abs() < 1e-12);
        // Empty window
        let mut empty: Vec<TradePrint> = vec![];
        assert_eq!(trade_window_features(&mut empty, 0.0), (0.0, 0.0));
    }
}
