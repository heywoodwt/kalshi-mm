//! Per-ticker and account-level trading state + fill accounting.
//!
//! Port of the fill-booking logic in `live_trader_v2.py::_process_fill`,
//! restructured as one TickerState struct per market (the Python version
//! kept ~15 parallel dicts keyed by ticker).
//!
//! Timestamps are plain floats: epoch seconds for wall-clock facts (fills,
//! market close), monotonic seconds for intervals (quote throttle, backoff).

use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;

use crate::book::{ceil_cent_fee, Book, TradePrint};

/// Fill-id dedupe only needs to cover the fills endpoint's 60s overlap
/// re-reads; 20k ids is hours of headroom at any realistic fill rate while
/// keeping memory flat over a multi-week run.
const FILL_ID_CAP: usize = 20_000;
const TRADE_LOG_CAP: usize = 10_000;

/// Maker/taker fee at Kalshi's variance-based schedule (ceil to next cent).
pub fn fill_fee(rate: f64, contracts: i64, price_yes: f64) -> f64 {
    ceil_cent_fee(rate, contracts as f64, price_yes)
}

/// One exchange fill folded into YES-equivalent terms.
#[derive(Debug, Clone)]
pub struct NormalizedFill {
    pub fill_id: String,
    pub ticker: String,
    /// True = this fill made us longer YES.
    pub long_yes: bool,
    /// Dollars, YES-equivalent.
    pub price_yes: f64,
    /// Whole contracts.
    pub size: i64,
    pub is_taker: bool,
    pub order_id: Option<String>,
}

fn num_field(fill: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        match fill.get(*key) {
            Some(Value::Number(n)) => return n.as_f64(),
            Some(Value::String(s)) => return s.parse().ok(),
            Some(Value::Null) | None => continue,
            _ => continue,
        }
    }
    None
}

fn str_field<'a>(fill: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| fill.get(*k).and_then(Value::as_str)).filter(|s| !s.is_empty())
}

/// Fold a raw /portfolio/fills entry into YES-equivalent terms.
///
/// ```text
/// buy yes / sell no -> long YES,  price = yes_price
/// sell yes / buy no -> short YES, price = 1 - no_price
/// ```
///
/// Returns None for malformed fills and for sub-1-contract fractional fills
/// (count_fp can be 0.5; flooring those to 0 while still booking fees and
/// counters corrupted the accounting — position sync reconciles any drift).
pub fn normalize_fill(fill: &Value) -> Option<NormalizedFill> {
    let ticker = str_field(fill, &["market_ticker", "ticker"])?;
    let action = fill.get("action").and_then(Value::as_str).unwrap_or("");
    let side = fill.get("side").and_then(Value::as_str).unwrap_or("");
    if !matches!(action, "buy" | "sell") || !matches!(side, "yes" | "no") {
        return None;
    }
    let price_yes = if side == "yes" {
        num_field(fill, &["yes_price_dollars", "yes_price"]).unwrap_or(0.0)
    } else {
        1.0 - num_field(fill, &["no_price_dollars", "no_price"]).unwrap_or(0.0)
    };
    // round_ties_even matches Python's round(): count_fp 0.5 -> 0 (rejected),
    // NOT 1 — f64::round would round half away from zero and book phantom fills
    let size = num_field(fill, &["count_fp", "count"]).unwrap_or(1.0).round_ties_even() as i64;
    if size <= 0 {
        return None;
    }
    let order_id = fill.get("order_id").and_then(Value::as_str).map(str::to_string);
    let fill_id = str_field(fill, &["trade_id", "fill_id"])
        .map(str::to_string)
        .unwrap_or_else(|| {
            // Last-resort id: order id + exchange timestamp
            let ts = num_field(fill, &["ts"]).unwrap_or(0.0);
            format!("{}-{}", order_id.as_deref().unwrap_or(""), ts)
        });
    Some(NormalizedFill {
        fill_id,
        ticker: ticker.to_string(),
        long_yes: (action == "buy") == (side == "yes"),
        price_yes,
        size,
        is_taker: fill.get("is_taker").and_then(Value::as_bool).unwrap_or(false),
        order_id,
    })
}

/// Everything the bot tracks about one market.
#[derive(Debug, Default)]
pub struct TickerState {
    pub category: String,
    pub book: Book,
    /// Signed YES-equivalent inventory.
    pub position: i64,
    /// Weighted avg entry; None when flat.
    pub entry_price: Option<f64>,
    pub fills_buy: i64,
    pub fills_sell: i64,
    /// Per-ticker realized PnL — feeds obs [14].
    pub realized_pnl: f64,
    /// Bounded (20) by sample_mid; one entry per 60s window.
    pub mid_history: Vec<f64>,
    /// Monotonic ts of the last 60s mid sample.
    pub last_mid_sample: f64,
    /// Pruned in place by trade_window_features.
    pub recent_trades: Vec<TradePrint>,
    /// Monotonic; 1s quote throttle (engine advances it).
    pub last_quote_time: f64,
    /// Dollars — last quoted prices (tick-move keep check).
    pub quoted_bid: Option<f64>,
    pub quoted_ask: Option<f64>,
    /// Resting order ids.
    pub bid_order_id: Option<String>,
    pub ask_order_id: Option<String>,
    /// Exempts the ticker from the max_open_orders budget once it has
    /// quoted (matches the Python active_orders membership semantics).
    pub ever_quoted: bool,
    /// Market close, epoch seconds (parsed once at startup).
    pub close_time_s: Option<f64>,
    /// Price granularity in dollars (0.001 = subpenny market).
    pub tick: f64,
}

impl TickerState {
    pub fn new(category: &str) -> Self {
        Self {
            category: category.to_string(),
            tick: 0.01,
            ..Default::default()
        }
    }
}

/// Account-level state plus the per-ticker map.
#[derive(Debug, Default)]
pub struct TraderState {
    pub tickers: HashMap<String, TickerState>,
    pub daily_pnl: f64,
    pub cumulative_pnl: f64,
    pub quotes_sent: u64,
    pub fills: u64,
    pub wins: u64,
    pub losses: u64,
    pub taker_fills: u64,
    pub maker_fills: u64,
    pub fees_paid: f64,
    pub halted_categories: HashSet<String>,
    pub consecutive_losses: HashMap<String, u64>,
    /// Monotonic deadline; quoting pauses after insufficient_balance.
    pub balance_backoff_until: f64,
    /// (epoch_s, ticker, side, price_cents, size) — bounded ring.
    pub trade_log: VecDeque<(f64, String, &'static str, f64, i64)>,
    /// Epoch seconds of the last daily counter reset.
    pub last_reset: f64,
    // Bounded dedupe: set for O(1) lookup, deque for FIFO eviction.
    fill_ids: HashSet<String>,
    fill_id_fifo: VecDeque<String>,
}

impl TraderState {
    pub fn new(now_epoch_s: f64) -> Self {
        Self {
            last_reset: now_epoch_s,
            ..Default::default()
        }
    }

    /// Get-or-create the TickerState for a market.
    pub fn ticker(&mut self, ticker: &str, category: &str) -> &mut TickerState {
        self.tickers
            .entry(ticker.to_string())
            .or_insert_with(|| TickerState::new(category))
    }

    /// True if this fill id was already processed; records it otherwise.
    pub fn seen_fill(&mut self, fill_id: &str) -> bool {
        if self.fill_ids.contains(fill_id) {
            return true;
        }
        self.fill_ids.insert(fill_id.to_string());
        self.fill_id_fifo.push_back(fill_id.to_string());
        if self.fill_id_fifo.len() > FILL_ID_CAP {
            if let Some(old) = self.fill_id_fifo.pop_front() {
                self.fill_ids.remove(&old);
            }
        }
        false
    }

    /// Concurrent resting orders across all tickers (max_open_orders cap).
    pub fn open_order_count(&self) -> usize {
        self.tickers
            .values()
            .map(|ts| usize::from(ts.bid_order_id.is_some()) + usize::from(ts.ask_order_id.is_some()))
            .sum()
    }

    pub fn fill_rate(&self) -> f64 {
        if self.quotes_sent == 0 {
            return 0.0;
        }
        self.fills as f64 / self.quotes_sent as f64
    }

    pub fn win_rate(&self) -> f64 {
        let total = self.wins + self.losses;
        if total == 0 {
            return 0.0;
        }
        self.wins as f64 / total as f64
    }

    /// Capital locked in open positions (contracts * entry price).
    ///
    /// With `active`, counts only markets this bot quotes — legacy positions
    /// from prior deployments must not consume the risk budget (the limit
    /// caps risk THIS bot adds, not inventory it can't unwind).
    pub fn position_value(&self, active: Option<&HashSet<String>>) -> f64 {
        let mut total = 0.0;
        for (ticker, ts) in &self.tickers {
            if ts.position == 0 {
                continue;
            }
            if let Some(active) = active {
                if !active.contains(ticker) {
                    continue;
                }
            }
            let entry = ts.entry_price.unwrap_or(0.50); // unknown entry -> 0.50
            total += ts.position.unsigned_abs() as f64 * entry;
        }
        total
    }

    /// Book one exchange fill: fee, position, entry price, realized PnL.
    /// Returns the realized PnL (gross of fees — the fee is booked here too,
    /// separately). Entry-price transitions:
    /// ```text
    /// flat/flip -> entry = fill price
    /// extended  -> size-weighted average of old entry and fill price
    /// reduced   -> entry unchanged;  flattened -> entry cleared
    /// ```
    /// Realized PnL always uses the PRE-fill entry price.
    pub fn apply_fill(&mut self, ticker: &str, nf: &NormalizedFill, fee: f64, now_epoch_s: f64) -> f64 {
        // Fee at the actual rate the exchange charged (maker or 4x taker)
        self.daily_pnl -= fee;
        self.cumulative_pnl -= fee;
        self.fees_paid += fee;
        if nf.is_taker {
            self.taker_fills += 1;
        } else {
            self.maker_fills += 1;
        }

        let ts = self
            .tickers
            .get_mut(ticker)
            .expect("apply_fill requires an existing TickerState");

        let old_inv = ts.position;
        let entry_before = ts.entry_price.unwrap_or(nf.price_yes);
        if nf.long_yes {
            ts.position += nf.size;
            ts.fills_buy += nf.size;
        } else {
            ts.position -= nf.size;
            ts.fills_sell += nf.size;
        }
        let new_inv = ts.position;

        if new_inv == 0 {
            ts.entry_price = None;
        } else if old_inv == 0 || (old_inv > 0) != (new_inv > 0) {
            ts.entry_price = Some(nf.price_yes); // opened or flipped
        } else if new_inv.abs() > old_inv.abs() {
            // Extended the same direction — size-weighted average entry
            ts.entry_price = Some(
                (old_inv.abs() as f64 * entry_before + nf.size as f64 * nf.price_yes)
                    / new_inv.abs() as f64,
            );
        }
        // else: reduced toward zero — entry unchanged

        // A consumed resting order must release its slot so the next quote
        // cycle replaces that side instead of skipping it
        if let Some(oid) = &nf.order_id {
            if ts.bid_order_id.as_deref() == Some(oid) {
                ts.bid_order_id = None;
            }
            if ts.ask_order_id.as_deref() == Some(oid) {
                ts.ask_order_id = None;
            }
        }

        // Realized PnL when the fill moves inventory toward zero
        let mut realized = 0.0;
        if !nf.long_yes && old_inv > 0 {
            // Sell closes (part of) a long
            realized = nf.size.min(old_inv) as f64 * (nf.price_yes - entry_before);
        } else if nf.long_yes && old_inv < 0 {
            // Buy closes (part of) a short
            realized = nf.size.min(old_inv.abs()) as f64 * (entry_before - nf.price_yes);
        }
        if realized != 0.0 {
            ts.realized_pnl += realized;
            let category = ts.category.clone();
            self.daily_pnl += realized;
            self.cumulative_pnl += realized;
            if realized > 0.0 {
                self.wins += 1;
                self.consecutive_losses.insert(category, 0);
            } else {
                self.losses += 1;
                *self.consecutive_losses.entry(category).or_insert(0) += 1;
            }
        }

        self.fills += 1;
        self.trade_log.push_back((
            now_epoch_s,
            nf.ticker.clone(),
            if nf.long_yes { "buy" } else { "sell" },
            nf.price_yes * 100.0,
            nf.size,
        ));
        if self.trade_log.len() > TRADE_LOG_CAP {
            self.trade_log.pop_front();
        }
        realized
    }

    /// Zero the daily counters (called at local-date rollover).
    pub fn reset_daily(&mut self, now_epoch_s: f64) {
        self.daily_pnl = 0.0;
        self.quotes_sent = 0;
        self.fills = 0;
        self.last_reset = now_epoch_s;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MAKER: f64 = 0.0175;
    const TAKER: f64 = 0.07;

    fn fill_json(action: &str, side: &str, price: f64, count: f64, taker: bool, id: &str) -> Value {
        let price_key = if side == "yes" { "yes_price_dollars" } else { "no_price_dollars" };
        json!({
            "trade_id": id, "market_ticker": "KXADP-26JUL-T0",
            "action": action, "side": side, price_key: price,
            "count": count, "is_taker": taker,
        })
    }

    /// Normalize, dedupe, compute fee at the charged rate, book.
    fn apply(state: &mut TraderState, raw: &Value) -> f64 {
        let nf = normalize_fill(raw).expect("valid fill");
        if state.seen_fill(&nf.fill_id) {
            return 0.0;
        }
        let rate = if nf.is_taker { TAKER } else { MAKER };
        let fee = fill_fee(rate, nf.size, nf.price_yes);
        state.ticker(&nf.ticker.clone(), "KXADP");
        state.apply_fill(&nf.ticker.clone(), &nf, fee, 0.0)
    }

    #[test]
    fn normalize_yes_buy_and_no_buy() {
        let nf = normalize_fill(&fill_json("buy", "yes", 0.40, 1.0, false, "a")).unwrap();
        assert!(nf.long_yes);
        assert_eq!(nf.price_yes, 0.40);
        // Buying NO @ 0.55 = shorting YES @ 0.45
        let nf = normalize_fill(&fill_json("buy", "no", 0.55, 1.0, false, "b")).unwrap();
        assert!(!nf.long_yes);
        assert!((nf.price_yes - 0.45).abs() < 1e-12);
    }

    #[test]
    fn normalize_rejects_sub1_fractional() {
        let raw = json!({"trade_id": "x", "market_ticker": "T", "action": "buy",
                         "side": "yes", "yes_price_dollars": 0.5, "count_fp": "0.5"});
        assert!(normalize_fill(&raw).is_none());
    }

    #[test]
    fn round_trip_books_fees_and_realized_pnl() {
        let mut s = TraderState::new(0.0);
        // Maker buy 1 @ 0.40: fee ceil(0.0175*0.40*0.60*100)/100 = $0.01
        apply(&mut s, &fill_json("buy", "yes", 0.40, 1.0, false, "f1"));
        assert_eq!(s.tickers["KXADP-26JUL-T0"].position, 1);
        assert!((s.daily_pnl - (-0.01)).abs() < 1e-12);
        // Maker sell 1 @ 0.45 flattens: +0.05 gross, -0.01 fee.
        // Regression: realized PnL must use the PRE-fill entry (0.40).
        let realized = apply(&mut s, &fill_json("sell", "yes", 0.45, 1.0, false, "f2"));
        assert!((realized - 0.05).abs() < 1e-12);
        let ts = &s.tickers["KXADP-26JUL-T0"];
        assert_eq!(ts.position, 0);
        assert_eq!(ts.entry_price, None);
        assert!((s.daily_pnl - 0.03).abs() < 1e-12);
        assert_eq!(s.wins, 1);
        assert!((ts.realized_pnl - 0.05).abs() < 1e-12);
    }

    #[test]
    fn consecutive_losses_track_and_reset() {
        // Underpins the per-category kill switch in main.rs::process_fill,
        // which halts a category once this counter reaches the config limit.
        let mut s = TraderState::new(0.0);
        // Two losing round-trips in a row (buy 0.50, sell 0.45 = -0.05 each)
        for i in 0..2 {
            apply(&mut s, &fill_json("buy", "yes", 0.50, 1.0, false, &format!("b{i}")));
            apply(&mut s, &fill_json("sell", "yes", 0.45, 1.0, false, &format!("s{i}")));
        }
        assert_eq!(s.consecutive_losses.get("KXADP").copied(), Some(2));
        // A winning close (buy 0.50, sell 0.60) resets the streak to 0
        apply(&mut s, &fill_json("buy", "yes", 0.50, 1.0, false, "bw"));
        apply(&mut s, &fill_json("sell", "yes", 0.60, 1.0, false, "sw"));
        assert_eq!(s.consecutive_losses.get("KXADP").copied(), Some(0));
    }

    #[test]
    fn taker_fill_charged_at_taker_rate() {
        let mut s = TraderState::new(0.0);
        // Taker buy 1 @ 0.50: fee ceil(0.07*0.25*100)/100 = $0.02
        apply(&mut s, &fill_json("buy", "yes", 0.50, 1.0, true, "t1"));
        assert!((s.daily_pnl - (-0.02)).abs() < 1e-12);
        assert_eq!((s.taker_fills, s.maker_fills), (1, 0));
    }

    #[test]
    fn duplicate_fill_ids_ignored() {
        let mut s = TraderState::new(0.0);
        let f = fill_json("buy", "yes", 0.40, 1.0, false, "dup");
        apply(&mut s, &f);
        apply(&mut s, &f); // fills endpoint overlaps windows by design
        assert_eq!(s.tickers["KXADP-26JUL-T0"].position, 1);
        assert_eq!(s.fills, 1);
    }

    #[test]
    fn extend_uses_weighted_average_entry() {
        let mut s = TraderState::new(0.0);
        apply(&mut s, &fill_json("buy", "yes", 0.40, 1.0, false, "a"));
        apply(&mut s, &fill_json("buy", "yes", 0.50, 1.0, false, "b"));
        let ts = &s.tickers["KXADP-26JUL-T0"];
        assert_eq!(ts.position, 2);
        assert!((ts.entry_price.unwrap() - 0.45).abs() < 1e-12);
    }

    #[test]
    fn reduce_keeps_entry_price() {
        let mut s = TraderState::new(0.0);
        apply(&mut s, &fill_json("buy", "yes", 0.40, 2.0, false, "a"));
        apply(&mut s, &fill_json("sell", "yes", 0.44, 1.0, false, "b"));
        let ts = &s.tickers["KXADP-26JUL-T0"];
        assert_eq!(ts.position, 1);
        assert_eq!(ts.entry_price, Some(0.40));
    }

    #[test]
    fn flip_resets_entry_to_fill_price() {
        let mut s = TraderState::new(0.0);
        apply(&mut s, &fill_json("buy", "yes", 0.40, 1.0, false, "a"));
        apply(&mut s, &fill_json("sell", "yes", 0.44, 2.0, false, "b"));
        let ts = &s.tickers["KXADP-26JUL-T0"];
        assert_eq!(ts.position, -1);
        assert_eq!(ts.entry_price, Some(0.44));
    }

    #[test]
    fn short_round_trip_realizes_gain() {
        let mut s = TraderState::new(0.0);
        apply(&mut s, &fill_json("sell", "yes", 0.60, 1.0, false, "a")); // short @ .60
        assert_eq!(s.tickers["KXADP-26JUL-T0"].entry_price, Some(0.60));
        let realized = apply(&mut s, &fill_json("buy", "yes", 0.55, 1.0, false, "b"));
        assert!((realized - 0.05).abs() < 1e-12);
        assert_eq!(s.tickers["KXADP-26JUL-T0"].position, 0);
    }

    #[test]
    fn fill_releases_consumed_order_id() {
        let mut s = TraderState::new(0.0);
        s.ticker("KXADP-26JUL-T0", "KXADP").bid_order_id = Some("o-1".into());
        let mut raw = fill_json("buy", "yes", 0.40, 1.0, false, "f");
        raw["order_id"] = json!("o-1");
        let nf = normalize_fill(&raw).unwrap();
        let fee = fill_fee(MAKER, nf.size, nf.price_yes);
        s.apply_fill("KXADP-26JUL-T0", &nf, fee, 0.0);
        assert_eq!(s.tickers["KXADP-26JUL-T0"].bid_order_id, None);
    }

    #[test]
    fn dedupe_is_bounded() {
        let mut s = TraderState::new(0.0);
        for i in 0..25_000 {
            s.seen_fill(&format!("id-{i}"));
        }
        assert!(s.fill_ids.len() <= FILL_ID_CAP);
        // Newest ids still deduped, oldest evicted
        assert!(s.seen_fill("id-24999"));
        assert!(!s.seen_fill("id-0"));
    }

    #[test]
    fn position_value_scopes_to_active() {
        let mut s = TraderState::new(0.0);
        let a = s.ticker("A", "CAT");
        a.position = 2;
        a.entry_price = Some(0.40);
        let b = s.ticker("B", "OLD");
        b.position = 5;
        b.entry_price = Some(0.50);
        let active: HashSet<String> = ["A".to_string()].into();
        assert!((s.position_value(Some(&active)) - 0.80).abs() < 1e-12);
        assert!((s.position_value(None) - 3.30).abs() < 1e-12);
    }
}
