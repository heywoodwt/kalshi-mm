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
/// Kalshi reports fills in the YES frame: `action` ALONE gives the
/// yes-equivalent direction (buy = acquire/long, sell = shed/short), and
/// `side` (yes/no) only selects which price field to read:
/// ```text
/// action=buy  -> long YES  (+size)
/// action=sell -> short YES (-size)
/// price_yes = side=="yes" ? yes_price : 1 - no_price
/// ```
/// This was verified against the exchange: summing an account's real fills
/// as buy=+ / sell=- reproduces `position_fp` exactly, whereas the earlier
/// `(action=="buy")==(side=="yes")` XNOR mis-signed every (sell, no) fill —
/// the bulk of real sells — booking a position-reducing sell as a
/// long-adding buy (the Python bot shares this latent bug).
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
        long_yes: action == "buy",
        price_yes,
        size,
        is_taker: fill.get("is_taker").and_then(Value::as_bool).unwrap_or(false),
        order_id,
    })
}

/// Entry-price transition rule for one fill, factored out of `apply_fill` so
/// it can also be replayed over fill history (see `replay_entry_price`)
/// without touching PnL, fees, or win/loss counters:
/// ```text
/// flat/flip -> entry = fill price
/// extended  -> size-weighted average of old entry and fill price
/// reduced   -> entry unchanged;  flattened -> entry cleared
/// ```
fn fold_entry_price(
    old_inv: i64,
    new_inv: i64,
    entry_before: Option<f64>,
    size: i64,
    price: f64,
) -> Option<f64> {
    if new_inv == 0 {
        None
    } else if old_inv == 0 || (old_inv > 0) != (new_inv > 0) {
        Some(price)
    } else if new_inv.abs() > old_inv.abs() {
        let entry_before = entry_before.unwrap_or(price);
        Some((old_inv.abs() as f64 * entry_before + size as f64 * price) / new_inv.abs() as f64)
    } else {
        entry_before
    }
}

/// Reconstruct (position, entry_price) for one ticker by replaying its full
/// fill history from flat, ignoring PnL/fees/win-loss (those were already
/// booked live when each fill first happened — or, for fills from before
/// this process existed, were never ours to book). This exists because
/// `entry_price` is in-memory only: a restart re-syncs `position` from the
/// exchange (authoritative) but has no cost basis for it, so the next fill
/// after a restart was silently treated as the position's entire history —
/// wildly overstating realized losses on the first close after a redeploy.
/// `fills` must be chronologically ascending (oldest first).
pub fn replay_entry_price(fills: &[NormalizedFill]) -> (i64, Option<f64>) {
    let mut position = 0i64;
    let mut entry_price = None;
    for nf in fills {
        let old_inv = position;
        position += if nf.long_yes { nf.size } else { -nf.size };
        entry_price = fold_entry_price(old_inv, position, entry_price, nf.size, nf.price_yes);
    }
    (position, entry_price)
}

/// One /portfolio/settlements record reduced to what booking needs.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedSettlement {
    pub ticker: String,
    /// Some(true)=settled yes, Some(false)=settled no. None for anything
    /// else (voided/scalar results) — the position is cleared without PnL.
    pub result_yes: Option<bool>,
}

/// Reduce a raw /portfolio/settlements entry. Returns None only when the
/// record is unusable (no ticker). The settlement `fee_cost` field is the
/// AGGREGATE of the trading fees already booked per-fill (verified against
/// live records), so it is deliberately not extracted — booking it again
/// would double-count fees.
pub fn normalize_settlement(rec: &Value) -> Option<NormalizedSettlement> {
    let ticker = str_field(rec, &["ticker", "market_ticker"])?;
    let result_yes = match rec.get("market_result").and_then(Value::as_str) {
        Some("yes") => Some(true),
        Some("no") => Some(false),
        _ => None,
    };
    Some(NormalizedSettlement {
        ticker: ticker.to_string(),
        result_yes,
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
    /// separately). Entry-price transitions follow `fold_entry_price`.
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

        ts.entry_price = fold_entry_price(old_inv, new_inv, ts.entry_price, nf.size, nf.price_yes);

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
            self.book_realized_close(category, realized);
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

    /// Book a market settlement for a still-open local position.
    ///
    /// This closes the accounting gap where winners that EXPIRE (ITM longs
    /// paying $1, shorts expiring worthless) never reached the PnL counters:
    /// only fill-based round trips were booked, so the log understated real
    /// PnL and the win/loss-driven kill switches saw a systematically
    /// pessimistic picture (losers get stopped out via fills; winners expire).
    ///
    /// In the YES frame a binary settles at $1 iff result==yes, else $0, so
    /// realized = position × (settle_value − entry). Wins/losses and the
    /// per-category consecutive-loss streak update exactly as a fill-based
    /// close would. The position and entry are cleared in all cases (the
    /// market no longer exists). Returns the realized PnL, or None when
    /// nothing was booked: ticker flat/unknown, no cost basis (post-restart
    /// backfill failed — inventing one would corrupt what this fixes), or a
    /// non-binary result (void = refunded at cost).
    pub fn apply_settlement(&mut self, ticker: &str, result_yes: Option<bool>) -> Option<f64> {
        let ts = self.tickers.get_mut(ticker)?;
        if ts.position == 0 {
            return None; // round trip already booked via fills
        }
        let position = ts.position;
        let entry = ts.entry_price;
        ts.position = 0;
        ts.entry_price = None;
        let (Some(entry), Some(result_yes)) = (entry, result_yes) else {
            return None;
        };
        let settle_value = if result_yes { 1.0 } else { 0.0 };
        let realized = position as f64 * (settle_value - entry);
        let category = ts.category.clone();
        ts.realized_pnl += realized;
        self.book_realized_close(category, realized);
        Some(realized)
    }

    /// Fold one realized close — fill-based or settlement-based — into the
    /// account counters: daily/cumulative PnL, win/loss tally, and the
    /// per-category consecutive-loss streak the kill switch reads. Shared so
    /// the two close paths can't drift apart. No-op at exactly zero (a
    /// scratch close is neither a win nor a loss).
    fn book_realized_close(&mut self, category: String, realized: f64) {
        if realized == 0.0 {
            return;
        }
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
    fn normalize_signs_by_action_reads_price_by_side() {
        // Direction is `action` alone (YES frame). `side` only picks the price.
        // buy yes @ 0.40 -> long, price 0.40
        let nf = normalize_fill(&fill_json("buy", "yes", 0.40, 1.0, false, "a")).unwrap();
        assert!(nf.long_yes);
        assert_eq!(nf.price_yes, 0.40);
        // sell no @ no 0.55 -> SHORT (the case the old XNOR mis-signed as long),
        // yes-equivalent price 0.45
        let nf = normalize_fill(&fill_json("sell", "no", 0.55, 1.0, false, "b")).unwrap();
        assert!(!nf.long_yes);
        assert!((nf.price_yes - 0.45).abs() < 1e-12);
        // sell yes @ 0.40 -> SHORT, price 0.40
        let nf = normalize_fill(&fill_json("sell", "yes", 0.40, 1.0, false, "c")).unwrap();
        assert!(!nf.long_yes);
        assert_eq!(nf.price_yes, 0.40);
    }

    #[test]
    fn position_matches_exchange_from_real_fills() {
        // Regression for the fill-sign bug: replaying the exact real fill
        // sequence that diverged in prod (buy 1, buy 1, sell 2, sell 4) must
        // land on the exchange-reported position of -4, not the old XNOR's +8.
        let mut s = TraderState::new(0.0);
        for (act, sd, px, n, id) in [
            ("buy", "yes", 0.70, 1.0, "r1"),
            ("buy", "yes", 0.71, 1.0, "r2"),
            ("sell", "no", 0.40, 2.0, "r3"), // sell 2 (yes-equiv 0.60)
            ("sell", "no", 0.45, 4.0, "r4"), // sell 4 (yes-equiv 0.55)
        ] {
            apply(&mut s, &fill_json(act, sd, px, n, false, id));
        }
        assert_eq!(s.tickers["KXADP-26JUL-T0"].position, -4);
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
    fn replay_reconstructs_weighted_average_entry_lost_by_restart() {
        // Regression for the entry-price-reset bug: a restart wipes
        // entry_price in memory but not position (position is re-synced from
        // the exchange), so the next live fill was silently treated as the
        // position's entire cost basis — wildly overstating realized PnL on
        // the first close after a redeploy. replay_entry_price must
        // reproduce exactly what a never-restarted bot would have.
        // Mirrors a real prod short: 4 (sell,no) extends to -4, weighted avg
        // entry 0.67 (verified against Kalshi's own realized_pnl on close).
        let live_fills = [
            fill_json("sell", "no", 0.30, 1.0, false, "a"), // yes-equiv 0.70
            fill_json("sell", "no", 0.32, 1.0, false, "b"), // yes-equiv 0.68
            fill_json("sell", "no", 0.32, 1.0, false, "c"), // yes-equiv 0.68
            fill_json("sell", "no", 0.38, 1.0, false, "d"), // yes-equiv 0.62
        ];
        let mut live = TraderState::new(0.0);
        for f in &live_fills {
            apply(&mut live, f);
        }
        let live_entry = live.tickers["KXADP-26JUL-T0"].entry_price.unwrap();
        assert!((live_entry - 0.67).abs() < 1e-9);

        let normalized: Vec<_> = live_fills.iter().map(|f| normalize_fill(f).unwrap()).collect();
        let (position, replayed_entry) = replay_entry_price(&normalized);
        assert_eq!(position, -4);
        assert!((replayed_entry.unwrap() - live_entry).abs() < 1e-9);
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

    /// Raw /portfolio/settlements record with the exact field shapes the
    /// live API returns (string counts/fees, integer-cents revenue).
    fn sett_json(ticker: &str, result: &str) -> Value {
        json!({
            "ticker": ticker, "event_ticker": "KXADP-26JUL",
            "market_result": result, "revenue": 100,
            "yes_count_fp": "1.00", "no_count_fp": "0.00",
            "yes_total_cost_dollars": "0.790000", "no_total_cost_dollars": "0.000000",
            "fee_cost": "0.018600", "settled_time": "2026-07-17T21:02:27.000000Z",
        })
    }

    #[test]
    fn normalize_settlement_parses_ticker_and_result() {
        let s = normalize_settlement(&sett_json("KXADP-26JUL-T0", "yes")).unwrap();
        assert_eq!(s.ticker, "KXADP-26JUL-T0");
        assert_eq!(s.result_yes, Some(true));
        let s = normalize_settlement(&sett_json("KXADP-26JUL-T0", "no")).unwrap();
        assert_eq!(s.result_yes, Some(false));
        // Voided/scalar results still identify the market but carry no
        // binary payout — caller clears the position without booking PnL
        let s = normalize_settlement(&sett_json("KXADP-26JUL-T0", "void")).unwrap();
        assert_eq!(s.result_yes, None);
        // No ticker -> unusable
        assert!(normalize_settlement(&json!({"market_result": "yes"})).is_none());
    }

    #[test]
    fn settlement_books_long_expiring_itm_as_win() {
        // The prod leak: buy 1 @ 0.79, market settles yes -> contract pays
        // $1.00. The +$0.21 was previously never booked (only the -$0.01 fee
        // was), so the log counter showed a loss on a profitable trade.
        let mut s = TraderState::new(0.0);
        apply(&mut s, &fill_json("buy", "yes", 0.79, 1.0, false, "a"));
        let realized = s.apply_settlement("KXADP-26JUL-T0", Some(true));
        assert!((realized.unwrap() - 0.21).abs() < 1e-9);
        let ts = &s.tickers["KXADP-26JUL-T0"];
        assert_eq!(ts.position, 0);
        assert_eq!(ts.entry_price, None);
        assert!((ts.realized_pnl - 0.21).abs() < 1e-9);
        assert_eq!((s.wins, s.losses), (1, 0));
        // 0.21 payout minus the $0.01 maker fee booked on the fill
        assert!((s.daily_pnl - 0.20).abs() < 1e-9);
        assert!((s.cumulative_pnl - 0.20).abs() < 1e-9);
        assert_eq!(s.consecutive_losses.get("KXADP").copied(), Some(0));
    }

    #[test]
    fn settlement_books_short_expiring_worthless_as_win() {
        // Prod case (FRAENG TIE): short 4 @ 0.24 avg, result no -> shorts
        // keep the full premium, +0.96
        let mut s = TraderState::new(0.0);
        for i in 0..4 {
            apply(&mut s, &fill_json("sell", "yes", 0.24, 1.0, false, &format!("s{i}")));
        }
        let realized = s.apply_settlement("KXADP-26JUL-T0", Some(false));
        assert!((realized.unwrap() - 0.96).abs() < 1e-9);
        assert_eq!(s.tickers["KXADP-26JUL-T0"].position, 0);
        assert_eq!(s.wins, 1);
    }

    #[test]
    fn settlement_books_short_run_over_as_loss_feeding_kill_switch() {
        // Short 2 @ 0.232 settles yes -> pay $1 each: -2 * (1 - 0.232)
        let mut s = TraderState::new(0.0);
        apply(&mut s, &fill_json("sell", "yes", 0.232, 2.0, false, "s"));
        let realized = s.apply_settlement("KXADP-26JUL-T0", Some(true));
        assert!((realized.unwrap() - (-1.536)).abs() < 1e-9);
        assert_eq!((s.wins, s.losses), (0, 1));
        // Settlement losses must feed the same per-category streak the
        // fill-based kill switch reads, or expiring losers evade it
        assert_eq!(s.consecutive_losses.get("KXADP").copied(), Some(1));
    }

    #[test]
    fn settlement_books_nothing_when_flat_or_unknown() {
        let mut s = TraderState::new(0.0);
        // Never-traded ticker: nothing to book
        assert_eq!(s.apply_settlement("KXADP-26JUL-T0", Some(true)), None);
        // Flat ticker (round trip already booked via fills): nothing to book
        apply(&mut s, &fill_json("buy", "yes", 0.40, 1.0, false, "a"));
        apply(&mut s, &fill_json("sell", "yes", 0.45, 1.0, false, "b"));
        assert_eq!(s.apply_settlement("KXADP-26JUL-T0", Some(true)), None);
        assert_eq!((s.wins, s.losses), (1, 0)); // unchanged by settlement

        // Unknown cost basis (post-restart, backfill failed): clear the
        // position so it stops counting as open, but book no PnL — a made-up
        // basis would corrupt the counters this exists to fix
        let ts = s.ticker("KXADP-26JUL-T1", "KXADP");
        ts.position = 3;
        ts.entry_price = None;
        assert_eq!(s.apply_settlement("KXADP-26JUL-T1", Some(true)), None);
        assert_eq!(s.tickers["KXADP-26JUL-T1"].position, 0);

        // Void result: refunded at cost, clear without PnL
        let ts = s.ticker("KXADP-26JUL-T2", "KXADP");
        ts.position = -2;
        ts.entry_price = Some(0.30);
        let daily_before = s.daily_pnl;
        assert_eq!(s.apply_settlement("KXADP-26JUL-T2", None), None);
        assert_eq!(s.tickers["KXADP-26JUL-T2"].position, 0);
        assert!((s.daily_pnl - daily_before).abs() < 1e-12);
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
