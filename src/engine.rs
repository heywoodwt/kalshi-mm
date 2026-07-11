//! Pure quote / exit / risk decision logic — no I/O, no clock reads.
//!
//! Port of `live_trader_v2.py::_execute_action` / `_position_exit_loop` /
//! `_check_risk_limits`. Ordering, thresholds, and early-returns are copied
//! exactly — the executor performs plans, main.rs wires everything together.
//!
//! Rounding note: Python's round() is round-half-even on the binary double;
//! `round_ties_even` after decimal scaling reproduces it (differences would
//! need a value within one ulp of a .5 decimal boundary — not observed in
//! practice, and the parity fixtures would catch one).

use std::collections::HashSet;

use crate::book::{clamp_quotes, quote_edge_ok};
use crate::config::{LiveConfig, MmParams};
use crate::state::{TickerState, TraderState};

/// Exit thresholds (from _position_exit_loop).
pub const EXPIRY_BUFFER_S: f64 = 120.0; // exit when market closes sooner
pub const STOP_LOSS_FLOOR: f64 = 0.05; // per-contract floor; widens with spread

fn round_dp(x: f64, dp: i32) -> f64 {
    let scale = 10f64.powi(dp);
    (x * scale).round_ties_even() / scale
}

/// Model action -> (half_spread, skew), same mapping as mm_env.scale_action:
/// half_spread in [0.01, 0.50], skew in [-0.05, 0.05]. mm_env.py rounds both
/// to 3dp before returning — skipping that is invisible on most actions (the
/// downstream 2dp/3dp price rounding usually absorbs the difference) but
/// occasionally flips the quoted cent; caught by the "rounding_boundary"
/// parity fixture.
pub fn scale_action(action: [f64; 2]) -> (f64, f64) {
    let a0 = action[0].clamp(-1.0, 1.0);
    let half_spread = 0.01 + (a0 + 1.0) / 2.0 * 0.49;
    let a1 = action[1].clamp(-1.0, 1.0);
    (round_dp(half_spread, 3), round_dp(a1 * 0.05, 3))
}

/// What the executor should do for one ticker's two-sided quote.
/// Cents are f64: whole numbers on penny markets, 1-decimal on subpenny.
#[derive(Debug, Clone, PartialEq)]
pub struct QuotePlan {
    pub bid_cents: f64,
    pub ask_cents: f64,
    /// False when the inventory cap blocks that side.
    pub place_bid: bool,
    pub place_ask: bool,
    /// Stale resting order ids to cancel first.
    pub cancel_ids: Vec<String>,
    /// Dollars — stored on TickerState after execution (tick-move check).
    pub quoted_bid: f64,
    pub quoted_ask: f64,
}

/// Crossing IOC order that unwinds a position immediately.
#[derive(Debug, Clone, PartialEq)]
pub struct ExitPlan {
    pub side: &'static str, // "buy" | "sell"
    pub price_cents: f64,
    pub size: i64,
    pub reason: &'static str, // "STOP-LOSS" | "EXPIRY"
}

/// Model action -> quote plan, or None wherever the Python returned early.
///
/// Safety layers, in order (same as _execute_action):
///   1. 1s per-ticker throttle (this function owns advancing the clock)
///   2. balance backoff after an insufficient_balance rejection
///   3. quote band — extreme-priced books lock collateral for pennies
///   4. global resting-order budget (ever-quoted tickers keep refreshing)
///   5. clamp_quotes — never cross the touch (no accidental taker)
///   6. quote_edge_ok — don't quote spreads that ceil'd fees consume
///   7. tick-move check — keep resting orders (and FIFO queue priority)
///      unless the desired price moved >= 1 tick
///   8. per-side inventory caps
pub fn plan_quotes(
    action: [f64; 2],
    ts: &mut TickerState,
    mm: &MmParams,
    max_inventory: i64,
    open_order_count: usize,
    balance_backoff_until: f64,
    now_mono: f64,
) -> Option<QuotePlan> {
    if now_mono - ts.last_quote_time < 1.0 {
        return None;
    }
    ts.last_quote_time = now_mono;

    if now_mono < balance_backoff_until {
        return None;
    }

    if !ts.book.is_valid() {
        return None;
    }
    let best_bid = ts.book.best_bid()?;
    let best_ask = ts.book.best_ask()?;
    let mid = (best_bid + best_ask) / 2.0;

    // Extreme-priced books lock ~full collateral for pennies of spread
    if !(mm.quote_band_lo..=mm.quote_band_hi).contains(&mid) {
        return None;
    }

    // Global cap on concurrent resting orders; a ticker that has ever quoted
    // keeps refreshing its slot (matches Python's active_orders membership)
    if open_order_count >= mm.max_open_orders && !ts.ever_quoted {
        return None;
    }

    let (half_spread, skew) = scale_action(action);
    let mut our_bid = mid - half_spread + skew;
    let mut our_ask = mid + half_spread + skew;

    let tick = ts.tick;
    let supports_subpenny = tick <= 0.001;
    if supports_subpenny && mm.subpenny_enabled {
        // Subpenny adjustment for queue priority
        our_bid += 0.001;
        our_ask -= 0.001;
    }

    // Snap to the market's tick grid and legal price range
    let decimals = if supports_subpenny { 3 } else { 2 };
    our_bid = round_dp(our_bid, decimals).clamp(0.01, 0.99);
    our_ask = round_dp(our_ask, decimals).clamp(0.01, 0.99);
    if our_ask <= our_bid {
        return None; // model quotes collapsed
    }

    let (our_bid, our_ask) = clamp_quotes(our_bid, our_ask, best_bid, best_ask, tick)?;

    if !quote_edge_ok(our_bid, our_ask, mm.maker_fee_rate, 1, mm.min_quote_edge) {
        return None;
    }

    // Keep resting orders when the quote hasn't moved a full tick —
    // cancel-replace on every tick means permanent back of the FIFO queue
    if let (Some(qb), Some(qa)) = (ts.quoted_bid, ts.quoted_ask) {
        if ts.bid_order_id.is_some()
            && ts.ask_order_id.is_some()
            && (qb - our_bid).abs() < tick * 0.999
            && (qa - our_ask).abs() < tick * 0.999
        {
            return None;
        }
    }

    let (bid_cents, ask_cents) = if supports_subpenny {
        (round_dp(our_bid * 100.0, 1), round_dp(our_ask * 100.0, 1))
    } else {
        ((our_bid * 100.0).round(), (our_ask * 100.0).round())
    };

    let cancel_ids = [ts.bid_order_id.clone(), ts.ask_order_id.clone()]
        .into_iter()
        .flatten()
        .collect();

    Some(QuotePlan {
        bid_cents,
        ask_cents,
        place_bid: (ts.position + 1).abs() <= max_inventory,
        place_ask: (ts.position - 1).abs() <= max_inventory,
        cancel_ids,
        quoted_bid: our_bid,
        quoted_ask: our_ask,
    })
}

/// Stop-loss / expiry check for one open position.
///
/// Stop threshold = max(floor, 2x spread): in a wide market the mid wobbles
/// from one side refreshing — exiting on that noise converts temporary marks
/// into realized taker losses. Exits cross the book (long -> sell at bid).
pub fn exit_decision(ts: &TickerState, now_s: f64) -> Option<ExitPlan> {
    if ts.position == 0 {
        return None;
    }
    let entry = ts.entry_price?;
    if !ts.book.is_valid() {
        return None;
    }
    let best_bid = ts.book.best_bid()?;
    let best_ask = ts.book.best_ask()?;
    let mid = (best_bid + best_ask) / 2.0;
    let spread = best_ask - best_bid;

    let stop_threshold = STOP_LOSS_FLOOR.max(2.0 * spread);
    let unrealized_per = (mid - entry).abs();
    let losing = (ts.position > 0 && mid < entry) || (ts.position < 0 && mid > entry);
    let stop_triggered = losing && unrealized_per >= stop_threshold;

    let expiry_triggered = ts
        .close_time_s
        .is_some_and(|close_s| close_s - now_s < EXPIRY_BUFFER_S);

    if !stop_triggered && !expiry_triggered {
        return None;
    }

    let reason = if stop_triggered { "STOP-LOSS" } else { "EXPIRY" };
    if ts.position > 0 {
        Some(ExitPlan {
            side: "sell",
            price_cents: round_dp(best_bid * 100.0, 1),
            size: ts.position,
            reason,
        })
    } else {
        Some(ExitPlan {
            side: "buy",
            price_cents: round_dp(best_ask * 100.0, 1),
            size: ts.position.abs(),
            reason,
        })
    }
}

/// Account-level risk gates. Returns (ok_to_quote, halt_reason).
///
/// halt_reason = Some(..) means the trader must halt entirely (cancel all
/// orders, stop). ok=false with no halt reason just blocks new quotes
/// (position value cap — scoped to active tickers so legacy inventory
/// can't freeze the bot).
pub fn check_risk_limits(
    state: &TraderState,
    live: &LiveConfig,
    active: Option<&HashSet<String>>,
) -> (bool, Option<&'static str>) {
    if state.daily_pnl <= -live.max_daily_loss {
        return (false, Some("Daily loss limit"));
    }
    if state.cumulative_pnl <= live.stop_loss_threshold {
        return (false, Some("Stop loss"));
    }
    if state.position_value(active) >= live.max_position_value {
        return (false, None);
    }
    (true, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MmParams;
    use serde_json::json;

    // action [-0.5, 0] -> half_spread 0.1325; on mid 0.40: bid 0.27, ask 0.53
    const ACTION: [f64; 2] = [-0.5, 0.0];

    fn mm() -> MmParams {
        MmParams::default()
    }

    /// bid 0.30, ask 0.50 — wide enough to pass the fee gate.
    fn wide_ts() -> TickerState {
        let mut ts = TickerState::new("KXADP");
        ts.book
            .load_snapshot(&json!({"yes": [[0.30, 50]], "no": [[0.50, 40]]}));
        ts
    }

    #[test]
    fn plan_quotes_basic() {
        let mut ts = wide_ts();
        let plan = plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.0).unwrap();
        assert_eq!(plan.bid_cents, 27.0);
        assert_eq!(plan.ask_cents, 53.0);
        assert!(plan.place_bid && plan.place_ask);
        assert!(plan.cancel_ids.is_empty());
        assert!((plan.quoted_bid - 0.27).abs() < 1e-9);
        assert_eq!(ts.last_quote_time, 100.0); // throttle clock advanced
    }

    #[test]
    fn throttle_blocks_within_1s() {
        let mut ts = wide_ts();
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.0).is_some());
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.5).is_none());
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 101.1).is_some());
    }

    #[test]
    fn balance_backoff_blocks() {
        let mut ts = wide_ts();
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 200.0, 100.0).is_none());
        // The throttle clock still advanced (same ordering as Python)
        assert_eq!(ts.last_quote_time, 100.0);
    }

    #[test]
    fn quote_band_blocks_extreme_mid() {
        let mut ts = TickerState::new("X");
        // bid 0.965, ask 0.98 -> mid 0.9725 > 0.95
        ts.book
            .load_snapshot(&json!({"yes": [[0.965, 10]], "no": [[0.02, 10]]}));
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.0).is_none());
    }

    #[test]
    fn order_budget_exempts_ever_quoted() {
        let cap = mm().max_open_orders;
        let mut ts = wide_ts();
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, cap, 0.0, 100.0).is_none());
        let mut ts2 = wide_ts();
        ts2.ever_quoted = true; // already-quoted tickers keep refreshing
        assert!(plan_quotes(ACTION, &mut ts2, &mm(), 5, cap, 0.0, 100.0).is_some());
    }

    #[test]
    fn fee_gate_blocks_narrow_spread() {
        let mut ts = TickerState::new("X");
        // Market bid 0.48 / ask 0.50: max passive spread ~2c, fees eat it
        ts.book
            .load_snapshot(&json!({"yes": [[0.48, 50]], "no": [[0.50, 40]]}));
        assert!(plan_quotes([-1.0, 0.0], &mut ts, &mm(), 5, 0, 0.0, 100.0).is_none());
    }

    #[test]
    fn tick_move_keep_preserves_queue() {
        let mut ts = wide_ts();
        ts.quoted_bid = Some(0.27);
        ts.quoted_ask = Some(0.53);
        ts.bid_order_id = Some("b1".into());
        ts.ask_order_id = Some("a1".into());
        assert!(plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.0).is_none());
    }

    #[test]
    fn stale_orders_cancelled_on_price_move() {
        let mut ts = wide_ts();
        ts.quoted_bid = Some(0.20);
        ts.quoted_ask = Some(0.60);
        ts.bid_order_id = Some("b1".into());
        ts.ask_order_id = Some("a1".into());
        let plan = plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.0).unwrap();
        let mut ids = plan.cancel_ids.clone();
        ids.sort();
        assert_eq!(ids, vec!["a1".to_string(), "b1".to_string()]);
    }

    #[test]
    fn inventory_cap_blocks_one_side() {
        let mut ts = wide_ts();
        ts.position = 5;
        let plan = plan_quotes(ACTION, &mut ts, &mm(), 5, 0, 0.0, 100.0).unwrap();
        assert!(!plan.place_bid); // |5+1| > 5
        assert!(plan.place_ask); // |5-1| <= 5
        // Short side mirror
        let mut ts2 = wide_ts();
        ts2.position = -5;
        let plan2 = plan_quotes(ACTION, &mut ts2, &mm(), 5, 0, 0.0, 100.0).unwrap();
        assert!(plan2.place_bid && !plan2.place_ask);
    }

    #[test]
    fn subpenny_adjustment_and_decimals() {
        let mut ts = wide_ts();
        ts.tick = 0.001;
        // action [0,0]: half 0.255 -> bid .145+.001=.146, ask .655-.001=.654
        let plan = plan_quotes([0.0, 0.0], &mut ts, &mm(), 5, 0, 0.0, 100.0).unwrap();
        assert!((plan.bid_cents - 14.6).abs() < 1e-9);
        assert!((plan.ask_cents - 65.4).abs() < 1e-9);
    }

    fn exit_ts(position: i64, entry: f64) -> TickerState {
        let mut ts = TickerState::new("X");
        // bid 0.40, ask 0.42 -> mid 0.41, spread 0.02
        ts.book
            .load_snapshot(&json!({"yes": [[0.40, 50]], "no": [[0.58, 40]]}));
        ts.position = position;
        ts.entry_price = Some(entry);
        ts
    }

    #[test]
    fn exit_stop_loss_long() {
        // threshold max(0.05, 0.04) = 0.05; loss 0.09 >= 0.05 -> sell at bid
        let ts = exit_ts(1, 0.50);
        let plan = exit_decision(&ts, 1000.0).unwrap();
        assert_eq!(plan.side, "sell");
        assert_eq!(plan.size, 1);
        assert!((plan.price_cents - 40.0).abs() < 1e-9);
        assert_eq!(plan.reason, "STOP-LOSS");
    }

    #[test]
    fn exit_no_trigger_when_winning_or_within_noise() {
        assert!(exit_decision(&exit_ts(1, 0.30), 1000.0).is_none()); // winning
        assert!(exit_decision(&exit_ts(1, 0.44), 1000.0).is_none()); // loss 0.03 < 0.05
    }

    #[test]
    fn exit_expiry_covers_short_at_ask() {
        let mut ts = exit_ts(-2, 0.45);
        ts.close_time_s = Some(1000.0 + 60.0); // closes in 60s < 120s buffer
        let plan = exit_decision(&ts, 1000.0).unwrap();
        assert_eq!(plan.side, "buy");
        assert_eq!(plan.size, 2);
        assert!((plan.price_cents - 42.0).abs() < 1e-9);
        assert_eq!(plan.reason, "EXPIRY");
    }

    #[test]
    fn risk_limits_halt_and_block() {
        let live = LiveConfig {
            capital: 95.0,
            max_daily_loss: 5.0,
            max_position_value: 40.0,
            stop_loss_threshold: -10.0,
            checkpoint_prefix: "x".into(),
            halt_on_consecutive_losses: 0,
        };
        let mut state = TraderState::new(0.0);
        assert_eq!(check_risk_limits(&state, &live, None), (true, None));
        state.daily_pnl = -5.0;
        assert_eq!(
            check_risk_limits(&state, &live, None),
            (false, Some("Daily loss limit"))
        );
        state.daily_pnl = 0.0;
        state.cumulative_pnl = -10.0;
        assert_eq!(check_risk_limits(&state, &live, None), (false, Some("Stop loss")));
        state.cumulative_pnl = 0.0;
        let big = state.ticker("A", "C");
        big.position = 100;
        big.entry_price = Some(0.50);
        // Position cap blocks quoting but does NOT halt
        assert_eq!(check_risk_limits(&state, &live, None), (false, None));
    }
}
