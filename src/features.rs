//! Pure 20-dim observation builder — byte-identical math to
//! `rl_bot/live_trader_v2.py::_build_observation` (which itself mirrors
//! training's `mm_env.py`). Every normalization constant must match training
//! or the model receives out-of-distribution inputs and acts on garbage.
//!
//! Pure: the current time enters as parameters (`now_s` wall clock for trade
//! windows and TTE, `now_mono` for the 60s mid-sample cadence). The only
//! mutation is the documented in-place maintenance of the mid history and
//! trade-print list on the TickerState.

use tracing::{info, warn};

use crate::book::{sample_mid, trade_window_features};
use crate::config::MmParams;
use crate::state::TickerState;

/// Observation clip bounds — must match mm_env.py's observation_space.
pub const OBS_LOW: [f32; 20] = [
    0.01, 0.01, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, 0.0, -1.0, -1.0, 0.0, -0.1, -1.0, 0.0, -1.0,
    -0.1, -1.0, 0.0,
];
pub const OBS_HIGH: [f32; 20] = [
    0.99, 0.50, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 4.0, 0.1, 1.0, 1.0, 1.0, 0.1,
    1.0, 1.0,
];

/// Rolling stddev of the last 20 mid prices — same formula as mm_env obs[19]
/// (numpy population std). Returns 0.0 with fewer than 3 samples.
pub fn realized_vol(mid_history: &[f64]) -> f64 {
    if mid_history.len() < 3 {
        return 0.0;
    }
    let window = &mid_history[mid_history.len().saturating_sub(20)..];
    let n = window.len() as f64;
    let mean = window.iter().sum::<f64>() / n;
    let var = window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    var.sqrt()
}

/// Build the 20-dim observation for one market, or None when the book is
/// unusable (invalid/one-sided), outside the training distribution
/// (spread > 0.50), or filtered (realized vol above the MM-hostile 0.05).
pub fn build_observation(
    ticker: &str,
    ts: &mut TickerState,
    mm: &MmParams,
    now_s: f64,
    now_mono: f64,
) -> Option<[f32; 20]> {
    if !ts.book.is_valid() {
        return None;
    }
    let best_bid = ts.book.best_bid()?;
    let best_ask = ts.book.best_ask()?;

    // Spread beyond the training clip bound (0.50) is out-of-distribution
    let raw_spread = best_ask - best_bid;
    if raw_spread > 0.50 {
        warn!("{ticker}: Spread {raw_spread:.3} > 0.50 training bound, skipping");
        return None;
    }

    // [0] mid price
    let mid_price = (best_bid + best_ask) / 2.0;

    // Mid history on the TRAINING cadence (one entry per 60s window, last
    // entry refreshed in place) — momentum/velocity/vol and the 0.05 vol
    // threshold were all calibrated on 60s windows
    ts.last_mid_sample = sample_mid(&mut ts.mid_history, ts.last_mid_sample, now_mono, mid_price);

    // Volatility filter: MM loses when prices trend (informed flow = adverse
    // selection). Skip quoting above the threshold dividing MM-friendly from
    // MM-hostile markets.
    let vol = realized_vol(&ts.mid_history);
    if vol > mm.vol_filter_threshold {
        info!(
            "{ticker}: vol {vol:.4} > {} threshold — skipping quote",
            mm.vol_filter_threshold
        );
        return None;
    }

    // [1] spread — clipped like mm_env.py
    let spread = raw_spread.clamp(0.01, 0.50);

    // [2-7] orderbook depths — normalized by /100.0 like mm_env.py, with the
    // same fallback defaults for missing levels. Levels come best-first.
    let bids = ts.book.bid_levels(3);
    let asks = ts.book.ask_levels(3);
    let depth = |levels: &[(f64, f64)], i: usize, default: f64| -> f64 {
        levels.get(i).map_or(default, |&(_, q)| (q / 100.0).min(1.0))
    };
    let bid_l0 = depth(&bids, 0, 0.1);
    let ask_l0 = depth(&asks, 0, 0.1);
    let bid_l1 = depth(&bids, 1, 0.05);
    let ask_l1 = depth(&asks, 1, 0.05);
    let bid_l2 = depth(&bids, 2, 0.02);
    let ask_l2 = depth(&asks, 2, 0.02);

    // [8] book imbalance — top-3 depth each side
    let total_bid: f64 = bids.iter().map(|&(_, q)| q).sum();
    let total_ask: f64 = asks.iter().map(|&(_, q)| q).sum();
    let book_imbalance = (total_bid - total_ask) / (total_bid + total_ask + 1e-8);

    // [9] trade_volume_1m and [16] flow from REAL trade prints
    let (trade_volume, trade_flow) = trade_window_features(&mut ts.recent_trades, now_s);

    // [10] inventory / max_inventory (20 in training)
    let inventory = ts.position as f64;
    let max_inv = mm.max_inventory.max(1) as f64;
    let inv_norm = inventory / max_inv;

    // [11] unrealized PnL / (max_inventory * 0.5)
    let entry_price = ts.entry_price.unwrap_or(mid_price);
    let unrealized_pnl = if ts.position != 0 {
        inventory * (mid_price - entry_price)
    } else {
        0.0
    };
    let unrealized_norm = unrealized_pnl / (max_inv * 0.5).max(1.0);

    // [12] tte_log = log(1 + tte_hours), capped at the 24h episode start
    // (the model never saw > log(25)); pure float math, no datetime
    let tte_hours = match ts.close_time_s {
        Some(close_s) => ((close_s - now_s) / 3600.0).clamp(0.0, 24.0),
        None => 24.0,
    };
    let tte_log = (1.0 + tte_hours).ln();

    // [13] momentum: mid - mid_5_windows_ago
    let hist = &ts.mid_history;
    let momentum = if hist.len() >= 5 {
        mid_price - hist[hist.len() - 5]
    } else {
        0.0
    };

    // [14] realized PnL / 50.0 — per-TICKER, like a training episode
    let realized_pnl_norm = ts.realized_pnl / 50.0;

    // [15] fills ratio
    let quote_size = (mm.quote_size as f64).max(1.0);
    let fills_ratio = (ts.fills_buy + ts.fills_sell) as f64 / quote_size;

    // [17] price velocity over last 3 windows
    let velocity = if hist.len() >= 3 {
        (mid_price - hist[hist.len() - 3]) / 3.0
    } else {
        0.0
    };

    // [18] fill toxicity: one-sided own-fill flow = informed flow hitting us
    let total_fills = ts.fills_buy + ts.fills_sell;
    let fill_toxicity = if total_fills > 0 {
        (ts.fills_buy - ts.fills_sell) as f64 / total_fills as f64
    } else {
        0.0
    };

    // [19] realized vol / 0.05 threshold, clipped to [0, 1]
    let vol_norm = (vol / 0.05).clamp(0.0, 1.0);

    let raw: [f64; 20] = [
        mid_price,
        spread,
        bid_l0,
        ask_l0,
        bid_l1,
        ask_l1,
        bid_l2,
        ask_l2,
        book_imbalance,
        trade_volume,
        inv_norm,
        unrealized_norm,
        tte_log,
        momentum,
        realized_pnl_norm,
        fills_ratio,
        trade_flow,
        velocity,
        fill_toxicity,
        vol_norm,
    ];

    // Cast to f32 THEN clip against the f32 bounds — same order as Python
    // (np.array(..., dtype=f32) then np.clip against f32 arrays).
    let mut obs = [0.0f32; 20];
    for i in 0..20 {
        obs[i] = (raw[i] as f32).clamp(OBS_LOW[i], OBS_HIGH[i]);
    }
    Some(obs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MmParams;
    use serde_json::json;

    const NOW_S: f64 = 1_760_000_000.0;
    const NOW_MONO: f64 = 1000.0;

    fn ts_with_book(yes: serde_json::Value, no: serde_json::Value) -> TickerState {
        let mut ts = TickerState::new("KXADP");
        ts.book.load_snapshot(&json!({"yes": yes, "no": no}));
        ts
    }

    fn mm() -> MmParams {
        MmParams::default()
    }

    #[test]
    fn basic_observation_values() {
        // bid 0.40 (yes), ask 0.42 (NO bid at 0.58)
        let mut ts = ts_with_book(
            json!([[0.40, 50], [0.39, 30], [0.38, 10]]),
            json!([[0.58, 40], [0.57, 20], [0.56, 5]]),
        );
        ts.close_time_s = Some(NOW_S + 10.0 * 3600.0); // 10h to close
        let obs = build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).unwrap();
        assert!((obs[0] - 0.41).abs() < 1e-6); // mid
        assert!((obs[1] - 0.02).abs() < 1e-6); // spread
        assert_eq!(obs[2], 0.50); // bid_l0 = 50/100
        assert_eq!(obs[3], 0.40); // ask_l0
        assert_eq!(obs[4], 0.30);
        assert_eq!(obs[5], 0.20);
        assert_eq!(obs[6], 0.10);
        assert_eq!(obs[7], 0.05);
        assert!((obs[8] - (90.0 - 65.0) / 155.0).abs() < 1e-6); // imbalance
        assert_eq!(obs[9], 0.0); // no trade prints
        assert_eq!(obs[10], 0.0); // flat inventory
        assert!((obs[12] - (11.0f32).ln()).abs() < 1e-6); // tte_log
        assert_eq!(obs[19], 0.0); // 1 sample -> vol 0
        assert_eq!(ts.mid_history.len(), 1); // sample_mid appended
    }

    #[test]
    fn wide_spread_returns_none() {
        // ask 0.70, spread 0.60 > 0.50 training bound
        let mut ts = ts_with_book(json!([[0.10, 10]]), json!([[0.30, 10]]));
        assert!(build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).is_none());
    }

    #[test]
    fn vol_filter_returns_none() {
        let mut ts = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        ts.mid_history = [0.30, 0.50].repeat(10); // std 0.10 > 0.05
        ts.last_mid_sample = NOW_MONO; // same window: refresh in place
        assert!(build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).is_none());
    }

    #[test]
    fn tte_capped_at_24h_and_default() {
        let mut ts = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        ts.close_time_s = Some(NOW_S + 100.0 * 3600.0); // 100h out
        let obs = build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).unwrap();
        assert!((obs[12] - (25.0f32).ln()).abs() < 1e-6);
        // Missing close time defaults to the 24h cap too
        let mut ts2 = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        let obs2 = build_observation("T", &mut ts2, &mm(), NOW_S, NOW_MONO).unwrap();
        assert_eq!(obs[12], obs2[12]);
    }

    #[test]
    fn depth_defaults_when_levels_missing() {
        let mut ts = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        let obs = build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).unwrap();
        assert_eq!(obs[4], 0.05); // bid_l1 default
        assert_eq!(obs[6], 0.02); // bid_l2 default
        assert_eq!(obs[5], 0.05); // ask_l1 default
        assert_eq!(obs[7], 0.02); // ask_l2 default
    }

    #[test]
    fn inventory_and_unrealized_norms() {
        let mut ts = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        ts.position = 5;
        ts.entry_price = Some(0.36);
        let obs = build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).unwrap();
        assert!((obs[10] - 5.0 / 20.0).abs() < 1e-6);
        assert!((obs[11] - (5.0 * (0.41 - 0.36) / 10.0) as f32).abs() < 1e-6);
    }

    #[test]
    fn momentum_and_velocity_from_history() {
        let mut ts = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        // 6 closed windows; builder refreshes the last in place (mid 0.41)
        ts.mid_history = vec![0.30, 0.32, 0.34, 0.36, 0.38, 0.40];
        ts.last_mid_sample = NOW_MONO;
        let obs = build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).unwrap();
        // hist becomes [.30,.32,.34,.36,.38,.41]; hist[-5]=0.32, hist[-3]=0.36
        assert!((obs[13] - (0.41 - 0.32) as f32).abs() < 1e-6); // momentum (within ±0.1 bound)
        assert!((obs[17] - ((0.41 - 0.36) / 3.0) as f32).abs() < 1e-6); // velocity
    }

    #[test]
    fn realized_vol_matches_numpy_population_std() {
        assert_eq!(realized_vol(&[0.5, 0.5]), 0.0); // < 3 samples
        let v = realized_vol(&[0.3, 0.5, 0.3, 0.5]);
        assert!((v - 0.1).abs() < 1e-12);
    }

    #[test]
    fn clip_bounds_applied_after_f32_cast() {
        let mut ts = ts_with_book(json!([[0.40, 50]]), json!([[0.58, 40]]));
        ts.realized_pnl = 500.0; // pnl_norm 10 -> clipped to 1.0
        ts.fills_buy = 99; // fills_ratio 99 -> clipped to 1.0
        let obs = build_observation("T", &mut ts, &mm(), NOW_S, NOW_MONO).unwrap();
        assert_eq!(obs[14], 1.0);
        assert_eq!(obs[15], 1.0);
        assert_eq!(obs[18], 1.0); // toxicity all-buys = 1.0 (at bound)
    }
}
