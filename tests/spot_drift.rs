//! Replay of the confirmed failure mode: spot drifts smoothly while Kalshi
//! books lag. Asserts the spec's three defense properties:
//!  (a) quotes lean with the drift,
//!  (b) SPOT-ADVERSE fires while the mid-based stop-loss is still blind,
//!  (c) leaned entry quotes never cross the touch.

use kalshi_mm::config::{MmParams, SpotParams};
use kalshi_mm::engine::{exit_decision, plan_quotes, spot_unwind_decision};
use kalshi_mm::ladder::Ladders;
use kalshi_mm::spot::SpotState;
use kalshi_mm::state::TickerState;
use serde_json::json;
use std::collections::HashMap;

const ACTION: [f64; 2] = [-0.5, 0.0]; // half_spread 0.1325

fn ladder_fixture() -> (Ladders, HashMap<String, f64>) {
    let tickers =
        ["KXBTCD-26JUL1517-T64000", "KXBTCD-26JUL1517-T64250", "KXBTCD-26JUL1517-T64500"];
    let ladders = Ladders::build(tickers.into_iter());
    let mids: HashMap<String, f64> = [
        ("KXBTCD-26JUL1517-T64000".to_string(), 0.80),
        ("KXBTCD-26JUL1517-T64250".to_string(), 0.50),
        ("KXBTCD-26JUL1517-T64500".to_string(), 0.20),
    ]
    .into();
    (ladders, mids)
}

/// Mirror of main.rs::fv_shift_for for the middle strike.
fn shift_for(spot: &SpotState, ladders: &Ladders, mids: &HashMap<String, f64>, sp: &SpotParams) -> f64 {
    let dist = spot.latest().unwrap() - spot.ema().unwrap();
    let delta = ladders
        .delta_for("KXBTCD-26JUL1517-T64250", sp.delta_max, |t| mids.get(t).copied())
        .unwrap();
    (delta * dist).clamp(-sp.fv_shift_max, sp.fv_shift_max)
}

#[test]
fn smooth_drift_is_defended() {
    let sp = SpotParams::default();
    let mm = MmParams::default();
    let (ladders, mids) = ladder_fixture();
    let mut spot = SpotState::new(sp.fv_ema_tau_s);

    // Kalshi book for the middle strike: bid 0.40 / ask 0.60 (mid 0.50),
    // NEVER updated during the replay — the lag we're defending against.
    let mut ts = TickerState::new("KXBTCD");
    ts.book.load_snapshot(&json!({"yes": [[0.40, 50]], "no": [[0.40, 50]]}));
    ts.position = 1; // long 1 @ 0.50 from earlier maker fill
    ts.entry_price = Some(0.50);

    // Phase 1 — slow upward drift: +$0.53/s for 10 min (~0.5% on $64k).
    // 60s ret ≈ 0.05% < 0.15% gate: slides UNDER the trend gate (this is
    // exactly the failure mode). The lean must defend instead.
    let mut t = 0.0;
    for i in 0..600 {
        t = i as f64;
        spot.on_tick(64_250.0 + 0.53 * t, t);
    }
    let ret60 = spot.ret(60.0, t).unwrap();
    assert!(ret60.abs() < sp.gate_ret_60s, "drift {ret60} should slide under the gate");

    // (a) Quotes lean UP with the drift: delta 0.0012 * lag(~32) ≈ +0.038
    let up_shift = shift_for(&spot, &ladders, &mids, &sp);
    assert!(up_shift > 0.02, "expected upward lean, got {up_shift}");
    let mut q = TickerState::new("KXBTCD");
    q.book.load_snapshot(&json!({"yes": [[0.40, 50]], "no": [[0.40, 50]]}));
    let base = plan_quotes(ACTION, 0.0, &mut q, &mm, 5, 0, 0.0, 100.0).unwrap();
    let mut q = TickerState::new("KXBTCD");
    q.book.load_snapshot(&json!({"yes": [[0.40, 50]], "no": [[0.40, 50]]}));
    let leaned = plan_quotes(ACTION, up_shift, &mut q, &mm, 5, 0, 0.0, 100.0).unwrap();
    assert!(leaned.bid_cents > base.bid_cents, "bid must lean up");
    assert!(leaned.ask_cents > base.ask_cents, "ask must lean up");
    // (c) and never cross the (stale) touch
    assert!(leaned.bid_cents < 60.0 && leaned.ask_cents > 40.0);

    // Phase 2 — sharp slide DOWN against our long: -$10/s from the drifted
    // level. The book still shows mid 0.50 = entry, so the mid-based stop
    // is blind the whole time; SPOT-ADVERSE must fire from spot alone.
    let peak = spot.latest().unwrap();
    let mut fired_at = None;
    for i in 1..=60 {
        let now = t + i as f64;
        spot.on_tick(peak - 10.0 * i as f64, now);
        let shift = shift_for(&spot, &ladders, &mids, &sp);
        assert!(exit_decision(&ts, now).is_none(), "mid-based stop must still be blind");
        if let Some(plan) = spot_unwind_decision(&ts, shift, sp.unwind_shift) {
            assert_eq!(plan.reason, "SPOT-ADVERSE");
            assert_eq!(plan.side, "sell"); // long exits at the still-good bid
            assert!((plan.price_cents - 40.0).abs() < 1e-9);
            fired_at = Some(i);
            break;
        }
    }
    let secs = fired_at.expect("SPOT-ADVERSE never fired during a $600 slide");
    assert!(secs <= 45, "unwind too slow: {secs}s");
}
