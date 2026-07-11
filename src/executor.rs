//! Order I/O: turns engine plans into API calls and updates order-id state.
//!
//! All awaits live here; no decision logic (that's engine.rs). The two quote
//! sides cancel and place concurrently — in the Python bot the sequential
//! blocking HTTP round trips were the main source of quote latency.

use serde_json::Value;
use tracing::{error, info, warn};

use crate::api::{ApiError, OrderApi};
use crate::config::MmParams;
use crate::engine::{ExitPlan, QuotePlan};
use crate::state::TraderState;

/// Outcome of one order attempt.
enum SendResult {
    Placed(Option<String>),
    InsufficientBalance,
}

async fn send_order<A: OrderApi>(
    api: &A,
    category: &str,
    ticker: &str,
    side: &str,
    price_cents: f64,
    size: i64,
    post_only: bool,
    time_in_force: &str,
) -> SendResult {
    info!(
        "ORDER: {category}/{ticker} {side} {size} @ {price_cents}c{}{}",
        if post_only { " post_only" } else { "" },
        if time_in_force == "immediate_or_cancel" { " IOC" } else { "" },
    );
    match api
        .place_limit_order(ticker, side, price_cents, size, post_only, time_in_force)
        .await
    {
        Ok(resp) => {
            let order_id = resp.get("order_id").and_then(Value::as_str).map(str::to_string);
            if let Some(id) = &order_id {
                info!("Order placed: {id}");
            }
            SendResult::Placed(order_id)
        }
        Err(ApiError(body)) if body.contains("insufficient_balance") => {
            SendResult::InsufficientBalance
        }
        Err(e) => {
            error!("Order failed: {e}");
            SendResult::Placed(None)
        }
    }
}

/// Cancel stale sides, place fresh post-only quotes concurrently, record
/// ids and quoted prices on the TickerState.
pub async fn apply_quote_plan<A: OrderApi>(
    api: &A,
    mm: &MmParams,
    category: &str,
    ticker: &str,
    plan: &QuotePlan,
    state: &mut TraderState,
    now_mono: f64,
) {
    // Cancel both stale sides concurrently ("already filled or canceled"
    // failures are expected and ignored, same as Python)
    match plan.cancel_ids.as_slice() {
        [] => {}
        [one] => {
            let _ = api.cancel_order(one).await;
        }
        [first, second, ..] => {
            let _ = tokio::join!(api.cancel_order(first), api.cancel_order(second));
        }
    }

    // Both sides in flight at once; None futures resolve immediately
    let bid = async {
        if plan.place_bid {
            Some(send_order(api, category, ticker, "buy", plan.bid_cents, 1, true,
                            "good_till_canceled").await)
        } else {
            None
        }
    };
    let ask = async {
        if plan.place_ask {
            Some(send_order(api, category, ticker, "sell", plan.ask_cents, 1, true,
                            "good_till_canceled").await)
        } else {
            None
        }
    };
    let (bid_res, ask_res) = tokio::join!(bid, ask);

    let mut insufficient = false;
    let mut extract = |res: Option<SendResult>, placed: &mut bool| -> Option<String> {
        match res {
            None => None,
            Some(SendResult::Placed(id)) => {
                *placed = true;
                id
            }
            Some(SendResult::InsufficientBalance) => {
                insufficient = true;
                *placed = true; // attempt was made — counts as a sent quote
                None
            }
        }
    };
    let (mut bid_sent, mut ask_sent) = (false, false);
    let bid_id = extract(bid_res, &mut bid_sent);
    let ask_id = extract(ask_res, &mut ask_sent);

    if insufficient {
        // Out of collateral: pause ALL quoting instead of spamming thousands
        // of doomed order attempts (observed 4,900+ in one Python session)
        state.balance_backoff_until = now_mono + mm.balance_backoff_s;
        warn!("Insufficient balance — pausing quoting {:.0}s", mm.balance_backoff_s);
    }

    // Quotes counted per attempted side, matching the Python counter
    state.quotes_sent += u64::from(bid_sent) + u64::from(ask_sent);

    let ts = state.ticker(ticker, category);
    ts.bid_order_id = bid_id;
    ts.ask_order_id = ask_id;
    ts.quoted_bid = Some(plan.quoted_bid);
    ts.quoted_ask = Some(plan.quoted_ask);
    ts.ever_quoted = true;
}

/// Crossing IOC order that unwinds a position — an unfilled remainder
/// cancels instead of resting at a price that no longer makes sense.
pub async fn apply_exit_plan<A: OrderApi>(api: &A, category: &str, ticker: &str, plan: &ExitPlan) {
    send_order(api, category, ticker, plan.side, plan.price_cents, plan.size, false,
               "immediate_or_cancel")
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MmParams;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeApi {
        placed: Mutex<Vec<(String, String, f64, i64, bool, String)>>,
        canceled: Mutex<Vec<String>>,
        counter: AtomicU64,
        fail_insufficient: bool,
    }

    impl OrderApi for FakeApi {
        async fn place_limit_order(
            &self,
            ticker: &str,
            side: &str,
            price_cents: f64,
            size: i64,
            post_only: bool,
            time_in_force: &str,
        ) -> Result<Value, ApiError> {
            if self.fail_insufficient {
                return Err(ApiError("400 - Response: insufficient_balance".into()));
            }
            let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
            self.placed.lock().unwrap().push((
                ticker.into(), side.into(), price_cents, size, post_only, time_in_force.into(),
            ));
            Ok(json!({"order_id": format!("o{n}")}))
        }

        async fn cancel_order(&self, order_id: &str) -> Result<Value, ApiError> {
            self.canceled.lock().unwrap().push(order_id.into());
            Ok(json!({}))
        }
    }

    fn plan() -> QuotePlan {
        QuotePlan {
            bid_cents: 27.0,
            ask_cents: 53.0,
            place_bid: true,
            place_ask: true,
            cancel_ids: vec![],
            quoted_bid: 0.27,
            quoted_ask: 0.53,
        }
    }

    #[tokio::test]
    async fn places_both_sides_post_only() {
        let api = FakeApi::default();
        let mut state = TraderState::new(0.0);
        state.ticker("T", "CAT");
        apply_quote_plan(&api, &MmParams::default(), "CAT", "T", &plan(), &mut state, 100.0).await;
        let ts = &state.tickers["T"];
        assert!(ts.bid_order_id.is_some() && ts.ask_order_id.is_some());
        assert_ne!(ts.bid_order_id, ts.ask_order_id);
        assert_eq!(state.quotes_sent, 2);
        assert_eq!(ts.quoted_bid, Some(0.27));
        assert!(ts.ever_quoted);
        let placed = api.placed.lock().unwrap();
        assert!(placed.iter().all(|p| p.4)); // post_only on quotes
        let sides: Vec<&str> = placed.iter().map(|p| p.1.as_str()).collect();
        assert!(sides.contains(&"buy") && sides.contains(&"sell"));
    }

    #[tokio::test]
    async fn cancels_stale_orders_first() {
        let api = FakeApi::default();
        let mut state = TraderState::new(0.0);
        state.ticker("T", "CAT");
        let mut p = plan();
        p.cancel_ids = vec!["old1".into(), "old2".into()];
        apply_quote_plan(&api, &MmParams::default(), "CAT", "T", &p, &mut state, 100.0).await;
        let mut canceled = api.canceled.lock().unwrap().clone();
        canceled.sort();
        assert_eq!(canceled, vec!["old1".to_string(), "old2".to_string()]);
    }

    #[tokio::test]
    async fn inventory_blocked_side_not_placed() {
        let api = FakeApi::default();
        let mut state = TraderState::new(0.0);
        state.ticker("T", "CAT");
        let mut p = plan();
        p.place_bid = false;
        apply_quote_plan(&api, &MmParams::default(), "CAT", "T", &p, &mut state, 100.0).await;
        let ts = &state.tickers["T"];
        assert_eq!(ts.bid_order_id, None);
        assert!(ts.ask_order_id.is_some());
        assert_eq!(state.quotes_sent, 1);
    }

    #[tokio::test]
    async fn insufficient_balance_sets_backoff() {
        let api = FakeApi {
            fail_insufficient: true,
            ..Default::default()
        };
        let mm = MmParams::default();
        let mut state = TraderState::new(0.0);
        state.ticker("T", "CAT");
        apply_quote_plan(&api, &mm, "CAT", "T", &plan(), &mut state, 100.0).await;
        assert_eq!(state.balance_backoff_until, 100.0 + mm.balance_backoff_s);
        let ts = &state.tickers["T"];
        assert_eq!(ts.bid_order_id, None);
        assert_eq!(ts.ask_order_id, None);
    }

    #[tokio::test]
    async fn exit_plan_sends_ioc() {
        let api = FakeApi::default();
        let plan = ExitPlan {
            side: "sell",
            price_cents: 40.0,
            size: 3,
            reason: "STOP-LOSS",
        };
        apply_exit_plan(&api, "CAT", "T", &plan).await;
        let placed = api.placed.lock().unwrap();
        let (_, side, price, size, post_only, tif) = placed[0].clone();
        assert_eq!((side.as_str(), price, size), ("sell", 40.0, 3));
        assert!(!post_only);
        assert_eq!(tif, "immediate_or_cancel");
    }
}
