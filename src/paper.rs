//! Paper-trading client: REAL market data (when live credentials exist),
//! SIMULATED orders. Port of KalshiPaperTradingClient — safe on a
//! real-money account because nothing order-shaped ever leaves the process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::{json, Value};
use tracing::info;

use crate::api::{ApiError, KalshiClient, MarketApi, OrderApi};

pub struct PaperClient {
    /// Read-only live client for market data; None = fully offline.
    real: Option<KalshiClient>,
    /// order_id -> order record ("resting" | "canceled").
    orders: Mutex<Vec<Value>>,
    counter: AtomicU64,
}

impl PaperClient {
    pub fn new(real: Option<KalshiClient>) -> Self {
        if real.is_none() {
            info!("[PAPER] No API credentials — market data reads return empty");
        }
        Self {
            real,
            orders: Mutex::new(Vec::new()),
            counter: AtomicU64::new(0),
        }
    }
}

impl OrderApi for PaperClient {
    async fn place_limit_order(
        &self,
        ticker: &str,
        side: &str,
        price_cents: f64,
        size: i64,
        post_only: bool,
        _time_in_force: &str,
    ) -> Result<Value, ApiError> {
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let order_id = format!("paper_order_{n}");
        info!("[PAPER] Order: {order_id} {side} {size} {ticker} @ {price_cents}c post_only={post_only}");
        self.orders.lock().unwrap().push(json!({
            "order_id": order_id,
            "market_ticker": ticker,
            "side": side,
            "price_cents": price_cents,
            "size": size,
            "status": "resting",
        }));
        Ok(json!({ "order_id": order_id }))
    }

    async fn cancel_order(&self, order_id: &str) -> Result<Value, ApiError> {
        for order in self.orders.lock().unwrap().iter_mut() {
            if order.get("order_id").and_then(Value::as_str) == Some(order_id) {
                order["status"] = json!("canceled");
            }
        }
        Ok(json!({"order_id": order_id, "status": "canceled"}))
    }
}

impl MarketApi for PaperClient {
    // --- market data: real when possible --------------------------------------

    async fn get_markets(
        &self,
        series_ticker: Option<&str>,
        status: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Value, ApiError> {
        match &self.real {
            Some(c) => c.get_markets(series_ticker, status, limit, cursor).await,
            None => Ok(json!({"markets": [], "cursor": null})),
        }
    }

    async fn get_market(&self, ticker: &str) -> Result<Value, ApiError> {
        match &self.real {
            Some(c) => c.get_market(ticker).await,
            None => Ok(json!({"market": {}})),
        }
    }

    async fn get_orderbook(&self, ticker: &str, depth: u32) -> Result<Value, ApiError> {
        match &self.real {
            Some(c) => c.get_orderbook(ticker, depth).await,
            // Wide-spread book for fully-offline testing
            None => Ok(json!({"orderbook": {"yes": [[0.01, 100], [0.02, 100]],
                                            "no": [[0.99, 100], [0.98, 100]]}})),
        }
    }

    // --- account/orders: simulated ------------------------------------------------

    async fn get_positions(&self) -> Result<Value, ApiError> {
        Ok(json!({"positions": []})) // paper fills never happen -> no positions
    }

    async fn get_fills(
        &self,
        _min_ts: i64,
        _limit: u32,
        _cursor: Option<&str>,
        _ticker: Option<&str>,
    ) -> Result<Value, ApiError> {
        Ok(json!({"fills": []})) // resting paper orders are never matched
    }

    async fn get_settlements(&self, _limit: u32) -> Result<Value, ApiError> {
        Ok(json!({"settlements": []})) // paper positions never settle on-exchange
    }

    async fn get_orders(&self, status: Option<&str>, _limit: u32) -> Result<Value, ApiError> {
        let orders: Vec<Value> = self
            .orders
            .lock()
            .unwrap()
            .iter()
            .filter(|o| {
                status.is_none() || o.get("status").and_then(Value::as_str) == status
            })
            .cloned()
            .collect();
        Ok(json!({ "orders": orders }))
    }

    async fn cancel_all_orders(&self) -> Result<u64, ApiError> {
        let mut count = 0;
        for order in self.orders.lock().unwrap().iter_mut() {
            if order.get("status").and_then(Value::as_str) == Some("resting") {
                order["status"] = json!("canceled");
                count += 1;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn orders_are_simulated_and_cancelable() {
        let paper = PaperClient::new(None);
        let resp = paper
            .place_limit_order("KXADP-1", "buy", 40.0, 1, true, "good_till_canceled")
            .await
            .unwrap();
        let oid = resp["order_id"].as_str().unwrap().to_string();
        assert!(oid.starts_with("paper_order_"));

        let resting = paper.get_orders(Some("resting"), 100).await.unwrap();
        assert_eq!(resting["orders"].as_array().unwrap().len(), 1);

        paper.cancel_order(&oid).await.unwrap();
        let resting = paper.get_orders(Some("resting"), 100).await.unwrap();
        assert!(resting["orders"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn offline_reads_are_safe_defaults() {
        let paper = PaperClient::new(None);
        assert_eq!(paper.get_positions().await.unwrap()["positions"], json!([]));
        assert_eq!(paper.get_fills(0, 10, None, None).await.unwrap()["fills"], json!([]));
        let book = paper.get_orderbook("X", 10).await.unwrap();
        assert!(book["orderbook"]["yes"].is_array());
        assert_eq!(paper.cancel_all_orders().await.unwrap(), 0);
    }
}
