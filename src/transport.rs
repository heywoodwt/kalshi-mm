//! Kalshi WebSocket transport: auth, subscriptions, message routing,
//! reconnect. Port of `KalshiWebSocketClient` from live_trader_v2.py, with
//! Python's callbacks replaced by an mpsc channel of typed events — the
//! trader consumes events; this module owns nothing but the socket.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use rsa::pss::SigningKey;
use rsa::sha2::Sha256;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::api::{load_private_key, sign_request};

pub const PROD_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
/// Path signed for the WS auth handshake (Python: "GET/trade-api/ws/v2").
const WS_SIGNING_PATH: &str = "/trade-api/ws/v2";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// One parsed market-data message, routed by the trader's event loop.
#[derive(Debug)]
pub enum WsEvent {
    /// orderbook_snapshot or orderbook_delta — payload is the "msg" object
    /// (Book::load_snapshot / apply_delta distinguish the two shapes).
    Book { ticker: String, msg: Value },
    /// Public trade print (feeds obs [9] volume and [16] flow).
    Trade { ticker: String, msg: Value },
    /// External spot price tick (Coinbase), for spot-bound categories.
    Spot { price: f64 },
}

pub struct WsClient {
    url: String,
    api_key: String,
    signing_key: SigningKey<Sha256>,
}

impl WsClient {
    pub fn new(api_key: &str, api_secret: &str, url: &str) -> Result<Self> {
        Ok(Self {
            url: url.to_string(),
            api_key: api_key.to_string(),
            signing_key: SigningKey::new(load_private_key(api_secret)?),
        })
    }

    /// Connect + subscribe + pump events until the connection drops, then
    /// reconnect after 5s — forever, while `running` is true. Subscriptions
    /// don't survive a reconnect, so they are re-issued on every connect.
    pub async fn run(
        &self,
        tickers: Vec<String>,
        tx: mpsc::Sender<WsEvent>,
        running: Arc<AtomicBool>,
    ) {
        while running.load(Ordering::Relaxed) {
            match self.connect_and_pump(&tickers, &tx, &running).await {
                Ok(()) => warn!("WebSocket disconnected, reconnecting in {RECONNECT_DELAY:?}..."),
                Err(e) => error!("WebSocket error: {e}, reconnecting in {RECONNECT_DELAY:?}..."),
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    async fn connect_and_pump(
        &self,
        tickers: &[String],
        tx: &mpsc::Sender<WsEvent>,
        running: &Arc<AtomicBool>,
    ) -> Result<()> {
        info!("Connecting to WebSocket: {}", self.url);
        // Auth headers: same RSA-PSS scheme as REST, path fixed to the WS v2 root
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let signature = sign_request(&self.signing_key, &timestamp, "GET", WS_SIGNING_PATH);
        let mut request = self.url.as_str().into_client_request()?;
        let headers = request.headers_mut();
        headers.insert("KALSHI-ACCESS-KEY", self.api_key.parse()?);
        headers.insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
        headers.insert("KALSHI-ACCESS-TIMESTAMP", timestamp.parse()?);

        let (ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .context("WebSocket connect")?;
        info!("✓ WebSocket connected");
        let (mut sink, mut stream) = ws.split();

        // Subscribe to orderbook deltas + trade prints for every market
        for (i, ticker) in tickers.iter().enumerate() {
            let msg = json!({
                "id": chrono::Utc::now().timestamp_millis() + i as i64,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta", "trade"],
                    "market_ticker": ticker,
                },
            });
            sink.send(Message::Text(msg.to_string().into())).await?;
        }
        info!("✓ Subscribed to {} markets, listening...", tickers.len());

        while let Some(msg) = stream.next().await {
            if !running.load(Ordering::Relaxed) {
                return Ok(());
            }
            match msg.context("WebSocket read")? {
                Message::Text(text) => {
                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
                        if let Some(event) = route_message(&data) {
                            // Trader gone = shutdown; stop pumping
                            if tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                // tungstenite queues pongs automatically, but only flushes on
                // write — answer explicitly since our sink is otherwise idle
                Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                Message::Close(frame) => {
                    debug!("WebSocket close frame: {frame:?}");
                    return Ok(());
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Route one WS message to a typed event (or None for acks/other channels).
fn route_message(data: &Value) -> Option<WsEvent> {
    let msg_type = data.get("type").and_then(Value::as_str)?;
    let msg = data.get("msg")?;
    let ticker = msg.get("market_ticker").and_then(Value::as_str)?.to_string();
    match msg_type {
        "orderbook_snapshot" | "orderbook_delta" => Some(WsEvent::Book {
            ticker,
            msg: msg.clone(),
        }),
        "trade" => Some(WsEvent::Trade {
            ticker,
            msg: msg.clone(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_book_and_trade_messages() {
        let snap = json!({"type": "orderbook_snapshot",
                          "msg": {"market_ticker": "KXADP-1", "yes": [[0.4, 5]]}});
        assert!(matches!(route_message(&snap), Some(WsEvent::Book { ticker, .. }) if ticker == "KXADP-1"));
        let delta = json!({"type": "orderbook_delta",
                           "msg": {"market_ticker": "KXADP-1", "side": "yes",
                                   "price_dollars": 0.4, "delta_fp": "1"}});
        assert!(matches!(route_message(&delta), Some(WsEvent::Book { .. })));
        let trade = json!({"type": "trade",
                           "msg": {"market_ticker": "KXADP-1", "count": 2, "taker_side": "no"}});
        assert!(matches!(route_message(&trade), Some(WsEvent::Trade { .. })));
        // Subscription acks and unknown channels are dropped
        let ack = json!({"type": "subscribed", "msg": {"market_ticker": "KXADP-1"}});
        assert!(route_message(&ack).is_none());
        let no_msg = json!({"type": "error"});
        assert!(route_message(&no_msg).is_none());
    }
}
