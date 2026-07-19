//! Async Kalshi REST client (V2 trade API) — port of the endpoint surface
//! the live bot uses from `rl_bot/kalshi_api.py`, on reqwest instead of the
//! blocking `requests` (whose synchronous calls from async callbacks froze
//! every ticker for a full HTTP round trip — the main Python latency bug).
//!
//! Request signing per Kalshi spec: RSA-PSS(SHA256, salt = digest length)
//! over `timestamp_ms + METHOD + path-without-query`, base64-encoded.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde_json::{json, Value};

pub const PROD_BASE_URL: &str = "https://external-api.kalshi.com/trade-api/v2";

/// API failure carrying the response body — the executor matches
/// "insufficient_balance" inside it to trigger the quoting backoff.
#[derive(Debug, thiserror::Error)]
#[error("API request failed: {0}")]
pub struct ApiError(pub String);

/// Load an RSA private key from a PEM file path or a PEM string,
/// accepting both PKCS#8 and PKCS#1 encodings (same tolerance as Python's
/// load_pem_private_key).
pub fn load_private_key(secret: &str) -> Result<RsaPrivateKey> {
    // KALSHI_API_SECRET is either a path (absolute, or relative to this
    // binary's CWD) to a PEM file, or the inline PEM content itself.
    let pem = if Path::new(secret).exists() {
        std::fs::read_to_string(secret).with_context(|| format!("reading key file {secret}"))?
    } else {
        secret.to_string()
    };
    RsaPrivateKey::from_pkcs8_pem(&pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
        .context("parsing RSA private key (PKCS#8/PKCS#1 PEM)")
}

/// Sign `timestamp + METHOD + path` (query params stripped) per the Kalshi
/// spec — shared by the REST client and the WebSocket auth handshake.
pub fn sign_request(
    signing_key: &SigningKey<Sha256>,
    timestamp_ms: &str,
    method: &str,
    path: &str,
) -> String {
    let path_no_query = path.split('?').next().unwrap_or(path);
    let message = format!("{timestamp_ms}{method}{path_no_query}");
    let sig = signing_key.sign_with_rng(&mut rand::thread_rng(), message.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
}

/// Format a dollar price the way the Python client does: 3 decimals for
/// subpenny prices, 2 decimals for whole-cent prices.
pub fn format_price(dollars: f64) -> String {
    let two_dp = (dollars * 100.0).round_ties_even() / 100.0;
    if dollars != two_dp {
        format!("{dollars:.3}")
    } else {
        format!("{dollars:.2}")
    }
}

pub struct KalshiClient {
    http: reqwest::Client,
    base_url: String,
    /// Path prefix of base_url (e.g. /trade-api/v2) — signing needs the
    /// full path from the API root, without query params.
    base_path: String,
    api_key: String,
    signing_key: SigningKey<Sha256>,
}

impl KalshiClient {
    pub fn new(api_key: &str, api_secret: &str, base_url: &str) -> Result<Self> {
        let key = load_private_key(api_secret)?;
        let base_url = base_url.trim_end_matches('/').to_string();
        let base_path = reqwest::Url::parse(&base_url)
            .context("parsing base_url")?
            .path()
            .to_string();
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .pool_max_idle_per_host(10)
                .build()?,
            base_url,
            base_path,
            api_key: api_key.to_string(),
            // SigningKey::new uses salt length = digest length (Kalshi spec,
            // same as Python's padding.PSS.DIGEST_LENGTH)
            signing_key: SigningKey::new(key),
        })
    }

    /// Sign `timestamp + METHOD + path` (query params stripped).
    pub fn sign(&self, timestamp_ms: &str, method: &str, path: &str) -> String {
        sign_request(&self.signing_key, timestamp_ms, method, path)
    }

    async fn request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<Value, ApiError> {
        let mut url = format!("{}{}", self.base_url, endpoint);
        if !params.is_empty() {
            let query: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
            url = format!("{url}?{}", query.join("&"));
        }
        let signing_path = format!("{}{}", self.base_path, endpoint);
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let signature = self.sign(&timestamp, method.as_str(), &signing_path);

        let mut req = self
            .http
            .request(method, &url)
            .header("Content-Type", "application/json")
            .header("KALSHI-ACCESS-KEY", &self.api_key)
            .header("KALSHI-ACCESS-SIGNATURE", signature)
            .header("KALSHI-ACCESS-TIMESTAMP", timestamp);
        if let Some(body) = body {
            req = req.body(body.to_string());
        }

        let resp = req.send().await.map_err(|e| ApiError(e.to_string()))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| ApiError(e.to_string()))?;
        if !status.is_success() {
            // Body preserved: the executor matches "insufficient_balance"
            return Err(ApiError(format!("{status} - Response: {text}")));
        }
        serde_json::from_str(&text).map_err(|e| ApiError(format!("bad JSON: {e}")))
    }

    // --- market data --------------------------------------------------------

    pub async fn get_markets(
        &self,
        series_ticker: Option<&str>,
        status: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Value, ApiError> {
        let mut params = vec![("limit", limit.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        if let Some(s) = series_ticker {
            params.push(("series_ticker", s.to_string()));
        }
        if let Some(s) = status {
            params.push(("status", s.to_string()));
        }
        self.request(reqwest::Method::GET, "/markets", &params, None).await
    }

    pub async fn get_market(&self, ticker: &str) -> Result<Value, ApiError> {
        self.request(reqwest::Method::GET, &format!("/markets/{ticker}"), &[], None)
            .await
    }

    pub async fn get_orderbook(&self, ticker: &str, depth: u32) -> Result<Value, ApiError> {
        self.request(
            reqwest::Method::GET,
            &format!("/markets/{ticker}/orderbook"),
            &[("depth", depth.to_string())],
            None,
        )
        .await
    }

    // --- account -------------------------------------------------------------

    pub async fn get_positions(&self) -> Result<Value, ApiError> {
        self.request(reqwest::Method::GET, "/portfolio/positions", &[], None).await
    }

    pub async fn get_fills(
        &self,
        min_ts: i64,
        limit: u32,
        cursor: Option<&str>,
        ticker: Option<&str>,
    ) -> Result<Value, ApiError> {
        let mut params = vec![
            ("limit", limit.to_string()),
            ("min_ts", min_ts.to_string()),
        ];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        if let Some(t) = ticker {
            params.push(("ticker", t.to_string()));
        }
        self.request(reqwest::Method::GET, "/portfolio/fills", &params, None).await
    }

    /// Newest-first page of settled markets. No ticker filter server-side:
    /// callers search the page — a settlement being resolved just happened,
    /// so it's always near the top.
    pub async fn get_settlements(&self, limit: u32) -> Result<Value, ApiError> {
        let params = vec![("limit", limit.to_string())];
        self.request(reqwest::Method::GET, "/portfolio/settlements", &params, None).await
    }

    pub async fn get_orders(&self, status: Option<&str>, limit: u32) -> Result<Value, ApiError> {
        let mut params = vec![("limit", limit.to_string())];
        if let Some(s) = status {
            params.push(("status", s.to_string()));
        }
        self.request(reqwest::Method::GET, "/portfolio/orders", &params, None).await
    }

    // --- orders ---------------------------------------------------------------

    /// Place a limit order. `side` is "buy"/"sell" (converted to V2
    /// bid/ask), `price_cents` supports subpenny (e.g. 50.1).
    pub async fn place_limit_order(
        &self,
        ticker: &str,
        side: &str,
        price_cents: f64,
        size: i64,
        post_only: bool,
        time_in_force: &str,
    ) -> Result<Value, ApiError> {
        let body = json!({
            "ticker": ticker,
            "side": if side == "buy" { "bid" } else { "ask" },
            "price": format_price(price_cents / 100.0),
            "count": format!("{:.2}", size as f64),
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": post_only,
            "client_order_id": format!("mm_{}", chrono::Utc::now().timestamp_millis()),
        });
        self.request(reqwest::Method::POST, "/portfolio/events/orders", &[], Some(body))
            .await
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<Value, ApiError> {
        self.request(
            reqwest::Method::DELETE,
            &format!("/portfolio/events/orders/{order_id}"),
            &[],
            None,
        )
        .await
    }

    /// V2 has no bulk cancel: fetch resting orders, cancel individually.
    pub async fn cancel_all_orders(&self) -> Result<u64, ApiError> {
        let resp = self.get_orders(Some("resting"), 1000).await?;
        let mut canceled = 0;
        if let Some(orders) = resp.get("orders").and_then(Value::as_array) {
            for order in orders {
                if let Some(oid) = order.get("order_id").and_then(Value::as_str) {
                    if self.cancel_order(oid).await.is_ok() {
                        canceled += 1;
                    }
                }
            }
        }
        Ok(canceled)
    }
}

/// Order placement/cancel — the only surface the executor needs. Separate
/// from MarketApi so executor tests fake two methods, not nine.
pub trait OrderApi: Send + Sync {
    fn place_limit_order(
        &self,
        ticker: &str,
        side: &str,
        price_cents: f64,
        size: i64,
        post_only: bool,
        time_in_force: &str,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;

    fn cancel_order(
        &self,
        order_id: &str,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
}

/// Full API surface the trader uses — implemented by the live KalshiClient
/// and the paper client (real reads, simulated orders).
pub trait MarketApi: OrderApi {
    fn get_markets(
        &self,
        series_ticker: Option<&str>,
        status: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn get_market(&self, ticker: &str)
        -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn get_orderbook(
        &self,
        ticker: &str,
        depth: u32,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn get_positions(&self) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn get_fills(
        &self,
        min_ts: i64,
        limit: u32,
        cursor: Option<&str>,
        ticker: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn get_settlements(
        &self,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn get_orders(
        &self,
        status: Option<&str>,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<Value, ApiError>> + Send;
    fn cancel_all_orders(&self) -> impl std::future::Future<Output = Result<u64, ApiError>> + Send;
}

impl OrderApi for KalshiClient {
    async fn place_limit_order(
        &self,
        ticker: &str,
        side: &str,
        price_cents: f64,
        size: i64,
        post_only: bool,
        time_in_force: &str,
    ) -> Result<Value, ApiError> {
        KalshiClient::place_limit_order(self, ticker, side, price_cents, size, post_only, time_in_force)
            .await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<Value, ApiError> {
        KalshiClient::cancel_order(self, order_id).await
    }
}

impl MarketApi for KalshiClient {
    async fn get_markets(
        &self,
        series_ticker: Option<&str>,
        status: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Value, ApiError> {
        KalshiClient::get_markets(self, series_ticker, status, limit, cursor).await
    }
    async fn get_market(&self, ticker: &str) -> Result<Value, ApiError> {
        KalshiClient::get_market(self, ticker).await
    }
    async fn get_orderbook(&self, ticker: &str, depth: u32) -> Result<Value, ApiError> {
        KalshiClient::get_orderbook(self, ticker, depth).await
    }
    async fn get_positions(&self) -> Result<Value, ApiError> {
        KalshiClient::get_positions(self).await
    }
    async fn get_fills(
        &self,
        min_ts: i64,
        limit: u32,
        cursor: Option<&str>,
        ticker: Option<&str>,
    ) -> Result<Value, ApiError> {
        KalshiClient::get_fills(self, min_ts, limit, cursor, ticker).await
    }
    async fn get_settlements(&self, limit: u32) -> Result<Value, ApiError> {
        KalshiClient::get_settlements(self, limit).await
    }
    async fn get_orders(&self, status: Option<&str>, limit: u32) -> Result<Value, ApiError> {
        KalshiClient::get_orders(self, status, limit).await
    }
    async fn cancel_all_orders(&self) -> Result<u64, ApiError> {
        KalshiClient::cancel_all_orders(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use rsa::pss::VerifyingKey;
    use rsa::signature::Verifier;

    fn test_client() -> (KalshiClient, RsaPrivateKey) {
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
        let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap().to_string();
        let client = KalshiClient::new("key-id", &pem, PROD_BASE_URL).unwrap();
        (client, key)
    }

    #[test]
    fn signature_verifies_and_strips_query() {
        let (client, key) = test_client();
        let sig_b64 = client.sign("1234", "GET", "/trade-api/v2/markets?limit=5");
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .unwrap();
        let verifying: VerifyingKey<Sha256> = VerifyingKey::new(key.to_public_key());
        // Query params must be stripped from the signed message
        let sig = rsa::pss::Signature::try_from(sig_bytes.as_slice()).unwrap();
        verifying
            .verify(b"1234GET/trade-api/v2/markets", &sig)
            .expect("signature must verify against the query-stripped path");
    }

    #[test]
    fn price_formatting_matches_python() {
        assert_eq!(format_price(0.40), "0.40"); // whole cents -> 2dp
        assert_eq!(format_price(0.501), "0.501"); // subpenny -> 3dp
        assert_eq!(format_price(0.05), "0.05");
        assert_eq!(format_price(40.0 / 100.0), "0.40"); // cents/100 path
        assert_eq!(format_price(50.1 / 100.0), "0.501");
    }
}
