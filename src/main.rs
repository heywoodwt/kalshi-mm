//! kalshi-mm: Rust port of the live market-making bot
//! (`rl_bot/live_trader_v2.py`). Behavior contract:
//! docs/superpowers/specs/2026-07-09-rust-live-bot-design.md.
//!
//! Concurrency model: ONE event loop owns all trading state (no locks) —
//! the same single-threaded semantics as Python's asyncio. The WebSocket
//! runs as a separate task and feeds typed events through a channel;
//! periodic work (fills 5s, position sync 30s, exits 10s, daily reset,
//! hourly report) are select! timers on the same loop.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::Parser;
use fs2::FileExt;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer as _; // per-layer with_filter

use kalshi_mm::api::{KalshiClient, MarketApi, PROD_BASE_URL};
use kalshi_mm::book::TakerSide;
use kalshi_mm::config::{CategoryConfig, Config};
use kalshi_mm::engine::{check_risk_limits, exit_decision, plan_quotes, spot_unwind_decision};
use kalshi_mm::features::build_observation;
use kalshi_mm::ladder::{self, Ladders};
use kalshi_mm::model::Policy;
use kalshi_mm::paper::PaperClient;
use kalshi_mm::spot::{trend_gated, SpotFeed, SpotState, COINBASE_WS_URL};
use kalshi_mm::state::{normalize_fill, normalize_settlement, replay_entry_price, TraderState};
use kalshi_mm::transport::{WsClient, WsEvent, PROD_WS_URL};
use kalshi_mm::{book, executor, state};

// --- clocks -------------------------------------------------------------------

fn epoch_now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

static MONO_START: OnceLock<Instant> = OnceLock::new();
fn mono_now() -> f64 {
    MONO_START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

// --- CLI ------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "kalshi-mm", about = "Kalshi market-making bot (Rust)")]
struct Args {
    /// Config name (config/<name>.toml) or a .toml path
    #[arg(long, default_value = "lowvol")]
    config: String,
    /// Paper mode: real market data, simulated orders (also PAPER_MODE=true)
    #[arg(long)]
    paper: bool,
    /// Directory holding <prefix>_<CATEGORY>_final.onnx policies, relative
    /// to the CWD this binary is launched from (typically rust/).
    #[arg(long, default_value = "models")]
    models_dir: String,
}

/// Minimal .env loader (KEY=VALUE lines; existing env wins) — replaces
/// Python's load_dotenv without another dependency. Reads only the CWD's
/// .env (rust/ is self-contained: it never reaches outside its own tree).
fn load_dotenv() {
    let Ok(text) = std::fs::read_to_string(".env") else { return };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            if std::env::var(key).is_err() {
                std::env::set_var(key, value.trim().trim_matches('"'));
            }
        }
    }
}

/// Exclusive lock so only ONE Rust bot runs per account. Separate file from
/// the Python bot's lock — they are MEANT to run side by side during the
/// paper-parity phase. The handle must stay alive for the process lifetime.
fn acquire_instance_lock() -> Result<std::fs::File> {
    let path = dirs::home_dir().context("no home dir")?.join(".kalshi_mm_rust.lock");
    let mut file = OpenOptions::new().create(true).write(true).open(&path)?;
    file.try_lock_exclusive()
        .context("another kalshi-mm instance is already running (~/.kalshi_mm_rust.lock)")?;
    write!(file, "{}", std::process::id())?;
    Ok(file)
}

fn setup_logging(config_name: &str) -> tracing_appender::non_blocking::WorkerGuard {
    let log_name = config_name.replace(['/', '\\'], "_").replace(".toml", "");
    let appender = tracing_appender::rolling::never(".", format!("live_trading_rust_{log_name}.log"));
    let (file_writer, guard) = tracing_appender::non_blocking(appender);
    // Default to INFO: dependency TRACE logs (tungstenite logs every frame
    // AND the auth headers) produced 2.2M lines in minutes. RUST_LOG overrides.
    let filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stdout).with_filter(filter()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(file_writer)
                .with_ansi(false)
                .with_filter(filter()),
        )
        .init();
    guard
}

fn main() -> Result<()> {
    let args = Args::parse();
    load_dotenv();
    let _log_guard = setup_logging(&args.config);
    let _lock = acquire_instance_lock()?;

    let cfg = Config::load(&args.config)?;
    let paper = args.paper || std::env::var("PAPER_MODE").map(|v| v.to_lowercase() == "true").unwrap_or(false);
    let api_key = std::env::var("KALSHI_API_KEY").ok();
    let api_secret = std::env::var("KALSHI_API_SECRET").ok();
    // Missing credentials force paper mode, same as Python
    let paper = paper || api_key.is_none() || api_secret.is_none();

    info!("{}", "=".repeat(80));
    info!("KALSHI LIVE TRADING BOT (kalshi-mm)");
    info!("Mode: {}", if paper { "PAPER TRADING" } else { "LIVE TRADING" });
    info!("Config: {} | Capital: ${:.2}", args.config, cfg.live.capital);
    info!("Categories ({}): {:?}", cfg.categories.len(),
          cfg.categories.iter().map(|c| c.name.as_str()).collect::<Vec<_>>());
    info!("Risk limits: daily loss ${}, stop loss ${}",
          cfg.live.max_daily_loss, cfg.live.stop_loss_threshold);
    info!("{}", "=".repeat(80));

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async move {
        let real_client = match (&api_key, &api_secret) {
            (Some(k), Some(s)) => Some(KalshiClient::new(k, s, PROD_BASE_URL)?),
            _ => None,
        };
        if paper {
            let client = PaperClient::new(real_client);
            run_trader(client, cfg, args, api_key, api_secret).await
        } else {
            run_trader(real_client.unwrap(), cfg, args, api_key, api_secret).await
        }
    })
}

// --- trader ------------------------------------------------------------------------

struct Trader<M: MarketApi> {
    api: M,
    cfg: Config,
    cats: HashMap<String, CategoryConfig>,
    state: TraderState,
    policies: HashMap<String, Policy>,
    /// ticker -> category, for every market the bot quotes.
    active_tickers: HashMap<String, String>,
    active_set: HashSet<String>,
    running: Arc<AtomicBool>,
    /// Spot defense state (always present; inert when no category binds a feed).
    spot: SpotState,
    ladders: Ladders,
    /// Categories with spot_feed set in config.
    spot_bound: HashSet<String>,
    /// Trend-gate edge detection for the one-shot cancel.
    spot_gate_on: bool,
    /// ticker -> next allowed unwind evaluation (mono s; 1s throttle).
    spot_unwind_next: HashMap<String, f64>,
    /// ticker -> mono deadline after a FIRED unwind IOC. Until it passes,
    /// neither the unwind nor the mid-based stop may fire again: the IOC's
    /// fill may be unbooked for up to 5s (fills poll), and a second
    /// full-size exit would drive the position through flat into a reversal.
    spot_unwind_fired_until: HashMap<String, f64>,
    /// ticker -> how many syncs a vanished position has waited for its
    /// settlement record (the settlements feed can lag the positions feed).
    settlement_checks: HashMap<String, u32>,
    /// Publishes the tradeable universe to the WebSocket task after a refresh
    /// so it can subscribe to newly-opened markets. None when running without
    /// a WebSocket (REST polling / no credentials).
    universe_tx: Option<watch::Sender<Vec<String>>>,
}

/// Give a vanished position this many sync cycles (30s each) to show up in
/// /portfolio/settlements before falling back to plain drift-zeroing.
const MAX_SETTLEMENT_CHECKS: u32 = 4;

/// How often to re-discover open markets. Hourly series (KXBTCD) expire and
/// reopen continuously; without a refresh the bot's universe is frozen at boot
/// and it silently stops quoting once that cohort expires (the 2026-07-24
/// outage). 15 min is well inside an hourly market's life and costs one
/// paginated /markets walk per category.
const MARKET_REFRESH_S: u64 = 900;

async fn run_trader<M: MarketApi>(
    api: M,
    cfg: Config,
    args: Args,
    api_key: Option<String>,
    api_secret: Option<String>,
) -> Result<()> {
    let cats: HashMap<String, CategoryConfig> =
        cfg.categories.iter().map(|c| (c.name.clone(), c.clone())).collect();
    let spot_bound: HashSet<String> =
        cfg.categories.iter().filter(|c| c.spot_feed.is_some()).map(|c| c.name.clone()).collect();
    let spot_tau = cfg.spot.fv_ema_tau_s;
    let mut trader = Trader {
        api,
        cats,
        cfg,
        state: TraderState::new(epoch_now()),
        policies: HashMap::new(),
        active_tickers: HashMap::new(),
        active_set: HashSet::new(),
        running: Arc::new(AtomicBool::new(true)),
        spot: SpotState::new(spot_tau),
        ladders: Ladders::default(),
        spot_bound,
        spot_gate_on: false,
        spot_unwind_next: HashMap::new(),
        spot_unwind_fired_until: HashMap::new(),
        settlement_checks: HashMap::new(),
        universe_tx: None,
    };

    trader.optimize_startup_orders().await;
    trader.load_policies(&args.models_dir)?;
    trader.discover_markets().await;
    trader.fetch_market_details(&trader.all_entries()).await;
    trader.sync_positions().await;

    trader.ladders = Ladders::build(trader.active_tickers.keys().map(String::as_str));

    // WebSocket task feeds the event loop; without credentials fall back to
    // REST polling (10s), same as Python.
    let (tx, rx) = mpsc::channel::<WsEvent>(8192);
    let use_ws = match (&api_key, &api_secret) {
        (Some(key), Some(secret)) => match WsClient::new(key, secret, PROD_WS_URL) {
            Ok(ws) => {
                // Watch channel (not a fixed list): periodic re-discovery
                // republishes the universe here so the socket picks up
                // newly-opened markets as hourly series roll over.
                let tickers: Vec<String> = trader.active_tickers.keys().cloned().collect();
                let (universe_tx, universe_rx) = watch::channel(tickers);
                trader.universe_tx = Some(universe_tx);
                let running = trader.running.clone();
                let tx = tx.clone();
                tokio::spawn(async move { ws.run(universe_rx, tx, running).await });
                true
            }
            Err(e) => {
                warn!("WebSocket unavailable ({e}) - using REST API polling");
                false
            }
        },
        _ => {
            warn!("No credentials for WebSocket - using REST API polling");
            false
        }
    };

    // Public spot feed for spot-bound categories — works in paper mode too
    // (no credentials needed). One distinct product supported.
    let mut spot_products: Vec<String> =
        trader.cfg.categories.iter().filter_map(|c| c.spot_feed.clone()).collect();
    spot_products.sort();
    spot_products.dedup();
    if spot_products.len() > 1 {
        warn!("Multiple spot products {spot_products:?} — only {} is used", spot_products[0]);
    }
    let has_spot = !spot_products.is_empty();
    if let Some(product) = spot_products.first() {
        let feed = SpotFeed::new(COINBASE_WS_URL, product);
        let tx = tx.clone();
        let running = trader.running.clone();
        tokio::spawn(async move { feed.run(tx, running).await });
    }

    // Seed the books with REST snapshots (WS deltas need a baseline)
    trader.fetch_initial_snapshots(&trader.all_entries()).await;

    info!("{}", "=".repeat(80));
    info!("Initialization complete - trading on {} markets", trader.active_tickers.len());
    info!("{}", "=".repeat(80));

    trader.event_loop(rx, use_ws, has_spot).await;
    Ok(())
}

impl<M: MarketApi> Trader<M> {
    /// Cancel orders that ADD to positions, keep profit-taking orders
    /// (startup hygiene from live_trader_v2.initialize()).
    async fn optimize_startup_orders(&mut self) {
        info!("Optimizing orders (canceling inventory-building, keeping profit-taking)...");
        let positions = match self.api.get_positions().await {
            Ok(resp) => parse_positions(&resp),
            Err(e) => {
                warn!("Could not read positions: {e}");
                return;
            }
        };
        info!("Found {} open positions", positions.len());
        let orders = match self.api.get_orders(Some("resting"), 1000).await {
            Ok(resp) => resp.get("orders").and_then(Value::as_array).cloned().unwrap_or_default(),
            Err(e) => {
                warn!("Could not read orders: {e}");
                return;
            }
        };
        let (mut canceled, mut kept) = (0, 0);
        for order in orders {
            let ticker = order
                .get("ticker")
                .or_else(|| order.get("market_ticker"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let position = positions.get(ticker).copied().unwrap_or(0);
            if order_reduces_position(&order, position) {
                kept += 1;
            } else if let Some(oid) = order.get("order_id").and_then(Value::as_str) {
                if self.api.cancel_order(oid).await.is_ok() {
                    canceled += 1;
                }
            }
        }
        info!("✓ Canceled {canceled} orders, kept {kept} profit-taking orders");
    }

    /// Load ONNX policies; categories without a valid 20-dim model are
    /// DISABLED (same guard as Python's checkpoint dim check).
    fn load_policies(&mut self, models_dir: &str) -> Result<()> {
        let prefix = self.cfg.live.checkpoint_prefix.clone();
        for cat in &self.cfg.categories {
            let path = format!("{models_dir}/{prefix}_{}_final.onnx", cat.name);
            if !std::path::Path::new(&path).exists() {
                warn!("No ONNX checkpoint for {} — category disabled", cat.name);
                continue;
            }
            match Policy::load(&path) {
                Ok(policy) => {
                    info!("✓ Loaded model: {} ({path})", cat.name);
                    self.policies.insert(cat.name.clone(), policy);
                }
                Err(e) => warn!("✗ {}: {e}", cat.name),
            }
        }
        info!("Loaded {} models", self.policies.len());
        if self.policies.is_empty() {
            bail!("No category has a 20-dim ONNX policy in {models_dir}/ — nothing to trade. \
                   Run scripts/export_policy_onnx.py first.");
        }
        Ok(())
    }

    /// Find all open markets for each category with a loaded policy.
    /// Ask Kalshi which markets are open right now, WITHOUT touching trader
    /// state. Returns ticker -> category. Kept side-effect free so the
    /// periodic refresh can diff the result against what we already hold.
    async fn discover_universe(&self) -> HashMap<String, String> {
        let mut found: HashMap<String, String> = HashMap::new();
        let categories: Vec<String> = self.policies.keys().cloned().collect();
        for category in categories {
            let mut n = 0;
            // Follow the pagination cursor so a category with >200 open markets
            // isn't silently truncated (KXBTCD alone can exceed 200). Cap pages
            // defensively against a cursor that never clears.
            let mut cursor: Option<String> = None;
            for _ in 0..50 {
                let resp = match self
                    .api
                    .get_markets(Some(&category), Some("open"), 200, cursor.as_deref())
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!("Error finding tickers for {category}: {e}");
                        break;
                    }
                };
                for market in resp.get("markets").and_then(Value::as_array).into_iter().flatten() {
                    let Some(ticker) = market.get("ticker").and_then(Value::as_str) else {
                        continue;
                    };
                    // Guard against prefix aliasing from the API
                    if !ticker.starts_with(&category) {
                        warn!("Filtered out ticker {ticker} — does not match category {category}");
                        continue;
                    }
                    found.insert(ticker.to_string(), category.clone());
                    n += 1;
                }
                // A blank/absent cursor marks the last page.
                cursor = resp.get("cursor").and_then(Value::as_str)
                    .filter(|c| !c.is_empty()).map(str::to_string);
                if cursor.is_none() {
                    break;
                }
            }
            if n > 0 {
                info!("✓ {category}: {n} markets");
            } else {
                warn!("✗ No active market for {category}");
            }
        }
        found
    }

    /// Startup discovery: seed the tradeable universe from scratch.
    async fn discover_markets(&mut self) {
        info!("Finding active markets...");
        self.active_tickers = self.discover_universe().await;
        self.active_set = self.active_tickers.keys().cloned().collect();
    }

    /// Periodic re-discovery — the fix for the 2026-07-24 outage.
    ///
    /// Hourly series (KXBTCD) roll continuously: the tickers open at boot are
    /// all expired within a day. Without this the bot keeps running happily
    /// against a universe of dead markets and quotes nothing, with no error
    /// anywhere. Here we re-discover, subscribe to what's new, and drop what
    /// has expired.
    ///
    /// Pruning rule: only tickers that are BOTH gone from the exchange's open
    /// list AND flat are dropped. A settled-but-still-held position keeps its
    /// entry in `active_tickers` so the existing settlement / drift / entry-
    /// price-backfill paths (which look up category there) behave unchanged.
    async fn refresh_markets(&mut self) {
        let discovered = self.discover_universe().await;
        // An API failure returns an empty map; treating that as "everything
        // expired" would drop the whole universe, so leave state untouched
        // and try again on the next tick.
        if discovered.is_empty() {
            warn!("Market refresh found no open markets — keeping current universe");
            return;
        }

        let (added, removed) = diff_universe(&self.active_tickers, &discovered);
        // Drop only the expired tickers we hold no inventory in.
        let prunable: Vec<String> = removed
            .into_iter()
            .filter(|t| self.state.tickers.get(t).is_none_or(|ts| ts.position == 0))
            .collect();
        if added.is_empty() && prunable.is_empty() {
            return;
        }

        for ticker in &prunable {
            self.active_tickers.remove(ticker);
        }
        for ticker in &added {
            if let Some(category) = discovered.get(ticker) {
                self.active_tickers.insert(ticker.clone(), category.clone());
            }
        }
        self.active_set = self.active_tickers.keys().cloned().collect();

        // New markets need tick size + close time before they can be quoted,
        // and a REST snapshot to seed the book that WS deltas then update.
        if !added.is_empty() {
            let entries: Vec<(String, String)> = added
                .iter()
                .filter_map(|t| discovered.get(t).map(|c| (t.clone(), c.clone())))
                .collect();
            self.fetch_market_details(&entries).await;
            self.fetch_initial_snapshots(&entries).await;
        }

        // Strike ladders are keyed off the ticker set — rebuild after churn.
        self.ladders = Ladders::build(self.active_tickers.keys().map(String::as_str));

        // Hand the refreshed universe to the WebSocket task so it subscribes
        // to the additions without waiting for a reconnect.
        if let Some(tx) = &self.universe_tx {
            let _ = tx.send(self.active_tickers.keys().cloned().collect());
        }

        info!("Market refresh: +{} new, -{} expired, now {} active",
              added.len(), prunable.len(), self.active_tickers.len());
    }

    /// Every (ticker, category) pair currently in the tradeable universe.
    fn all_entries(&self) -> Vec<(String, String)> {
        self.active_tickers.iter().map(|(t, c)| (t.clone(), c.clone())).collect()
    }

    /// Tick sizes (subpenny detection) + close times, stored per ticker.
    /// Takes an explicit list so the periodic refresh can fetch details for
    /// only the newly-opened markets instead of re-walking the whole universe.
    async fn fetch_market_details(&mut self, entries: &[(String, String)]) {
        info!("Fetching market details for {} markets...", entries.len());
        let mut subpenny = 0;
        for (ticker, category) in entries {
            let ts = self.state.ticker(ticker, category);
            match self.api.get_market(ticker).await {
                Ok(resp) => {
                    let market = resp.get("market").cloned().unwrap_or_default();
                    ts.tick = market
                        .get("price_ranges")
                        .and_then(Value::as_array)
                        .map(|ranges| {
                            ranges
                                .iter()
                                .filter_map(|r| json_f64(r.get("step")))
                                .fold(f64::INFINITY, f64::min)
                        })
                        .filter(|m| m.is_finite())
                        .unwrap_or(0.01);
                    if ts.tick <= 0.001 {
                        subpenny += 1;
                    }
                    // ISO close time -> epoch once at startup; hot paths use the float
                    let close = market
                        .get("close_time")
                        .or_else(|| market.get("expected_expiration_time"))
                        .and_then(Value::as_str);
                    if let Some(close) = close {
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(close) {
                            ts.close_time_s = Some(dt.timestamp_millis() as f64 / 1000.0);
                        }
                    }
                }
                Err(_) => ts.tick = 0.01,
            }
        }
        info!("✓ Market details loaded: {subpenny}/{} support subpenny", entries.len());
    }

    /// REST snapshots seed the books that WS deltas then update. Takes an
    /// explicit list so a refresh only snapshots the newly-opened markets.
    async fn fetch_initial_snapshots(&mut self, entries: &[(String, String)]) {
        info!("Fetching initial orderbook snapshots for {} markets...", entries.len());
        let (mut ok, mut failed) = (0u32, 0u32);
        for (ticker, category) in entries {
            match self.api.get_orderbook(ticker, 10).await {
                Ok(resp) => {
                    let payload = resp.get("orderbook").unwrap_or(&resp);
                    self.state.ticker(ticker, category).book.load_snapshot(payload);
                    ok += 1;
                    if ok % 50 == 0 {
                        info!("Snapshot progress: {ok}/{}", entries.len());
                    }
                    // REST rate-limit headroom (10 req/s is safe)
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => failed += 1,
            }
        }
        info!("✓ Fetched {ok}/{} initial orderbook snapshots ({failed} failed)", entries.len());
    }

    // --- event loop -----------------------------------------------------------

    async fn event_loop(&mut self, mut rx: mpsc::Receiver<WsEvent>, use_ws: bool, has_spot: bool) {
        info!("Starting live trading...");
        let mut fills_tick = tokio::time::interval(Duration::from_secs(5));
        let mut sync_tick = tokio::time::interval(Duration::from_secs(30));
        let mut exit_tick = tokio::time::interval(Duration::from_secs(10));
        let mut daily_tick = tokio::time::interval(Duration::from_secs(60));
        let mut hourly_tick = tokio::time::interval(Duration::from_secs(3600));
        let mut poll_tick = tokio::time::interval(Duration::from_secs(10));
        // Re-discover markets every 15 min. Hourly series roll on the hour, so
        // this picks up a new cohort well within the hour it opens, while
        // staying cheap (one paginated /markets walk per category).
        let mut discover_tick = tokio::time::interval(Duration::from_secs(MARKET_REFRESH_S));
        // The first interval tick fires immediately; consume them so the
        // hourly report doesn't print at startup etc.
        fills_tick.tick().await;
        sync_tick.tick().await;
        exit_tick.tick().await;
        daily_tick.tick().await;
        hourly_tick.tick().await;
        poll_tick.tick().await;
        discover_tick.tick().await;

        // Fills lookback starts 60s before boot; the API takes epoch SECONDS
        let mut fills_last_ts = epoch_now() as i64 - 60;

        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                Some(event) = rx.recv(), if use_ws || has_spot => match event {
                    WsEvent::Book { ticker, msg } => self.on_book_update(&ticker, &msg).await,
                    WsEvent::Trade { ticker, msg } => self.on_trade(&ticker, &msg),
                    WsEvent::Spot { price } => self.on_spot_tick(price).await,
                },
                _ = fills_tick.tick() => {
                    // Page through the cursor so a burst of >200 fills in the
                    // 60s overlap window can't silently drop any (dedupe by id
                    // handles the overlap re-reads). Cap pages defensively.
                    let mut cursor: Option<String> = None;
                    let mut ok = true;
                    for _ in 0..25 {
                        match self.api.get_fills(fills_last_ts - 60, 200, cursor.as_deref(), None).await {
                            Ok(resp) => {
                                for fill in resp.get("fills").and_then(Value::as_array).into_iter().flatten() {
                                    self.process_fill(fill);
                                }
                                cursor = resp.get("cursor").and_then(Value::as_str)
                                    .filter(|c| !c.is_empty()).map(str::to_string);
                                if cursor.is_none() {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Error in fill reconciliation loop: {e}");
                                ok = false;
                                break;
                            }
                        }
                    }
                    // Only advance the watermark on a clean sweep; a failed
                    // page keeps the old lookback so nothing is skipped.
                    if ok {
                        fills_last_ts = epoch_now() as i64;
                    }
                }
                _ = sync_tick.tick() => self.sync_positions().await,
                _ = exit_tick.tick() => {
                    // Timer path for the gate: a silently-dead spot feed
                    // produces no ticks, so this is what pulls resting quotes
                    self.update_spot_gate().await;
                    self.check_exits().await;
                }
                _ = daily_tick.tick() => self.maybe_daily_reset(),
                _ = hourly_tick.tick() => self.hourly_report(),
                _ = discover_tick.tick() => self.refresh_markets().await,
                _ = poll_tick.tick(), if !use_ws => self.rest_poll().await,
            }
        }
        info!("Trading stopped.");
    }

    /// True = spot-bound categories must not quote right now. Evaluated
    /// fresh (not cached): staleness can develop without any tick arriving.
    fn spot_gated(&self, now_mono: f64) -> bool {
        if self.spot.is_stale(now_mono, self.cfg.spot.stale_max_s) {
            return true;
        }
        match self.spot.ret(60.0, now_mono) {
            // Hysteresis band keyed on the latched gate state so a return
            // hovering at the threshold doesn't flap the gate (each flap
            // cancels + re-places every resting quote, losing FIFO priority)
            Some(r) => trend_gated(
                r.abs(),
                self.spot_gate_on,
                self.cfg.spot.gate_ret_60s,
                self.cfg.spot.gate_release_ratio,
            ),
            None => true, // <60s of history — conservative until warm
        }
    }

    /// Spot-implied fair-value shift for one ticker, clamped. None = orphan
    /// whose worst-case unknown shift is >= 1 tick (skip quoting; spec's
    /// calm-only rule). Some(0.0) for non-bound categories.
    fn fv_shift_for(&self, ticker: &str, category: &str) -> Option<f64> {
        if !self.spot_bound.contains(category) {
            return Some(0.0);
        }
        let (Some(spot_now), Some(ema)) = (self.spot.latest(), self.spot.ema()) else {
            return None;
        };
        let dist = spot_now - ema;
        let mid_of = |t: &str| self.state.tickers.get(t).and_then(|ts| ts.book.mid());
        match self.ladders.delta_for(ticker, self.cfg.spot.delta_max, mid_of) {
            Some(delta) => {
                Some((delta * dist).clamp(-self.cfg.spot.fv_shift_max, self.cfg.spot.fv_shift_max))
            }
            // 0.01 = one tick: unknown shift smaller than a tick can't move the quote
            None if self.cfg.spot.delta_max * dist.abs() < 0.01 => Some(0.0),
            None => None,
        }
    }

    /// (allow_bid, allow_ask) for `ticker` under the per-ladder tail cap.
    /// Suppressing a side stops ADDING exposure in the direction whose
    /// worst-case ladder loss already reached the cap; existing positions
    /// stay managed by stops/settlement. Non-strike tickers are never gated.
    fn ladder_tail_gate(&self, ticker: &str) -> (bool, bool) {
        let cap = self.cfg.live.max_ladder_tail_loss;
        if cap <= 0.0 {
            return (true, true);
        }
        let Some(event) = ladder::ladder_prefix(ticker) else {
            return (true, true);
        };
        let entries = self.state.tickers.iter().filter_map(|(t, ts)| {
            if ts.position == 0 || ladder::ladder_prefix(t) != Some(event) {
                return None;
            }
            // Mark preference: live mid, else cost basis, else 0.5
            let mark = ts.book.mid().or(ts.entry_price).unwrap_or(0.5);
            Some((ts.position, mark))
        });
        let (up_loss, down_loss) = ladder::tail_losses(entries);
        (down_loss < cap, up_loss < cap) // bids add down-risk, asks add up-risk
    }

    /// Fold one spot tick: trend-gate edge detection (one-shot cancel of
    /// resting spot-bound quotes) + proactive unwind of adverse inventory.
    async fn on_spot_tick(&mut self, price: f64) {
        let now = mono_now();
        if !self.spot.on_tick(price, now) {
            return; // outlier dropped
        }
        self.update_spot_gate().await;
        self.check_spot_unwind(now).await;
    }

    /// Evaluate the gate and fire the one-shot cancel on its rising edge.
    /// Called on every spot tick AND on the 10s exit timer: a silently-dead
    /// feed produces no ticks, so without the timer path a staleness gate
    /// would never pull resting quotes — they'd sit as pickoff bait for the
    /// whole outage. Staleness must fail safe even when no data arrives.
    async fn update_spot_gate(&mut self) {
        if self.spot_bound.is_empty() {
            return;
        }
        let now = mono_now();
        let gated = self.spot_gated(now);
        if gated && !self.spot_gate_on {
            let ret = self.spot.ret(60.0, now).unwrap_or(0.0);
            warn!("SPOT GATE ON (60s ret {:+.3}%) — canceling spot-bound quotes", ret * 100.0);
            self.cancel_spot_bound_quotes().await;
        } else if !gated && self.spot_gate_on {
            let ret = self.spot.ret(60.0, now).unwrap_or(0.0);
            info!("SPOT GATE OFF (60s ret {:+.3}%)", ret * 100.0);
        }
        self.spot_gate_on = gated;
    }

    /// One-shot on the gate's rising edge: resting quotes are pickoff bait.
    async fn cancel_spot_bound_quotes(&mut self) {
        let mut ids = Vec::new();
        for ts in self.state.tickers.values_mut() {
            if self.spot_bound.contains(&ts.category) {
                ids.extend(ts.bid_order_id.take());
                ids.extend(ts.ask_order_id.take());
            }
        }
        for id in &ids {
            let _ = self.api.cancel_order(id).await;
        }
        if !ids.is_empty() {
            info!("Canceled {} resting spot-bound quotes", ids.len());
        }
    }

    /// SPOT-ADVERSE exits, evaluated on spot ticks (1/s per ticker). Runs
    /// even for halted categories — unwinding REDUCES risk.
    async fn check_spot_unwind(&mut self, now_mono: f64) {
        // Local position only updates when the 5s fills poll books the fill.
        // Re-evaluating at 1s would re-fire a full-size IOC at the (stale
        // but fillable) book up to ~4 more times before the first fill
        // lands — overshooting a long straight through flat into a short.
        // 10s matches check_exits' cadence, which is safe against the 5s
        // poll by construction.
        const UNWIND_REFIRE_HOLDOFF_S: f64 = 10.0;
        if self.spot.is_stale(now_mono, self.cfg.spot.stale_max_s) {
            return;
        }
        let tickers: Vec<String> = self
            .state
            .tickers
            .iter()
            .filter(|(_, ts)| ts.position != 0 && self.spot_bound.contains(&ts.category))
            .map(|(t, _)| t.clone())
            .collect();
        for ticker in tickers {
            let next = self.spot_unwind_next.get(&ticker).copied().unwrap_or(f64::NEG_INFINITY);
            let fired =
                self.spot_unwind_fired_until.get(&ticker).copied().unwrap_or(f64::NEG_INFINITY);
            if now_mono < next || now_mono < fired {
                continue;
            }
            self.spot_unwind_next.insert(ticker.clone(), now_mono + 1.0);
            let category = self.state.tickers[&ticker].category.clone();
            let Some(shift) = self.fv_shift_for(&ticker, &category) else { continue };
            let ts = &self.state.tickers[&ticker];
            let Some(plan) = spot_unwind_decision(ts, shift, self.cfg.spot.unwind_shift) else {
                continue;
            };
            info!("EXIT (SPOT-ADVERSE): {ticker} inv={} fv_shift={shift:+.3}", ts.position);
            executor::apply_exit_plan(&self.api, &category, &ticker, &plan).await;
            self.spot_unwind_fired_until
                .insert(ticker.clone(), now_mono + UNWIND_REFIRE_HOLDOFF_S);
        }
    }

    /// Hot path: fold the message into the book, then decide and quote.
    /// Ordering matches the Python callback exactly.
    async fn on_book_update(&mut self, ticker: &str, msg: &Value) {
        let Some(category) = self.active_tickers.get(ticker).cloned() else { return };
        let ts = self.state.ticker(ticker, &category);
        // ALWAYS fold the message first — even when throttled or halted;
        // skipping updates leaves a stale book that poisons later decisions
        if msg.get("price_dollars").is_some() && msg.get("delta_fp").is_some() {
            ts.book.apply_delta(
                msg.get("side").and_then(Value::as_str).unwrap_or(""),
                json_f64(msg.get("price_dollars")).unwrap_or(0.0),
                json_f64(msg.get("delta_fp")).unwrap_or(0.0),
            );
        } else {
            ts.book.load_snapshot(msg);
        }

        if !ts.book.is_valid() {
            return;
        }
        // Throttle peek BEFORE the expensive path (obs + inference dominate
        // callback time); plan_quotes owns advancing the throttle clock
        if mono_now() - ts.last_quote_time < 1.0 {
            return;
        }
        if self.state.halted_categories.contains(&category) {
            return;
        }
        let (ok, halt_reason) = check_risk_limits(&self.state, &self.cfg.live, Some(&self.active_set));
        if let Some(reason) = halt_reason {
            self.halt_all_trading(reason).await;
            return;
        }
        if !ok {
            return;
        }

        // Spot defense (layers 9-11, spot-bound categories only): staleness
        // and trend gates block quoting; the FV shift re-centers quotes.
        let fv_shift = if self.spot_bound.contains(&category) {
            if self.spot_gated(mono_now()) {
                return;
            }
            match self.fv_shift_for(ticker, &category) {
                Some(shift) => shift,
                None => return, // orphan strike while spot is moving
            }
        } else {
            0.0
        };

        let (allow_bid, allow_ask) = self.ladder_tail_gate(ticker);

        let ts = self.state.ticker(ticker, &category);
        let Some(obs) = build_observation(ticker, ts, &self.cfg.mm, epoch_now(), mono_now()) else {
            return;
        };
        let Some(policy) = self.policies.get_mut(&category) else { return };
        let action = match policy.predict(&obs) {
            Ok(action) => action,
            Err(e) => {
                error!("Inference error {category}/{ticker}: {e}");
                return;
            }
        };

        let max_inventory = self.cats.get(&category).map_or(5, |c| c.max_inventory);
        let open_orders = self.state.open_order_count();
        let backoff = self.state.balance_backoff_until;
        let ts = self.state.ticker(ticker, &category);
        let plan = plan_quotes(
            action,
            fv_shift,
            ts,
            &self.cfg.mm,
            max_inventory,
            open_orders,
            backoff,
            mono_now(),
        );
        if let Some(mut plan) = plan {
            plan.place_bid &= allow_bid;
            plan.place_ask &= allow_ask;
            executor::apply_quote_plan(&self.api, &self.cfg.mm, &category, ticker, &plan,
                                       &mut self.state, mono_now())
            .await;
        }
    }

    /// Record a public trade print (feeds obs [9] volume and [16] flow).
    fn on_trade(&mut self, ticker: &str, msg: &Value) {
        let count = json_f64(msg.get("count_fp").or_else(|| msg.get("count"))).unwrap_or(1.0) as i64;
        let taker_side = match msg.get("taker_side").and_then(Value::as_str) {
            Some("yes") => TakerSide::Yes,
            Some("no") => TakerSide::No,
            _ => return,
        };
        let now = epoch_now();
        let category = self.active_tickers.get(ticker).cloned().unwrap_or_default();
        let trades = &mut self.state.ticker(ticker, &category).recent_trades;
        trades.push((now, count, taker_side));
        // Prune to the 60s window on every print so this list stays bounded
        // even for a market whose book never becomes valid (which would
        // otherwise never reach the prune inside build_observation).
        book::prune_trades(trades, now);
    }

    /// Book one exchange-reported fill: position, entry, fee, PnL.
    fn process_fill(&mut self, fill: &Value) {
        let Some(nf) = normalize_fill(fill) else { return };
        if self.state.seen_fill(&nf.fill_id) {
            return;
        }
        let category = self
            .active_tickers
            .get(&nf.ticker)
            .cloned()
            .unwrap_or_else(|| nf.ticker.split('-').next().unwrap_or("").to_string());
        info!("FILL: {category}/{} {} {} @ {:.1}¢ ({})",
              nf.ticker, if nf.long_yes { "buy" } else { "sell" }, nf.size,
              nf.price_yes * 100.0, if nf.is_taker { "TAKER" } else { "maker" });

        let rate = if nf.is_taker { self.cfg.mm.taker_fee_rate } else { self.cfg.mm.maker_fee_rate };
        let fee = state::fill_fee(rate, nf.size, nf.price_yes);
        self.state.ticker(&nf.ticker, &category);
        let entry_before = self.state.tickers[&nf.ticker].entry_price;
        let ticker = nf.ticker.clone();
        let realized = self.state.apply_fill(&ticker, &nf, fee, epoch_now());
        if realized != 0.0 {
            info!("PNL: {ticker} realized ${realized:+.4} (entry={:.3} exit={:.3})",
                  entry_before.unwrap_or(nf.price_yes), nf.price_yes);
        }

        self.enforce_category_halt(&category);
    }

    /// Per-category kill switch: after N consecutive losing closes in a
    /// category, halt it (stop quoting its markets). apply_fill and
    /// apply_settlement maintain consecutive_losses (reset to 0 on any
    /// winning close); this only reads it. 0 = disabled. The halt is a DAILY
    /// circuit breaker: it stays tripped for the rest of the local day, then
    /// reset_daily clears it (and the streak) so the category quotes again the
    /// next day instead of latching until a manual restart.
    fn enforce_category_halt(&mut self, category: &str) {
        let limit = self.cfg.live.halt_on_consecutive_losses;
        if limit > 0 && !self.state.halted_categories.contains(category) {
            let losses = self.state.consecutive_losses.get(category).copied().unwrap_or(0);
            if losses >= limit {
                warn!("HALTING CATEGORY {category}: {losses} consecutive losses (limit {limit})");
                self.state.halted_categories.insert(category.to_string());
            }
        }
    }

    /// Reconcile local inventory with the exchange's authoritative state.
    async fn sync_positions(&mut self) {
        let resp = match self.api.get_positions().await {
            Ok(resp) => resp,
            Err(e) => {
                error!("Error syncing positions: {e}");
                return;
            }
        };
        let exchange = parse_positions(&resp);
        let local_nonzero: Vec<String> = self
            .state
            .tickers
            .iter()
            .filter(|(_, ts)| ts.position != 0)
            .map(|(t, _)| t.clone())
            .collect();
        let mut drift = 0;
        let mut vanished: Vec<String> = Vec::new();
        let now_epoch = epoch_now();
        let all: HashSet<String> = exchange.keys().cloned().chain(local_nonzero).collect();
        for ticker in all {
            let remote = exchange.get(&ticker).copied().unwrap_or(0);
            let category = self.active_tickers.get(&ticker).cloned().unwrap_or_default();
            let ts = self.state.ticker(&ticker, &category);
            if ts.position != remote {
                // A held position on a market this bot quotes that DISAPPEARS
                // from the exchange almost always means the market SETTLED.
                // Plain zeroing here silently discarded the settlement PnL
                // (winners that expire ITM were never booked, so the counters
                // and loss-halts ran systematically pessimistic) — route
                // through the settlement lookup instead of correcting now.
                if remote == 0 && self.active_tickers.contains_key(&ticker) {
                    vanished.push(ticker);
                    continue;
                }
                // A fill booked seconds ago means this positions snapshot and
                // the fills poll are mid-race (observed walking a position
                // 1->2->3->2 live); give it one cycle to converge — real
                // drift is still corrected on the next sync.
                if now_epoch - ts.last_fill_epoch < 10.0 {
                    continue;
                }
                warn!("POSITION DRIFT: {ticker} local={} exchange={remote}, correcting", ts.position);
                // A flat, never-filled local ticker picking up an exchange
                // position = inventory carried across a restart, not this
                // process's trading — its closes are exempt from the
                // kill-switch streak (see TickerState::carried)
                if ts.position == 0 && ts.last_fill_epoch == 0.0 && remote != 0 {
                    ts.carried = true;
                }
                ts.position = remote;
                drift += 1;
            }
        }
        info!("Synced {} positions from Kalshi{}", exchange.len(),
              if drift > 0 { format!(" ({drift} drifts corrected)") } else { String::new() });
        if !vanished.is_empty() {
            self.resolve_vanished_positions(vanished).await;
        }

        // entry_price is in-memory only — a restart re-syncs `position` from
        // the exchange above but has no cost basis for it, so the next fill
        // would otherwise be treated as the position's entire history
        // (wildly overstating realized PnL on the first close post-redeploy).
        // Only backfill markets this bot actually manages: the account also
        // holds ~200 legacy positions from other categories that this bot
        // reconciles but never quotes or closes, and querying fills for all
        // of them on every restart risks rate-limiting (429s already seen).
        let needs_backfill: Vec<String> = self
            .state
            .tickers
            .iter()
            .filter(|(t, ts)| {
                ts.position != 0 && ts.entry_price.is_none() && self.active_tickers.contains_key(*t)
            })
            .map(|(t, _)| t.clone())
            .collect();
        for ticker in needs_backfill {
            self.backfill_entry_price(&ticker).await;
        }
    }

    /// Book settlements for held positions that vanished from the exchange.
    ///
    /// One newest-first settlements page covers every ticker at once (a
    /// daily ladder settles as a batch). A ticker found there books through
    /// `apply_settlement` — realized PnL, win/loss, and the same per-category
    /// kill switch a fill-based close feeds. A ticker NOT found stays
    /// untouched and retries next sync: the settlements feed can lag the
    /// positions feed, and booking is only possible while the local position
    /// still exists. After MAX_SETTLEMENT_CHECKS misses it's zeroed as plain
    /// drift so a true desync can't wedge the accounting forever.
    async fn resolve_vanished_positions(&mut self, tickers: Vec<String>) {
        let resp = match self.api.get_settlements(200).await {
            Ok(resp) => resp,
            Err(e) => {
                error!("Error fetching settlements: {e}");
                return; // positions untouched; next sync retries
            }
        };
        let settled: HashMap<String, Option<bool>> = resp
            .get("settlements")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(normalize_settlement)
            .map(|s| (s.ticker, s.result_yes))
            .collect();
        for ticker in tickers {
            // The fills poll (5s) may have flattened it since the position
            // snapshot was taken — nothing left to resolve then
            if self.state.tickers.get(&ticker).map(|ts| ts.position).unwrap_or(0) == 0 {
                self.settlement_checks.remove(&ticker);
                continue;
            }
            let Some(&result_yes) = settled.get(&ticker) else {
                let checks = self.settlement_checks.entry(ticker.clone()).or_insert(0);
                *checks += 1;
                if *checks >= MAX_SETTLEMENT_CHECKS {
                    warn!("POSITION DRIFT: {ticker} vanished with no settlement record \
                           after {checks} checks — zeroing, PnL may be unbooked");
                    if let Some(ts) = self.state.tickers.get_mut(&ticker) {
                        ts.position = 0;
                        ts.entry_price = None;
                    }
                    self.settlement_checks.remove(&ticker);
                }
                continue;
            };
            self.settlement_checks.remove(&ticker);
            match self.state.apply_settlement(&ticker, result_yes) {
                Some(realized) => {
                    let result = match result_yes {
                        Some(true) => "yes",
                        Some(false) => "no",
                        None => "void",
                    };
                    info!("SETTLEMENT: {ticker} result={result} realized ${realized:+.4}");
                    let category = self.active_tickers.get(&ticker).cloned().unwrap_or_default();
                    self.enforce_category_halt(&category);
                }
                None => info!("SETTLEMENT: {ticker} cleared without PnL (no cost basis or non-binary result)"),
            }
        }
    }

    /// Reconstruct a lost cost basis for `ticker` by replaying its full fill
    /// history from flat. Only called right after `sync_positions` finds a
    /// nonzero position with no local entry price (see there for why that
    /// happens). Leaves `entry_price` untouched (still None) on any error or
    /// on a sanity-check mismatch — an unknown entry falls back safely
    /// elsewhere (position_value, PNL logs) rather than booking a wrong one.
    async fn backfill_entry_price(&mut self, ticker: &str) {
        let mut raw_fills: Vec<Value> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..10 {
            let resp = match self.api.get_fills(0, 200, cursor.as_deref(), Some(ticker)).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Error backfilling entry price for {ticker}: {e}");
                    return;
                }
            };
            raw_fills.extend(
                resp.get("fills").and_then(Value::as_array).cloned().unwrap_or_default(),
            );
            cursor = resp.get("cursor").and_then(Value::as_str)
                .filter(|c| !c.is_empty()).map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        // The API returns fills newest-first; the replay needs oldest-first.
        raw_fills.sort_by(|a, b| {
            json_f64(a.get("ts")).partial_cmp(&json_f64(b.get("ts"))).unwrap_or(std::cmp::Ordering::Equal)
        });
        let fills: Vec<_> = raw_fills.iter().filter_map(normalize_fill).collect();
        let (replayed_position, entry_price) = replay_entry_price(&fills);
        let Some(entry_price) = entry_price else {
            warn!("Entry-price backfill for {ticker}: replay landed flat, nothing to set");
            return;
        };
        let live_position = self.state.tickers.get(ticker).map(|ts| ts.position).unwrap_or(0);
        if replayed_position != live_position {
            warn!("Entry-price backfill for {ticker}: replayed position {replayed_position} \
                   != live {live_position} (fill history likely truncated), leaving entry unset");
            return;
        }
        info!("Reconstructed entry price for {ticker}: {entry_price:.3} (from {} fills)", fills.len());
        if let Some(ts) = self.state.tickers.get_mut(ticker) {
            ts.entry_price = Some(entry_price);
        }
    }

    /// Stop-loss + expiry exits (crossing IOC), every 10 seconds.
    async fn check_exits(&mut self) {
        let now_s = epoch_now();
        let tickers: Vec<String> = self
            .state
            .tickers
            .iter()
            .filter(|(_, ts)| ts.position != 0)
            .map(|(t, _)| t.clone())
            .collect();
        for ticker in tickers {
            // A spot unwind IOC may be filled-but-unbooked (5s fills poll):
            // firing the mid-based stop inside the holdoff would double-exit
            // the already-flattened position
            let fired =
                self.spot_unwind_fired_until.get(&ticker).copied().unwrap_or(f64::NEG_INFINITY);
            if mono_now() < fired {
                continue;
            }
            let ts = &self.state.tickers[&ticker];
            let Some(plan) = exit_decision(ts, now_s) else { continue };
            let mid = ts.book.mid().unwrap_or(0.0);
            let entry = ts.entry_price.unwrap_or(mid);
            info!("EXIT ({}): {ticker} inv={} entry={entry:.3} mid={mid:.3} unrealized={:+.4}",
                  plan.reason, ts.position, ts.position as f64 * (mid - entry));
            let category = self.active_tickers.get(&ticker).cloned()
                .unwrap_or_else(|| "UNKNOWN".to_string());
            executor::apply_exit_plan(&self.api, &category, &ticker, &plan).await;
        }
    }

    async fn halt_all_trading(&mut self, reason: &str) {
        error!("HALTING ALL TRADING: {reason}");
        self.running.store(false, Ordering::Relaxed);
        match self.api.cancel_all_orders().await {
            Ok(n) => info!("Canceled all open orders ({n})"),
            Err(e) => error!("Error canceling orders: {e}"),
        }
    }

    fn maybe_daily_reset(&mut self) {
        use chrono::Datelike;
        let now = chrono::Local::now();
        let last = chrono::DateTime::from_timestamp(self.state.last_reset as i64, 0)
            .unwrap_or_default()
            .with_timezone(&chrono::Local);
        if (now.year(), now.ordinal()) != (last.year(), last.ordinal()) {
            info!("Daily reset - PnL: ${:.2}, Cumulative: ${:.2}",
                  self.state.daily_pnl, self.state.cumulative_pnl);
            info!("Fill rate: {:.1}%, Win rate: {:.1}%",
                  self.state.fill_rate() * 100.0, self.state.win_rate() * 100.0);
            self.state.reset_daily(epoch_now());
        }
    }

    fn hourly_report(&self) {
        let s = &self.state;
        let total = s.taker_fills + s.maker_fills;
        let taker_frac = if total > 0 { s.taker_fills as f64 / total as f64 } else { 0.0 };
        // Deployment gate: a market maker should be ~0% taker; >10% means
        // post-only/clamping isn't doing its job (or exits dominate)
        let gate = if taker_frac < 0.10 { "PASS" } else { "FAIL — investigate" };
        let open = s.tickers.values().filter(|ts| ts.position != 0).count();
        info!("{}", "=".repeat(80));
        info!("HOURLY SUMMARY");
        info!("Quotes sent: {} | Fills: {} (fill rate {:.1}%)",
              s.quotes_sent, s.fills, s.fill_rate() * 100.0);
        info!("Taker/maker: {}/{} (taker {:.1}%) — gate: {gate}",
              s.taker_fills, s.maker_fills, taker_frac * 100.0);
        info!("Fees paid: ${:.2} | Wins: {} Losses: {} (win rate {:.1}%)",
              s.fees_paid, s.wins, s.losses, s.win_rate() * 100.0);
        info!("Daily PnL: ${:.2} | Cumulative PnL: ${:.2} | Positions open: {open}",
              s.daily_pnl, s.cumulative_pnl);
        // Spot-feed health: a connected-but-silent feed gates all spot-bound
        // quoting with no other log signal — this line is the 3am diagnostic
        if !self.spot_bound.is_empty() {
            let now = mono_now();
            match self.spot.latest() {
                Some(px) => info!(
                    "Spot: {px:.2} (gated: {}, stale: {})",
                    self.spot_gated(now),
                    self.spot.is_stale(now, self.cfg.spot.stale_max_s),
                ),
                None => info!("Spot: NO DATA — spot-bound categories are dark"),
            }
        }
        info!("{}", "=".repeat(80));
    }

    /// Fallback market data via REST when the WebSocket is unavailable —
    /// routes through the same hot path as WS ticks.
    async fn rest_poll(&mut self) {
        let entries: Vec<String> = self.active_tickers.keys().cloned().collect();
        for ticker in entries {
            match self.api.get_orderbook(&ticker, 10).await {
                Ok(resp) => {
                    let payload = resp.get("orderbook").cloned().unwrap_or(Value::Null);
                    self.on_book_update(&ticker, &payload).await;
                }
                Err(e) => error!("Error polling {ticker}: {e}"),
            }
        }
    }
}

// --- small parsers ---------------------------------------------------------------

/// True if a resting order REDUCES the given signed YES-frame position.
/// Direction is `action` ALONE (buy = +yes, sell = -yes) — the same
/// exchange rule as fills; `side` (yes/no) only selects the price field and
/// must never be used for direction (the original heuristic did, and
/// canceled every position-reducing order at startup).
fn order_reduces_position(order: &Value, position: i64) -> bool {
    match order.get("action").and_then(Value::as_str) {
        Some("sell") => position > 0,
        Some("buy") => position < 0,
        _ => false,
    }
}

fn json_f64(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Positions payload -> ticker -> signed contracts. V2 nests under
/// "market_positions" with position_fp strings; older shapes used
/// "positions"/"position". Accept all (same tolerance as Python).
fn parse_positions(resp: &Value) -> HashMap<String, i64> {
    let rows = resp
        .get("market_positions")
        .or_else(|| resp.get("positions"))
        .and_then(Value::as_array);
    let mut out = HashMap::new();
    for pos in rows.into_iter().flatten() {
        let ticker = pos
            .get("ticker")
            .or_else(|| pos.get("market_ticker"))
            .and_then(Value::as_str);
        let Some(ticker) = ticker else { continue };
        let position = json_f64(pos.get("position_fp").or_else(|| pos.get("position")))
            .unwrap_or(0.0)
            .round_ties_even() as i64;
        out.insert(ticker.to_string(), position);
    }
    out
}

/// Compare the tradeable universe we currently hold against a freshly
/// discovered one, returning `(added, removed)` tickers.
///
/// Why this exists: Kalshi series like KXBTCD are HOURLY — tickers open and
/// expire continuously. A bot that discovers markets only at startup slowly
/// goes blind as its boot-time cohort expires, which is exactly what took the
/// bot down on 2026-07-24. The trader calls this on a timer so it can
/// subscribe to what's new and forget what's dead.
fn diff_universe(
    old: &HashMap<String, String>,
    new: &HashMap<String, String>,
) -> (Vec<String>, Vec<String>) {
    let added = new.keys().filter(|t| !old.contains_key(*t)).cloned().collect();
    let removed = old.keys().filter(|t| !new.contains_key(*t)).cloned().collect();
    (added, removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn startup_hygiene_keeps_position_reducing_orders() {
        // Regression for the 2026-07-19 deploy: the old classifier read
        // `market_ticker` (live records carry `ticker`) and keyed direction
        // off `side` (yes/no — which only selects the PRICE field). It
        // therefore canceled all 45 legacy-unwind exit orders on every boot.
        // Direction is `action` alone (same YES-frame rule as fills).
        let order = |action: &str, side: &str| {
            json!({"ticker": "T", "action": action, "side": side, "order_id": "o"})
        };
        // Long: sells reduce (keep), buys extend (cancel) — either side field
        assert!(order_reduces_position(&order("sell", "yes"), 3));
        assert!(order_reduces_position(&order("sell", "no"), 3));
        assert!(!order_reduces_position(&order("buy", "yes"), 3));
        // Short: buys reduce (keep), sells extend (cancel)
        assert!(order_reduces_position(&order("buy", "no"), -2));
        assert!(order_reduces_position(&order("buy", "yes"), -2));
        assert!(!order_reduces_position(&order("sell", "no"), -2));
        // Flat or unknown: nothing to reduce — cancel
        assert!(!order_reduces_position(&order("sell", "yes"), 0));
        assert!(!order_reduces_position(&json!({"ticker": "T"}), 3));
    }

    #[test]
    fn parse_positions_handles_v2_and_legacy() {
        let v2 = json!({"market_positions": [
            {"ticker": "A", "position_fp": "3.00"},
            {"market_ticker": "B", "position_fp": "-2.00"},
        ]});
        let map = parse_positions(&v2);
        assert_eq!(map["A"], 3);
        assert_eq!(map["B"], -2);
        let legacy = json!({"positions": [{"ticker": "C", "position": 5}]});
        assert_eq!(parse_positions(&legacy)["C"], 5);
    }

    // --- periodic market re-discovery ------------------------------------
    // Regression for the 2026-07-24 outage: `discover_markets()` ran ONCE at
    // startup, so the tradeable universe was frozen at boot. KXBTCD is an
    // hourly series — every ticker found at boot had expired ~24h later, and
    // the bot then quoted nothing for 7 days while position sync and the spot
    // gate kept running, so it looked perfectly healthy. `diff_universe` is
    // what lets the trader refresh that universe on a timer.

    /// Build a ticker -> category map from a list, for terser tests.
    fn universe(tickers: &[&str]) -> HashMap<String, String> {
        tickers.iter().map(|t| (t.to_string(), "KXBTCD".to_string())).collect()
    }

    #[test]
    fn diff_universe_reports_added_and_removed() {
        let old = universe(&["A", "B", "C"]);
        let new = universe(&["B", "C", "D"]);
        let (mut added, mut removed) = diff_universe(&old, &new);
        added.sort();
        removed.sort();
        assert_eq!(added, vec!["D"], "D is newly open and must be picked up");
        assert_eq!(removed, vec!["A"], "A expired and must be dropped");
    }

    #[test]
    fn diff_universe_handles_total_rollover() {
        // The exact outage shape: every boot-time ticker expires and a wholly
        // new cohort opens. Everything old goes, everything new arrives.
        let old = universe(&["KXBTCD-26JUL2417-T62499.99", "KXBTCD-26JUL2417-T62999.99"]);
        let new = universe(&["KXBTCD-26AUG0317-T72499.99"]);
        let (added, removed) = diff_universe(&old, &new);
        assert_eq!(added, vec!["KXBTCD-26AUG0317-T72499.99"]);
        assert_eq!(removed.len(), 2, "both expired tickers must be dropped");
    }

    #[test]
    fn diff_universe_is_empty_when_nothing_changed() {
        // The common case: discovery finds the same set, so no churn and no
        // needless re-subscribe / snapshot refetch.
        let same = universe(&["A", "B"]);
        let (added, removed) = diff_universe(&same, &same);
        assert!(added.is_empty() && removed.is_empty());
    }

    // --- refresh_markets end-to-end --------------------------------------
    // `diff_universe` alone doesn't prove the bot recovers from a rollover —
    // that needs the whole refresh cycle. These drive it against a scripted
    // API whose open-market list changes between calls, which is precisely
    // what the live exchange does every hour.

    use kalshi_mm::api::ApiError;
    use std::sync::Mutex;

    /// Minimal MarketApi double. `get_markets` replays a script: call N
    /// returns page N (the final entry repeats), letting a test simulate an
    /// hourly rollover between two discovery passes.
    struct MockApi {
        pages: Mutex<Vec<Vec<String>>>,
        call: std::sync::atomic::AtomicUsize,
    }

    impl MockApi {
        fn new(pages: Vec<Vec<&str>>) -> Self {
            Self {
                pages: Mutex::new(
                    pages.iter().map(|p| p.iter().map(|t| t.to_string()).collect()).collect(),
                ),
                call: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl kalshi_mm::api::OrderApi for MockApi {
        async fn place_limit_order(
            &self, _t: &str, _s: &str, _p: f64, _n: i64, _po: bool, _tif: &str,
        ) -> Result<Value, ApiError> {
            Ok(json!({}))
        }
        async fn cancel_order(&self, _id: &str) -> Result<Value, ApiError> {
            Ok(json!({}))
        }
    }

    impl MarketApi for MockApi {
        async fn get_markets(
            &self, _series: Option<&str>, _status: Option<&str>, _limit: u32,
            _cursor: Option<&str>,
        ) -> Result<Value, ApiError> {
            let pages = self.pages.lock().unwrap();
            let i = self.call.fetch_add(1, Ordering::Relaxed).min(pages.len() - 1);
            let markets: Vec<Value> =
                pages[i].iter().map(|t| json!({"ticker": t})).collect();
            // No cursor => single page, discovery stops after one call.
            Ok(json!({"markets": markets}))
        }
        async fn get_market(&self, _ticker: &str) -> Result<Value, ApiError> {
            Ok(json!({"market": {
                "price_ranges": [{"step": 0.01}],
                "close_time": "2026-08-03T21:00:00Z",
            }}))
        }
        async fn get_orderbook(&self, _t: &str, _d: u32) -> Result<Value, ApiError> {
            Ok(json!({"orderbook": {"yes": [], "no": []}}))
        }
        async fn get_positions(&self) -> Result<Value, ApiError> {
            Ok(json!({"market_positions": []}))
        }
        async fn get_fills(
            &self, _min_ts: i64, _l: u32, _c: Option<&str>, _t: Option<&str>,
        ) -> Result<Value, ApiError> {
            Ok(json!({"fills": []}))
        }
        async fn get_settlements(&self, _l: u32) -> Result<Value, ApiError> {
            Ok(json!({"settlements": []}))
        }
        async fn get_orders(&self, _s: Option<&str>, _l: u32) -> Result<Value, ApiError> {
            Ok(json!({"orders": []}))
        }
        async fn cancel_all_orders(&self) -> Result<u64, ApiError> {
            Ok(0)
        }
    }

    /// Trader wired to a mock API, with one real policy so `discover_universe`
    /// (which iterates loaded policies) sees the KXBTCD category. Returns None
    /// when the ONNX model isn't present so the suite still runs without it.
    fn mock_trader(api: MockApi) -> Option<Trader<MockApi>> {
        let model = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/models/realistic_20dim_KXBTCD_final.onnx"
        );
        let Ok(policy) = Policy::load(model) else {
            eprintln!("SKIP: {model} missing");
            return None;
        };
        let cfg = Config::load("prod").expect("config/prod.toml loads");
        let mut policies = HashMap::new();
        policies.insert("KXBTCD".to_string(), policy);
        Some(Trader {
            api,
            cats: cfg.categories.iter().map(|c| (c.name.clone(), c.clone())).collect(),
            state: TraderState::new(epoch_now()),
            policies,
            active_tickers: HashMap::new(),
            active_set: HashSet::new(),
            running: Arc::new(AtomicBool::new(true)),
            spot: SpotState::new(cfg.spot.fv_ema_tau_s),
            ladders: Ladders::default(),
            spot_bound: HashSet::new(),
            spot_gate_on: false,
            spot_unwind_next: HashMap::new(),
            spot_unwind_fired_until: HashMap::new(),
            settlement_checks: HashMap::new(),
            universe_tx: None,
            cfg,
        })
    }

    #[tokio::test]
    async fn refresh_markets_recovers_from_hourly_rollover() {
        // THE regression test for the 2026-07-24 outage. Discovery first sees
        // the July 24 cohort; an hour later the exchange only lists the Aug 3
        // cohort. Before the fix the bot stayed pinned to the dead July
        // tickers forever and quoted nothing.
        let api = MockApi::new(vec![
            vec!["KXBTCD-26JUL2417-T62499.99", "KXBTCD-26JUL2417-T62999.99"],
            vec!["KXBTCD-26AUG0317-T72499.99", "KXBTCD-26AUG0317-T72749.99"],
        ]);
        let Some(mut trader) = mock_trader(api) else { return };

        trader.discover_markets().await;
        assert_eq!(trader.active_tickers.len(), 2, "boot cohort discovered");
        assert!(trader.active_tickers.contains_key("KXBTCD-26JUL2417-T62499.99"));

        trader.refresh_markets().await;

        assert_eq!(trader.active_tickers.len(), 2, "universe rolled, not grew");
        assert!(
            trader.active_tickers.contains_key("KXBTCD-26AUG0317-T72499.99"),
            "newly-opened market must be picked up — this is what was broken"
        );
        assert!(
            !trader.active_tickers.contains_key("KXBTCD-26JUL2417-T62499.99"),
            "expired flat ticker must be pruned"
        );
        assert_eq!(trader.active_set, trader.active_tickers.keys().cloned().collect());
    }

    #[tokio::test]
    async fn refresh_markets_keeps_expired_ticker_that_still_holds_inventory() {
        // Pruning must not orphan a position: the settlement / drift / entry-
        // price paths look the category up in active_tickers, so a ticker we
        // still hold stays until it settles flat.
        let api = MockApi::new(vec![
            vec!["KXBTCD-26JUL2417-T62499.99"],
            vec!["KXBTCD-26AUG0317-T72499.99"],
        ]);
        let Some(mut trader) = mock_trader(api) else { return };

        trader.discover_markets().await;
        trader.state.ticker("KXBTCD-26JUL2417-T62499.99", "KXBTCD").position = 5;

        trader.refresh_markets().await;

        assert!(
            trader.active_tickers.contains_key("KXBTCD-26JUL2417-T62499.99"),
            "expired ticker with an open position must be retained"
        );
        assert!(trader.active_tickers.contains_key("KXBTCD-26AUG0317-T72499.99"));
    }

    #[tokio::test]
    async fn refresh_markets_keeps_universe_when_discovery_returns_nothing() {
        // An API failure yields an empty list. Treating that as "everything
        // expired" would blow away the whole universe and stop all quoting —
        // exactly the outage we're fixing. Leave state alone and retry later.
        let api = MockApi::new(vec![
            vec!["KXBTCD-26AUG0317-T72499.99"],
            vec![], // failed / empty discovery
        ]);
        let Some(mut trader) = mock_trader(api) else { return };

        trader.discover_markets().await;
        trader.refresh_markets().await;

        assert_eq!(
            trader.active_tickers.len(), 1,
            "an empty discovery must not clear the universe"
        );
    }
}
