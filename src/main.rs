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
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer as _; // per-layer with_filter

use kalshi_mm::api::{KalshiClient, MarketApi, PROD_BASE_URL};
use kalshi_mm::book::TakerSide;
use kalshi_mm::config::{CategoryConfig, Config};
use kalshi_mm::engine::{check_risk_limits, exit_decision, plan_quotes};
use kalshi_mm::features::build_observation;
use kalshi_mm::model::Policy;
use kalshi_mm::paper::PaperClient;
use kalshi_mm::state::{normalize_fill, TraderState};
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
}

async fn run_trader<M: MarketApi>(
    api: M,
    cfg: Config,
    args: Args,
    api_key: Option<String>,
    api_secret: Option<String>,
) -> Result<()> {
    let cats: HashMap<String, CategoryConfig> =
        cfg.categories.iter().map(|c| (c.name.clone(), c.clone())).collect();
    let mut trader = Trader {
        api,
        cats,
        cfg,
        state: TraderState::new(epoch_now()),
        policies: HashMap::new(),
        active_tickers: HashMap::new(),
        active_set: HashSet::new(),
        running: Arc::new(AtomicBool::new(true)),
    };

    trader.optimize_startup_orders().await;
    trader.load_policies(&args.models_dir)?;
    trader.discover_markets().await;
    trader.fetch_market_details().await;
    trader.sync_positions().await;

    // WebSocket task feeds the event loop; without credentials fall back to
    // REST polling (10s), same as Python.
    let (tx, rx) = mpsc::channel::<WsEvent>(8192);
    let use_ws = match (&api_key, &api_secret) {
        (Some(key), Some(secret)) => match WsClient::new(key, secret, PROD_WS_URL) {
            Ok(ws) => {
                let tickers: Vec<String> = trader.active_tickers.keys().cloned().collect();
                let running = trader.running.clone();
                tokio::spawn(async move { ws.run(tickers, tx, running).await });
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

    // Seed the books with REST snapshots (WS deltas need a baseline)
    trader.fetch_initial_snapshots().await;

    info!("{}", "=".repeat(80));
    info!("Initialization complete - trading on {} markets", trader.active_tickers.len());
    info!("{}", "=".repeat(80));

    trader.event_loop(rx, use_ws).await;
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
            let ticker = order.get("market_ticker").and_then(Value::as_str).unwrap_or("");
            let side = order.get("side").and_then(Value::as_str).unwrap_or("");
            let position = positions.get(ticker).copied().unwrap_or(0);
            // Long: keep sell ("no") orders; short: keep buy ("yes") orders
            let keep = (position > 0 && side == "no") || (position < 0 && side == "yes");
            if keep {
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
    async fn discover_markets(&mut self) {
        info!("Finding active markets...");
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
                    self.active_tickers.insert(ticker.to_string(), category.clone());
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
        self.active_set = self.active_tickers.keys().cloned().collect();
    }

    /// Tick sizes (subpenny detection) + close times, stored per ticker.
    async fn fetch_market_details(&mut self) {
        info!("Fetching market details from Kalshi API...");
        let mut subpenny = 0;
        let entries: Vec<(String, String)> =
            self.active_tickers.iter().map(|(t, c)| (t.clone(), c.clone())).collect();
        for (ticker, category) in entries {
            let ts = self.state.ticker(&ticker, &category);
            match self.api.get_market(&ticker).await {
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
        info!("✓ Market details loaded: {subpenny}/{} support subpenny", self.active_tickers.len());
    }

    /// REST snapshots seed the books that WS deltas then update.
    async fn fetch_initial_snapshots(&mut self) {
        info!("Fetching initial orderbook snapshots for {} markets...", self.active_tickers.len());
        let entries: Vec<(String, String)> =
            self.active_tickers.iter().map(|(t, c)| (t.clone(), c.clone())).collect();
        let (mut ok, mut failed) = (0u32, 0u32);
        for (ticker, category) in entries {
            match self.api.get_orderbook(&ticker, 10).await {
                Ok(resp) => {
                    let payload = resp.get("orderbook").unwrap_or(&resp);
                    self.state.ticker(&ticker, &category).book.load_snapshot(payload);
                    ok += 1;
                    if ok % 50 == 0 {
                        info!("Snapshot progress: {ok}/{}", self.active_tickers.len());
                    }
                    // REST rate-limit headroom (10 req/s is safe)
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => failed += 1,
            }
        }
        info!("✓ Fetched {ok}/{} initial orderbook snapshots ({failed} failed)",
              self.active_tickers.len());
    }

    // --- event loop -----------------------------------------------------------

    async fn event_loop(&mut self, mut rx: mpsc::Receiver<WsEvent>, use_ws: bool) {
        info!("Starting live trading...");
        let mut fills_tick = tokio::time::interval(Duration::from_secs(5));
        let mut sync_tick = tokio::time::interval(Duration::from_secs(30));
        let mut exit_tick = tokio::time::interval(Duration::from_secs(10));
        let mut daily_tick = tokio::time::interval(Duration::from_secs(60));
        let mut hourly_tick = tokio::time::interval(Duration::from_secs(3600));
        let mut poll_tick = tokio::time::interval(Duration::from_secs(10));
        // The first interval tick fires immediately; consume them so the
        // hourly report doesn't print at startup etc.
        fills_tick.tick().await;
        sync_tick.tick().await;
        exit_tick.tick().await;
        daily_tick.tick().await;
        hourly_tick.tick().await;
        poll_tick.tick().await;

        // Fills lookback starts 60s before boot; the API takes epoch SECONDS
        let mut fills_last_ts = epoch_now() as i64 - 60;

        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                Some(event) = rx.recv(), if use_ws => match event {
                    WsEvent::Book { ticker, msg } => self.on_book_update(&ticker, &msg).await,
                    WsEvent::Trade { ticker, msg } => self.on_trade(&ticker, &msg),
                    WsEvent::Spot { .. } => {} // wired in Task 7
                },
                _ = fills_tick.tick() => {
                    // Page through the cursor so a burst of >200 fills in the
                    // 60s overlap window can't silently drop any (dedupe by id
                    // handles the overlap re-reads). Cap pages defensively.
                    let mut cursor: Option<String> = None;
                    let mut ok = true;
                    for _ in 0..25 {
                        match self.api.get_fills(fills_last_ts - 60, 200, cursor.as_deref()).await {
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
                _ = exit_tick.tick() => self.check_exits().await,
                _ = daily_tick.tick() => self.maybe_daily_reset(),
                _ = hourly_tick.tick() => self.hourly_report(),
                _ = poll_tick.tick(), if !use_ws => self.rest_poll().await,
            }
        }
        info!("Trading stopped.");
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
        let plan = plan_quotes(action, ts, &self.cfg.mm, max_inventory, open_orders, backoff, mono_now());
        if let Some(plan) = plan {
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

        // Per-category kill switch: after N consecutive losing round-trips in
        // a category, halt it (stop quoting its markets). apply_fill maintains
        // consecutive_losses (reset to 0 on any winning close); we only read
        // it here. 0 = disabled. Halt is sticky for the session — a tripped
        // breaker stays tripped until restart.
        let limit = self.cfg.live.halt_on_consecutive_losses;
        if limit > 0 && !self.state.halted_categories.contains(&category) {
            let losses = self.state.consecutive_losses.get(&category).copied().unwrap_or(0);
            if losses >= limit {
                warn!("HALTING CATEGORY {category}: {losses} consecutive losses (limit {limit})");
                self.state.halted_categories.insert(category);
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
        let all: HashSet<String> = exchange.keys().cloned().chain(local_nonzero).collect();
        for ticker in all {
            let remote = exchange.get(&ticker).copied().unwrap_or(0);
            let category = self.active_tickers.get(&ticker).cloned().unwrap_or_default();
            let ts = self.state.ticker(&ticker, &category);
            if ts.position != remote {
                warn!("POSITION DRIFT: {ticker} local={} exchange={remote}, correcting", ts.position);
                ts.position = remote;
                drift += 1;
            }
        }
        info!("Synced {} positions from Kalshi{}", exchange.len(),
              if drift > 0 { format!(" ({drift} drifts corrected)") } else { String::new() });
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
