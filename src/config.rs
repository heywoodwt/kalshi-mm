//! Typed deployment configuration (TOML).
//!
//! Mirrors two Python sources exactly:
//!   - `rl_bot/mm_config.py` MMConfig defaults -> [mm] / `MmParams`
//!   - `rl_bot/live_config_*.py` TRADING_CONFIG + category lists -> [live] + [[categories]]
//! Every default here must match the Python value or the two bots diverge.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// Strategy hyperparameters shared with training (`rl_bot/mm_config.py`).
/// Serde defaults ARE the MMConfig defaults — a config file only overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MmParams {
    /// Inventory normalization bound used in obs [10] (training used 20).
    pub max_inventory: i64,
    /// Contracts per quote (1-lot quoting; also obs [15] denominator).
    pub quote_size: i64,
    /// Kalshi maker fee rate (fee = ceil(rate*n*p*(1-p) to cent)).
    pub maker_fee_rate: f64,
    /// Kalshi taker rate — 4x maker; charged when a fill reports is_taker.
    pub taker_fee_rate: f64,
    /// Subpenny queue-priority adjustment (+0.001 bid / -0.001 ask).
    pub subpenny_enabled: bool,
    /// Skip quoting when rolling mid vol exceeds this (MM loses in trends).
    pub vol_filter_threshold: f64,
    /// Required per-contract edge beyond round-trip ceil'd fees.
    pub min_quote_edge: f64,
    /// Global cap on concurrent resting orders (collateral budget).
    pub max_open_orders: usize,
    /// Pause quoting this long after an insufficient_balance rejection.
    pub balance_backoff_s: f64,
    /// Don't quote when mid is outside [lo, hi] — extreme-priced contracts
    /// lock ~full collateral for pennies of spread.
    pub quote_band_lo: f64,
    pub quote_band_hi: f64,
    /// Quote size used when a side's planned price sits in the extreme band
    /// (bid >= 1-band or ask <= band). The extreme-carry trades (deep-ITM
    /// longs to $1, far-wing shorts to $0) are the measured edge and BTC
    /// maker fees are $0 — carry more size there, never mid-book.
    pub extreme_quote_size: i64,
    /// Band width defining "extreme" (dollars from 0/1).
    pub extreme_band: f64,
}

impl Default for MmParams {
    fn default() -> Self {
        // Values from rl_bot/mm_config.py MMConfig — do not "improve".
        Self {
            max_inventory: 20,
            quote_size: 1,
            maker_fee_rate: 0.0175,
            taker_fee_rate: 0.07,
            subpenny_enabled: true,
            vol_filter_threshold: 0.05,
            min_quote_edge: 0.01,
            max_open_orders: 60,
            balance_backoff_s: 60.0,
            quote_band_lo: 0.05,
            quote_band_hi: 0.95,
            // NEW knobs, not mm_config.py mirrors — the "do not improve"
            // rule applies to the Python-shared values above
            extreme_quote_size: 2,
            extreme_band: 0.10,
        }
    }
}

/// Spot-feed defense parameters (see 2026-07-15-btc-spot-fv-defense spec).
/// Only consulted for categories that set `spot_feed`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpotParams {
    /// Feed silent longer than this (seconds) => bound categories stop quoting.
    pub stale_max_s: f64,
    /// |60s spot return| above this => trend gate (cancel + pause quoting).
    pub gate_ret_60s: f64,
    /// Trend-gate hysteresis: once tripped, the gate releases only when the
    /// return falls below gate_ret_60s * this ratio. In (0, 1]; < 1 prevents
    /// a return hovering at the threshold from flapping the gate.
    pub gate_release_ratio: f64,
    /// EMA horizon (seconds) measuring "unabsorbed" spot movement.
    pub fv_ema_tau_s: f64,
    /// Clamp on the quote re-centering shift (dollars).
    pub fv_shift_max: f64,
    /// Adverse FV shift (dollars) that triggers a SPOT-ADVERSE exit.
    pub unwind_shift: f64,
    /// Per-$ probability-density clamp (guards a crossed/junk ladder).
    pub delta_max: f64,
}

impl Default for SpotParams {
    fn default() -> Self {
        Self {
            stale_max_s: 10.0,
            gate_ret_60s: 0.0015,
            gate_release_ratio: 0.7,
            fv_ema_tau_s: 60.0,
            fv_shift_max: 0.10,
            unwind_shift: 0.04,
            delta_max: 0.005,
        }
    }
}

/// Per-category deployment settings (mirrors CategoryConfig in the Python
/// config modules; defaults from live_config_lowvol.py).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)] // schema mirrors the Python CategoryConfig; not all fields drive logic yet
pub struct CategoryConfig {
    pub name: String,
    #[serde(default = "default_max_contracts")]
    pub max_contracts: i64,
    #[serde(default = "default_cat_max_inventory")]
    pub max_inventory: i64,
    #[serde(default = "default_capital_allocation")]
    pub capital_allocation: f64,
    #[serde(default)]
    pub vol_3mo: f64,
    /// Spot product id (e.g. "BTC-USD") binding this category's ladders to
    /// the spot-feed defense. None = zero behavior change.
    #[serde(default)]
    pub spot_feed: Option<String>,
}

fn default_max_contracts() -> i64 {
    1
}
fn default_cat_max_inventory() -> i64 {
    5
}
fn default_capital_allocation() -> f64 {
    19.0
}

/// Account-level risk limits (TRADING_CONFIG). No defaults on the loss
/// limits — a missing limit must fail at startup, not default to 0 and
/// halt on the first fill.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveConfig {
    pub capital: f64,
    pub max_daily_loss: f64,
    pub max_position_value: f64,
    pub stop_loss_threshold: f64,
    #[serde(default = "default_checkpoint_prefix")]
    pub checkpoint_prefix: String,
    /// Halt a CATEGORY after this many consecutive losing round-trips in it
    /// (per-category kill switch). 0 disables — the default, so configs that
    /// don't opt in behave exactly as before.
    #[serde(default)]
    pub halt_on_consecutive_losses: u64,
    /// Per-ladder worst-case loss cap (dollars): stop ADDING exposure on a
    /// side once the ladder's scenario loss in that direction reaches this.
    /// 0 disables (documented opt-out, unlike the no-default loss limits —
    /// existing configs predate this cap and must keep loading).
    #[serde(default = "default_max_ladder_tail_loss")]
    pub max_ladder_tail_loss: f64,
}

fn default_max_ladder_tail_loss() -> f64 {
    5.0
}

fn default_checkpoint_prefix() -> String {
    // Not fail-fast on purpose: checkpoint choice isn't safety-critical the
    // way loss limits are — worst case a category loads no model and is skipped.
    "june".to_string()
}

/// One deployment config file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub live: LiveConfig,
    #[serde(default)]
    pub mm: MmParams,
    #[serde(default)]
    pub spot: SpotParams,
    pub categories: Vec<CategoryConfig>,
}

impl Config {
    /// Load from a name ("lowvol" -> config/lowvol.toml, relative to the
    /// CWD this binary is launched from) or an explicit .toml path.
    pub fn load(name_or_path: &str) -> Result<Config> {
        let path = resolve_config_path(name_or_path)?;
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()
            .with_context(|| format!("validating config {}", path.display()))?;
        Ok(cfg)
    }

    /// Fail-fast checks beyond serde's shape validation — same philosophy as
    /// the no-default loss limits: a bad safety param must die at startup,
    /// not silently disable a gate at 3am.
    fn validate(&self) -> Result<()> {
        if self.categories.is_empty() {
            bail!("no [[categories]]");
        }
        // gate_ret_60s <= 0 gates on every tick (permanent dark);
        // stale_max_s <= 0 marks the feed stale the instant it ticks;
        // gate_release_ratio outside (0, 1] breaks the hysteresis band
        // (>1 releases above the trip level, so the gate can never latch).
        if self.categories.iter().any(|c| c.spot_feed.is_some())
            && (self.spot.gate_ret_60s <= 0.0
                || self.spot.stale_max_s <= 0.0
                || !(0.0 < self.spot.gate_release_ratio && self.spot.gate_release_ratio <= 1.0))
        {
            bail!(
                "spot gate params invalid (gate_ret_60s={}, stale_max_s={}, gate_release_ratio={})",
                self.spot.gate_ret_60s,
                self.spot.stale_max_s,
                self.spot.gate_release_ratio
            );
        }
        if self.live.max_ladder_tail_loss < 0.0 {
            bail!(
                "max_ladder_tail_loss must be >= 0 (0 disables), got {}",
                self.live.max_ladder_tail_loss
            );
        }
        Ok(())
    }
}

fn resolve_config_path(name_or_path: &str) -> Result<PathBuf> {
    let direct = Path::new(name_or_path);
    if direct.extension().is_some() {
        if direct.exists() {
            return Ok(direct.to_path_buf());
        }
        bail!("config file not found: {}", name_or_path);
    }
    // Bare name: look in config/, relative to the CWD this binary runs from.
    let candidate = Path::new("config").join(format!("{name_or_path}.toml"));
    if candidate.exists() {
        return Ok(candidate);
    }
    bail!("no config named '{name_or_path}' in config/");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse from a string so tests don't depend on CWD (validated like load).
    fn parse(text: &str) -> Result<Config> {
        let cfg = toml::from_str::<Config>(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn zero_spot_gate_param_fails_when_bound() {
        let broken = r#"
            [live]
            capital = 95.0
            max_daily_loss = 5.0
            max_position_value = 40.0
            stop_loss_threshold = -10.0

            [spot]
            gate_ret_60s = 0.0

            [[categories]]
            name = "KXBTCD"
            spot_feed = "BTC-USD"
        "#;
        assert!(parse(broken).is_err());
        // Same zero param is FINE when no category binds a spot feed
        let unbound = broken.replace("spot_feed = \"BTC-USD\"", "");
        assert!(parse(&unbound).is_ok());
        // Hysteresis ratio outside (0, 1] is rejected when bound
        let bad_ratio = broken.replace("gate_ret_60s = 0.0", "gate_release_ratio = 1.5");
        assert!(parse(&bad_ratio).is_err());
        let zero_ratio = broken.replace("gate_ret_60s = 0.0", "gate_release_ratio = 0.0");
        assert!(parse(&zero_ratio).is_err());
    }

    const LOWVOL: &str = include_str!("../config/lowvol.toml");

    #[test]
    fn lowvol_round_trips_with_mm_defaults() {
        let cfg = parse(LOWVOL).unwrap();
        assert_eq!(cfg.live.capital, 95.00);
        assert_eq!(cfg.live.max_daily_loss, 5.00);
        assert_eq!(cfg.live.max_position_value, 40.00);
        assert_eq!(cfg.live.stop_loss_threshold, -10.00);
        assert_eq!(cfg.live.checkpoint_prefix, "realistic_20dim");
        assert_eq!(cfg.live.halt_on_consecutive_losses, 3);
        // MmParams defaults = rl_bot/mm_config.py values
        assert_eq!(cfg.mm.max_inventory, 20);
        assert_eq!(cfg.mm.maker_fee_rate, 0.0175);
        assert_eq!(cfg.mm.taker_fee_rate, 0.07);
        assert_eq!(cfg.mm.vol_filter_threshold, 0.05);
        assert_eq!(cfg.mm.max_open_orders, 60);
        assert_eq!(cfg.mm.quote_band_lo, 0.05);
        assert_eq!(cfg.mm.quote_band_hi, 0.95);
        // Categories with per-category defaults
        assert_eq!(cfg.categories.len(), 4);
        let adp = cfg.categories.iter().find(|c| c.name == "KXADP").unwrap();
        assert_eq!(adp.max_inventory, 5);
        assert_eq!(adp.vol_3mo, 0.021);
    }

    #[test]
    fn ladder_tail_cap_defaults_and_validation() {
        let base = r#"
            [live]
            capital = 95.0
            max_daily_loss = 5.0
            max_position_value = 40.0
            stop_loss_threshold = -10.0

            [[categories]]
            name = "KXBTCD"
        "#;
        // Absent -> $5 default (0 would silently disable a risk limit)
        assert_eq!(parse(base).unwrap().live.max_ladder_tail_loss, 5.0);
        // Explicit 0 = documented opt-out; negative is a config error
        let zeroed =
            base.replace("capital = 95.0", "capital = 95.0\nmax_ladder_tail_loss = 0.0");
        assert_eq!(parse(&zeroed).unwrap().live.max_ladder_tail_loss, 0.0);
        let neg = base.replace("capital = 95.0", "capital = 95.0\nmax_ladder_tail_loss = -1.0");
        assert!(parse(&neg).is_err());
    }

    #[test]
    fn missing_loss_limit_fails() {
        // stop_loss_threshold absent -> hard error, never a silent default
        let broken = r#"
            [live]
            capital = 95.0
            max_daily_loss = 5.0
            max_position_value = 40.0

            [[categories]]
            name = "KXADP"
        "#;
        assert!(parse(broken).is_err());
    }

    #[test]
    fn unknown_key_fails() {
        // Typos in risk limits must not be silently ignored
        let typo = r#"
            [live]
            capital = 95.0
            max_daily_loss = 5.0
            max_position_value = 40.0
            stop_loss_threshold = -10.0
            max_daily_los = 1.0

            [[categories]]
            name = "KXADP"
        "#;
        assert!(parse(typo).is_err());
    }

    #[test]
    fn spot_params_defaults_and_category_binding() {
        let text = r#"
            [live]
            capital = 95.0
            max_daily_loss = 5.0
            max_position_value = 40.0
            stop_loss_threshold = -10.0

            [[categories]]
            name = "KXBTCD"
            spot_feed = "BTC-USD"

            [[categories]]
            name = "KXWCGAME"
        "#;
        let cfg = parse(text).unwrap();
        // Defaults from the spec
        assert_eq!(cfg.spot.stale_max_s, 10.0);
        assert_eq!(cfg.spot.gate_ret_60s, 0.0015);
        assert_eq!(cfg.spot.gate_release_ratio, 0.7);
        assert_eq!(cfg.spot.fv_ema_tau_s, 60.0);
        assert_eq!(cfg.spot.fv_shift_max, 0.10);
        assert_eq!(cfg.spot.unwind_shift, 0.04);
        assert_eq!(cfg.spot.delta_max, 0.005);
        // Binding is per-category and optional
        assert_eq!(cfg.categories[0].spot_feed.as_deref(), Some("BTC-USD"));
        assert_eq!(cfg.categories[1].spot_feed, None);
    }
}
