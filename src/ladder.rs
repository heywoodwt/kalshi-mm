//! Strike ladders: parse threshold tickers (SERIES-EXPIRY-T<strike>), group
//! them per expiry, and read each contract's spot sensitivity ("delta",
//! probability per $) off adjacent strike mids — the ladder is its own
//! pricing model (see 2026-07-15-btc-spot-fv-defense spec).

use std::collections::HashMap;

/// "KXBTCD-26JUL1517-T63999.99" -> ("26JUL1517", 63999.99).
/// Non-threshold legs (brackets etc.) and malformed tickers return None —
/// they are treated as orphans (calm-only quoting) by the caller.
pub fn parse_strike(ticker: &str) -> Option<(String, f64)> {
    let mut parts = ticker.split('-');
    let _series = parts.next()?;
    let expiry = parts.next()?;
    let strike = parts.next()?.strip_prefix('T')?.parse::<f64>().ok().filter(|k| k.is_finite())?;
    if parts.next().is_some() || expiry.is_empty() {
        return None;
    }
    Some((expiry.to_string(), strike))
}

/// Worst-case scenario PnL for one ladder's open positions, model-free:
/// if spot settles ABOVE every strike each YES pays $1 (shorts pay full
/// freight), BELOW every strike each YES pays $0 (longs lose their marks).
/// This bounds gap risk that per-strike deltas miss — a 5c wing short has
/// near-zero delta but a 95c gap loss, and adjacent wing shorts all lose
/// TOGETHER in one move. `entries` = (signed position, YES-frame mark).
/// Returns (up_tail_loss, down_tail_loss), both >= 0 dollars.
pub fn tail_losses(entries: impl IntoIterator<Item = (i64, f64)>) -> (f64, f64) {
    let (mut up_pnl, mut down_pnl) = (0.0, 0.0);
    for (pos, mark) in entries {
        up_pnl += pos as f64 * (1.0 - mark);
        down_pnl -= pos as f64 * mark;
    }
    ((-up_pnl).max(0.0), (-down_pnl).max(0.0))
}

/// "KXBTCD-26JUL2417-T60499.99" -> "KXBTCD-26JUL2417" for threshold legs,
/// None otherwise. Unlike `parse_strike`'s expiry-only group key this keeps
/// the series, so same-expiry ladders from different series never pool.
pub fn ladder_prefix(ticker: &str) -> Option<&str> {
    parse_strike(ticker)?;
    ticker.rsplit_once('-').map(|(prefix, _)| prefix)
}

/// Per-expiry strike ladders for one category universe. Rebuilt at startup
/// from the discovered tickers (market set is fixed for a session).
#[derive(Debug, Default)]
pub struct Ladders {
    /// expiry group -> (strike, ticker) sorted by strike ascending.
    groups: HashMap<String, Vec<(f64, String)>>,
    by_ticker: HashMap<String, (String, f64)>,
}

impl Ladders {
    pub fn build<'a>(tickers: impl Iterator<Item = &'a str>) -> Self {
        let mut out = Ladders::default();
        for t in tickers {
            if let Some((group, strike)) = parse_strike(t) {
                out.groups.entry(group.clone()).or_default().push((strike, t.to_string()));
                out.by_ticker.insert(t.to_string(), (group, strike));
            }
        }
        for ladder in out.groups.values_mut() {
            ladder.sort_by(|a, b| a.0.total_cmp(&b.0));
        }
        out
    }

    /// Central-difference delta (prob per $) for `ticker`, in [0, delta_max].
    /// One-sided at ladder edges. None = orphan/junk (single strike, invalid
    /// neighbor book, crossed mids, zero width): the caller must NOT quote
    /// un-shifted into a moving market for these.
    pub fn delta_for(
        &self,
        ticker: &str,
        delta_max: f64,
        mid_of: impl Fn(&str) -> Option<f64>,
    ) -> Option<f64> {
        let (group, strike) = self.by_ticker.get(ticker)?;
        let ladder = self.groups.get(group)?;
        let i = ladder.iter().position(|(_, t)| t == ticker)?;
        let lower = i.checked_sub(1).map(|j| &ladder[j]);
        let upper = ladder.get(i + 1);
        // Resolve (k_lo, mid_lo, k_hi, mid_hi); the ticker itself is one
        // endpoint at ladder edges (one-sided difference).
        let (k_lo, m_lo, k_hi, m_hi) = match (lower, upper) {
            (Some((kl, tl)), Some((ku, tu))) => (*kl, mid_of(tl)?, *ku, mid_of(tu)?),
            (Some((kl, tl)), None) => (*kl, mid_of(tl)?, *strike, mid_of(ticker)?),
            (None, Some((ku, tu))) => (*strike, mid_of(ticker)?, *ku, mid_of(tu)?),
            (None, None) => return None,
        };
        let width = k_hi - k_lo;
        if width <= 0.0 {
            return None;
        }
        // P(>K) decreases in K, so density = (mid_lo - mid_hi) / width
        let density = (m_lo - m_hi) / width;
        if density < 0.0 {
            return None; // crossed ladder mids = junk data, not "zero delta"
        }
        Some(density.min(delta_max))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn mids(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(t, m)| (t.to_string(), *m)).collect()
    }

    #[test]
    fn parses_threshold_tickers_only() {
        assert_eq!(
            parse_strike("KXBTCD-26JUL1517-T63999.99"),
            Some(("26JUL1517".to_string(), 63999.99))
        );
        assert_eq!(parse_strike("KXBTCD-26JUL1517-B64000"), None); // bracket leg
        assert_eq!(parse_strike("KXWCGAME-26JUL15"), None); // no strike leg
        assert_eq!(parse_strike("garbage"), None);
        assert_eq!(parse_strike("KXBTCD-26JUL1517-Tnan"), None); // NaN strike = junk
        assert_eq!(parse_strike("KXBTCD-26JUL1517-Tinf"), None);
    }

    #[test]
    fn central_difference_delta() {
        // Strikes 250 apart; P(>K) mids 0.80 / 0.50 / 0.20
        let l = Ladders::build(
            ["KXBTCD-26JUL1517-T64000", "KXBTCD-26JUL1517-T64250", "KXBTCD-26JUL1517-T64500"]
                .into_iter(),
        );
        let m = mids(&[
            ("KXBTCD-26JUL1517-T64000", 0.80),
            ("KXBTCD-26JUL1517-T64250", 0.50),
            ("KXBTCD-26JUL1517-T64500", 0.20),
        ]);
        let mid_of = |t: &str| m.get(t).copied();
        // Middle strike: (0.80 - 0.20) / 500 = 0.0012
        let d = l.delta_for("KXBTCD-26JUL1517-T64250", 0.005, mid_of).unwrap();
        assert!((d - 0.0012).abs() < 1e-12);
        // Edge strike (one-sided): (0.80 - 0.50) / 250 = 0.0012
        let d = l.delta_for("KXBTCD-26JUL1517-T64000", 0.005, mid_of).unwrap();
        assert!((d - 0.0012).abs() < 1e-12);
        // delta_max clamp
        let d = l.delta_for("KXBTCD-26JUL1517-T64250", 0.001, mid_of).unwrap();
        assert!((d - 0.001).abs() < 1e-12);
    }

    #[test]
    fn tail_losses_measure_worst_case_scenarios() {
        // Wing-short book (the real JUL24 shape): shorts lose if spot settles
        // above every strike (up-tail), longs lose if below every strike.
        // Short 4 @ mark 0.05 and short 2 @ 0.10: up-tail = 4*0.95 + 2*0.90
        let (up, down) = tail_losses([(-4, 0.05), (-2, 0.10)]);
        assert!((up - 5.6).abs() < 1e-9);
        assert!((down - 0.0).abs() < 1e-9); // shorts GAIN on a down move
        // Long 2 @ 0.93: down-tail = 2*0.93, up-tail 0
        let (up, down) = tail_losses([(2, 0.93)]);
        assert!((up - 0.0).abs() < 1e-9);
        assert!((down - 1.86).abs() < 1e-9);
        // Longs offset shorts: short 1 @ 0.50 + long 1 @ 0.40 -> up move nets
        // -0.50 + 0.60 = +0.10 (no loss); down move nets +0.50 - 0.40 = +0.10
        let (up, down) = tail_losses([(-1, 0.50), (1, 0.40)]);
        assert_eq!((up, down), (0.0, 0.0));
        // Empty ladder: nothing at risk (typed empty array for inference)
        let none: [(i64, f64); 0] = [];
        assert_eq!(tail_losses(none), (0.0, 0.0));
    }

    #[test]
    fn ladder_prefix_groups_by_series_and_expiry() {
        // Grouping must include the series: two series could share an expiry
        // string, and their tail risks are unrelated.
        assert_eq!(ladder_prefix("KXBTCD-26JUL2417-T60499.99"), Some("KXBTCD-26JUL2417"));
        assert_eq!(ladder_prefix("KXWCGAME-26JUL19ESPARG-TIE"), None); // not a strike leg
        assert_eq!(ladder_prefix("garbage"), None);
    }

    #[test]
    fn orphans_and_junk_return_none() {
        let l = Ladders::build(
            ["KXBTCD-26JUL1517-T64000", "KXBTCD-26JUL1517-T64250", "LONE-26JUL-T5"].into_iter(),
        );
        // Single-strike ladder: no neighbor to difference against
        assert_eq!(l.delta_for("LONE-26JUL-T5", 0.005, |_| Some(0.5)), None);
        // Crossed mids (density negative = junk data): None, NOT Some(0)
        let m = mids(&[("KXBTCD-26JUL1517-T64000", 0.20), ("KXBTCD-26JUL1517-T64250", 0.80)]);
        assert_eq!(l.delta_for("KXBTCD-26JUL1517-T64000", 0.005, |t| m.get(t).copied()), None);
        // Neighbor book invalid (mid None): None
        assert_eq!(l.delta_for("KXBTCD-26JUL1517-T64000", 0.005, |_| None), None);
        // Unknown ticker
        assert_eq!(l.delta_for("NOPE-1-T2", 0.005, |_| Some(0.5)), None);
        // Different expiry groups never mix into one ladder
        let l2 = Ladders::build(["X-26JUL1517-T100", "X-26JUL1717-T200"].into_iter());
        assert_eq!(l2.delta_for("X-26JUL1517-T100", 0.005, |_| Some(0.5)), None);
    }
}
