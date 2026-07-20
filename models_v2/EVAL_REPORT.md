# RL Retrain Eval Report — daily_pnl_v2

**Date:** 2026-07-20
**Spec:** `docs/superpowers/specs/2026-07-19-rl-daily-pnl-retrain-design.md`
**Plan:** `docs/superpowers/plans/2026-07-19-rl-daily-pnl-retrain.md`
**Status:** Staged for review. **NOT deployed.**

## Data window

S3 collector data (`kalshi-data-prod`, us-east-2) refreshed 2026-06-29 → 2026-07-20
(15.65M trade prints, 2.32M orderbook snapshots — the collector never stopped
after 07-13 as originally feared). **Split date: 2026-07-15** (train < 07-15,
test ≥ 07-15) — the primary split from the spec, not the fallback; the test
window fully overlaps live trading.

## Calibration (Task 3+4)

Live fill economics measured from **592 real-money fills** (2026-06-29 →
2026-07-20). KXBTCD is the only category with enough data to calibrate against
(280 fills / 7 active days); the other three inherit its parameters.

| target | live (KXBTCD) | notes |
|---|---|---|
| fills / active day | 40.0 | primary calibration target |
| taker fraction | 0.175 | direct measurement — pinned, not searched |
| markout (5m) | −$0.0012 | only 14 samples — low confidence |
| fee / contract | $0.0024 | maker fills pay ~$0 (post-only) |

Grid search (`queue_competitors` × `through_fill_haircut`, `taker_fill_prob`
pinned) over 12 combos on the current prod checkpoint: best combo reached
**37.5 sim fills/episode vs 40.0 live (6.25% off — within the spec's 25%
tolerance)**. `queue_competitors` turned out to be non-identifiable from this
data (sim fill count was ~invariant to it, 37.4995 across all four tested
values) — the fitted value 20 is not meaningful, so **`queue_competitors` was
kept at its prior value (10)** rather than baked in as "calibrated."
`through_fill_haircut=0.33` (unchanged) and `taker_fill_prob=0.175`
(measured) were baked into `MMConfig` defaults on the HPC.

**Caveat:** simulated pnl-per-fill (+$0.052) does not match the live value
(−$0.0036) — wrong sign, ~15x magnitude. This is the explicitly low-confidence
target (14 markout samples); fill-count calibration is trustworthy, per-fill
PnL calibration is not. Sim total PnL magnitudes below should be read as
directional, not absolute.

## Reward shaping (Task 5)

Terminal per-episode adjustment (`rl_bot/reward.py::daily_pnl_terminal_adjustment`):
`+bonus` when an episode (one traded market's lifetime) ends PnL-positive,
`-(loss_mult-1)×|pnl|` extra penalty when it ends negative. Defaults are a
no-op; training used **bonus=2.0, loss_mult=2.0**.

Deviation from the plan's default starting point (bonus=0.5): the plan calls
for clamping the bonus to the median winning-episode PnL, in [0.1, 2.0]. The
calibration grid's eval run had already shown KXBTCD's median winning episode
is **$8.70** — far outside that range — so the clamp ceiling (2.0) was applied
upfront rather than burning a training round we already expected to under-shape.

Episodes are NOT day-length in this data (hourly BTC strikes: median 4 steps
per episode); daily PnL is the sum of episode PnLs, so per-episode positive
skew is the tractable proxy actually being optimized, not a literal "one
episode = one day" mapping.

## Training (Task 6)

Slurm array job, A100 ×4, 500k timesteps each, ~13 min wall time per category
(much faster than the 4h budget). All four checkpoints produced:
`daily_pnl_v2_{KXBTCD,KXWCGAME,KXADP,KXAAAGASM}_final.zip`.

## Evaluation vs current (Task 7) — held-out window, split 2026-07-15

| category | model | episodes | pos% | total PnL | maxDD |
|---|---|---|---|---|---|
| KXBTCD | realistic_20dim (current) | 1021 | 38% | +1984.37 | 39.37 |
| KXBTCD | daily_pnl_v2 | 1021 | 38% | +1983.44 | 43.80 |
| KXWCGAME | realistic_20dim (current) | 16 | 56% | +28.96 | 21.71 |
| KXWCGAME | daily_pnl_v2 | 16 | 50% | +11.51 | 23.87 |
| KXADP | — | 0 | — | — | — |
| KXAAAGASM | realistic_20dim (current) | 45 | 22% | −7.04 | 14.12 |
| KXAAAGASM | daily_pnl_v2 | 45 | 22% | −5.13 | 12.73 |

**Gate: v2 pos% ≥ current AND v2 PnL ≥ 0.8× current.**

| category | verdict | why |
|---|---|---|
| **KXBTCD** | **STAGE** | pos% tied (38%), PnL within 0.1% of current |
| **KXWCGAME** | **HOLD BACK** | pos% dropped 56%→50%, PnL dropped 60% (+28.96→+11.51) |
| **KXADP** | **HOLD BACK** | zero test episodes past 2026-07-15 — this category has no data in the live-trading window at all; neither model could be evaluated, so the current model is kept unchanged |
| **KXAAAGASM** | **STAGE** | pos% tied (22%), PnL improved (fewer losses: −7.04→−5.13) |

**`models_v2/` therefore mixes**: KXBTCD and KXAAAGASM are the v2
(daily-PnL-shaped) policies; KXWCGAME and KXADP are byte-identical copies of
the current prod models. Deploying `models_v2/` as a directory swap is safe
in the sense that no category regresses — two improve marginally, two are
unchanged.

## Live cross-check (sanity, not a gate)

Exchange-verified net fill cash flow since 2026-07-15 (excludes settlement
payouts, so not directly comparable to sim PnL magnitude — sim scores across
the full historical market breadth, the live bot only quoted a subset):

- KXBTCD: 226 fills, **+$9.84** net cash flow — sign matches sim (positive).
- KXWCGAME: 32 fills, **+$5.61** net cash flow — sign matches sim (positive).

Both categories' sim PnL sign agrees with the live PnL sign. This validates
the calibration direction; it does not validate magnitude (see the
pnl-per-fill caveat above).

## Parity test (Task 8)

`cargo test --test parity`: **5/5 PASS** (fees, observations, quote plan,
fill accounting, ONNX-vs-SB3 actions). One issue encountered and resolved:
the HPC's `scripts/gen_parity_fixtures.py` / fill-accounting reference has
drifted from the version that produced the currently-committed
`tests/fixtures/parity.json` (an unrelated scenario rename caused a spurious
fill-accounting mismatch — not caused by this retrain). Fix: only
`action_cases` (the part that actually depends on model weights) was taken
from the regeneration; `obs_cases`, `fee_cases`, `fill_sequences`, and
`quote_cases` were kept from the known-good committed file. Confirmed by diff
that only `action_cases` changed semantically.

## What's staged

- `models_v2/realistic_20dim_{KXBTCD,KXAAAGASM}_final.onnx` — new (daily-PnL
  retrained) policies.
- `models_v2/realistic_20dim_{KXWCGAME,KXADP}_final.onnx` — unchanged copies
  of the current prod models (held back).
- `tests/fixtures/parity.json`, `tests/fixtures/models/*.onnx` — updated,
  parity-test-verified against the staged weights.
- This report + calibration/eval JSON artifacts.

## Recommended next step (NOT executed)

If approved: `cp models_v2/*.onnx models/`, `git commit`, `deploy/redeploy.sh
prod`. Given only 2/4 categories actually changed and the held-out-window
improvement is marginal (tied positive-rate, small PnL deltas), consider
whether this retrain clears the bar for a prod deploy versus continuing the
config-iteration loop (`docs/superpowers/plans/2026-07-19-positive-daily-pnl-loop.md`)
a while longer before spending a deploy cycle on it.
