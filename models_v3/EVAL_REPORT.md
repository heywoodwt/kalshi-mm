# RL Retrain Eval Report — daily_pnl_v3 (KXBTCD)

**Date:** 2026-08-03
**Spec:** `docs/superpowers/specs/2026-08-03-rl-daily-pnl-v3-kxbtcd-design.md`
**Status:** Staged for review. **NOT deployed.**

## Recommendation: HOLD BACK

More data did **not** produce a better policy. On the held-out window v3 is a
statistical tie with the deployed v2 policy (paired t = +0.97, p = 0.33) and its
max drawdown is **19% worse**. There is no evidence-based reason to swap.

The run was still worth doing: it produced a materially better-calibrated
simulator and it invalidated the scariest caveat in the v2 report. Those results
are the actual deliverable — see below.

## What changed vs. daily_pnl_v2

Exactly one class of input: data volume. Reward shaping, timesteps, PPO epochs,
partition and simulator parameters were copied from `hpc/train_daily_pnl_v2.slurm`
and held identical.

| input | v2 | v3 |
|---|---|---|
| S3 window | 2026-06-29 → 07-20 | 2026-06-29 → **08-03** |
| trade prints | 15,652,257 | **25,829,031** (+65%) |
| orderbook snapshots | 2,317,160 | **3,733,079** (+61%) |
| KXBTCD prints | 689,695 | **1,118,167** (+62%) |
| KXBTCD tickers | 4,312 | **7,061** (+64%) |
| real-money fills (KXBTCD) | 592 (280) | **779** (452) |
| split date | 2026-07-15 | **2026-07-27** |

Config held constant: `-p gpu --gres=gpu:a100:1`, `--timesteps 500000`,
`--ppo-epochs 10`, `--daily-pnl-bonus 2.0`, `--daily-loss-mult 2.0`,
`--markets rl_all_markets_3mo.parquet`. Simulator left at v2's baked-in
`MMConfig` defaults (`taker_fill_prob=0.175`, `through_fill_haircut=0.33`,
`queue_competitors=10`) — see "Why the new calibration was not baked in".

Training: job 17937782, A100, **15:04** wall time, 501,760 timesteps.

## Result — held-out window 2026-07-27 → 08-03, 1492 paired episodes

Both policies scored on the identical episode set, so the comparison is paired.

| model | episodes | positive | total PnL | mean/ep | maxDD |
|---|---|---|---|---|---|
| daily_pnl_v2 (**deployed**) | 1492 | 602 (40.3%) | +3407.57 | +2.284 | **45.39** |
| daily_pnl_v3 (new) | 1492 | 606 (40.6%) | +3447.64 | +2.311 | **54.22** |

**Paired difference (v3 − v2):** mean +0.027/episode, sd 1.066, se 0.028,
**t = +0.97, two-sided p = 0.33.** Not significant.

The +$40 headline gain (+1.18%) is not a broad edge. Only **272 of 1492 episodes
differ at all**, and among those v3 is worse more often than better (**157 worse,
115 better**) — it wins on magnitude via a few large swings, not on frequency.
The extremes are ±$10-15 on single episodes (worst −10.62, best +15.21). That is
the signature of noise.

Behaviour is near-identical: mean steps/episode 6.36 for both, mean fills/episode
37.19 (v3) vs 37.34 (v2), and neither policy ends any episode holding inventory
in the sim.

**maxDD 45.39 → 54.22 is the one clearly-directional change, and it is the wrong
direction.** With PnL flat, a 19% worse drawdown is a reason not to deploy.

## Calibration — the real win

Live fill economics re-measured from the refreshed record
(`live_fill_economics_v3.json`):

| target | v2 (280 fills) | v3 (452 fills) |
|---|---|---|
| fills / active day | 40.0 | 37.67 |
| active days | 7 | 12 |
| markout 5m | −0.0012 | **+0.0078** |
| fee / contract | $0.0024 | $0.0023 |
| taker fraction | 0.175 | 0.186 |
| **markout samples** | **14** | **37** |

**Fill-count calibration error at the deployed config: 0.78%** (sim 37.371 vs
live 37.667), versus **6.25%** for v2. The simulator now reproduces live fill
frequency roughly eight times more accurately, purely from having 452 fills over
12 active days instead of 280 over 7. This is the durable result of the run.

### The v2 report's headline caveat was a small-sample artifact

v2 flagged that simulated per-fill PnL had the **wrong sign** versus live (sim
+0.052 vs live −0.0036) and disclaimed all sim PnL magnitudes on that basis. With
37 markout samples instead of 14, the live estimate flips to **+0.0078** — the
same sign as the sim. The contradiction resolved by the *live* number moving,
which is itself the clearest available evidence of how unstable a 14-sample
estimate was.

**This does not mean per-fill PnL is now trustworthy.** 37 samples is still below
the ~50 the spec set as the bar, and the magnitude remains ~8x off (sim 0.0594 vs
live 0.0055). **Sim PnL magnitudes in this report remain directional, not
absolute.** The *paired* comparison above is unaffected — both policies ran in
the same simulator, so a shared bias cancels.

### Why the new calibration was not baked in

The grid's lowest-error combo is `through_fill_haircut=0.50`, not the deployed
0.33. It was deliberately **not** adopted, because the ranking is an artifact of
the untrustworthy term:

| combo | fill-count error | per-fill PnL error | total err | share of err from PnL term |
|---|---|---|---|---|
| qc=10, **tfh=0.33** (deployed) | **0.78%** | 10.2× | 5.104 | 99.8% |
| qc=10, tfh=0.50 (grid "best") | 2.48% | 9.9× | 4.970 | 99.5% |

99.5% of the score comes from the 37-sample PnL target. On the one target that
*is* reliable — fill count — the deployed `tfh=0.33` is **3x more accurate**.
Adopting the grid's pick would have made the simulator measurably worse at the
only thing it calibrates well. `queue_competitors` is again **non-identifiable**:
sim fills move only 38.569 → 38.601 across qc ∈ {3,5,10,20}, a 0.08% spread, so
the argmin landing on 10 is noise (same finding as v2).

## Export and parity

`daily_pnl_v3_KXBTCD_final.zip` → `realistic_20dim_KXBTCD_final.onnx`
(md5 `6055445a60809aba46b0a4d14060ff6c`; deployed v2 is `c992da04…`).

Parity verified against the **actual Rust runtime**: all 5 `tests/parity.rs`
tests pass with the v3 ONNX and v3-generated SB3 expectations, including
`onnx_actions_match_sb3` across all 8 observation vectors (tolerance 1e-4).

The check was confirmed to be live via a negative control — pairing the v3
expectations with the *v2* ONNX makes `onnx_actions_match_sb3` fail on
`basic_flat` (`rust=[-1.0, -1.0]` vs `python=[-1.0, -0.935]`). A passing parity
test that had silently skipped would have been worthless, so this matters.

Committed fixtures were restored afterwards and re-verified clean
(`git diff tests/` empty, 5/5 passing). Only `action_cases` were regenerated —
`scripts/gen_parity_fixtures.py` on the HPC has drifted from the version that
produced the committed `parity.json`, so regenerating the whole file would add
spurious diffs.

## Out of scope — do not misread this report

The bot's live losses are **not** a quoting-policy problem and nothing here
addresses them. Measured over a clean 2-hour window on 2026-08-03: trading
round-trips **+$0.55** (5 wins, 0 losses); settlements **−$3.79** (0 wins, 8
losses). The money is lost carrying inventory into settlement because
`engine.rs::exit_decision` has never fired once — EXPIRY is gated behind
`entry_price` being set and `book.is_valid()`, and a market whose outcome is
decided goes one-sided exactly when the forced flatten matters most.

That is a Rust bug. **It is the highest-value fix available and no retrain
substitutes for it.**

## Contents

| file | what |
|---|---|
| `realistic_20dim_KXBTCD_final.onnx` | the v3 policy, named for drop-in use |
| `calibration_result_v3.json` | full 12-combo grid |
| `live_fill_economics_v3.json` | live targets from 779 fills, all categories |
| `eval_v3window_daily_pnl_v3_KXBTCD.csv` | per-episode held-out results, v3 |
| `eval_v3window_daily_pnl_v2_KXBTCD.csv` | same window, deployed v2 |

**This directory is NOT a drop-in replacement for `models/`.** Unlike
`models_v2/`, it holds only KXBTCD — copying the whole directory over `models/`
would leave the other three policies missing. If v3 were ever approved despite
the recommendation, the deploy is a single-file copy of
`realistic_20dim_KXBTCD_final.onnx`.

## Reproducing

HPC `mtk9va@login.hpc.virginia.edu`, `/scratch/mtk9va/kalshi_v5`:

```
sbatch hpc/calib_v3.slurm            # live fill economics + calibration grid
sbatch hpc/train_daily_pnl_v3.slurm  # train KXBTCD + eval v3 and v2
```

Inputs `output/rl_kalshi_trades_s3_aug03.parquet` and
`output/s3_orderbooks_aug03.parquet`. v2's inputs are untouched, so v2 remains
reproducible; the analysis scripts take paths from the environment and still
default to v2's files (`analysis/*.py.pre_v3` are the pre-patch originals).

The S3 sync must run laptop-side — the HPC gets `AccessDenied` on
`kalshi-data-prod`. Consolidation must follow `consolidate_s3_data.py`: trades
are **not** deduplicated (deduping on `(ticker, ts)` silently drops ~37% of
prints, since many trades share a ticker within one second) and orderbooks
**must** be renamed with a derived `imbalance` before `mm_env` can read them.
