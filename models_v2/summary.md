| category | model | episodes | pos% | total PnL | maxDD |
|---|---|---|---|---|---|
| KXAAAGASM | daily_pnl_v2 | 45 | 22% | -5.13 | 12.73 |
| KXBTCD | daily_pnl_v2 | 1021 | 38% | +1983.44 | 43.80 |
| KXWCGAME | daily_pnl_v2 | 16 | 50% | +11.51 | 23.87 |
| KXAAAGASM | realistic_20dim | 45 | 22% | -7.04 | 14.12 |
| KXBTCD | realistic_20dim | 1021 | 38% | +1984.37 | 39.37 |
| KXWCGAME | realistic_20dim | 16 | 56% | +28.96 | 21.71 |

**Spec gate (per category): v2 pos% >= current, v2 PnL >= 0.8x current**

- KXBTCD: pos 38% -> 38% (OK), pnl +1984.37 -> +1983.44 (OK) => STAGE
- KXWCGAME: pos 56% -> 50% (FAIL), pnl +28.96 -> +11.51 (FAIL) => HOLD BACK (keep current)
- KXADP: MISSING EVAL (no held-out test data past split date) — NOT STAGED, current model kept
- KXAAAGASM: pos 22% -> 22% (OK), pnl -7.04 -> -5.13 (OK) => STAGE
