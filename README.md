# kalshi-mm (Rust) — how the live trading bot works

This is a from-scratch Rust rewrite of a live market-making bot that originally
ran as a Python program (`live_trader_v2.py`, kept in a separate research
monorepo alongside the training code). It watches a handful of Kalshi
prediction markets, decides where to quote, and places small buy/sell orders
trying to earn the bid-ask spread. This document explains how the pieces fit
together and why they're built the way they are — not what each line does (the
code comments cover that), but the shape of the system as a whole.

References to `rl_bot/*.py` throughout this README and the source comments
point at that Python original (the parity reference), which is not part of
this repository.

## Why a rewrite, and why this design

The Python bot works, but has a structural problem: the event loop is
`asyncio`, and every so often a blocking call (a synchronous HTTP request
buried inside an "async" function) froze the *entire* bot for the length of
one network round trip — every other market's quotes went stale while one
ticker waited on the network. Rust's async model doesn't let you accidentally
write a blocking call inside an async function without it being obvious, so
that whole class of bug is structurally harder to reintroduce.

The rewrite is a **direct, line-for-line port**, not a redesign. Every
threshold, formula constant, and the *order* in which safety checks run is
copied from the Python bot on purpose — see "Parity testing" below for how
that's enforced. The goal was "same behavior, better runtime," not "improve
the strategy while we're in here."

## The mental model: a single cashier, not a call center

Picture the bot as **one cashier at a counter**, not a call center with many
agents. The Tokio runtime underneath uses a thread pool, but only one task
ever touches trading state: `main.rs`'s event loop in `Trader::event_loop`.
Market data arrives from a WebSocket connection running as a separate
background task, but that task doesn't make decisions itself — it just drops
parsed messages into a queue (an `mpsc` channel) for the cashier to pick up
one at a time. This matches the Python bot's single-threaded `asyncio` model
exactly (same semantics, safer guarantees, since Rust's type system won't
let two tasks share that state without a lock, and there simply isn't one).
This matters because a lot of the decision logic — like "don't requote if we
quoted less than a second ago" — depends on nothing running concurrently
with the cashier's own bookkeeping.

Everything else (fill reconciliation every 5s, position sync every 30s, exit
checks every 10s, a daily PnL reset, an hourly report) is a timer that also
feeds into that same single loop via `tokio::select!`. Nothing touches shared
trading state from two places at once — there are no locks in this codebase
because there's no contention to lock against.

## Where a market order actually comes from: the two-file split

The single most important architectural decision in this codebase is
splitting **decide** from **do**:

- **`engine.rs`** decides. Given a model's output and the current book, it
  computes "quote 27¢ / 53¢, cancel these two stale orders" — or decides to
  do nothing. It touches no network, reads no clock (the caller passes time
  in as a parameter), and can be tested with plain function calls.
- **`executor.rs`** does. It takes the plan `engine.rs` produced and actually
  fires the HTTP requests, in parallel (cancel both stale sides at once,
  place both new sides at once — the Python bot did these sequentially,
  which is where a lot of its latency came from).

The Python bot fuses these two responsibilities into one big function
(`_execute_action`), which made it hard to test the decision logic without
also mocking out the network. Splitting them here means `engine.rs`'s 15
unit tests run in microseconds with no I/O, mocks, or async runtime at all.

### The quote decision pipeline, in order

`engine::plan_quotes` runs through a fixed sequence of gates, and **the order
matters** — later gates assume earlier ones already ran (e.g., the throttle
gate has to advance a clock *before* the balance-backoff gate checks it, even
if backoff then blocks the quote — this exact ordering is deliberately
copied from Python and pinned down by a fixture, see below).

1. **Throttle** — skip if we quoted this ticker less than 1 second ago.
   Requoting on every book tick would mean constantly canceling and
   replacing orders, which loses your place in the exchange's FIFO queue.
2. **Balance backoff** — if a recent order was rejected for insufficient
   collateral, stop trying to quote *anything* for a cooldown period instead
   of hammering the API with doomed requests (the Python bot once sent
   4,900 failed orders in 6 minutes before this gate existed).
3. **Quote band** — skip contracts trading outside 5¢–95¢ mid price. A 97¢
   contract locks 97¢ of collateral to capture maybe a penny of spread —
   that's a lottery ticket, not market making.
4. **Order budget** — a global cap on how many resting orders we're allowed
   at once (each two-sided quote locks about $1 of collateral). A ticker
   that's already quoting is exempt from this cap so it can keep refreshing
   its own orders even when the budget is technically full.
5. **Clamp to passive** (`clamp_quotes` in `book.rs`) — the model's output
   is nudged so it never crosses the current best bid/ask. A quote that
   crosses the touch executes immediately as a *taker* order, paying the
   spread plus a 4x-higher taker fee — exactly the wrong side of market
   making.
6. **Fee gate** (`quote_edge_ok`) — Kalshi rounds every fill's fee up to the
   next whole cent. On a 1-contract order near 50¢, that's 1¢ each way, 2¢
   round trip — so a 2¢-wide quote guarantees a loss before it even fills.
   This gate skips any spread too narrow to clear fees plus a minimum edge.
7. **Tick-move-keep** — if the desired price hasn't moved by a full tick
   since we last quoted, do nothing and leave the resting orders alone
   (canceling and replacing on every tiny wobble means constant
   back-of-queue).
8. **Inventory caps** — each side (buy/sell) is independently blocked if
   taking that fill would push our position past the configured max. A
   trader long 5 contracts can still post an offer to sell, just not
   another bid to buy.

Only after all of that does `plan_quotes` return a `QuotePlan` for
`executor.rs` to actually place.

### Exits are separate from quoting

`engine::exit_decision` is a second, simpler pipeline that runs every 10
seconds against any open position, independent of the quoting loop. It
checks two triggers:

- **Stop-loss**: exit if the mark has moved against us by more than
  `max(5¢, 2× the current spread)`. The 2×-spread floor exists so that a
  wide, noisy market doesn't trigger a stop on its own bid-ask wobble.
- **Expiry**: exit if the market closes within 120 seconds — you don't want
  to be sitting on inventory when a contract settles to $0 or $1.

Unlike quotes, exits are allowed to cross the spread (an `immediate_or_cancel`
order at the touch) — the whole point is to get out *now*, not to earn
another spread on the way out.

## The 20-dim observation vector

`features.rs::build_observation` builds the exact same 20-number input the
PPO policy was trained on (mirrored from `rl_bot/mm_env.py`). Think of it as
a snapshot the model reads before every decision — mid price, spread, how
much size is resting at the top 3 price levels on each side, our current
inventory, unrealized PnL, time until the market closes, recent price
momentum, and a rolling realized-volatility estimate. If any of these numbers
drift out of the range the model saw during training (checked via `.clamp()`
against `OBS_LOW`/`OBS_HIGH`), the model would be making decisions on
inputs it's never seen — so several early-return gates exist specifically to
avoid ever calling the model outside its training distribution (spread over
50¢, or realized volatility above the 0.05 threshold that separates
mean-reverting markets, where this strategy works, from trending ones, where
it loses).

## Inference: ONNX instead of loading a Python model

The trading policy was trained in Python with Stable-Baselines3 (PPO), which
depends on PyTorch — not something you want to link into a small, latency-
sensitive Rust binary. Instead, `scripts/export_policy_onnx.py` (Python side)
exports the trained policy's *forward pass only* — observation in, action out
— to the ONNX format, a portable graph representation. `model.rs` loads that
graph with the `ort` crate (a Rust binding to Microsoft's ONNX Runtime) and
runs inference directly, no Python process involved at runtime. Each category
gets its own `Policy` (one ONNX session), and a category whose model file is
missing or has the wrong input width is simply disabled rather than fed
garbage — the equivalent Python check exists for the same reason (a model
trained on an older, differently-sized observation vector would silently
produce nonsense actions if loaded blind).

## Talking to Kalshi: `api.rs` and `transport.rs`

Two separate concerns, both authenticating with the same RSA-PSS signature
scheme Kalshi requires (sign `timestamp + HTTP method + path` with your
private key; the exchange verifies it with your public key on file):

- **`api.rs`** is the REST client — one-shot requests: fetch markets, fetch
  an orderbook snapshot, place an order, cancel an order, ask for recent
  fills. Built on `reqwest` with a pooled connection and `rustls` (no
  dependency on the system's OpenSSL).
- **`transport.rs`** is the WebSocket client — a long-lived connection that
  streams book updates and trade prints in real time, since polling REST
  every second for 286 markets isn't fast enough to react to a moving
  market. If the socket drops, `WsClient::run` reconnects after 5 seconds,
  forever, and re-subscribes to everything (subscriptions don't survive a
  reconnect).

If the WebSocket is unavailable at all (bad credentials, network issue), the
bot falls back to REST polling every 10 seconds — slower, but it still
trades rather than sitting idle.

## Safety net: paper mode

`paper.rs`'s `PaperClient` is the reason it's safe to run this bot against
the *real* Kalshi API without risking real money. It reads real market data
(orderbooks, prices) from the live API when credentials are present, but
every order-shaped call — place, cancel, positions, fills — is answered
entirely in memory. Nothing that looks like an order ever reaches the
exchange. This is the mode used for the pre-deployment smoke test (confirming
the bot connects, discovers markets, and plans sane quotes against live data)
and it's also how the parity-fixture generator (below) safely drives the real
decision code without a Kalshi account at all.

## Parity testing: proving the port matches the original

Because this is a rewrite of a bot that trades real money, "the tests pass"
isn't enough — the tests need to prove Rust produces the *same numbers* as
the Python bot on the same inputs, not just numbers that look plausible in
isolation. That's what `tests/parity.rs` and `tests/fixtures/parity.json`
are for.

The fixtures aren't hand-written. A generator in the Python monorepo
(`gen_parity_fixtures.py`) **imports and calls the real, unmodified Python
trading code** (`live_trader_v2.py`) on a battery of synthetic scenarios, and
records what it actually did, writing `tests/fixtures/parity.json` here:

- observation vectors for various book/position combinations,
- fees for a grid of contract counts and prices,
- fill accounting through sequences of buys/sells/flips,
- ONNX action outputs on real exported models,
- and quote decisions — driving `LiveTrader._execute_action` through a paper
  client and recording what it placed/canceled, covering every gate in the
  pipeline above (throttle, backoff, band, budget, fee gate, tick-move-keep,
  inventory caps, subpenny pricing).

Rust then replays every one of those scenarios through the real Rust
functions and asserts the outputs match — observations to 1e-6, fees and
fill accounting exactly, ONNX actions to 1e-4. This setup has already caught
one real bug: `engine.rs`'s `scale_action` was missing a 3-decimal rounding
step that Python's `mm_env.py` applies, which is invisible on most model
outputs (later 2-decimal price rounding usually absorbs the difference) but
occasionally flips the quoted price by a cent. It's fixed now, and the
specific case that exposed it (found by brute-forcing two million random
actions until one crossed the rounding boundary) is a permanent fixture, so
a regression here can't silently reappear.

To regenerate the fixtures after changing either side's logic, run the
generator in the Python monorepo (it writes `tests/fixtures/parity.json`
here), then re-run the Rust tests:

```
python gen_parity_fixtures.py   # in the Python monorepo
cargo test                      # here
```

## Self-containment

This repository builds and runs as a standalone unit — no path outside it is
required at runtime. `.env` (credentials), `config/*.toml` (deployment
parameters), and `models/*.onnx` (exported policies) all live at the repo
root; see `.env.example` and `start_live_trading.sh`. The parity-fixture
*generator* is the one thing kept elsewhere (the Python monorepo), since its
whole job is calling into the Python codebase; the fixtures it produces are
committed here (`tests/fixtures/`) so the tests run without it.

## Quick start

```
cp .env.example .env          # fill in KALSHI_API_KEY + KALSHI_API_SECRET path
mkdir -p models               # drop this deployment's *_final.onnx checkpoints here
cargo test                    # 67 tests incl. Python-parity fixtures
cargo run --release -- --paper --config lowvol   # paper mode against live data
```

`start_live_trading.sh` wraps the live (real-money) launch with pre-flight
checks; `deploy/aws_deploy.sh` provisions an EC2 instance and runs it there.

## File map

| File | Responsibility |
|---|---|
| `main.rs` | CLI, startup sequence, the event loop, log/lock setup |
| `config.rs` | Deployment config (`config/*.toml`): risk limits, categories, MM hyperparameters |
| `book.rs` | Orderbook state + pure safety helpers (`clamp_quotes`, `quote_edge_ok`, fee math) |
| `features.rs` | Builds the 20-dim observation vector fed to the model |
| `model.rs` | Loads and runs the ONNX policy |
| `engine.rs` | Pure decision logic: quote planning, exit decisions, risk-limit checks |
| `executor.rs` | Turns engine decisions into concurrent API calls |
| `state.rs` | Per-ticker and account-level state, fill accounting |
| `api.rs` | Kalshi REST client + request signing |
| `transport.rs` | Kalshi WebSocket client with auto-reconnect |
| `paper.rs` | Paper-trading client (real market data, simulated orders) |
| `lib.rs` | Just re-exports the modules above for the binary and the test suite |

## What's still open

The bot has been smoke-tested in paper mode against live market data
(connects, discovers all markets, plans sane quotes, places simulated
orders) but has not yet been run live with real money. CI
(`.github/workflows/ci.yml`) and deployment scripts (`deploy/aws_deploy.sh`)
exist but the AWS deployment has not been executed.
