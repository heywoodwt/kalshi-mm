#!/usr/bin/env python3
"""Exchange-verified daily PnL scoreboard for the kalshi-mm bot.

Why this exists: the bot's internal Cumulative PnL only knows this process's
lifetime and (pre-2026-07-19 builds) missed settlement revenue entirely. The
exchange's own fills + settlements are the scoreboard.

Attribution: each ticker's PnL (sum over its fills since DEPLOY_TS of
signed x (settle_value - fill_price), minus its fees) books on its UTC
SETTLEMENT date. Open tickers are shown as a pending bucket. Summing all
days + pending equals total realized-and-marked PnL since DEPLOY_TS.

Usage:  python3 scripts/daily_pnl.py            (run from repo root)
"""
import os, time, base64, json, urllib.request, datetime, collections
from urllib.parse import urlparse
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# daily_pnl_v2 deploy: KXBTCD + KXAAAGASM got retrained policies (commit
# e76d6ec). KXWCGAME + KXADP are unchanged, but scored from the same point
# so the four categories share one scoreboard window.
DEPLOY_TS = int(datetime.datetime(2026, 7, 20, 15, 29, 52,
                                  tzinfo=datetime.timezone.utc).timestamp())
CATS = ("KXBTCD", "KXWCGAME", "KXADP", "KXAAAGASM")

def env(k):
    for line in open(".env"):
        line = line.strip()
        if line.startswith(k + "="):
            return line.split("=", 1)[1].strip().strip('"')
    return None

key_ref = env("KALSHI_API_SECRET")
api_key = env("KALSHI_API_KEY")
base = env("KALSHI_BASE_URL").rstrip("/")
base_path = urlparse(base).path
pem = open(key_ref).read() if os.path.exists(key_ref) else key_ref
priv = serialization.load_pem_private_key(pem.encode(), password=None)

def get(path, query=""):
    ts = str(int(time.time() * 1000))
    sig = base64.b64encode(priv.sign(
        (ts + "GET" + base_path + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=hashes.SHA256().digest_size),
        hashes.SHA256())).decode()
    req = urllib.request.Request(base + path + query, method="GET")
    req.add_header("KALSHI-ACCESS-KEY", api_key)
    req.add_header("KALSHI-ACCESS-SIGNATURE", sig)
    req.add_header("KALSHI-ACCESS-TIMESTAMP", ts)
    return json.load(urllib.request.urlopen(req, timeout=20))

def paged(path, base_q, key, pages=20):
    out, cursor = [], None
    for _ in range(pages):
        q = base_q + (f"&cursor={cursor}" if cursor else "")
        d = get(path, q)
        out += d.get(key, [])
        cursor = d.get("cursor")
        if not cursor or not d.get(key):
            break
    return out

fills = paged("/portfolio/fills", f"?limit=200&min_ts={DEPLOY_TS}", "fills")
setts = paged("/portfolio/settlements", "?limit=200", "settlements")

def iso_ts(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))

# per-ticker fill aggregates (YES frame)
per = {}
for f in fills:
    t = f.get("ticker", "")
    if not t.startswith(CATS):
        continue
    n = float(f["count_fp"])
    signed = n if f["action"] == "buy" else -n
    price = float(f["yes_price_dollars"])
    p = per.setdefault(t, {"net": 0.0, "cash": 0.0, "fees": 0.0})
    p["net"] += signed
    p["cash"] -= signed * price
    p["fees"] += float(f.get("fee_cost", 0) or 0)

settled = {}  # ticker -> (utc_date, result)
for s in setts:
    t = s.get("ticker", "")
    st = s.get("settled_time", "")
    if t.startswith(CATS) and st and iso_ts(st).timestamp() >= DEPLOY_TS:
        settled[t] = (iso_ts(st).date(), s.get("market_result", "?"))

by_day = collections.defaultdict(float)
by_day_cat = collections.defaultdict(lambda: collections.defaultdict(float))
pending_cash, pending = 0.0, []
pending_by_cat = collections.defaultdict(float)
for t, p in per.items():
    cat = next(c for c in CATS if t.startswith(c))
    if t in settled:
        day, result = settled[t]
        pay = p["net"] * (1.0 if result == "yes" else 0.0)
        pnl = p["cash"] + pay - p["fees"]
        by_day[day] += pnl
        by_day_cat[day][cat] += pnl
    else:
        pending_cash += p["cash"] - p["fees"]
        pending_by_cat[cat] += p["cash"] - p["fees"]
        if p["net"]:
            pending.append((t, p["net"]))

print(f"=== settled-day PnL (bot categories, since deploy) ===")
total = 0.0
for day in sorted(by_day):
    total += by_day[day]
    breakdown = ", ".join(f"{c}=${by_day_cat[day][c]:+.2f}" for c in CATS if by_day_cat[day][c])
    print(f"{day}  ${by_day[day]:+7.2f}   ({breakdown})")
print(f"{'TOTAL':10s}  ${total:+7.2f}")

print(f"\npending (open tickers): net fill cash ${pending_cash:+.2f} "
      f"across {len(pending)} open positions")
for c in CATS:
    if pending_by_cat[c]:
        print(f"  {c}: ${pending_by_cat[c]:+.2f}")

days = sorted(by_day)[-7:]
pos = sum(1 for d in days if by_day[d] > 0)
wk = sum(by_day[d] for d in days)
print(f"\ntrailing {len(days)} settle-days: {pos} positive, sum ${wk:+.2f}")
print("SUCCESS CRITERION:", "MET" if (pos >= 5 and wk >= 10.0 and len(days) >= 7)
      else "not yet (need >=5 of 7 positive and 7-day sum >= +$10)")
