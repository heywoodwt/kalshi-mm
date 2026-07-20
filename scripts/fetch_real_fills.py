#!/usr/bin/env python3
"""Fetch the account's complete real-money fills + settlements to JSONL.

Why: these are the ground truth the RL retrain calibrates against (fill
economics) and validates against (daily PnL). The bot's internal logs reset
on restart and are NOT a trade record; the exchange is.

Output: real_fills.jsonl and real_settlements.jsonl in CWD — one raw API
object per line, no transformation (the HPC side normalizes with polars).

Usage: python3 scripts/fetch_real_fills.py   (from kalshi-mm repo root)
"""
import os, time, base64, json, urllib.request
from urllib.parse import urlparse
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

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

def paged(path, base_q, key, pages=100):
    out, cursor = [], None
    for _ in range(pages):
        q = base_q + (f"&cursor={cursor}" if cursor else "")
        d = get(path, q)
        out += d.get(key, [])
        cursor = d.get("cursor")
        if not cursor or not d.get(key):
            break
    return out

fills = paged("/portfolio/fills", "?limit=200", "fills")
setts = paged("/portfolio/settlements", "?limit=200", "settlements")
with open("real_fills.jsonl", "w") as f:
    for x in fills:
        f.write(json.dumps(x) + "\n")
with open("real_settlements.jsonl", "w") as f:
    for x in setts:
        f.write(json.dumps(x) + "\n")
print(f"wrote {len(fills)} fills, {len(setts)} settlements")
