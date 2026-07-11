#!/bin/bash
# Start the Rust kalshi-mm live trading bot.
# WARNING: LIVE MODE - REAL MONEY TRADING. For paper mode instead, run
# `cargo run --release -- --paper --config <name>` directly.
set -e
cd "$(dirname "$0")"

echo "=========================================="
echo "Starting Kalshi MM Trading Bot (Rust)"
echo "Mode: LIVE (REAL MONEY)"
echo "=========================================="

if [ ! -f ".env" ]; then
    echo "ERROR: .env not found in rust/ (copy .env.example and fill it in)"
    exit 1
fi
if grep -q "PAPER_MODE=true" .env; then
    echo "ERROR: PAPER_MODE is still true in rust/.env"
    echo "Set PAPER_MODE=false for live trading."
    exit 1
fi
if ! grep -q "KALSHI_API_KEY=" .env || ! grep -q "KALSHI_API_SECRET=" .env; then
    echo "ERROR: Missing Kalshi API credentials in rust/.env"
    exit 1
fi
if [ -z "$(ls models/*.onnx 2>/dev/null)" ]; then
    echo "ERROR: models/ has no ONNX checkpoints — copy this deployment's"
    echo "<prefix>_<CATEGORY>_final.onnx files there first."
    exit 1
fi

echo
echo "Environment verified:"
echo "  - Live mode enabled"
echo "  - API credentials present"
echo "  - ONNX checkpoints present"
echo
echo "Building + starting (config: ${1:-lowvol})..."
echo "Logs: live_trading_rust_${1:-lowvol}.log"
echo

exec cargo run --release --locked -- --config "${1:-lowvol}"
