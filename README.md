# Crypto Signals Bot → Discord

Scans altcoins on an exchange (via `ccxt`), detects signals (EMA5/EMA50 cross with EMA200 trend filter + ATR pullback), and posts clean embeds to Discord.

## Quick start

1. Clone this repo, create `.env` from `.env.example`, set your `DISCORD_WEBHOOK_URL`.
2. `pip install -r requirements.txt`
3. `python main.py`

## Docker


