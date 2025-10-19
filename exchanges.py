import ccxt
import pandas as pd

def make_exchange(name: str):
    name = (name or "binance").lower()
    if not hasattr(ccxt, name):
        raise ValueError(f"Unsupported exchange: {name}")
    ex = getattr(ccxt, name)()
    ex.options = getattr(ex, "options", {})
    return ex

def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    o = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(o, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    return df

def fetch_funding_rate(exchange, symbol: str):
    try:
        # ccxt unified fetch (not all exchanges support)
        if getattr(exchange, "has", {}).get("fetchFundingRate", False):
            fr = exchange.fetch_funding_rate(symbol)
            # return percent value (e.g., 0.01 means 1%)
            val = fr.get("fundingRate")
            if val is None:
                return None
            # normalize to percent
            return float(val) * 100.0 if abs(val) < 1 else float(val)
        return None
    except Exception:
        return None

# exchanges.py (or wherever ccxt is initialized)
import os

USE_CCXT = os.getenv("EXCHANGE", "").strip().lower() not in ("", "none", "hyperliquid")

exchange = None
if USE_CCXT:
    import ccxt  # noqa
    # build the selected ccxt exchange instance as before
    # exchange = ccxt.binance() ... etc.

def load_markets_or_empty():
    return exchange.load_markets() if exchange else {}

def has_market(symbol: str) -> bool:
    mkts = load_markets_or_empty()
    return symbol in mkts if mkts else True   # if no ccxt, don’t block




