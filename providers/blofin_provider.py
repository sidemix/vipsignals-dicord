import os, math, time
import pandas as pd

from .base import BaseProvider

# --- Config via env (REST fallback) ---
BLOFIN_REST_BASE   = os.getenv("BLOFIN_REST_BASE", "https://openapi.blofin.com")
BLOFIN_REST_KLINES = os.getenv("BLOFIN_REST_KLINES", "/api/v1/market/candles") 
# NOTE: If BloFin’s actual path differs, just change BLOFIN_REST_KLINES in Render env.
# Common patterns you can try if needed:
#   /api/v1/public/candles
#   /api/v1/public/market/candles
#   /v1/market/candles

# Map TV-style TF to common API strings (edit in env if needed)
DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","4h":"4h","6h":"6h","12h":"12h",
    "1d":"1d"
}
TF_MAP = {**DEFAULT_TF_MAP}  # can be overridden later

def _symbol_to_blofin_spot(symbol: str) -> str:
    # "MTL/USDT" -> "MTLUSDT"
    return symbol.replace("/", "")

class BlofinProvider(BaseProvider):
    def __init__(self):
        self.sdk = None
        self._markets = {}
        # Try SDK
        try:
            from blofin import Blofin  # type: ignore
            key    = os.getenv("BLOFIN_API_KEY")
            secret = os.getenv("BLOFIN_API_SECRET")
            passph = os.getenv("BLOFIN_API_PASSPHRASE")
            # Public data typically doesn’t require keys; pass None safely
            self.sdk = Blofin(api_key=key, api_secret=secret, passphrase=passph)
        except Exception:
            self.sdk = None

    def load_markets(self) -> dict:
        # Minimal “allow-all” map so your startup check doesn’t fail.
        # If you want strict validation, query instruments via SDK/REST and build this dict.
        return self._markets

    # --- SDK path (if available) ---
    def _sdk_fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        # This block assumes the SDK has a public candles endpoint similar to:
        #   sdk.public.get_candlesticks(instId="BTC-USDT", bar="5m", limit=400)
        # If names differ, tweak here. If it errors, we fall back to REST.
        inst = symbol.replace("/", "-")  # "BTC/USDT" -> "BTC-USDT"
        bar  = TF_MAP.get(timeframe, timeframe)
        data = self.sdk.public.get_candlesticks(instId=inst, bar=bar, limit=limit)  # type: ignore[attr-defined]
        # Expected shape: list of items with [ts, open, high, low, close, volume] or dicts; normalize below
        rows = []
        for x in data:
            # tolerate dict or list
            if isinstance(x, dict):
                ts   = int(x.get("ts") or x.get("time") or x.get("t"))
                op   = float(x.get("open"))
                hi   = float(x.get("high"))
                lo   = float(x.get("low"))
                cl   = float(x.get("close"))
                vol  = float(x.get("volume"))
            else:
                ts, op, hi, lo, cl, vol = int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])
            rows.append([ts, op, hi, lo, cl, vol])
        df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        return df

    # --- REST fallback ---
    def _rest_fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        import httpx
        pair = _symbol_to_blofin_spot(symbol)  # "MTL/USDT" -> "MTLUSDT"
        bar  = TF_MAP.get(timeframe, timeframe)
        url  = BLOFIN_REST_BASE.rstrip("/") + BLOFIN_REST_KLINES
        # The most common query params pattern; if BloFin uses different keys,
        # adjust them via the env variable BLOFIN_REST_KLINES or tweak here.
        params = {
            "symbol": pair,   # try "instId" if "symbol" doesn’t work
            "interval": bar,  # try "bar" if "interval" doesn’t work
            "limit": limit
        }
        r = httpx.get(url, params=params, timeout=15)
        r.raise_for_status()
        payload = r.json()
        # Try to normalize multiple possible shapes:
        data = payload.get("data") or payload.get("result") or payload
        rows = []
        for x in data:
            if isinstance(x, dict):
                ts = int(x.get("ts") or x.get("time") or x.get("t"))
                op = float(x.get("open"))
                hi = float(x.get("high"))
                lo = float(x.get("low"))
                cl = float(x.get("close"))
                vol = float(x.get("volume"))
            else:
                # Many exchanges return: [ts, open, high, low, close, volume, ...]
                ts, op, hi, lo, cl, vol = int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])
            rows.append([ts, op, hi, lo, cl, vol])
        df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        return df

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        if self.sdk is not None:
            try:
                return self._sdk_fetch_ohlcv_df(symbol, timeframe, limit)
            except Exception:
                pass
        return self._rest_fetch_ohlcv_df(symbol, timeframe, limit)
