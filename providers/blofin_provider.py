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

# ---------- Auto markets + top symbols ----------
BLOFIN_INSTRUMENTS = os.getenv("BLOFIN_INSTRUMENTS", "/api/v1/public/instruments")
BLOFIN_TICKERS     = os.getenv("BLOFIN_TICKERS", "/api/v1/public/tickers")

def _http_get_json(url, params=None, timeout=15):
    import httpx
    r = httpx.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _norm_symbol_from_inst(inst: dict) -> str:
    # Expect "instId": "BTC-USDT" or similar
    inst_id = inst.get("instId") or inst.get("symbol")
    if not inst_id:
        return None
    base, _, quote = inst_id.replace("_", "-").partition("-")
    if not quote:
        # fallback (BTCUSDT)
        s = inst_id.upper()
        if s.endswith("USDT"):
            base, quote = s[:-4], "USDT"
        else:
            return None
    return f"{base}/{quote}"

def list_blofin_symbols(inst_type="SWAP", want_quote="USDT"):
    """Return list of 'BASE/QUOTE' for given instrument type and quote."""
    base = BLOFIN_REST_BASE.rstrip("/")
    url  = base + BLOFIN_INSTRUMENTS
    payload = _http_get_json(url, params={"instType": inst_type})
    data = payload.get("data") or payload.get("result") or []
    out = []
    for inst in data:
        sym = _norm_symbol_from_inst(inst)
        if not sym:
            continue
        try:
            quote = sym.split("/")[-1]
        except Exception:
            continue
        if want_quote and quote.upper() != want_quote.upper():
            continue
        out.append(sym)
    return sorted(set(out))

def top_by_volume(symbols, inst_type="SWAP", want_quote="USDT", top_n=12, min_vol=0.0):
    """Attach 24h quote volume and pick best symbols."""
    if not symbols:
        return []
    base = BLOFIN_REST_BASE.rstrip("/")
    url  = base + BLOFIN_TICKERS
    payload = _http_get_json(url, params={"instType": inst_type})
    data = payload.get("data") or payload.get("result") or []

    # map instId -> 24h quote vol
    vols = {}
    for t in data:
        inst_id = t.get("instId") or t.get("symbol")
        if not inst_id:
            continue
        sym = _norm_symbol_from_inst({"instId": inst_id})
        if not sym:
            continue
        qv = t.get("volUsd") or t.get("quoteVolume") or t.get("vol24hQuote")
        try:
            qv = float(qv)
        except Exception:
            qv = 0.0
        vols[sym] = qv

    scored = [(s, vols.get(s, 0.0)) for s in symbols]
    if min_vol and min_vol > 0:
        scored = [x for x in scored if x[1] >= min_vol]
    scored.sort(key=lambda x: x[1], reverse=True)
    if top_n and top_n > 0:
        scored = scored[:top_n]
    return [s for s, _ in scored]

