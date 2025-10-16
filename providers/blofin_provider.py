import os
import pandas as pd

from .base import BaseProvider

# --- Config via env (REST fallback) ---
BLOFIN_REST_BASE   = os.getenv("BLOFIN_REST_BASE", "https://openapi.blofin.com")
BLOFIN_REST_KLINES = os.getenv("BLOFIN_REST_KLINES", "/api/v1/market/candles")
# If BloFin’s actual path differs, change BLOFIN_REST_KLINES in env.
# Common alternates you can try via env:
#   /api/v1/public/candles
#   /api/v1/public/market/candles
#   /v1/market/candles

# Map TV-style TF to common API strings (edit via env if needed)
DEFAULT_TF_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "4h": "4h", "6h": "6h", "12h": "12h",
    "1d": "1d"
}
TF_MAP = {**DEFAULT_TF_MAP}  # can be overridden later if required

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
        # Assumes the SDK exposes a public candles endpoint like:
        #   sdk.public.get_candlesticks(instId="BTC-USDT", bar="5m", limit=400)
        inst = symbol.replace("/", "-")  # "BTC/USDT" -> "BTC-USDT"
        bar  = TF_MAP.get(timeframe, timeframe)
        data = self.sdk.public.get_candlesticks(instId=inst, bar=bar, limit=limit)  # type: ignore[attr-defined]

        rows = []
        for x in data:
            if isinstance(x, dict):
                ts   = _to_int_ms(x.get("ts") or x.get("time") or x.get("t"))
                op   = float(x.get("open"))
                hi   = float(x.get("high"))
                lo   = float(x.get("low"))
                cl   = float(x.get("close"))
                vol  = float(x.get("volume"))
            else:
                ts, op, hi, lo, cl, vol = _to_int_ms(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])
            rows.append([ts, op, hi, lo, cl, vol])
        df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.sort_values("time", inplace=True)
        return df

    # --- REST fallback (robust to multiple payload shapes) ---
    def _rest_fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        import httpx

        def _first_list_like(obj):
            # find the first list-like value inside a dict
            for k in ("data", "result", "rows", "list", "candles", "klines", "kline", "items"):
                if isinstance(obj, dict):
                    v = obj.get(k)
                    if isinstance(v, list) and len(v) > 0:
                        return v
            if isinstance(obj, list):
                return obj
            return None

        def _dict_of_arrays_to_rows(d):
            # accept t/o/h/l/c/v or time/open/high/low/close/volume
            keys_short = all(k in d for k in ("t","o","h","l","c","v"))
            keys_long  = all(k in d for k in ("time","open","high","low","close","volume"))
            if not (keys_short or keys_long):
                return None
            t = d["t"] if keys_short else d["time"]
            o = d["o"] if keys_short else d["open"]
            h = d["h"] if keys_short else d["high"]
            l = d["l"] if keys_short else d["low"]
            c = d["c"] if keys_short else d["close"]
            v = d["v"] if keys_short else d["volume"]
            n = min(len(t), len(o), len(h), len(l), len(c), len(v))
            rows = []
            for i in range(n):
                rows.append([
                    _to_int_ms(t[i]),
                    float(o[i]), float(h[i]), float(l[i]), float(c[i]), float(v[i])
                ])
            return rows

        pair_dash = symbol.replace("/", "-")   # e.g., BTC/USDT -> BTC-USDT
        pair_cat  = symbol.replace("/", "")    # e.g., BTC/USDT -> BTCUSDT
        bar       = TF_MAP.get(timeframe, timeframe)

        base = BLOFIN_REST_BASE.rstrip("/")

        # Try a few common param conventions without code redeploys
        attempts = [
            (BLOFIN_REST_KLINES, {"instId": pair_dash, "bar": bar,      "limit": limit}),
            (BLOFIN_REST_KLINES, {"symbol": pair_cat,  "interval": bar, "limit": limit}),
            (BLOFIN_REST_KLINES, {"instId": pair_dash, "interval": bar, "limit": limit}),
            (BLOFIN_REST_KLINES, {"symbol": pair_dash, "bar": bar,      "limit": limit}),
        ]

        last_err = None
        for path, params in attempts:
            try:
                url = base + path
                r = httpx.get(url, params=params, timeout=15)
                r.raise_for_status()
                payload = r.json()

                # Extract the candles section
                data = payload
                rows = None
                if isinstance(payload, dict):
                    cand = _first_list_like(payload)
                    if cand is None:
                        # maybe dict-of-arrays
                        rows = _dict_of_arrays_to_rows(payload)
                    else:
                        data = cand

                if rows is None:
                    rows = []
                    if isinstance(data, list) and len(data) > 0:
                        if isinstance(data[0], dict):
                            for x in data:
                                ts = _to_int_ms(x.get("ts") or x.get("time") or x.get("t"))
                                op = float(x.get("open")  or x.get("o"))
                                hi = float(x.get("high")  or x.get("h"))
                                lo = float(x.get("low")   or x.get("l"))
                                cl = float(x.get("close") or x.get("c"))
                                vol= float(x.get("volume")or x.get("v"))
                                rows.append([ts, op, hi, lo, cl, vol])
                        elif isinstance(data[0], (list, tuple)):
                            # typical: [ts, open, high, low, close, volume, ...]
                            for x in data:
                                ts = _to_int_ms(x[0])
                                rows.append([ts, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
                        else:
                            # unexpected; try dict-of-arrays on first element
                            if isinstance(data[0], dict):
                                maybe = _dict_of_arrays_to_rows(data[0])
                                if maybe:
                                    rows = maybe

                if not rows and isinstance(payload, dict):
                    # last resort on root
                    rows = _dict_of_arrays_to_rows(payload)

                if not rows:
                    raise ValueError("Unrecognized kline payload shape")

                df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                df.sort_values("time", inplace=True)
                return df
            except Exception as e:
                last_err = e
                continue

        raise last_err or RuntimeError("Failed to fetch klines from BloFin")

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

def _norm_symbol_from_inst(inst: dict) -> str | None:
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


# --------- utilities ---------
def _to_int_ms(x):
    """Accept ms or sec; return milliseconds as int."""
    ts = int(float(x))
    if ts < 10_000_000_000:  # seconds → ms
        ts *= 1000
    return ts
