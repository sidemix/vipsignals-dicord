import os
import time
import pandas as pd
from .base import BaseProvider

HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz")
# leave HL_KLINES empty; the provider will try good defaults
HL_KLINES      = os.getenv("HL_KLINES", "")
HL_INSTRUMENTS = os.getenv("HL_INSTRUMENTS", "/api/v1/public/instruments")
HL_TICKERS     = os.getenv("HL_TICKERS", "/api/v1/public/tickers")

DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","12h":"12h",
    "1d":"1d"
}
TF_MAP = {**DEFAULT_TF_MAP}

def _debug(msg: str):
    if os.getenv("DEBUG", "").strip().lower() in ("1","true","yes","on"):
        try:
            from discord_sender import send_info
            send_info(f"[HL] {msg}")
        except Exception:
            print(f"[HL] {msg}")

def _http_post_json(url, json=None, timeout=15):
    import httpx
    r = httpx.post(url, json=json, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _http_get_json(url, params=None, timeout=15):
    import httpx
    r = httpx.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _extract_list(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("data","result","rows","list","candles","klines","kline","items"):
            v = obj.get(k)
            if isinstance(v, list):
                return v
    return None

def _to_ms(x):
    ts = int(float(x))
    if ts < 10_000_000_000:  # seconds→ms
        ts *= 1000
    return ts

def _parse_rows(payload):
    """
    Normalize various candle payloads to:
      [time_ms, open, high, low, close, volume]
    """
    data = payload
    rows = None

    if isinstance(payload, dict):
        maybe = _extract_list(payload)
        if maybe is None:
            # dict-of-arrays fallback
            short = all(k in payload for k in ("t","o","h","l","c","v"))
            long  = all(k in payload for k in ("time","open","high","low","close","volume"))
            if short or long:
                T = payload["t"] if short else payload["time"]
                O = payload["o"] if short else payload["open"]
                H = payload["h"] if short else payload["high"]
                L = payload["l"] if short else payload["low"]
                C = payload["c"] if short else payload["close"]
                V = payload["v"] if short else payload["volume"]
                n = min(len(T), len(O), len(H), len(L), len(C), len(V))
                rows = [[_to_ms(T[i]), float(O[i]), float(H[i]), float(L[i]), float(C[i]), float(V[i])] for i in range(n)]
                return rows
        else:
            data = maybe

    rows = []
    if isinstance(data, list) and data:
        # dict rows
        if isinstance(data[0], dict):
            for x in data:
                ts  = _to_ms(x.get("ts") or x.get("time") or x.get("t"))
                op  = float(x.get("open")  or x.get("o"))
                hi  = float(x.get("high")  or x.get("h"))
                lo  = float(x.get("low")   or x.get("l"))
                cl  = float(x.get("close") or x.get("c"))
                vol = float(x.get("volume")or x.get("v"))
                rows.append([ts, op, hi, lo, cl, vol])
        # array rows
        elif isinstance(data[0], (list, tuple)) and len(data[0]) >= 6:
            for x in data:
                ts = _to_ms(x[0])
                rows.append([ts, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
    return rows if rows else None

def _force_quote(sym: str) -> str:
    target = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()
    base, sep, quote = sym.upper().partition("/")
    if sep and target and quote != target:
        return f"{base}/{target}"
    return sym.upper()

def _interval_to_seconds(bar: str) -> int:
    # naive but fine for our bars
    u = bar[-1]
    n = int(bar[:-1])
    if u == "m": return n * 60
    if u == "h": return n * 3600
    if u == "d": return n * 86400
    return 60

class HyperliquidProvider(BaseProvider):
    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        return self._markets

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        symbol = _force_quote(symbol)          # e.g., BTC/USDT -> BTC/USD
        base = HL_REST_BASE.rstrip("/")
        bar  = TF_MAP.get(timeframe, timeframe)
        coin_base = symbol.split("/")[0]       # "BTC" from "BTC/USD"
        inst_dash = symbol.replace("/", "-")   # "BTC-USD"

        # prefer explicit override if you set HL_KLINES
        post_paths = []
        if HL_KLINES.strip():
            post_paths.append(HL_KLINES.strip())
        # known working POST entry points
        post_paths += [
            "/info",
            "/api/v1/info",
        ]

        now_ms = int(time.time() * 1000)
        sec_per_bar = _interval_to_seconds(bar)
        start_ms = now_ms - (limit + 5) * sec_per_bar * 1000

        # a set of likely body shapes Hyperliquid responds to
        # (we try both coin-only and full inst id)
        body_candidates = [
            {"type": "candleSnapshot", "coin": coin_base, "interval": bar, "startTime": start_ms, "endTime": now_ms},
            {"type": "candleSnapshot", "coin": inst_dash, "interval": bar, "startTime": start_ms, "endTime": now_ms},
            {"type": "candles", "symbol": inst_dash, "interval": bar, "startTime": start_ms, "endTime": now_ms},
            {"type": "candles", "symbol": coin_base, "interval": bar, "startTime": start_ms, "endTime": now_ms},
            {"type": "candleSnapshot", "req": {"coin": coin_base, "interval": bar, "startTime": start_ms, "endTime": now_ms}},
            {"type": "candleSnapshot", "req": {"coin": inst_dash, "interval": bar, "startTime": start_ms, "endTime": now_ms}},
            # n/limit-only snapshot
            {"type": "candleSnapshot", "coin": coin_base, "interval": bar, "n": limit},
            {"type": "candleSnapshot", "coin": inst_dash, "interval": bar, "n": limit},
        ]

        last_err = None
        for pth in post_paths:
            url = base + (("" if pth.startswith("/") else "/") + pth)
            for body in body_candidates:
                try:
                    _debug(f"POST {url} json={body}")
                    payload = _http_post_json(url, json=body, timeout=20)
                    rows = _parse_rows(payload)
                    if not rows:
                        # sometimes payload is nested, try common keys
                        lst = _extract_list(payload)
                        if lst:
                            rows = _parse_rows(lst)
                    if not rows:
                        last_err = RuntimeError("Unrecognized candle payload")
                        continue
                    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                    df.sort_values("time", inplace=True)
                    return df
                except Exception as e:
                    last_err = e
                    continue

        # as a last resort, try a GET on a few paths (some mirrors allow it)
        get_candidates = [
            "/candles",
            "/api/v1/candles",
            "/ohlcv",
            "/api/v1/ohlcv",
        ]
        params_list = [
            {"symbol": inst_dash, "interval": bar, "limit": limit},
            {"symbol": coin_base, "interval": bar, "limit": limit},
            {"instId": inst_dash, "interval": bar, "limit": limit},
        ]
        for pth in get_candidates:
            url = base + (("" if pth.startswith("/") else "/") + pth)
            for params in params_list:
                try:
                    _debug(f"GET {url} params={params}")
                    payload = _http_get_json(url, params=params, timeout=15)
                    rows = _parse_rows(payload)
                    if not rows:
                        continue
                    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                    df.sort_values("time", inplace=True)
                    return df
                except Exception as e:
                    last_err = e
                    continue

        raise last_err or RuntimeError("Failed to fetch Hyperliquid candles (POST/GET)")

    def fetch_funding_rate(self, symbol: str):
        return None


# ===================== Auto-markets =====================

def _norm_symbol_from_inst(inst: dict):
    inst_id = inst.get("instId") or inst.get("symbol") or inst.get("instrumentId")
    if not inst_id:
        return None
    s = str(inst_id).replace("_", "-").upper()
    if "-" in s:
        base, _, quote = s.partition("-")
        return f"{base}/{quote}"
    if s.endswith("USD"):
        return f"{s[:-3]}/USD"
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT"
    return None

def list_hl_symbols(inst_type="SWAP", want_quote="USD"):
    base = HL_REST_BASE.rstrip("/")
    url  = base + HL_INSTRUMENTS
    try:
        payload = _http_get_json(url, params={})
        items = _extract_list(payload) or []
        out = []
        for inst in items:
            sym = _norm_symbol_from_inst(inst)
            if not sym:
                continue
            quote = sym.split("/")[-1].upper()
            if want_quote and quote != want_quote.upper():
                continue
            out.append(sym)
        out = sorted(set(out))
        _debug(f"instruments -> {len(out)} symbols ({want_quote})")
        return out
    except Exception as e:
        _debug(f"instruments error: {e}")
        return []

def top_by_volume(symbols, inst_type="SWAP", want_quote="USD", top_n=12, min_vol=0.0):
    if not symbols:
        return []
    base = HL_REST_BASE.rstrip("/")
    url  = base + HL_TICKERS

    vols = {}
    try:
        payload = _http_get_json(url, params={})
        items = _extract_list(payload) or []
        for t in items:
            inst_id = t.get("instId") or t.get("symbol") or t.get("instrumentId")
            sym = _norm_symbol_from_inst({"instId": inst_id}) if inst_id else None
            if not sym or sym not in symbols:
                continue
            q = sym.split("/")[-1].upper()
            if want_quote and q != want_quote.upper():
                continue
            qv = t.get("volUsd") or t.get("quoteVolume") or t.get("vol24hQuote") or t.get("volUsd24h") or 0
            try:
                qv = float(qv)
            except Exception:
                qv = 0.0
            vols[sym] = max(vols.get(sym, 0.0), qv)
    except Exception as e:
        _debug(f"tickers/volume error: {e}")

    if vols:
        scored = [(s, vols.get(s, 0.0)) for s in symbols]
        if min_vol and min_vol > 0:
            scored = [x for x in scored if x[1] >= min_vol]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [s for s, _ in (scored[:top_n] if top_n else scored)]

    return symbols[:top_n] if top_n else symbols
