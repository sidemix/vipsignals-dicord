# providers/hyperliquid_provider.py
import os
import pandas as pd

from .base import BaseProvider

HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz")
HL_KLINES      = os.getenv("HL_KLINES", "")  # optional override; leave blank to let the provider try several
HL_INSTRUMENTS = os.getenv("HL_INSTRUMENTS", "/api/v1/public/instruments")
HL_TICKERS     = os.getenv("HL_TICKERS", "/api/v1/public/tickers")

# Map TF strings → common API values
DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","12h":"12h",
    "1d":"1d"
}
TF_MAP = {**DEFAULT_TF_MAP}

def _debug(msg: str):
    if os.getenv("DEBUG", "").strip().lower() in ("1","true","yes","on"):
        try:
            from discord_sender import send_info   # avoid hard dep on import
            send_info(f"[HL] {msg}")
        except Exception:
            pass

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
    Try to normalize several possible shapes to rows of:
    [time_ms, open, high, low, close, volume]
    """
    data = payload
    rows = None

    # If dict → try to find a list first
    if isinstance(payload, dict):
        maybe = _extract_list(payload)
        if maybe is None:
            # dict-of-arrays fallback: {t/o/h/l/c/v: [...]}
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
        if isinstance(data[0], dict):
            for x in data:
                ts  = _to_ms(x.get("ts") or x.get("time") or x.get("t"))
                op  = float(x.get("open")  or x.get("o"))
                hi  = float(x.get("high")  or x.get("h"))
                lo  = float(x.get("low")   or x.get("l"))
                cl  = float(x.get("close") or x.get("c"))
                vol = float(x.get("volume")or x.get("v"))
                rows.append([ts, op, hi, lo, cl, vol])
        elif isinstance(data[0], (list, tuple)):
            for x in data:
                ts = _to_ms(x[0])
                rows.append([ts, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
    return rows if rows else None

def _force_quote(sym: str) -> str:
    # Hyperliquid perps quote in USD, not USDT
    target = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()
    base, sep, quote = sym.upper().partition("/")
    if sep and target and quote != target:
        return f"{base}/{target}"
    return sym.upper()

class HyperliquidProvider(BaseProvider):
    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        # optional: you can populate this by querying instruments
        return self._markets

    # ---------- OHLCV ----------
    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        import httpx

        symbol = _force_quote(symbol)          # BTC/USDT -> BTC/USD
        pair_dash = symbol.replace("/", "-")   # BTC/USD  -> BTC-USD
        pair_cat  = symbol.replace("/", "")    # BTC/USD  -> BTCUSD
        bar       = TF_MAP.get(timeframe, timeframe)

        base = HL_REST_BASE.rstrip("/")

        # If HL_KLINES is explicitly set, try that path first; otherwise we try a robust list
        path_candidates = []
        if HL_KLINES.strip():
            path_candidates.append(HL_KLINES.strip())
        path_candidates += [
            "/api/v1/market/candles",  # what we tried first
            "/api/v1/candles",
            "/candles",
            "/ohlcv",
            "/api/v1/ohlcv",
            "/public/candles",
            "/info/candles",           # some APIs tuck candles under /info
            "/info"                    # a few return candles when given params
        ]

        # Try multiple param spellings
        param_candidates = [
            {"instId": pair_dash, "bar": bar, "limit": limit},
            {"symbol": pair_dash, "bar": bar, "limit": limit},
            {"symbol": pair_cat,  "bar": bar, "limit": limit},
            {"symbol": pair_dash, "interval": bar, "limit": limit},
            {"instId": pair_dash, "interval": bar, "limit": limit},
            {"symbol": pair_dash, "resolution": bar, "limit": limit},
            {"symbol": pair_dash, "granularity": bar, "limit": limit},
        ]

        last_err = None
        for pth in path_candidates:
            url = base + (("" if pth.startswith("/") else "/") + pth)
            for params in param_candidates:
                try:
                    _debug(f"GET {url} params={params}")
                    r = httpx.get(url, params=params, timeout=15)
                    if r.status_code >= 400:
                        last_err = RuntimeError(f"{r.status_code} {r.reason_phrase}")
                        continue
                    payload = r.json()
                    rows = _parse_rows(payload)
                    if not rows:
                        last_err = RuntimeError("Unrecognized kline payload shape")
                        continue
                    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                    df.sort_values("time", inplace=True)
                    return df
                except Exception as e:
                    last_err = e
                    continue

        raise last_err or RuntimeError("Failed to fetch Hyperliquid klines")

    # Funding hook (optional)
    def fetch_funding_rate(self, symbol: str):
        return None


# ===================== Auto-markets =====================

def _norm_symbol_from_inst(inst: dict) -> str | None:
    # Accept: BTC-USD / BTC_USD / BTCUSD
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
