# providers/hyperliquid_provider.py
import os
import time
import pandas as pd
from .base import BaseProvider

# ---------- Config (override via env if needed) ----------
HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz")
HL_INSTRUMENTS = os.getenv("HL_INSTRUMENTS", "/api/v1/public/instruments")
HL_TICKERS     = os.getenv("HL_TICKERS", "/api/v1/public/tickers")

# Map TF strings → API values Hyperliquid accepts (5m, 1h, 1d, etc.)
DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","12h":"12h",
    "1d":"1d"
}
TF_MAP = {**DEFAULT_TF_MAP}

# ---------- Tiny utils ----------
def _debug(msg: str):
    if os.getenv("DEBUG", "").strip().lower() in ("1","true","yes","on"):
        try:
            from discord_sender import send_info  # avoid hard dep at import time
            send_info(f"[HL] {msg}")
        except Exception:
            print(f"[HL] {msg}")

def _http_post(url, json, timeout=20):
    import httpx
    return httpx.post(url, json=json, timeout=timeout)

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
    if ts < 10_000_000_000:  # seconds → ms
        ts *= 1000
    return ts

def _parse_rows(payload):
    """
    Normalize various candle payloads to rows:
      [time_ms, open, high, low, close, volume]
    """
    data = payload
    rows = None

    if isinstance(payload, dict):
        maybe = _extract_list(payload)
        if maybe is None:
            # dict-of-arrays fallback: {t/o/h/l/c/v: [...] } or {time/open/...}
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
                return [[_to_ms(T[i]), float(O[i]), float(H[i]), float(L[i]), float(C[i]), float(V[i])] for i in range(n)]
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
        elif isinstance(data[0], (list, tuple)) and len(data[0]) >= 6:
            for x in data:
                ts = _to_ms(x[0])
                rows.append([ts, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
    return rows or None

def _force_quote(sym: str) -> str:
    # Hyperliquid perps quote in USD
    target = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()
    base, sep, quote = sym.upper().partition("/")
    return f"{base}/{target}" if sep and quote != target else sym.upper()

def _secs_per_bar(bar: str) -> int:
    try:
        n, u = int(bar[:-1]), bar[-1]
        return n * (60 if u == "m" else 3600 if u == "h" else 86400 if u == "d" else 60)
    except Exception:
        return 60

# ---------- Provider ----------
class HyperliquidProvider(BaseProvider):
    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        return self._markets

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """
        Fetch candles via POST /info with:
          {"type":"candleSnapshot","req":{"coin": <BASE>, "interval": <TF>, "startTime": <ms>, "endTime": <ms>}}
        Uses ms timestamps (required). Retries 429s with backoff.
        """
        symbol   = _force_quote(symbol)           # e.g., BTC/USDT -> BTC/USD
        coin     = symbol.split("/")[0]           # "BTC"
        interval = TF_MAP.get(timeframe, timeframe)
        url      = HL_REST_BASE.rstrip("/") + "/info"

        now_ms   = int(time.time() * 1000)
        spb_ms   = _secs_per_bar(interval) * 1000
        start_ms = now_ms - (limit + 5) * spb_ms  # a few extra bars for safety

        body = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_ms,
                "endTime": now_ms
            }
        }

        backoff = 0.6
        last_err = None
        for _ in range(6):  # retry a few times (handles 429s gracefully)
            try:
                _debug(f"POST {url} json={body}")
                r = _http_post(url, json=body, timeout=25)

                if r.status_code == 429:
                    _debug("429 rate limited; backing off…")
                    time.sleep(backoff)
                    backoff = min(backoff * 1.8, 6.0)
                    continue

                if r.status_code >= 400:
                    last_err = RuntimeError(f"{r.status_code} {r.reason_phrase}")
                    _debug(f"bad status: {last_err}")
                    continue

                payload = r.json()
                rows = _parse_rows(payload) or _parse_rows(_extract_list(payload) or {})
                if not rows:
                    last_err = RuntimeError("unrecognized_payload")
                    continue

                df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                df.sort_values("time", inplace=True)
                return df

            except Exception as e:
                last_err = e
                _debug(f"exception: {e}")
                time.sleep(0.25)
                continue

        raise last_err or RuntimeError("Failed to fetch Hyperliquid candles")

    def fetch_funding_rate(self, symbol: str):
        # Not implemented for HL in this bot; return None so the scanner doesn't block.
        return None


# ---------- Auto-markets helpers (optional) ----------
def _norm_symbol_from_inst(inst: dict):
    # Accept: "BTC-USD" / "BTC_USD" / "BTCUSD"
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
    """
    Tries to list instruments (if HL exposes it on your node).
    If it returns nothing, your bot will fall back to the SYMBOLS env.
    """
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
    """
    Attempts to rank by 24h quote volume (if HL exposes tickers on your node).
    Falls back to the first N symbols if no volume field is available.
    """
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
            # keep the max seen per symbol
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
