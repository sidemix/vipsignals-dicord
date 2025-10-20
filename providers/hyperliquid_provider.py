# providers/hyperliquid_provider.py
import os, time, math
import pandas as pd
from .base import BaseProvider

try:
    import httpx
except Exception:
    httpx = None

HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz")
HL_INSTRUMENTS = os.getenv("HL_INSTRUMENTS", "/api/v1/public/instruments")
HL_TICKERS     = os.getenv("HL_TICKERS", "/api/v1/public/tickers")

# ---- rate limit knobs (tune via env if needed) ----
# minimum delay between POSTs (seconds)
HL_MIN_INTERVAL = float(os.getenv("HL_MIN_INTERVAL", "0.25"))  # ~4 req/s
# max retries on 429 and transient errors
HL_MAX_RETRIES  = int(os.getenv("HL_MAX_RETRIES", "3"))

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

def _to_ms(x):
    ts = int(float(x))
    if ts < 10_000_000_000:
        ts *= 1000
    return ts

def _extract_list(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("data","result","rows","list","candles","klines","kline","items"):
            v = obj.get(k)
            if isinstance(v, list):
                return v
    return None

def _parse_rows(payload):
    data = payload
    rows = None
    if isinstance(payload, dict):
        maybe = _extract_list(payload)
        if maybe is None:
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
    return rows if rows else None

def _force_quote(sym: str) -> str:
    target = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()
    base, sep, quote = sym.upper().partition("/")
    if sep and target and quote != target:
        return f"{base}/{target}"
    return sym.upper()

def _interval_to_seconds(bar: str) -> int:
    u = bar[-1]
    n = int(bar[:-1])
    return n * (60 if u=="m" else 3600 if u=="h" else 86400)

class HyperliquidProvider(BaseProvider):
    _client = None
    _last_post = 0.0

    @classmethod
    def _get_client(cls):
        if cls._client is None:
            if httpx is None:
                raise RuntimeError("httpx not installed")
            cls._client = httpx.Client(
                base_url=HL_REST_BASE,
                timeout=20.0,
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=8),
                headers={"Accept": "application/json"}
            )
        return cls._client

    @classmethod
    def _rate_limit(cls):
        now = time.monotonic()
        wait = HL_MIN_INTERVAL - (now - cls._last_post)
        if wait > 0:
            time.sleep(wait)
        cls._last_post = time.monotonic()

    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        return self._markets

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        symbol = _force_quote(symbol)
        coin   = symbol.split("/")[0]
        bar    = TF_MAP.get(timeframe, timeframe)

        # Prefer bounded-by-count query; HL supports candle snapshots via POST /info
        body_candidates = [
            {"type": "candleSnapshot", "coin": coin, "interval": bar, "n": limit},
            # time-bounded variants as fallback
            self._time_bounded_body(coin, bar, limit),
        ]

        client = self._get_client()
        last_err = None
        for body in body_candidates:
            for attempt in range(1, HL_MAX_RETRIES + 1):
                try:
                    self._rate_limit()
                    _debug(f"POST /info json={body} (attempt {attempt})")
                    r = client.post("/info", json=body)
                    if r.status_code == 429:
                        retry = float(r.headers.get("Retry-After", 0)) or min(2.0 * attempt, 6.0)
                        _debug(f"429 rate limited; sleeping {retry:.2f}s")
                        time.sleep(retry)
                        continue
                    r.raise_for_status()
                    payload = r.json()
                    # payload is sometimes directly a list, sometimes {'data': [...]}
                    rows = _parse_rows(payload) or _parse_rows(_extract_list(payload) or [])
                    if not rows:
                        raise RuntimeError("Unrecognized candle payload")
                    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                    df.sort_values("time", inplace=True)
                    return df
                except Exception as e:
                    last_err = e
                    backoff = 0.4 * attempt
                    _debug(f"candle fetch failed: {e}; backoff {backoff:.2f}s")
                    time.sleep(backoff)
                    continue

        raise last_err or RuntimeError("Failed to fetch Hyperliquid candles via POST /info")

    def _time_bounded_body(self, coin: str, bar: str, limit: int):
        now_ms = int(time.time() * 1000)
        span   = (limit + 5) * _interval_to_seconds(bar) * 1000
        return {"type": "candleSnapshot", "coin": coin, "interval": bar,
                "startTime": now_ms - span, "endTime": now_ms}

    def fetch_funding_rate(self, symbol: str):
        return None


# -------- auto-symbol helpers (same as before) --------
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
    try:
        client = HyperliquidProvider._get_client()
        r = client.get(HL_INSTRUMENTS)
        r.raise_for_status()
        items = _extract_list(r.json()) or []
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
    vols = {}
    try:
        client = HyperliquidProvider._get_client()
        r = client.get(HL_TICKERS)
        r.raise_for_status()
        items = _extract_list(r.json()) or []
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
