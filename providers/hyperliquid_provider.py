import os
import time
import pandas as pd
from .base import BaseProvider

HL_REST_BASE = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz").rstrip("/")

TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","8h":"8h","12h":"12h",
    "1d":"1d","3d":"3d","1w":"1w","1M":"1M"
}

def _debug(msg: str):
    if os.getenv("DEBUG", "").strip().lower() in ("1","true","yes","on"):
        try:
            from discord_sender import send_info
            send_info(f"[HL] {msg}")
        except Exception:
            print(f"[HL] {msg}")

def _secs_per_bar(bar: str) -> int:
    try:
        n, u = int(bar[:-1]), bar[-1]
        if u == "m": return n * 60
        if u == "h": return n * 3600
        if u == "d": return n * 86400
        if u == "w": return n * 86400 * 7
        if u == "M": return n * 86400 * 30
        return 60
    except Exception:
        return 60

def _http_post(url, body, timeout=25):
    import httpx
    return httpx.post(url, json=body, timeout=timeout)

def _to_ms(x):
    ts = int(float(x))
    if ts < 10_000_000_000:
        ts *= 1000
    return ts

def _parse_rows(payload):
    data = payload
    if isinstance(payload, dict):
        for k in ("data","result","rows","list","candles","klines","items"):
            v = payload.get(k)
            if isinstance(v, list):
                data = v
                break
        else:
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

def _force_usd(sym: str) -> str:
    base, sep, quote = sym.upper().partition("/")
    return f"{base}/USD" if sep and quote != "USD" else sym.upper()

# -------- available coin cache via /info allMids --------
_AVAILABLE_COINS = None
_AVAILABLE_TS = 0

def list_available_coins() -> set:
    """Cached set of HL perp coin bases (BTC, ETH, …)."""
    global _AVAILABLE_COINS, _AVAILABLE_TS
    now = time.time()
    if _AVAILABLE_COINS is not None and (now - _AVAILABLE_TS) < 600:
        return _AVAILABLE_COINS

    body = {"type": "allMids"}
    try:
        r = _http_post(f"{HL_REST_BASE}/info", body, timeout=20)
        if r.status_code >= 400:
            _debug(f"allMids {r.status_code}: {r.text[:200]}")
            return _AVAILABLE_COINS or set()
        data = r.json()
        if isinstance(data, dict):
            _AVAILABLE_COINS = set(data.keys())
            _AVAILABLE_TS = now
            _debug(f"available coins: {len(_AVAILABLE_COINS)}")
            return _AVAILABLE_COINS
    except Exception as e:
        _debug(f"allMids error: {e}")
    return _AVAILABLE_COINS or set()

# ------------ Provider ------------
class HyperliquidProvider(BaseProvider):
    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        return self._markets

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """
        Minimal stable fetch:
          POST /info  {"type":"candleSnapshot","req":{"coin":BASE,"interval":TF,"startTime":ms,"endTime":ms}}
        Retries gently; shrinks window if the node complains.
        """
        symbol   = _force_usd(symbol)
        base     = symbol.split("/")[0]
        coins    = list_available_coins()
        if coins and base not in coins:
            raise ValueError(f"hyperliquid does not list coin '{base}' (skip)")

        interval = TF_MAP.get(timeframe, timeframe)
        now_ms   = int(time.time() * 1000)
        spb_ms   = _secs_per_bar(interval) * 1000
        start_ms = now_ms - (limit + 5) * spb_ms

        url = HL_REST_BASE + "/info"

        def body(ms_from, ms_to):
            return {
                "type": "candleSnapshot",
                "req": {
                    "coin": base,
                    "interval": interval,
                    "startTime": int(ms_from),
                    "endTime": int(ms_to)
                }
            }

        backoff = 0.6
        last_err = None
        ms_from = start_ms
        ms_to   = now_ms

        for _ in range(8):
            try:
                b = body(ms_from, ms_to)
                _debug(f"POST {url} json={b}")
                r = _http_post(url, b, timeout=25)

                if r.status_code in (429, 500, 502, 503, 504):
                    _debug(f"{r.status_code} server: {r.text[:200] if hasattr(r,'text') else ''}")
                    time.sleep(backoff)
                    backoff = min(backoff * 1.8, 6.0)
                    continue

                if r.status_code >= 400:
                    last_err = RuntimeError(f"{r.status_code} {r.reason_phrase}: {r.text[:200]}")
                    _debug(str(last_err))
                    # shrink window ~25% from the left and retry
                    ms_from = int(ms_from + 0.25 * (ms_to - ms_from))
                    continue

                payload = r.json()
                rows = _parse_rows(payload)
                if not rows:
                    last_err = RuntimeError("unrecognized_payload")
                    _debug("unrecognized_payload; shrinking window")
                    ms_from = int(ms_from + 0.25 * (ms_to - ms_from))
                    continue

                df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                df.sort_values("time", inplace=True)
                if len(df) > limit:
                    df = df.iloc[-limit:]
                return df

            except Exception as e:
                last_err = e
                _debug(f"exception: {e}")
                time.sleep(0.3)

        raise last_err or RuntimeError("Failed to fetch Hyperliquid candles")

    def fetch_funding_rate(self, symbol: str):
        return None
