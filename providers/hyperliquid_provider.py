import os
import time
import pandas as pd
from typing import Any, Dict, Optional, List, Tuple
from .base import BaseProvider

HL_REST_BASE = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz").rstrip("/")
HL_INFO_PATH = os.getenv("HL_INFO", "/info")
HL_FORCE_QUOTE = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()

TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","8h":"8h","12h":"12h",
    "1d":"1d","3d":"3d","1w":"1w","1M":"1M"
}

def _debug_on() -> bool:
    return str(os.getenv("DEBUG","")).lower() in ("1","true","yes","on")

def _debug(msg: str):
    if _debug_on():
        try:
            from discord_sender import send_info  # optional
            send_info(f"[HL] {msg}")
        except Exception:
            print(f"[HL] {msg}")

def _http_post_json(url: str, body: Dict[str, Any], timeout: float = 25.0) -> Any:
    import httpx
    backoff = 0.6
    last = None
    for _ in range(6):
        try:
            r = httpx.post(url, json=body, timeout=timeout)
            if r.status_code in (429,500,502,503,504):
                _debug(f"{r.status_code} transient: {r.text[:200]}")
                time.sleep(backoff); backoff = min(backoff*1.8, 6.0)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            _debug(f"POST error: {e}")
            time.sleep(0.3)
    raise last or RuntimeError("POST failed")

def _secs_per_bar(bar: str) -> int:
    try:
        n, u = int(bar[:-1]), bar[-1]
        return n * (60 if u=="m" else 3600 if u=="h" else 86400 if u=="d" else 604800 if u=="w" else 2592000)
    except Exception:
        return 60

def _to_ms(x: Any) -> int:
    t = int(float(x))
    return t*1000 if t < 10_000_000_000 else t

def _parse_rows(payload: Any) -> List[List[float]]:
    # Accept list-of-lists or list-of-dicts, or dict-of-arrays
    data = payload
    if isinstance(payload, dict):
        for k in ("data","result","rows","list","candles","klines","items"):
            v = payload.get(k)
            if isinstance(v, list):
                data = v
                break
    rows: List[List[float]] = []
    if isinstance(data, list) and data:
        f = data[0]
        if isinstance(f, (list, tuple)) and len(f) >= 6:
            for x in data:
                rows.append([_to_ms(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
            return rows
        if isinstance(f, dict):
            ks_short = all(k in f for k in ("t","o","h","l","c","v"))
            ks_long  = all(k in f for k in ("time","open","high","low","close","volume"))
            if ks_short or ks_long:
                for x in data:
                    ts = _to_ms(x["t"] if ks_short else x["time"])
                    o  = float(x["o"] if ks_short else x["open"])
                    h  = float(x["h"] if ks_short else x["high"])
                    l  = float(x["l"] if ks_short else x["low"])
                    c  = float(x["c"] if ks_short else x["close"])
                    v  = float(x["v"] if ks_short else x["volume"])
                    rows.append([ts,o,h,l,c,v])
                return rows
            # dict-of-arrays in a list
            d = f
            ks_short = all(k in d for k in ("t","o","h","l","c","v"))
            ks_long  = all(k in d for k in ("time","open","high","low","close","volume"))
            if ks_short or ks_long:
                T = d["t"] if ks_short else d["time"]
                O = d["o"] if ks_short else d["open"]
                H = d["h"] if ks_short else d["high"]
                L = d["l"] if ks_short else d["low"]
                C = d["c"] if ks_short else d["close"]
                V = d["v"] if ks_short else d["volume"]
                n = min(len(T),len(O),len(H),len(L),len(C),len(V))
                for i in range(n):
                    rows.append([_to_ms(T[i]), float(O[i]), float(H[i]), float(L[i]), float(C[i]), float(V[i])])
                return rows
    raise ValueError("unrecognized_payload")

# -------- autodiscover coin bases from /info --------
_BASES_CACHE: Optional[set] = None
_BASES_TS = 0.0

def _discover_bases() -> set:
    global _BASES_CACHE, _BASES_TS
    now = time.time()
    if _BASES_CACHE is not None and (now - _BASES_TS) < 600:
        return _BASES_CACHE
    url = f"{HL_REST_BASE}{HL_INFO_PATH}"
    # Try GET, then POST {}
    js = None
    try:
        import httpx
        r = httpx.get(url, timeout=8)
        if r.status_code == 405:
            r = httpx.post(url, json={}, timeout=8)
        r.raise_for_status()
        js = r.json()
    except Exception as e:
        _debug(f"/info discovery fallback: {e}")
    bases = set()
    if isinstance(js, dict):
        for key in ("universe","coins","tickers","mids","allMids","symbols"):
            arr = js.get(key)
            if isinstance(arr, list):
                for x in arr:
                    s = x if isinstance(x, str) else (x.get("symbol") or x.get("instId") or x.get("name") or "")
                    s = str(s).replace("_","-").upper()
                    if "-" in s:
                        bases.add(s.split("-")[0])
                    else:
                        for q in ("USD","USDT","USDC"):
                            if s.endswith(q):
                                bases.add(s[:-len(q)])
                                break
    if not bases:
        # seed with a conservative list so we still run
        bases = {
            "BTC","ETH","SOL","BNB","LINK","AVAX","AAVE","ADA","DOGE","XRP",
            "TAO","SNX","FARTCOIN","PAXG","SUI","MNT","BIO","STBL","ZRO","ZORA",
            "NEAR","APEX","CRV","TIA","ARB","ETHFI","FET","LDO","SEI","ZEREBRO","KFLOKI",
        }
    _BASES_CACHE, _BASES_TS = bases, now
    _debug(f"Discovered {len(bases)} bases")
    return bases

def _is_supported(symbol: str) -> bool:
    base, quote = symbol.split("/")
    return quote.upper() == HL_FORCE_QUOTE and base.upper() in _discover_bases()

def _force_usd(sym: str) -> str:
    b,q = sym.upper().split("/")
    return f"{b}/{HL_FORCE_QUOTE}"

def _inst_id(sym: str) -> str:
    b,q = sym.upper().split("/")
    return f"{b}-{q}"

class HyperliquidProvider(BaseProvider):
    def __init__(self):
        try: _discover_bases()
        except Exception as e: _debug(f"discover on init: {e}")

    def load_markets(self):
        return {f"{b}/{HL_FORCE_QUOTE}":{"active":True} for b in sorted(_discover_bases())}

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        symbol = _force_usd(symbol)
        if not _is_supported(symbol):
            raise ValueError(f"hyperliquid does not list coin '{symbol.split('/')[0]}' (skip)")

        base = symbol.split("/")[0]
        interval = TF_MAP.get(timeframe, timeframe)
        now_ms = int(time.time()*1000)
        spb_ms = _secs_per_bar(interval)*1000
        start_ms = now_ms - (limit + 5) * spb_ms

        url = HL_REST_BASE + HL_INFO_PATH
        body = {"type":"candleSnapshot","req":{"coin":base,"interval":interval,"startTime":start_ms,"endTime":now_ms}}

        backoff = 0.6
        last = None
        for _ in range(8):
            try:
                _debug(f"POST {url} json={body}")
                js = _http_post_json(url, body, timeout=25)
                rows = _parse_rows(js)
                if not rows:
                    raise ValueError("empty_rows")
                df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                df.sort_values("time", inplace=True)
                if len(df) > limit:
                    df = df.iloc[-limit:]
                return df
            except Exception as e:
                last = e
                _debug(f"snapshot error: {e}; shrinking window")
                start_ms = int(start_ms + 0.25*(now_ms - start_ms))
                body["req"]["startTime"] = start_ms
                time.sleep(backoff)
                backoff = min(backoff*1.5, 6.0)
        raise last or RuntimeError("Failed to fetch HL candles via /info")

    def fetch_funding_rate(self, symbol: str):
        return None
