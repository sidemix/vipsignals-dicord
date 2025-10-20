# providers/hyperliquid_provider.py

import os
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .base import BaseProvider


# -------------------- Config (overridable via env) --------------------

HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz").rstrip("/")
HL_KLINES_PATH = os.getenv("HL_KLINES", "/api/v1/ohlcv")          # expects instId, interval, limit
HL_INFO_PATH   = os.getenv("HL_INFO",   "/info")                  # for autodiscovery
HL_FORCE_QUOTE = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()

# Map our timeframe strings -> API values (adjustable via env if you need)
_DEFAULT_TF_MAP = {
    "1m":  "1m",
    "3m":  "3m",
    "5m":  "5m",
    "15m": "15m",
    "30m": "30m",
    "1h":  "1h",
    "4h":  "4h",
    "6h":  "6h",
    "12h": "12h",
    "1d":  "1d",
}
TF_MAP = {**_DEFAULT_TF_MAP}


# -------------------- Debug helper --------------------

def _debug_enabled() -> bool:
    return str(os.getenv("DEBUG", "")).strip().lower() in ("1", "true", "yes", "on")

def _debug(msg: str) -> None:
    if _debug_enabled():
        try:
            # Optional pretty log to Discord if your sender is available
            from discord_sender import send_info  # type: ignore
            send_info(f"[HL] {msg}")
        except Exception:
            print(f"[HL] {msg}")


# -------------------- Small HTTP helpers --------------------

def _http_request_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Any:
    import httpx

    # A tiny retry loop for transient 429/5xx
    backoff = 0.4
    for attempt in range(4):
        try:
            if method.upper() == "GET":
                r = httpx.get(url, params=params, timeout=timeout)
            else:
                r = httpx.post(url, params=params, json=json, timeout=timeout)
            # Some HL nodes answer 405 for GET /info; we’ll handle at the callsite.
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt >= 3:
                raise
            time.sleep(backoff)
            backoff *= 1.6


# -------------------- Symbol normalization --------------------

def _to_inst_id(symbol: str) -> str:
    """
    'BTC/USD' -> 'BTC-USD'
    """
    base, quote = symbol.split("/")
    return f"{base.upper()}-{quote.upper()}"

def _split_base_quote(symbol: str) -> Tuple[str, str]:
    base, quote = symbol.split("/")
    return base.upper(), quote.upper()


# -------------------- Autodiscovery (Option B) --------------------

# Cached set of supported bases (e.g., {"BTC","ETH","SOL", ...})
_HL_BASES_CACHE: Optional[set] = None

def _fetch_supported_bases() -> set:
    """
    Hit HL /info (GET or POST {}) and try to derive the universe of bases.
    Falls back to a sane seed set if the node returns minimal info.
    """
    global _HL_BASES_CACHE
    if _HL_BASES_CACHE is not None:
        return _HL_BASES_CACHE

    url = f"{HL_REST_BASE}{HL_INFO_PATH}"
    bases: set = set()

    # Try GET first, then POST {} if needed (some nodes require POST).
    try:
        js = _http_request_json("GET", url)
    except Exception:
        try:
            js = _http_request_json("POST", url, json={})
        except Exception as e:
            _debug(f"/info unavailable ({e}); using fallback base list")

            # A conservative fallback list (extend as you like)
            _HL_BASES_CACHE = {
                "BTC","ETH","SOL","BNB","LINK","AVAX","AAVE","ADA","DOGE","XRP",
                "TAO","SNX","FARTCOIN","PAXG","SUI","MNT","BIO","STBL","ZRO","ZORA",
                "NEAR","APEX","CRV","TIA","ARB","ETHFI","FET","LDO","SEI","ZEREBRO",
                "KFLOKI","DOT","WLD","TRUMP","TON","TRX","UNI","JUP","OP","BRETT","INJ",
            }
            return _HL_BASES_CACHE

    # Try to extract base symbols from several common shapes:
    for key in ("universe", "coins", "tickers", "mids", "allMids", "symbols"):
        arr = js.get(key)
        if isinstance(arr, list):
            for x in arr:
                if isinstance(x, str):
                    s = x
                elif isinstance(x, dict):
                    s = (x.get("symbol") or x.get("instId") or x.get("name") or x.get("id") or "")
                else:
                    continue
                s = str(s)
                # Normalize like 'BTC-USD' / 'BTC_USD' / 'BTCUSD'
                s_up = s.replace("_", "-").upper()
                if "-" in s_up:
                    base = s_up.split("-")[0]
                else:
                    # Split BTCUSD if it ends with USD/USDT/etc.
                    for q in ("USD", "USDT", "USDC"):
                        if s_up.endswith(q):
                            base = s_up[:-len(q)]
                            break
                    else:
                        base = s_up
                if base:
                    bases.add(base)

    if not bases:
        _debug("No bases discovered from /info; using fallback list.")
        bases = {
            "BTC","ETH","SOL","BNB","LINK","AVAX","AAVE","ADA","DOGE","XRP",
            "TAO","SNX","FARTCOIN","PAXG","SUI","MNT","BIO","STBL","ZRO","ZORA",
            "NEAR","APEX","CRV","TIA","ARB","ETHFI","FET","LDO","SEI","ZEREBRO",
            "KFLOKI","DOT","WLD","TRUMP","TON","TRX","UNI","JUP","OP","BRETT","INJ",
        }

    _HL_BASES_CACHE = bases
    _debug(f"Discovered {len(bases)} bases from /info")
    return bases

def _is_supported(symbol: str) -> bool:
    base, quote = _split_base_quote(symbol)
    if HL_FORCE_QUOTE and quote.upper() != HL_FORCE_QUOTE:
        return False
    return base in _fetch_supported_bases()


# -------------------- Parsing helpers --------------------

def _rows_from_ohlcv_payload(js: Any) -> List[List[float]]:
    """
    Accept common shapes:
      - [[ts, o, h, l, c, v], ...]
      - {"data":[...]} / {"result":[...]} / {"rows":[...]} / {"candles":[...]}
      - [{"t":..,"o":..,"h":..,"l":..,"c":..,"v":..}, ...]
    Returns rows with ts in **milliseconds**.
    """
    def _to_ms(x: Any) -> int:
        t = int(float(x))
        return t * 1000 if t < 10_000_000_000 else t

    # Extract list container if wrapped
    data = js
    if isinstance(js, dict):
        for key in ("data", "result", "rows", "candles", "kline", "klines", "list", "items"):
            if isinstance(js.get(key), list):
                data = js[key]
                break

    rows: List[List[float]] = []

    if isinstance(data, list) and data:
        first = data[0]
        # List-of-lists
        if isinstance(first, (list, tuple)) and len(first) >= 6:
            for x in data:
                ts = _to_ms(x[0])
                rows.append([ts, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
            return rows
        # List-of-dicts
        if isinstance(first, dict):
            # Either per-row dicts or dict-of-arrays; handle both
            keys_short = all(k in first for k in ("t","o","h","l","c","v"))
            keys_long  = all(k in first for k in ("time","open","high","low","close","volume"))
            if keys_short or keys_long:
                for x in data:
                    ts = _to_ms(x["t"] if keys_short else x["time"])
                    o  = float(x["o"] if keys_short else x["open"])
                    h  = float(x["h"] if keys_short else x["high"])
                    l  = float(x["l"] if keys_short else x["low"])
                    c  = float(x["c"] if keys_short else x["close"])
                    v  = float(x["v"] if keys_short else x["volume"])
                    rows.append([ts, o, h, l, c, v])
                return rows

            # dict-of-arrays inside a 1-element list
            d = first
            keys_short = all(k in d for k in ("t","o","h","l","c","v"))
            keys_long  = all(k in d for k in ("time","open","high","low","close","volume"))
            if keys_short or keys_long:
                t = d["t"] if keys_short else d["time"]
                o = d["o"] if keys_short else d["open"]
                h = d["h"] if keys_short else d["high"]
                l = d["l"] if keys_short else d["low"]
                c = d["c"] if keys_short else d["close"]
                v = d["v"] if keys_short else d["volume"]
                n = min(len(t), len(o), len(h), len(l), len(c), len(v))
                for i in range(n):
                    rows.append([_to_ms(t[i]), float(o[i]), float(h[i]), float(l[i]), float(c[i]), float(v[i])])
                return rows

    raise ValueError("Unrecognized OHLCV payload shape")


# -------------------- Provider --------------------

class HyperliquidProvider(BaseProvider):
    """
    Hyperliquid public data provider.
    - Auto-discovers supported bases from /info.
    - fetch_ohlcv_df(symbol, timeframe, limit) -> pandas DataFrame
    """

    def __init__(self) -> None:
        # Warm the cache so we can log once on startup
        try:
            _fetch_supported_bases()
        except Exception as e:
            _debug(f"Discovery failed (continuing with fallback): {e}")

    # Optional; not strictly used by the scanner’s startup
    def load_markets(self) -> Dict[str, Dict[str, Any]]:
        bases = _fetch_supported_bases()
        return {f"{b}/{HL_FORCE_QUOTE}": {"active": True} for b in sorted(bases)}

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        if not _is_supported(symbol):
            # Keep message terse; main loop already wraps errors
            raise ValueError(f"hyperliquid does not list coin '{symbol.split('/')[0]}' (skip)")

        inst   = _to_inst_id(symbol)                 # e.g., BTC-USD
        bar    = TF_MAP.get(timeframe, timeframe)    # '5m' etc.
        url    = f"{HL_REST_BASE}{HL_KLINES_PATH}"

        # Prefer POST (most nodes accept it); fallback: GET with params
        payload = {"instId": inst, "interval": bar, "limit": int(limit)}
        js: Any

        try:
            js = _http_request_json("POST", url, json=payload)
        except Exception as e_post:
            _debug(f"POST {url} failed ({e_post}); trying GET")
            params = {"instId": inst, "interval": bar, "limit": int(limit)}
            js = _http_request_json("GET", url, params=params)

        rows = _rows_from_ohlcv_payload(js)

        df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.sort_values("time", inplace=True)
        return df

    def fetch_funding_rate(self, symbol: str):
        # Not implemented for this bot; return None so the scanner doesn’t block.
        return None


# -------------------- Optional helpers the app may import --------------------

def list_hl_symbols(inst_type: str = "SWAP", want_quote: str = "USD") -> List[str]:
    """
    Return discovered symbols in 'BASE/QUOTE' form.
    (inst_type is ignored for now but kept for parity with other providers.)
    """
    want_quote = (want_quote or HL_FORCE_QUOTE or "USD").upper()
    return [f"{b}/{want_quote}" for b in sorted(_fetch_supported_bases())]

def top_by_volume(
    symbols: Sequence[str],
    inst_type: str = "SWAP",
    want_quote: str = "USD",
    top_n: int = 12,
    min_vol: float = 0.0,
) -> List[str]:
    """
    Simple passthrough for now: we don’t have a stable, public
    volume endpoint across all nodes. Keep order, optionally cap.
    """
    if not symbols:
        return []
    out = [s for s in symbols if _is_supported(s)]
    if top_n and top_n > 0:
        out = out[:top_n]
    return out
