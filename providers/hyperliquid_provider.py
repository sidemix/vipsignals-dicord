# providers/hyperliquid_provider.py
import os
import time
import pandas as pd
from .base import BaseProvider

# ---------- Config ----------
HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz").rstrip("/")
HL_INSTRUMENTS = os.getenv("HL_INSTRUMENTS", "/api/v1/public/instruments")  # may not exist on all nodes
HL_TICKERS     = os.getenv("HL_TICKERS", "/api/v1/public/tickers")          # may not exist on all nodes
HL_ALT_BASE    = os.getenv("HL_ALT_BASE", "").strip()                       # optional second host

# Max bars per POST (HL returns up to 500 per time-ranged request)
HL_MAX_CHUNK   = int(os.getenv("HL_MAX_CHUNK", "200"))

# Map TF strings → HL intervals (see docs/websocket list)
# https://hyperliquid.gitbook.io/.../websocket/subscriptions
DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","8h":"8h","12h":"12h",
    "1d":"1d","3d":"3d","1w":"1w","1M":"1M"
}
TF_MAP = {**DEFAULT_TF_MAP}

def _debug(msg: str):
    if os.getenv("DEBUG", "").strip().lower() in ("1","true","yes","on"):
        try:
            from discord_sender import send_info
            send_info(f"[HL] {msg}")
        except Exception:
            print(f"[HL] {msg}")

def _http_post_raw(url, json, timeout=25):
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
    Normalize to rows: [time_ms, open, high, low, close, volume]
    Supports list of dicts, list of arrays, and dict-of-arrays.
    """
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
    return rows or None

def _force_quote(sym: str) -> str:
    # Hyperliquid perps quote in USD (we only need the base for 'coin' though)
    target = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()
    base, sep, quote = sym.upper().partition("/")
    return f"{base}/{target}" if sep and quote != target else sym.upper()

def _secs_per_bar(bar: str) -> int:
    try:
        n, u = int(bar[:-1]), bar[-1]
        return n * (60 if u == "m" else 3600 if u == "h" else 86400 if u == "d" else 60*60*24*30 if u == "M" else 60)
    except Exception:
        return 60

# ---------- Available coins cache ----------
_AVAILABLE_COINS = None
_AVAILABLE_TS = 0

def _refresh_available_coins(force=False):
    """
    Uses the official /info with {type:'allMids'} to get the current perp coin set.
    Caches for ~10 minutes.
    """
    global _AVAILABLE_COINS, _AVAILABLE_TS
    now = time.time()
    if _AVAILABLE_COINS is not None and not force and (now - _AVAILABLE_TS) < 600:
        return _AVAILABLE_COINS

    body = {"type": "allMids"}
    try:
        r = _http_post_raw(f"{HL_REST_BASE}/info", json=body, timeout=20)
        if r.status_code >= 400:
            _debug(f"allMids {r.status_code} {r.reason_phrase}: {r.text[:200]}")
            # keep previous cache if any
            return _AVAILABLE_COINS or set()
        data = r.json()
        # Response is a dict like {"BTC":"...", "ETH":"...", ...}
        if isinstance(data, dict):
            _AVAILABLE_COINS = set(data.keys())
            _AVAILABLE_TS = now
            _debug(f"available coins: {len(_AVAILABLE_COINS)}")
            return _AVAILABLE_COINS
    except Exception as e:
        _debug(f"allMids error: {e}")
    return _AVAILABLE_COINS or set()

class HyperliquidProvider(BaseProvider):
    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        return self._markets
# ---- expose coin list so main.py can filter SYMBOLS cleanly ----
def list_available_coins() -> set[str]:
    """
    Returns a cached set of available perp coin bases from /info {type:'allMids'}.
    """
    # reuse the cache we already maintain
    return _refresh_available_coins(force=False) or set()

    # ---- internal: one chunk fetch ----
    def _fetch_chunk(self, base_url: str, coin: str, interval: str, start_ms: int, end_ms: int):
        body = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": int(start_ms),
                "endTime": int(end_ms),
            }
        }
        backoff = 0.6
        last_err = None
        for _ in range(5):
            try:
                _debug(f"POST {base_url}/info json={body}")
                r = _http_post_raw(base_url + "/info", json=body, timeout=25)
                if r.status_code in (429, 500, 502, 503, 504):
                    # Log body to help diagnose and back off
                    try:
                        _debug(f"server_resp({r.status_code}): {r.text[:240]}")
                    except Exception:
                        pass
                    time.sleep(backoff)
                    backoff = min(backoff * 1.8, 6.0)
                    continue
                if r.status_code >= 400:
                    try:
                        _debug(f"{r.status_code} {r.reason_phrase} body={r.text[:240]}")
                    except Exception:
                        pass
                    return None, RuntimeError(f"{r.status_code} {r.reason_phrase}")

                payload = r.json()
                rows = _parse_rows(payload) or _parse_rows(_extract_list(payload) or {})
                if not rows:
                    return None, RuntimeError("unrecognized_payload")
                return rows, None
            except Exception as e:
                last_err = e
                time.sleep(0.3)
                continue
        return None, last_err or RuntimeError("chunk_fetch_failed")

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """
        Fetch up to `limit` bars by paging in chunks (HL_MAX_CHUNK).
        Validates coin first via allMids to avoid 500s on unknown names.
        """
        symbol   = _force_quote(symbol)            # e.g., BTC/USDT -> BTC/USD
        coin     = symbol.split("/")[0]            # "BTC"
        interval = TF_MAP.get(timeframe, timeframe)

        # Validate coin against HL perp list
        avail = _refresh_available_coins()
        if avail and coin not in avail:
            # Skip cleanly; caller will just not get a signal for this one
            raise ValueError(f"hyperliquid does not list coin '{coin}' (skip)")

        spb_ms   = _secs_per_bar(interval) * 1000
        needed   = int(limit)
        now_ms   = int(time.time() * 1000)
        end_ms   = now_ms
        out_rows = []

        bases = [HL_REST_BASE]
        if HL_ALT_BASE:
            bases.append(HL_ALT_BASE.rstrip("/"))

        while needed > 0:
            chunk_n   = min(HL_MAX_CHUNK, needed)
            # Request window (pad 2 bars for safety)
            start_ms  = end_ms - (chunk_n + 2) * spb_ms
            got_rows  = None
            last_err  = None

            for base in bases:
                rows, err = self._fetch_chunk(base, coin, interval, start_ms, end_ms)
                if rows:
                    got_rows = rows
                    break
                last_err = err

            if not got_rows:
                # As a last try, shrink the window by half once
                sh_start = end_ms - (max(20, chunk_n // 2) + 2) * spb_ms
                for base in bases:
                    rows, err = self._fetch_chunk(base, coin, interval, sh_start, end_ms)
                    if rows:
                        got_rows = rows
                        break
                    last_err = err

            if not got_rows:
                # Give up on this symbol for now; bubble up (main loop will catch & continue)
                raise last_err or RuntimeError("chunk_fetch_failed")

            out_rows.extend(got_rows)

            # Move window back using earliest timestamp we received
            earliest_ms = min(r[0] for r in got_rows)
            end_ms = earliest_ms  # next chunk will end at earliest we have (no overlap)
            needed -= len(got_rows)

            # If server returned very few rows, likely near history start; stop
            if len(got_rows) < max(20, chunk_n // 3):
                break

            # Small pause between chunks
            time.sleep(0.12)

        if not out_rows:
            raise RuntimeError("No candles returned")

        # Build DF
        df = pd.DataFrame(out_rows, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.sort_values("time", inplace=True)
        df = df.drop_duplicates(subset=["time"], keep="last")

        # Keep exactly `limit` most recent closed bars
        if len(df) > limit:
            df = df.iloc[-limit:]
        return df

    def fetch_funding_rate(self, symbol: str):
        return None


# ---------- Optional discovery helpers (best-effort; many nodes don't expose these) ----------
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
    base = HL_REST_BASE
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
    base = HL_REST_BASE
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
