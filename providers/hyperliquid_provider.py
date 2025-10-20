# providers/hyperliquid_provider.py
import os
import time
import pandas as pd
from .base import BaseProvider

HL_REST_BASE   = os.getenv("HL_REST_BASE", "https://api.hyperliquid.xyz")
HL_INSTRUMENTS = os.getenv("HL_INSTRUMENTS", "/api/v1/public/instruments")
HL_TICKERS     = os.getenv("HL_TICKERS", "/api/v1/public/tickers")
HL_ALT_BASE    = os.getenv("HL_ALT_BASE", "").strip()  # optional fallback base, e.g. another HL API host

DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","6h":"6h","12h":"12h",
    "1d":"1d"
}
TF_MAP = {**DEFAULT_TF_MAP}

# max bars per POST; lower value -> fewer 500s/429s
HL_MAX_CHUNK = int(os.getenv("HL_MAX_CHUNK", "250"))

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
    if ts < 10_000_000_000:  # seconds -> ms
        ts *= 1000
    return ts

def _parse_rows(payload):
    """
    Normalize to rows: [time_ms, open, high, low, close, volume]
    Supports:
      - list of dicts with {t/ts/time, o/open, h/high, l/low, c/close, v/volume}
      - list of arrays [t, o, h, l, c, v]
      - dict of arrays {t,o,h,l,c,v} or {time,open,high,low,close,volume}
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
    target = (os.getenv("HL_FORCE_QUOTE", "USD") or "USD").upper()
    base, sep, quote = sym.upper().partition("/")
    return f"{base}/{target}" if sep and quote != target else sym.upper()

def _secs_per_bar(bar: str) -> int:
    try:
        n, u = int(bar[:-1]), bar[-1]
        return n * (60 if u == "m" else 3600 if u == "h" else 86400 if u == "d" else 60)
    except Exception:
        return 60

class HyperliquidProvider(BaseProvider):
    def __init__(self):
        self._markets = {}

    def load_markets(self) -> dict:
        return self._markets

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
                r = _http_post_raw(base_url.rstrip("/") + "/info", json=body, timeout=25)
                if r.status_code in (429, 500, 502, 503, 504):
                    # log server text to help diagnose
                    try:
                        _debug(f"server_resp: {r.text[:300]}")
                    except Exception:
                        pass
                    time.sleep(backoff)
                    backoff = min(backoff * 1.8, 6.0)
                    continue
                if r.status_code >= 400:
                    # 4xx (e.g., 422) — log body and bail this attempt
                    try:
                        _debug(f"{r.status_code} {r.reason_phrase} body={r.text[:300]}")
                    except Exception:
                        pass
                    last_err = RuntimeError(f"{r.status_code} {r.reason_phrase}")
                    return None, last_err
                payload = r.json()
                rows = _parse_rows(payload) or _parse_rows(_extract_list(payload) or {})
                if not rows:
                    last_err = RuntimeError("unrecognized_payload")
                    return None, last_err
                return rows, None
            except Exception as e:
                last_err = e
                time.sleep(0.3)
                continue
        return None, last_err or RuntimeError("chunk_fetch_failed")

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """
        Fetch up to `limit` bars by paging in chunks (HL_MAX_CHUNK).
        This reduces server stress and avoids 500s.
        """
        symbol   = _force_quote(symbol)           # e.g., BTC/USDT -> BTC/USD
        coin     = symbol.split("/")[0]           # "BTC"
        interval = TF_MAP.get(timeframe, timeframe)
        spb_ms   = _secs_per_bar(interval) * 1000

        needed   = int(limit)
        now_ms   = int(time.time() * 1000)
        end_ms   = now_ms
        out_rows = []

        bases = [HL_REST_BASE]
        if HL_ALT_BASE:
            bases.append(HL_ALT_BASE)

        while needed > 0:
            # request a chunk window
            chunk_n   = min(HL_MAX_CHUNK, needed)
            start_ms  = end_ms - (chunk_n + 2) * spb_ms  # pad 2 bars
            got_rows  = None
            last_err  = None

            for base in bases:
                rows, err = self._fetch_chunk(base, coin, interval, start_ms, end_ms)
                if rows:
                    got_rows = rows
                    break
                last_err = err

            if not got_rows:
                # couldn't fetch this chunk — break to avoid tight loop
                raise last_err or RuntimeError("Failed to fetch chunk")

            # extend and move the window backward
            out_rows.extend(got_rows)
            end_ms = start_ms + spb_ms  # step one bar earlier to avoid overlap
            needed -= len(got_rows)
            # if server returned fewer than asked (near history start), stop
            if len(got_rows) < chunk_n // 2:
                break

            # small pause between chunks to be kind to the API
            time.sleep(0.15)

        if not out_rows:
            raise RuntimeError("No candles returned")

        # build dataframe
        df = pd.DataFrame(out_rows, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.sort_values("time", inplace=True)
        # dedupe (if overlapping windows)
        df = df.drop_duplicates(subset=["time"], keep="last")
        # trim to exactly `limit` most recent closed bars
        if len(df) > limit:
            df = df.iloc[-limit:]
        return df

    def fetch_funding_rate(self, symbol: str):
        return None


# ---------- Optional auto-markets helpers ----------
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
