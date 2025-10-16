import os
import pandas as pd

from .base import BaseProvider

# ===== REST base & paths (override via Render env if needed) =====
BLOFIN_REST_BASE   = os.getenv("BLOFIN_REST_BASE", "https://openapi.blofin.com")
BLOFIN_REST_KLINES = os.getenv("BLOFIN_REST_KLINES", "/api/v1/market/candles")
BLOFIN_INSTRUMENTS = os.getenv("BLOFIN_INSTRUMENTS", "/api/v1/public/instruments")
BLOFIN_TICKERS     = os.getenv("BLOFIN_TICKERS", "/api/v1/public/tickers")

# Map TF strings → API values (customize via env if needed)
DEFAULT_TF_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","4h":"4h","6h":"6h","12h":"12h",
    "1d":"1d"
}
TF_MAP = {**DEFAULT_TF_MAP}

# ---------- tiny util/helpers ----------
def _symbol_to_blofin_spot(symbol: str) -> str:
    """'MTL/USDT' -> 'MTLUSDT' (some endpoints prefer this)."""
    return symbol.replace("/", "")

def _debug_log(msg: str):
    if os.getenv("DEBUG", "").strip().lower() in ("1","true","yes","on"):
        try:
            from discord_sender import send_info   # avoid hard dep at import time
            send_info(f"[BloFin] {msg}")
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
        for k in ("data","result","rows","list","items","instruments","symbols","tickers"):
            v = obj.get(k)
            if isinstance(v, list):
                return v
    return None

def _norm_symbol_from_inst(inst: dict):
    """
    Normalize instrument identifiers into 'BASE/QUOTE'.
    Accepts 'instId', 'symbol', or 'instrumentId' in forms:
      - 'BTC-USDT', 'BTC_USDT', 'BTCUSDT'
    """
    inst_id = inst.get("instId") or inst.get("symbol") or inst.get("instrumentId")
    if not inst_id:
        return None
    s = str(inst_id).replace("_", "-")
    if "-" in s:
        base, _, quote = s.partition("-")
        return f"{base}/{quote}"
    s = s.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT"
    if s.endswith("USD"):
        return f"{s[:-3]}/USD"
    return None


# ===================== Provider =====================
class BlofinProvider(BaseProvider):
    """
    Uses BloFin SDK if installed; otherwise robust REST.
    Exposes:
      - load_markets()
      - fetch_ohlcv_df(symbol, timeframe, limit)
      - fetch_funding_rate(symbol) -> None for now
    """
    def __init__(self):
        self.sdk = None
        self._markets = {}
        try:
            from blofin import Blofin  # type: ignore
            key    = os.getenv("BLOFIN_API_KEY")
            secret = os.getenv("BLOFIN_API_SECRET")
            passph = os.getenv("BLOFIN_API_PASSPHRASE")
            self.sdk = Blofin(api_key=key, api_secret=secret, passphrase=passph)
            _debug_log("SDK initialized")
        except Exception as e:
            self.sdk = None
            _debug_log(f"SDK unavailable, using REST. ({e})")

    def load_markets(self) -> dict:
        # Minimal stub (auto-symbols discovery is handled by helpers below).
        return self._markets

    # ---------- SDK path ----------
    def _sdk_fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        inst = symbol.replace("/", "-")          # BTC/USDT -> BTC-USDT
        bar  = TF_MAP.get(timeframe, timeframe)  # '5m', '1h', etc.
        data = self.sdk.public.get_candlesticks(instId=inst, bar=bar, limit=limit)  # type: ignore[attr-defined]

        rows = []
        for x in data:
            if isinstance(x, dict):
                ts  = int(float(x.get("ts") or x.get("time") or x.get("t")))
                op  = float(x.get("open")  or x.get("o"))
                hi  = float(x.get("high")  or x.get("h"))
                lo  = float(x.get("low")   or x.get("l"))
                cl  = float(x.get("close") or x.get("c"))
                vol = float(x.get("volume")or x.get("v"))
            else:
                ts, op, hi, lo, cl, vol = int(float(x[0])), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])
            if ts < 10_000_000_000:  # seconds -> ms
                ts *= 1000
            rows.append([ts, op, hi, lo, cl, vol])

        df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df.sort_values("time", inplace=True)
        return df

    # ---------- REST path (robust parser) ----------
    def _rest_fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        import httpx

        def _to_int_ms(x):
            ts = int(float(x))
            if ts < 10_000_000_000:
                ts *= 1000
            return ts

        def _first_list_like(obj):
            if isinstance(obj, list):
                return obj
            if isinstance(obj, dict):
                for k in ("data","result","rows","list","candles","klines","kline","items"):
                    v = obj.get(k)
                    if isinstance(v, list) and len(v) > 0:
                        return v
            return None

        def _dict_of_arrays_to_rows(d):
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
                rows.append([_to_int_ms(t[i]), float(o[i]), float(h[i]), float(l[i]), float(c[i]), float(v[i])])
            return rows

        pair_dash = symbol.replace("/", "-")   # BTC/USDT -> BTC-USDT
        pair_cat  = symbol.replace("/", "")    # BTC/USDT -> BTCUSDT
        bar       = TF_MAP.get(timeframe, timeframe)
        base      = BLOFIN_REST_BASE.rstrip("/")

        attempts = [
            (BLOFIN_REST_KLINES, {"instId": pair_dash, "bar": bar, "limit": limit}),
            (BLOFIN_REST_KLINES, {"symbol": pair_cat,  "interval": bar, "limit": limit}),
            (BLOFIN_REST_KLINES, {"instId": pair_dash, "interval": bar, "limit": limit}),
            (BLOFIN_REST_KLINES, {"symbol": pair_dash, "bar": bar, "limit": limit}),
        ]

        last_err = None
        for path, params in attempts:
            try:
                url = base + path
                _debug_log(f"GET {url} params={params}")
                r = httpx.get(url, params=params, timeout=15)
                r.raise_for_status()
                payload = r.json()

                data = payload
                rows = None

                if isinstance(payload, dict):
                    maybe = _first_list_like(payload)
                    if maybe is None:
                        rows = _dict_of_arrays_to_rows(payload)
                    else:
                        data = maybe

                if rows is None:
                    rows = []
                    if isinstance(data, list) and len(data) > 0:
                        if isinstance(data[0], dict):
                            for x in data:
                                ts  = _to_int_ms(x.get("ts") or x.get("time") or x.get("t"))
                                op  = float(x.get("open")  or x.get("o"))
                                hi  = float(x.get("high")  or x.get("h"))
                                lo  = float(x.get("low")   or x.get("l"))
                                cl  = float(x.get("close") or x.get("c"))
                                vol = float(x.get("volume")or x.get("v"))
                                rows.append([ts, op, hi, lo, cl, vol])
                        elif isinstance(data[0], (list, tuple)):
                            for x in data:
                                ts = _to_int_ms(x[0])
                                rows.append([ts, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
                        else:
                            if isinstance(data[0], dict):
                                maybe2 = _dict_of_arrays_to_rows(data[0])
                                if maybe2:
                                    rows = maybe2

                if not rows and isinstance(payload, dict):
                    rows = _dict_of_arrays_to_rows(payload)

                if not rows:
                    raise ValueError("Unrecognized kline payload shape")

                df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                df.sort_values("time", inplace=True)
                return df

            except Exception as e:
                last_err = e
                _debug_log(f"kline attempt failed: {e}")
                continue

        raise last_err or RuntimeError("Failed to fetch klines from BloFin")

    # Public method used by the bot
    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        if self.sdk is not None:
            try:
                return self._sdk_fetch_ohlcv_df(symbol, timeframe, limit)
            except Exception as e:
                _debug_log(f"SDK fetch failed, fallback to REST: {e}")
        return self._rest_fetch_ohlcv_df(symbol, timeframe, limit)

    # Funding rate hook (optional; return None for now)
    def fetch_funding_rate(self, symbol: str):
        return None


# ===================== Auto-markets / Top symbols =====================

def _probe_bases_default():
    # Popular majors + alts; adjust via PROBE_BASES if you want.
    return [
        "BTC","ETH","SOL","XRP","ADA","DOGE","TRX","DOT","MATIC","LTC","BCH","LINK","OP","ARB",
        "APT","SUI","ATOM","AVAX","NEAR","ETC","FIL","INJ","AAVE","UNI","MKR","DYDX","GMT","FTM",
        "RNDR","PEPE","WIF","ENA","JTO","PYTH","TIA","ONDO","JUP","BLUR","SEI","TAO","WLD","TON",
        "APT","IMX","APE","AEVO","JASMY","GALA","XLM","VET","ALGO","SAND","MANA","FLOW","HBAR",
        "KAS","ROSE","SKL","AR","OPUL","ADA","MTL","ENA","OP","SFP","SSV","BEAM","BONK","TIA"
    ]

def _parse_bases_env():
    bases = os.getenv("PROBE_BASES", "")
    if bases.strip():
        return [b.strip().upper() for b in bases.split(",") if b.strip()]
    return _probe_bases_default()

def _probe_pairs_via_klines(want_quote="USDT", top_n=50):
    """
    Last-resort discovery that asks the candles endpoint for a short list of bases.
    If the response yields any rows, we consider the pair listed.
    """
    import httpx
    want_quote = (want_quote or "USDT").upper()
    bases = _parse_bases_env()
    found = []

    base = BLOFIN_REST_BASE.rstrip("/")
    url  = base + BLOFIN_REST_KLINES
    bar  = TF_MAP.get("1h", "1h")

    for baseccy in bases:
        inst_dash = f"{baseccy}-{want_quote}"   # e.g., BTC-USDT
        params_try = [
            {"instId": inst_dash, "bar": bar, "limit": 5},
            {"symbol": f"{baseccy}{want_quote}", "interval": bar, "limit": 5},
            {"symbol": inst_dash, "bar": bar, "limit": 5},
        ]
        ok = False
        for params in params_try:
            try:
                _debug_log(f"probe {url} {params}")
                r = httpx.get(url, params=params, timeout=8)
                if r.status_code != 200:
                    continue
                js = r.json()
                lst = _extract_list(js)
                if lst is None and isinstance(js, dict):
                    # try dict-of-arrays
                    lst = js.get("t") or js.get("time")
                    if lst and isinstance(lst, list) and len(lst) > 0:
                        ok = True
                        break
                if isinstance(lst, list) and len(lst) > 0:
                    ok = True
                    break
            except Exception:
                continue
        if ok:
            found.append(f"{baseccy}/{want_quote}")
        if len(found) >= top_n:
            break

    _debug_log(f"probe -> {len(found)} pairs via klines")
    return sorted(set(found))

def list_blofin_symbols(inst_type="SWAP", want_quote="USDT"):
    """
    Robust discovery:
      1) Try instruments with several param names.
      2) If empty, derive from tickers (no instType required).
      3) If still empty, PROBE via klines for a curated list of bases.
    Filters by quote so we always end with *something*.
    """
    base = BLOFIN_REST_BASE.rstrip("/")
    inst_url = base + BLOFIN_INSTRUMENTS
    tick_url = base + BLOFIN_TICKERS

    attempts = [
        {"instType": inst_type},
        {"category": inst_type},
        {"type": inst_type},
        {},  # no filter; we filter by quote locally
    ]

    # 1) instruments
    for params in attempts:
        try:
            _debug_log(f"GET {inst_url} params={params}")
            payload = _http_get_json(inst_url, params=params)
            items = _extract_list(payload) or []
            syms = []
            for inst in items:
                sym = _norm_symbol_from_inst(inst)
                if not sym:
                    continue
                q = sym.split("/")[-1].upper()
                if want_quote and q != want_quote.upper():
                    continue
                itype = (inst.get("instType") or inst.get("category") or inst.get("type") or "").upper()
                if params and ("instType" in params or "category" in params or "type" in params):
                    if itype and inst_type and itype != inst_type.upper():
                        continue
                syms.append(sym)
            if syms:
                out = sorted(set(syms))
                _debug_log(f"instruments -> {len(out)} matches (quote={want_quote})")
                return out
        except Exception as e:
            _debug_log(f"instruments error: {e}")
            continue

    # 2) tickers (no instType)
    try:
        _debug_log(f"GET {tick_url} (no params)")
        payload = _http_get_json(tick_url, params={})
        items = _extract_list(payload) or []
        syms = []
        for t in items:
            inst_id = t.get("instId") or t.get("symbol") or t.get("instrumentId")
            if not inst_id:
                continue
            sym = _norm_symbol_from_inst({"instId": inst_id})
            if not sym:
                continue
            q = sym.split("/")[-1].upper()
            if want_quote and q != want_quote.upper():
                continue
            syms.append(sym)
        if syms:
            out = sorted(set(syms))
            _debug_log(f"tickers -> {len(out)} matches (quote={want_quote})")
            return out
        else:
            _debug_log("tickers -> 0 matches; probing via klines")
    except Exception as e:
        _debug_log(f"tickers error: {e}; probing via klines")

    # 3) probe klines (last resort)
    return _probe_pairs_via_klines(want_quote=want_quote, top_n=int(os.getenv("TOP_N","12")))

def top_by_volume(symbols, inst_type="SWAP", want_quote="USDT", top_n=12, min_vol=0.0):
    """
    Rank symbols by 24h quote volume using tickers endpoint (if available),
    otherwise just return first top_n from the provided list (e.g., probe list).
    """
    if not symbols:
        return []
    base = BLOFIN_REST_BASE.rstrip("/")
    url  = base + BLOFIN_TICKERS

    vols = {}
    try:
        _debug_log(f"GET {url} (no params) for volume")
        payload = _http_get_json(url, params={})
        items = _extract_list(payload) or []
        for t in items:
            inst_id = t.get("instId") or t.get("symbol") or t.get("instrumentId")
            if not inst_id:
                continue
            sym = _norm_symbol_from_inst({"instId": inst_id})
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
        _debug_log(f"volume error: {e}")

    if vols:
        scored = [(s, vols.get(s, 0.0)) for s in symbols]
        if min_vol and min_vol > 0:
            scored = [x for x in scored if x[1] >= min_vol]
        scored.sort(key=lambda x: x[1], reverse=True)
        if top_n and top_n > 0:
            scored = scored[:top_n]
        out = [s for s, _ in scored]
        _debug_log(f"top_by_volume -> picked {len(out)} of {len(symbols)} via tickers")
        return out

    # fallback: no volumes available; cap to top_n of discovered list
    out = symbols[:top_n] if top_n and top_n > 0 else symbols
    _debug_log(f"top_by_volume -> tickers missing; using first {len(out)} from probe")
    return out
