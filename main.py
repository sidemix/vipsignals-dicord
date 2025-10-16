import math
import time
import pandas as pd

from config import Config as C
from indicators import ema, atr, adx, sma
from discord_sender import send_signal_embed, send_info

# ---- Provider abstraction ----
from providers.base import BaseProvider
from providers.ccxt_provider import CcxtProvider
from providers.blofin_provider import BlofinProvider

# ---------------- Utility flags ----------------
QUIET = str(getattr(C, "DEBUG", False)).lower() not in ("1","true","yes","on") and \
        str(getattr(C, "QUIET", False) if hasattr(C, "QUIET") else False).lower() in ("1","true","yes","on")

def _maybe_info(msg: str):
    if not QUIET:
        try:
            send_info(msg)
        except Exception:
            pass

# --------------- Provider factory ---------------
def make_provider() -> BaseProvider:
    if C.PROVIDER.lower() == "blofin":
        return BlofinProvider()
    return CcxtProvider(C.EXCHANGE)

PROV = make_provider()

# ---------------- State (dedupe/cooldown) ----------------
sent = {}                # (symbol:tstamp_ns) -> True (one alert per closed bar)
last_bar_index = {}      # symbol -> last tstamp_ns when we sent a signal

# ----------------- Helpers -----------------
def format_tps(price: float, atr_val: float, multipliers):
    return [price + m * atr_val for m in multipliers]

def long_setup(close_price, atr_v):
    entry_high = close_price - C.PULL_U * atr_v
    entry_low  = close_price - C.PULL_L * atr_v
    sl         = close_price - C.RISK_ATR * atr_v
    return entry_high, entry_low, sl

def short_setup(close_price, atr_v):
    entry_low  = close_price + C.PULL_U * atr_v
    entry_high = close_price + C.PULL_L * atr_v
    sl         = close_price + C.RISK_ATR * atr_v
    return entry_high, entry_low, sl  # keep (eh, el, sl)

def passes_filters(df: pd.DataFrame):
    # Use last closed bar (-2)
    adx_last = float(df["adx"].iloc[-2])
    vol_last = float(df["volume"].iloc[-2])
    vol_sma  = float(df["vol_sma20"].iloc[-2])

    if math.isnan(adx_last) or adx_last < C.MIN_ADX:
        return False
    if math.isnan(vol_sma) or vol_last < C.VOL_MULT * vol_sma:
        return False
    return True

def htf_trend_ok(symbol: str, want_long: bool) -> bool:
    if not getattr(C, "REQUIRE_TREND_HTF", True):
        return True
    try:
        df_htf = PROV.fetch_ohlcv_df(symbol, C.HTF, 300)
        if len(df_htf) < 210:
            return True  # not enough data to judge; don't block
        df_htf["ema200"] = ema(df_htf["close"], 200)
        price  = float(df_htf["close"].iloc[-2])
        ema200 = float(df_htf["ema200"].iloc[-2])
        return price > ema200 if want_long else price < ema200
    except Exception:
        # If HTF fetch fails, allow signals (fail-open)
        return True

def scan_symbol(symbol: str):
    # Fetch main timeframe data
    df = PROV.fetch_ohlcv_df(symbol, C.TIMEFRAME, C.MIN_BARS)
    if len(df) < C.MIN_BARS:
        return

    # Indicators
    df["ema5"]   = ema(df["close"], 5)
    df["ema50"]  = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["atr"]    = atr(df, 14)
    df["adx"]    = adx(df, 14)
    df["vol_sma20"] = sma(df["volume"], 20)

    # Last closed bar (and previous for cross)
    row_prev  = df.iloc[-3]
    row       = df.iloc[-2]
    tstamp_ns = int(row["time"].value)

    # Per-bar de-dup (one alert per closed bar per symbol)
    key = f"{symbol}:{tstamp_ns}"
    if key in sent:
        return

    # EMA cross + MTF trend gate
    bullCross = (df["ema5"].iloc[-2] > df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] <= df["ema50"].iloc[-3]) and (row["close"] > df["ema200"].iloc[-2])
    bearCross = (df["ema5"].iloc[-2] < df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] >= df["ema50"].iloc[-3]) and (row["close"] < df["ema200"].iloc[-2])

    # Base filters
    if not passes_filters(df):
        return

    # Cooldown in bars (avoid repeated signals in chop)
    prev_t = last_bar_index.get(symbol)
    if prev_t is not None:
        bar_ns = int(df["time"].iloc[-1].value) - int(df["time"].iloc[-2].value)
        if bar_ns > 0:
            bars_since = (tstamp_ns - prev_t) // bar_ns
            if bars_since < C.COOLDOWN_BARS:
                return

    # Funding filter (optional; provider may return None)
    fr_text = None
    if getattr(C, "ENABLE_FUNDING_FILTER", False):
        try:
            fr = PROV.fetch_funding_rate(symbol)
            if fr is not None:
                fr_text = f"Funding: {fr:.4f}%"
                if abs(fr) > C.MAX_ABS_FUNDING:
                    return
        except Exception:
            pass

    # ----- LONG -----
    if bullCross and htf_trend_ok(symbol, want_long=True) and not math.isnan(row["atr"]):
        eh, el, sl = long_setup(row["close"], row["atr"])
        tps = format_tps(row["close"], row["atr"], C.TP_MULT)
        extras = {"TF": C.TIMEFRAME}
        if fr_text: extras["Info"] = fr_text
        send_signal_embed(symbol, "LONG", C.LEVERAGE, eh, el, sl, tps, extras=extras)
        sent[key] = True
        last_bar_index[symbol] = tstamp_ns
        return

    # ----- SHORT -----
    if bearCross and htf_trend_ok(symbol, want_long=False) and not math.isnan(row["atr"]):
        eh, el, sl = short_setup(row["close"], row["atr"])
        tps = [row["close"] - m * row["atr"] for m in C.TP_MULT]
        extras = {"TF": C.TIMEFRAME}
        if fr_text: extras["Info"] = fr_text
        send_signal_embed(symbol, "SHORT", C.LEVERAGE, eh, el, sl, tps, extras=extras)
        sent[key] = True
        last_bar_index[symbol] = tstamp_ns
        return

def maybe_auto_symbols():
    """
    If AUTO_SYMBOLS is enabled and provider is BloFin, try to select pairs automatically.
    Posts a single summary message with the final list (unless QUIET=true).
    """
    if C.PROVIDER.lower() != "blofin":
        return
    if not getattr(C, "AUTO_SYMBOLS", False):
        return
    try:
        from providers.blofin_provider import list_blofin_symbols, top_by_volume
        syms = list_blofin_symbols(
            inst_type=getattr(C, "BLOFIN_INST_TYPE", "SWAP"),
            want_quote=getattr(C, "BLOFIN_QUOTE", "USDT"),
        )
        if not syms:
            _maybe_info("Auto symbols: no matches; using SYMBOLS from env.")
            return
        picked = top_by_volume(
            syms,
            inst_type=getattr(C, "BLOFIN_INST_TYPE", "SWAP"),
            want_quote=getattr(C, "BLOFIN_QUOTE", "USDT"),
            top_n=getattr(C, "TOP_N", 12),
            min_vol=getattr(C, "MIN_24H_VOL_USDT", 0.0),
        ) or syms[: getattr(C, "TOP_N", 12)]
        C.SYMBOLS = picked
        _maybe_info(f"Auto symbols ({getattr(C, 'BLOFIN_QUOTE', 'USDT')}): {', '.join(C.SYMBOLS)}")
    except Exception as e:
        _maybe_info(f"Auto symbols error: `{e}` — using SYMBOLS from env.")

def main():
    # Optional auto-selection of symbols (BloFin)
    maybe_auto_symbols()

    # Startup banner (can be silenced with QUIET=true)
    banner = f"Started scanner on **{C.PROVIDER}**"
    if C.PROVIDER.lower() == "ccxt":
        banner += f" ({C.EXCHANGE})"
    banner += f" | TF **{C.TIMEFRAME}** | Symbols: {', '.join(C.SYMBOLS)}"
    _maybe_info(banner)

    # Main scan loop — only posts actual signals
    while True:
        try:
            for s in C.SYMBOLS:
                scan_symbol(s)
        except Exception as e:
            # Only surface real errors
            _maybe_info(f"Error: `{e}`")
        time.sleep(C.POLL_SECONDS)

if __name__ == "__main__":
    main()
