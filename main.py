import math
import time
import os
import itertools
import pandas as pd

from config import Config as C
from indicators import ema, atr, adx, sma
from discord_sender import send_signal_embed, send_info
from providers.base import BaseProvider
from providers.ccxt_provider import CcxtProvider
from providers.blofin_provider import BlofinProvider
from providers.hyperliquid_provider import HyperliquidProvider

# ---------------- Utility flags ----------------
QUIET = str(getattr(C, "DEBUG", False)).lower() not in ("1", "true", "yes", "on") and \
        str(getattr(C, "QUIET", False) if hasattr(C, "QUIET") else False).lower() in ("1", "true", "yes", "on")

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
    if C.PROVIDER.lower() == "hyperliquid":
        return HyperliquidProvider()
    return CcxtProvider(C.EXCHANGE)

PROV = make_provider()

# ---------------- State ----------------
sent = {}                # (symbol:tstamp_ns)
last_bar_index = {}      # symbol -> last tstamp_ns

# ---------------- Helpers ----------------
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
    return entry_high, entry_low, sl

def passes_filters(df: pd.DataFrame):
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
            return True
        df_htf["ema200"] = ema(df_htf["close"], 200)
        price  = float(df_htf["close"].iloc[-2])
        ema200 = float(df_htf["ema200"].iloc[-2])
        return price > ema200 if want_long else price < ema200
    except Exception:
        return True

def scan_symbol(symbol: str):
    df = PROV.fetch_ohlcv_df(symbol, C.TIMEFRAME, C.MIN_BARS)
    if len(df) < C.MIN_BARS:
        return

    df["ema5"]   = ema(df["close"], 5)
    df["ema50"]  = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["atr"]    = atr(df, 14)
    df["adx"]    = adx(df, 14)
    df["vol_sma20"] = sma(df["volume"], 20)

    row_prev  = df.iloc[-3]
    row       = df.iloc[-2]
    tstamp_ns = int(row["time"].value)

    key = f"{symbol}:{tstamp_ns}"
    if key in sent:
        return

    bullCross = (df["ema5"].iloc[-2] > df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] <= df["ema50"].iloc[-3]) and (row["close"] > df["ema200"].iloc[-2])
    bearCross = (df["ema5"].iloc[-2] < df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] >= df["ema50"].iloc[-3]) and (row["close"] < df["ema200"].iloc[-2])

    if not passes_filters(df):
        return

    prev_t = last_bar_index.get(symbol)
    if prev_t is not None:
        bar_ns = int(df["time"].iloc[-1].value) - int(df["time"].iloc[-2].value)
        if bar_ns > 0:
            bars_since = (tstamp_ns - prev_t) // bar_ns
            if bars_since < C.COOLDOWN_BARS:
                return

    # ----- LONG -----
    if bullCross and htf_trend_ok(symbol, True) and not math.isnan(row["atr"]):
        eh, el, sl = long_setup(row["close"], row["atr"])
        tps = format_tps(row["close"], row["atr"], C.TP_MULT)
        send_signal_embed(symbol, "LONG", C.LEVERAGE, eh, el, sl, tps, extras={"TF": C.TIMEFRAME})
        sent[key] = True
        last_bar_index[symbol] = tstamp_ns
        return

    # ----- SHORT -----
    if bearCross and htf_trend_ok(symbol, False) and not math.isnan(row["atr"]):
        eh, el, sl = short_setup(row["close"], row["atr"])
        tps = [row["close"] - m * row["atr"] for m in C.TP_MULT]
        send_signal_embed(symbol, "SHORT", C.LEVERAGE, eh, el, sl, tps, extras={"TF": C.TIMEFRAME})
        sent[key] = True
        last_bar_index[symbol] = tstamp_ns
        return

def maybe_auto_symbols():
    if C.PROVIDER.lower() not in ("blofin", "hyperliquid"):
        return
    if not getattr(C, "AUTO_SYMBOLS", False):
        return
    try:
        if C.PROVIDER.lower() == "blofin":
            from providers.blofin_provider import list_blofin_symbols, top_by_volume
            syms = list_blofin_symbols()
            picked = top_by_volume(syms)
        else:
            from providers.hyperliquid_provider import list_hl_symbols, top_by_volume
            syms = list_hl_symbols()
            picked = top_by_volume(syms)
        if picked:
            C.SYMBOLS = picked
            _maybe_info(f"Auto symbols: {', '.join(C.SYMBOLS)}")
    except Exception as e:
        _maybe_info(f"Auto symbols error: {e}")

def main():
    maybe_auto_symbols()

    banner = f"Started scanner on **{C.PROVIDER}** | TF {C.TIMEFRAME} | Symbols: {', '.join(C.SYMBOLS)}"
    _maybe_info(banner)

    throttle_ms = int(os.getenv("THROTTLE_MS", "150"))
    SCAN_BATCH  = int(os.getenv("SCAN_BATCH", "10"))

    # batch through symbols cyclically to avoid 429 rate limits
    symbols_cycle = itertools.cycle([C.SYMBOLS[i:i + SCAN_BATCH] for i in range(0, len(C.SYMBOLS), SCAN_BATCH)])

    while True:
        try:
            batch = next(symbols_cycle)
            for s in batch:
                scan_symbol(s)
                if throttle_ms > 0:
                    time.sleep(throttle_ms / 1000.0)
        except Exception as e:
            _maybe_info(f"Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
