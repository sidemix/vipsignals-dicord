import math, time
import pandas as pd
from datetime import datetime, timezone

from config import Config as C
from indicators import ema, atr, adx, sma
from discord_sender import send_signal_embed, send_info

# ---- Provider abstraction ----
from providers.base import BaseProvider
from providers.ccxt_provider import CcxtProvider
from providers.blofin_provider import BlofinProvider

def make_provider() -> BaseProvider:
    if C.PROVIDER.lower() == "blofin":
        return BlofinProvider()
    return CcxtProvider(C.EXCHANGE)

PROV = make_provider()

# Dedup + cooldown memory
sent = {}                # (symbol:tstamp_ns) -> True
last_bar_index = {}      # symbol -> last bar timestamp (ns) when we signaled

# -------- Helpers --------
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
    return entry_high, entry_low, sl  # keep same order (eh, el, sl)

def passes_filters(df: pd.DataFrame):
    # Work with last closed bar
    adx_last = df["adx"].iloc[-2]
    vol_last = df["volume"].iloc[-2]
    vol_sma  = df["vol_sma20"].iloc[-2]

    if math.isnan(adx_last) or adx_last < C.MIN_ADX:
        return False
    if math.isnan(vol_sma) or vol_last < C.VOL_MULT * vol_sma:
        return False
    return True

def htf_trend_ok(symbol: str, want_long: bool) -> bool:
    if not C.REQUIRE_TREND_HTF:
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
        # If HTF fetch fails, don't block signals
        return True

def scan_symbol(symbol: str):
    # Fetch main timeframe data
    df = PROV.fetch_ohlcv_df(symbol, C.TIMEFRAME, C.MIN_BARS)
    if len(df) < C.MIN_BARS:
        return

    # Indicators
