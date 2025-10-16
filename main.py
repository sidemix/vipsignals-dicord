import os, time, math
import pandas as pd
from datetime import datetime, timezone

from config import Config as C
from exchanges import make_exchange, fetch_ohlcv_df, fetch_funding_rate
from indicators import ema, atr, adx, sma
from discord_sender import send_signal_embed, send_info

# persistent dedupe in memory by (symbol, bar_time_ns)
sent = {}

def format_tps(price: float, atr_val: float, multipliers):
    return [price + m*atr_val for m in multipliers]

def long_setup(row_close, atr_v):
    entry_high = row_close - C.PULL_U * atr_v
    entry_low  = row_close - C.PULL_L * atr_v
    sl         = row_close - C.RISK_ATR * atr_v
    return entry_high, entry_low, sl

def short_setup(row_close, atr_v):
    entry_low  = row_close + C.PULL_U * atr_v
    entry_high = row_close + C.PULL_L * atr_v
    sl         = row_close + C.RISK_ATR * atr_v
    return entry_high, entry_low, sl  # keep same order (eh, el, sl)

def passes_filters(df):
    # Work with last closed bar
    last = df.iloc[-2]
    adx_last = df["adx"].iloc[-2]
    vol_last = df["volume"].iloc[-2]
    vol_sma  = df["vol_sma20"].iloc[-2]

    # ADX & Volume filters
    if math.isnan(adx_last) or adx_last < C.MIN_ADX:
        return False, f"ADX {adx_last:.1f} < {C.MIN_ADX}"
    if math.isnan(vol_sma) or vol_last < C.VOL_MULT * vol_sma:
        return False, f"Vol {vol_last:.0f} < {C.VOL_MULT:.1f}×SMA20 {vol_sma:.0f}"
    return True, "OK"

def scan_symbol(ex, symbol: str):
    df = fetch_ohlcv_df(ex, symbol, C.TIMEFRAME, C.MIN_BARS)
    if len(df) < C.MIN_BARS:
        return

    # Indicators
    df["ema5"]   = ema(df["close"], 5)
    df["ema50"]  = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["atr"]    = atr(df, 14)
    df["adx"]    = adx(df, 14)
    df["vol_sma20"] = sma(df["volume"], 20)

    # Last closed bar
    row_prev = df.iloc[-3]   # prev-1 for cross reference
    row      = df.iloc[-2]   # last closed
    tstamp_ns = int(row["time"].value)

    key = f"{symbol}:{tstamp_ns}"
    if key in sent:
        return

    # Cross + trend
    bullCross = (df["ema5"].iloc[-2] > df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] <= df["ema50"].iloc[-3]) and (row["close"] > df["ema200"].iloc[-2])
    bearCross = (df["ema5"].iloc[-2] < df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] >= df["ema50"].iloc[-3]) and (row["close"] < df["ema200"].iloc[-2])

    # Filters
    ok_filters, reason = passes_filters(df)
    if not ok_filters:
        return  # comment this to debug: reason has details

    # Funding filter (optional, for perps only; returns percent)
    fr_text = None
    if C.ENABLE_FUNDING_FILTER:
        fr = fetch_funding_rate(ex, symbol)
        if fr is not None:
            fr_text = f"Funding: {fr:.4f}%"
            if abs(fr) > C.MAX_ABS_FUNDING:
                return

    if bullCross and not math.isnan(row["atr"]):
        eh, el, sl = long_setup(row["close"], row["atr"])
        tps = format_tps(row["close"], row["atr"], C.TP_MULT)
        extras = {"TF": C.TIMEFRAME}
        if fr_text: extras["Info"] = fr_text
        send_signal_embed(symbol, "LONG", C.LEVERAGE, eh, el, sl, tps, extras=extras)
        sent[key] = True

    elif bearCross and not math.isnan(row["atr"]):
        eh, el, sl = short_setup(row["close"], row["atr"])
        tps = [row["close"] - m*row["atr"] for m in C.TP_MULT]
        extras = {"TF": C.TIMEFRAME}
        if fr_text: extras["Info"] = fr_text
        send_signal_embed(symbol, "SHORT", C.LEVERAGE, eh, el, sl, tps, extras=extras)
        sent[key] = True

def main():
    ex = make_exchange(C.EXCHANGE)
    send_info(f"Started scanner on **{C.EXCHANGE}** | TF **{C.TIMEFRAME}** | Symbols: {', '.join(C.SYMBOLS)}")
    while True:
        try:
            for s in C.SYMBOLS:
                scan_symbol(ex, s)
        except Exception as e:
            send_info(f"Error: `{e}`")
        time.sleep(C.POLL_SECONDS)

if __name__ == "__main__":
    main()

