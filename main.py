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

from discord_sender import send_info
send_info(f"Config → PROVIDER={C.PROVIDER}, AUTO_SYMBOLS={C.AUTO_SYMBOLS}, QUOTE={C.BLOFIN_QUOTE}")


# Auto-select BloFin symbols if requested
if C.PROVIDER.lower() == "blofin" and C.AUTO_SYMBOLS:
    try:
        from providers.blofin_provider import list_blofin_symbols, top_by_volume
        all_syms = list_blofin_symbols(inst_type=C.BLOFIN_INST_TYPE, want_quote=C.BLOFIN_QUOTE)
        picked   = top_by_volume(all_syms, inst_type=C.BLOFIN_INST_TYPE, want_quote=C.BLOFIN_QUOTE,
                                 top_n=C.TOP_N, min_vol=C.MIN_24H_VOL_USDT)
        if picked:
            C.SYMBOLS = picked
            send_info(f"Auto symbols ({C.BLOFIN_INST_TYPE}/{C.BLOFIN_QUOTE}): {', '.join(C.SYMBOLS)}")
        else:
            send_info("Auto symbols: no matches; using SYMBOLS from env.")
    except Exception as e:
        send_info(f"Auto symbols error: `{e}` — using SYMBOLS from env.")


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
    df["ema5"]   = ema(df["close"], 5)
    df["ema50"]  = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["atr"]    = atr(df, 14)
    df["adx"]    = adx(df, 14)
    df["vol_sma20"] = sma(df["volume"], 20)

    # Last closed bar and previous (for cross)
    row_prev  = df.iloc[-3]
    row       = df.iloc[-2]
    tstamp_ns = int(row["time"].value)

    # Per-bar de-dup
    key = f"{symbol}:{tstamp_ns}"
    if key in sent:
        return

    # EMA cross + trend
    bullCross = (df["ema5"].iloc[-2] > df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] <= df["ema50"].iloc[-3]) and (row["close"] > df["ema200"].iloc[-2])
    bearCross = (df["ema5"].iloc[-2] < df["ema50"].iloc[-2]) and (df["ema5"].iloc[-3] >= df["ema50"].iloc[-3]) and (row["close"] < df["ema200"].iloc[-2])

    # Filters (ADX + volume)
    if not passes_filters(df):
        return

    # Cooldown in bars (avoid repeated signals in chop)
    prev_t = last_bar_index.get(symbol)
    if prev_t is not None:
        # Estimate bar size by last two index values (ns)
        bar_ns = int(df["time"].iloc[-1].value) - int(df["time"].iloc[-2].value)
        if bar_ns > 0:
            bars_since = (tstamp_ns - prev_t) // bar_ns
            if bars_since < C.COOLDOWN_BARS:
                return

    # Optional funding filter (if provider supports it)
    fr_text = None
    if C.ENABLE_FUNDING_FILTER:
        try:
            fr = PROV.fetch_funding_rate(symbol)
            if fr is not None:
                fr_text = f"Funding: {fr:.4f}%"
                if abs(fr) > C.MAX_ABS_FUNDING:
                    return
        except Exception:
            pass

    # LONG
    if bullCross and htf_trend_ok(symbol, want_long=True) and not math.isnan(row["atr"]):
        eh, el, sl = long_setup(row["close"], row["atr"])
        tps = format_tps(row["close"], row["atr"], C.TP_MULT)
        extras = {"TF": C.TIMEFRAME}
        if fr_text: extras["Info"] = fr_text
        send_signal_embed(symbol, "LONG", C.LEVERAGE, eh, el, sl, tps, extras=extras)
        sent[key] = True
        last_bar_index[symbol] = tstamp_ns
        return

    # SHORT
    if bearCross and htf_trend_ok(symbol, want_long=False) and not math.isnan(row["atr"]):
        eh, el, sl = short_setup(row["close"], row["atr"])
        tps = [row["close"] - m * row["atr"] for m in C.TP_MULT]
        extras = {"TF": C.TIMEFRAME}
        if fr_text: extras["Info"] = fr_text
        send_signal_embed(symbol, "SHORT", C.LEVERAGE, eh, el, sl, tps, extras=extras)
        sent[key] = True
        last_bar_index[symbol] = tstamp_ns
        return

def main():
    # Startup banner
    banner = f"Started scanner on **{C.PROVIDER}**"
    if C.PROVIDER.lower() == "ccxt":
        banner += f" ({C.EXCHANGE})"
    banner += f" | TF **{C.TIMEFRAME}** | Symbols: {', '.join(C.SYMBOLS)}"
    send_info(banner)

    # If provider can supply markets, optionally warn on unknown symbols (non-fatal)
    try:
        markets = PROV.load_markets() or {}
        if markets:
            listed = set(markets.keys())
            bad = [s for s in C.SYMBOLS if s not in listed]
            if bad:
                send_info("⚠️ Not listed: " + ", ".join(bad))
    except Exception:
        pass

    # Main loop
    while True:
        try:
            for s in C.SYMBOLS:
                scan_symbol(s)
        except Exception as e:
            send_info(f"Error: `{e}`")
        time.sleep(C.POLL_SECONDS)

if __name__ == "__main__":
    main()
