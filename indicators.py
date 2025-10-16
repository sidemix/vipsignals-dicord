import numpy as np
import pandas as pd

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat(
        [(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()],
        axis=1
    ).max(axis=1)
    return tr.rolling(length).mean()

def adx(df: pd.DataFrame, length: int = 14) -> pd.Series:
    # Wilder’s ADX
    h, l, c = df["high"], df["low"], df["close"]
    up = h.diff()
    dn = -l.diff()

    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)

    tr_components = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1)
    tr = tr_components.max(axis=1)

    atr_w = tr.rolling(length).mean()

    plus_di = 100 * pd.Series(plus_dm, index=df.index).rolling(length).mean() / atr_w
    minus_di = 100 * pd.Series(minus_dm, index=df.index).rolling(length).mean() / atr_w

    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) ) * 100
    adx = dx.rolling(length).mean()
    return adx

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()

