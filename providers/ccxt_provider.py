import ccxt
import pandas as pd
from .base import BaseProvider

class CcxtProvider(BaseProvider):
    def __init__(self, exchange_name: str):
        if not hasattr(ccxt, exchange_name):
            raise ValueError(f"Unsupported exchange for ccxt: {exchange_name}")
        self.ex = getattr(ccxt, exchange_name)()

    def load_markets(self) -> dict:
        return self.ex.load_markets()

    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        o = self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(o, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        return df

    def fetch_funding_rate(self, symbol: str):
        try:
            if getattr(self.ex, "has", {}).get("fetchFundingRate", False):
                fr = self.ex.fetch_funding_rate(symbol)
                val = fr.get("fundingRate")
                if val is None:
                    return None
                return float(val) * 100.0 if abs(val) < 1 else float(val)
            return None
        except Exception:
            return None
