from abc import ABC, abstractmethod
import pandas as pd

class BaseProvider(ABC):
    @abstractmethod
    def load_markets(self) -> dict:
        ...

    @abstractmethod
    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        ...

    def fetch_funding_rate(self, symbol: str):
        # optional; return None when not available
        return None
