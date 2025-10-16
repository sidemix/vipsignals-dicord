import os

# add near the top
PROVIDER = os.getenv("PROVIDER", "ccxt")  # ccxt | blofin
EXCHANGE = os.getenv("EXCHANGE", "kraken")  # if PROVIDER=ccxt
# If PROVIDER=blofin, EXCHANGE is ignored.


def env_bool(name: str, default=False):
    val = os.getenv(name, str(default)).strip().lower()
    return val in ("1","true","yes","on")

class Config:
    EXCHANGE   = os.getenv("EXCHANGE", "binance")
    SYMBOLS    = [s.strip() for s in os.getenv("SYMBOLS","MTL/USDT").split(",") if s.strip()]
    TIMEFRAME  = os.getenv("TIMEFRAME", "5m")
    MIN_BARS   = int(os.getenv("MIN_BARS", "400"))

    LEVERAGE   = int(os.getenv("LEVERAGE", "20"))
    RISK_ATR   = float(os.getenv("RISK_ATR", "2.2"))
    PULL_L     = float(os.getenv("PULL_L", "0.35"))
    PULL_U     = float(os.getenv("PULL_U", "0.20"))
    TP_MULT    = [float(x.strip()) for x in os.getenv("TP_MULT","0.8,1.6,2.4,3.5,4.2,5.0").split(",")]

    MIN_ADX    = float(os.getenv("MIN_ADX", "18"))
    VOL_MULT   = float(os.getenv("VOL_MULT", "1.4"))
    ENABLE_FUNDING_FILTER = env_bool("ENABLE_FUNDING_FILTER", False)
    MAX_ABS_FUNDING = float(os.getenv("MAX_ABS_FUNDING", "0.05"))  # percent

    POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))

