import os

def _bool(name: str, default=False):
    val = os.getenv(name, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

class Config:
    # ------------ Provider / Exchange ------------
    # Choose data provider:
    #   ccxt  -> uses EXCHANGE (kraken, okx, coinbase, etc.)
    #   blofin -> uses BloFin SDK if available, else REST fallback
    PROVIDER   = os.getenv("PROVIDER", "ccxt")          # "ccxt" | "blofin"
    EXCHANGE   = os.getenv("EXCHANGE", "kraken")        # used when PROVIDER=ccxt

    # ------------ Symbols / timeframe ------------
    SYMBOLS    = [s.strip() for s in os.getenv("SYMBOLS", "BTC/USD,ETH/USD").split(",") if s.strip()]
    TIMEFRAME  = os.getenv("TIMEFRAME", "5m")
    MIN_BARS   = int(os.getenv("MIN_BARS", "400"))

    # ------------ Signal params ------------
    LEVERAGE   = int(os.getenv("LEVERAGE", "20"))
    RISK_ATR   = float(os.getenv("RISK_ATR", "2.2"))
    PULL_L     = float(os.getenv("PULL_L", "0.35"))
    PULL_U     = float(os.getenv("PULL_U", "0.20"))
    TP_MULT    = [float(x.strip()) for x in os.getenv("TP_MULT", "0.8,1.6,2.4,3.5,4.2,5.0").split(",")]

    # ------------ Filters ------------
    MIN_ADX    = float(os.getenv("MIN_ADX", "18"))
    VOL_MULT   = float(os.getenv("VOL_MULT", "1.4"))   # last bar vol >= VOL_MULT * SMA20

    # Funding filter (only applies if provider supports funding rates)
    ENABLE_FUNDING_FILTER = _bool("ENABLE_FUNDING_FILTER", False)
    MAX_ABS_FUNDING = float(os.getenv("MAX_ABS_FUNDING", "0.05"))  # percent

    # ------------ New: Cooldown + HTF confirm ------------
    COOLDOWN_BARS = int(os.getenv("COOLDOWN_BARS", "12"))  # lock out repeat signals for N closed bars
    REQUIRE_TREND_HTF = _bool("REQUIRE_TREND_HTF", True)
    HTF = os.getenv("HTF", "1h")  # higher timeframe used for trend confirmation

    # ------------ Runtime ------------
    POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
