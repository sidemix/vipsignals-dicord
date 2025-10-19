# exchanges.py
"""
Unified market access for the scanner.

- Primary data source: provider.fetch_ohlcv_df(...) (Hyperliquid/BloFin/etc.)
- Optional ccxt bridge (only if EXCHANGE is set to a real ccxt id).

Env keys this module respects:
  PROVIDER=hyperliquid|blofin|...
  EXCHANGE=none|kraken|binance|...
  EXCHANGE_LABEL=string shown in startup banner (fallback: PROVIDER/EXCHANGE)
  ENABLE_FUNDING_FILTER=true|false  (only applies when ccxt is enabled)
"""

from __future__ import annotations
import os
from typing import Dict, Any, Optional, List

# ------------------------
# Provider factory
# ------------------------

def make_provider(name: Optional[str] = None):
    """Return a provider instance based on env/argument."""
    provider = (name or os.getenv("PROVIDER", "")).strip().lower()
    if provider == "blofin":
        from providers.blofin_provider import BlofinProvider
        return BlofinProvider()
    if provider == "hyperliquid":
        from providers.hyperliquid_provider import HyperliquidProvider
        return HyperliquidProvider()
    # default to Hyperliquid if unspecified
    from providers.hyperliquid_provider import HyperliquidProvider
    return HyperliquidProvider()


# Build the global provider once
PROVIDER = make_provider()


# ------------------------
# ccxt (optional)
# ------------------------

_EXCHANGE_ID = os.getenv("EXCHANGE", "").strip().lower()
# Turn ccxt completely off if EXCHANGE is "", "none" or "hyperliquid"
_USE_CCXT = _EXCHANGE_ID not in ("", "none", "hyperliquid")

_ccxt = None
_ccxt_markets: Dict[str, Any] = {}

if _USE_CCXT:
    try:
        import ccxt  # type: ignore
        if not hasattr(ccxt, _EXCHANGE_ID):
            raise RuntimeError(f"Unknown ccxt exchange id '{_EXCHANGE_ID}'")
        _ccxt = getattr(ccxt, _EXCHANGE_ID)({
            "enableRateLimit": True,
            # you may add keys/secret here from env if you ever need private endpoints
        })
        # Load markets once (ignore failures; we can still run without them)
        try:
            _ccxt_markets = _ccxt.load_markets() or {}
        except Exception:
            _ccxt_markets = {}
    except Exception as e:
        # Hard-disable ccxt on any initialization error
        _USE_CCXT = False
        _ccxt = None
        _ccxt_markets = {}
        print(f"[ccxt] disabled: {e}")


def exchange_label() -> str:
    """What to print in the banner."""
    return (
        os.getenv("EXCHANGE_LABEL", "").strip()
        or (_EXCHANGE_ID if _USE_CCXT else os.getenv("PROVIDER", "scanner"))
    )


# ------------------------
# Public helpers used by the scanner
# ------------------------

def has_market(symbol: str) -> bool:
    """
    If ccxt is enabled, verify the symbol exists on that venue.
    If ccxt is disabled, never block — return True.
    """
    if not _USE_CCXT:
        return True
    return symbol in _ccxt_markets


def fetch_ohlcv_df(symbol: str, timeframe: str, limit: int):
    """
    Primary path: provider (Hyperliquid/BloFin/etc.)
    Optional fallback: ccxt (ONLY if provider fails and ccxt is enabled).
    """
    # 1) Provider first
    try:
        return PROVIDER.fetch_ohlcv_df(symbol=symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        # If provider fails and ccxt is allowed, try ccxt as a fallback
        if _USE_CCXT:
            try:
                # ccxt returns list of [ts, open, high, low, close, volume] in ms
                raw = _ccxt.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                import pandas as pd
                df = pd.DataFrame(raw, columns=["time","open","high","low","close","volume"])
                df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
                return df
            except Exception as ee:
                raise RuntimeError(f"Provider+ccxt OHLCV failed for {symbol}: {ee}") from ee
        else:
            raise RuntimeError(f"Provider OHLCV failed for {symbol}: {e}") from e


def fetch_funding_rate(symbol: str):
    """
    Optional funding rate query.
    - If provider implements it, use that.
    - If ccxt is enabled and provider returns None, try ccxt.
    - If neither is available, return None (don’t block scanning).
    """
    # Provider hook (many providers just return None)
    try:
        fr = getattr(PROVIDER, "fetch_funding_rate", lambda s: None)(symbol)
        if fr is not None:
            return fr
    except Exception:
        pass

    if _USE_CCXT and os.getenv("ENABLE_FUNDING_FILTER", "").strip().lower() in ("1","true","yes","on"):
        try:
            # ccxt perps funding (not all exchanges support this)
            if hasattr(_ccxt, "fetchFundingRate"):
                return _ccxt.fetchFundingRate(symbol)  # type: ignore
        except Exception:
            return None

    return None


# ------------------------
# Convenience (optional)
# ------------------------

def list_markets_from_ccxt() -> List[str]:
    """Return all market symbols from ccxt (empty if ccxt disabled)."""
    if not _USE_CCXT:
        return []
    return list(_ccxt_markets.keys())


def using_ccxt() -> bool:
    return _USE_CCXT
