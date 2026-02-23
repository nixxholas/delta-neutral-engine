"""
hl_client.py — Hyperliquid perp client for the funding farm.

Wraps the official hyperliquid-python-sdk for:
- Market open / close (with slippage control)
- Position fetching
- Funding rate fetching (hourly, annualised to APY)
- Account equity
- Leverage setting
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)

# Hyperliquid funding is hourly — annualise accordingly
HL_PERIODS_PER_YEAR = 24 * 365


@dataclass
class HLPosition:
    symbol: str
    size: float          # signed (positive = long, negative = short)
    entry_price: float
    unrealized_pnl: float
    funding_rate: float  # current hourly rate
    funding_apy: float   # annualised %


class HLClient:
    """
    Thin async-compatible wrapper around the Hyperliquid SDK.

    The SDK itself is synchronous, so all methods run synchronously
    but are designed to be called from asyncio via run_in_executor
    or directly (they're fast enough not to need that for farming).
    """

    MAINNET_URL = "https://api.hyperliquid.xyz"
    TESTNET_URL = "https://api.hyperliquid-testnet.xyz"

    def __init__(
        self,
        private_key: str,
        public_address: str,
        testnet: bool = False,
    ):
        self.private_key   = private_key
        self.public_address = public_address
        self.testnet       = testnet
        self._info         = None
        self._exchange     = None
        self._asset_idx: dict[str, int] = {}   # symbol → universe index (for sz decimals)
        self._sz_decimals: dict[str, int] = {}  # symbol → lot-size decimal places

    # ── Init ─────────────────────────────────────────────────────────────────

    def connect(self) -> None:
        from hyperliquid.info import Info
        from hyperliquid.exchange import Exchange
        import eth_account

        api_url = self.TESTNET_URL if self.testnet else self.MAINNET_URL
        self._info = Info(api_url, skip_ws=True)

        account = eth_account.Account.from_key(self.private_key)
        self._exchange = Exchange(
            account,
            api_url,
            account_address=self.public_address,
        )

        # Cache universe metadata for sz_decimals (lot-size precision)
        meta = self._info.meta()
        for i, asset in enumerate(meta.get("universe", [])):
            name = asset.get("name", "")
            self._asset_idx[name] = i
            self._sz_decimals[name] = int(asset.get("szDecimals", 3))

        mode = "testnet" if self.testnet else "mainnet"
        logger.info("hl_connected", mode=mode,
                    addr=self.public_address[:10] + "...",
                    assets=len(self._asset_idx))

    # ── Precision ─────────────────────────────────────────────────────────────

    def round_size(self, symbol: str, size: float) -> float:
        """Round size to exchange-specified decimal places."""
        decimals = self._sz_decimals.get(symbol, 3)
        return round(size, decimals)

    # ── Account ──────────────────────────────────────────────────────────────

    def get_equity(self) -> float:
        """Return account equity in USD."""
        try:
            state = self._info.user_state(self.public_address)
            return float(state.get("crossMarginSummary", {}).get("accountValue", "0"))
        except Exception as e:
            logger.error("hl_equity_failed", error=str(e)[:120])
            return 0.0

    def get_withdrawable(self) -> float:
        """Return withdrawable (free margin) USD."""
        try:
            state = self._info.user_state(self.public_address)
            return float(state.get("withdrawable", "0"))
        except Exception as e:
            logger.error("hl_withdrawable_failed", error=str(e)[:120])
            return 0.0

    # ── Market Data ──────────────────────────────────────────────────────────

    def get_mid(self, symbol: str) -> Optional[float]:
        """Return current mid price."""
        try:
            mids = self._info.all_mids()
            val = mids.get(symbol)
            return float(val) if val else None
        except Exception as e:
            logger.error("hl_mid_failed", symbol=symbol, error=str(e)[:120])
            return None

    def get_funding_rate(self, symbol: str) -> tuple[float, float]:
        """
        Return (hourly_rate, annualised_apy_pct) for symbol.
        rate < 0 → longs PAY shorts (same as Binance convention)
        """
        try:
            meta, ctxs = self._info.meta_and_asset_ctxs()
            universe = meta.get("universe", [])
            for i, asset in enumerate(universe):
                if asset.get("name") == symbol:
                    ctx = ctxs[i]
                    rate = float(ctx.get("funding", 0))
                    apy  = rate * HL_PERIODS_PER_YEAR * 100
                    return rate, apy
            return 0.0, 0.0
        except Exception as e:
            logger.error("hl_funding_failed", symbol=symbol, error=str(e)[:120])
            return 0.0, 0.0

    def get_all_funding_rates(self) -> dict[str, tuple[float, float]]:
        """
        Fetch all funding rates in one call.
        Returns {symbol: (hourly_rate, apy_pct)}.
        rate < 0 → longs earn (Binance-equivalent convention).
        """
        try:
            meta, ctxs = self._info.meta_and_asset_ctxs()
            universe = meta.get("universe", [])
            result = {}
            for i, asset in enumerate(universe):
                name = asset.get("name", "")
                if i < len(ctxs):
                    rate = float(ctxs[i].get("funding", 0))
                    result[name] = (rate, rate * HL_PERIODS_PER_YEAR * 100)
            return result
        except Exception as e:
            logger.error("hl_all_funding_failed", error=str(e)[:120])
            return {}

    def get_24h_volume(self, symbol: str) -> float:
        """Return 24h notional volume in USD."""
        try:
            meta, ctxs = self._info.meta_and_asset_ctxs()
            universe = meta.get("universe", [])
            for i, asset in enumerate(universe):
                if asset.get("name") == symbol and i < len(ctxs):
                    return float(ctxs[i].get("dayNtlVlm", 0))
            return 0.0
        except Exception as e:
            logger.error("hl_volume_failed", symbol=symbol, error=str(e)[:120])
            return 0.0

    # ── Positions ────────────────────────────────────────────────────────────

    def get_position(self, symbol: str) -> Optional[HLPosition]:
        """Return current open position for symbol, or None if flat."""
        try:
            state = self._info.user_state(self.public_address)
            # Also fetch current funding rate
            rate, apy = self.get_funding_rate(symbol)
            for pos in state.get("assetPositions", []):
                p = pos.get("position", {})
                if p.get("coin") == symbol:
                    size = float(p.get("szi", "0"))
                    if abs(size) > 1e-9:
                        return HLPosition(
                            symbol=symbol,
                            size=size,
                            entry_price=float(p.get("entryPx", "0")),
                            unrealized_pnl=float(p.get("unrealizedPnl", "0")),
                            funding_rate=rate,
                            funding_apy=apy,
                        )
            return None
        except Exception as e:
            logger.error("hl_position_failed", symbol=symbol, error=str(e)[:120])
            return None

    def get_all_positions(self) -> list[HLPosition]:
        """Return all non-zero perp positions."""
        try:
            state = self._info.user_state(self.public_address)
            rates = self.get_all_funding_rates()
            result = []
            for pos in state.get("assetPositions", []):
                p = pos.get("position", {})
                sym  = p.get("coin", "")
                size = float(p.get("szi", "0"))
                if abs(size) > 1e-9:
                    rate, apy = rates.get(sym, (0.0, 0.0))
                    result.append(HLPosition(
                        symbol=sym,
                        size=size,
                        entry_price=float(p.get("entryPx", "0")),
                        unrealized_pnl=float(p.get("unrealizedPnl", "0")),
                        funding_rate=rate,
                        funding_apy=apy,
                    ))
            return result
        except Exception as e:
            logger.error("hl_all_positions_failed", error=str(e)[:120])
            return []

    # ── Orders ───────────────────────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: int, cross: bool = True) -> bool:
        """Set leverage for a symbol."""
        try:
            resp = self._exchange.update_leverage(leverage, symbol, is_cross=cross)
            ok = resp.get("status") == "ok"
            logger.info("hl_leverage_set", symbol=symbol, leverage=leverage, ok=ok)
            return ok
        except Exception as e:
            logger.error("hl_leverage_failed", symbol=symbol, error=str(e)[:120])
            return False

    def market_open(
        self,
        symbol: str,
        is_buy: bool,
        size: float,
        slippage: float = 0.02,
    ) -> tuple[bool, str, float]:
        """
        Open a market position.
        Returns (success, order_id, fill_price).
        slippage: max acceptable slippage (default 2%).
        """
        size = self.round_size(symbol, size)
        if size <= 0:
            logger.error("hl_open_invalid_size", symbol=symbol, size=size)
            return False, "", 0.0
        try:
            resp = self._exchange.market_open(symbol, is_buy, size, slippage=slippage)
            if resp.get("status") == "ok":
                statuses = resp.get("response", {}).get("data", {}).get("statuses", [{}])
                s = statuses[0] if statuses else {}
                filled = s.get("filled", {})
                oid    = str(filled.get("oid", ""))
                px     = float(filled.get("avgPx", 0) or 0)
                logger.info("hl_market_open", symbol=symbol,
                            side="buy" if is_buy else "sell",
                            size=size, fill=px, oid=oid)
                return True, oid, px
            else:
                err = str(resp)
                logger.error("hl_market_open_failed", symbol=symbol, resp=err[:200])
                return False, "", 0.0
        except Exception as e:
            logger.error("hl_market_open_exception", symbol=symbol, error=str(e)[:200])
            return False, "", 0.0

    def market_close(
        self,
        symbol: str,
        size: Optional[float] = None,
        slippage: float = 0.02,
    ) -> tuple[bool, float]:
        """
        Close a position (fully or partially).
        Returns (success, fill_price).
        If size is None, closes the full position.
        """
        try:
            sz = self.round_size(symbol, size) if size else None
            resp = self._exchange.market_close(symbol, sz=sz, slippage=slippage)
            if resp.get("status") == "ok":
                statuses = resp.get("response", {}).get("data", {}).get("statuses", [{}])
                s = statuses[0] if statuses else {}
                filled = s.get("filled", {})
                px = float(filled.get("avgPx", 0) or 0)
                logger.info("hl_market_close", symbol=symbol, size=sz, fill=px)
                return True, px
            else:
                err = str(resp)
                logger.error("hl_market_close_failed", symbol=symbol, resp=err[:200])
                return False, 0.0
        except Exception as e:
            logger.error("hl_market_close_exception", symbol=symbol, error=str(e)[:200])
            return False, 0.0


# ── Factory ───────────────────────────────────────────────────────────────────

def make_hl_client() -> HLClient:
    """Create an HLClient from environment variables."""
    private_key = os.getenv("HL_PRIVATE_KEY", "")
    public_addr = os.getenv("HL_PUBLIC_ADDRESS", "")
    testnet     = os.getenv("HL_TESTNET", "false").lower() == "true"
    if not private_key or not public_addr:
        raise RuntimeError(
            "HL_PRIVATE_KEY and HL_PUBLIC_ADDRESS must be set in .env"
        )
    client = HLClient(private_key, public_addr, testnet)
    client.connect()
    return client
