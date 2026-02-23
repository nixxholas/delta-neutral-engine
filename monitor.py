"""
monitor.py — Real-time WebSocket position monitor for the funding farm.

Subscribes to Binance's !markPrice@arr@1s stream (single connection,
all symbols every second). Updates active position rates in real-time
and fires exit signals when rate crosses the exit threshold.

Architecture:
  - run_monitor() is an asyncio task that runs concurrently with farm.py
  - Communicates via asyncio.Queue (rate updates, exit signals)
  - Auto-reconnects with exponential backoff on disconnect
  - Filters to only symbols in active positions (no wasted work)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import structlog
import websockets
from dotenv import load_dotenv

load_dotenv("/Users/nicholas/workspace/funding-farm/.env")

logger = structlog.get_logger(__name__)

# Binance futures WS endpoints
_IS_DEMO    = os.getenv("BINANCE_DEMO", "false").lower() == "true"
_WS_BASE    = ("wss://stream.binancefuture.com"
               if _IS_DEMO else
               "wss://fstream.binance.com")
_STREAM_URL = f"{_WS_BASE}/ws/!markPrice@arr@1s"


@dataclass
class RateUpdate:
    symbol:       str    # e.g. "BTC"
    ccxt_symbol:  str    # e.g. "BTC/USDT:USDT"
    raw_symbol:   str    # e.g. "BTCUSDT"
    mark_price:   float
    funding_rate: float  # current 8h rate
    apy:          float
    next_funding_ts: float
    ts:           float  = 0.0

    def __post_init__(self):
        if not self.ts:
            self.ts = time.time()


class FarmMonitor:
    """
    Long-running asyncio task that maintains a real-time view of
    funding rates for all active positions.
    """

    def __init__(
        self,
        exit_apy_threshold: float,
        on_rate_update: Optional[Callable[[RateUpdate], None]] = None,
        on_exit_signal: Optional[Callable[[RateUpdate], None]] = None,
    ):
        self.exit_apy_threshold = exit_apy_threshold
        self.on_rate_update     = on_rate_update
        self.on_exit_signal     = on_exit_signal

        # Map rawSymbol (e.g. "BTCUSDT") → direction ("short_perp"|"long_perp")
        self._watched: Dict[str, str]         = {}
        # rawSymbol → latest RateUpdate
        self._latest:  Dict[str, RateUpdate]  = {}
        self._running  = False
        self._task: Optional[asyncio.Task]    = None

    # ── Public API ────────────────────────────────────────────────────────────

    def watch(self, ccxt_symbol: str, base: str, direction: str) -> None:
        """Register a symbol to monitor."""
        raw = ccxt_symbol.replace("/USDT:USDT", "USDT").upper()
        self._watched[raw] = direction
        logger.info("monitor_watching", symbol=base, direction=direction, raw=raw)

    def unwatch(self, ccxt_symbol: str) -> None:
        """Stop monitoring a symbol."""
        raw = ccxt_symbol.replace("/USDT:USDT", "USDT").upper()
        self._watched.pop(raw, None)
        self._latest.pop(raw, None)

    def get_latest(self, ccxt_symbol: str) -> Optional[RateUpdate]:
        raw = ccxt_symbol.replace("/USDT:USDT", "USDT").upper()
        return self._latest.get(raw)

    def all_rates(self) -> Dict[str, RateUpdate]:
        return dict(self._latest)

    async def start(self) -> None:
        """Start the monitor as a background task."""
        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        logger.info("monitor_started", url=_STREAM_URL)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _run_forever(self) -> None:
        """WebSocket loop with exponential backoff on disconnect."""
        backoff = 1.0
        while self._running:
            try:
                await self._connect_and_stream()
                backoff = 1.0   # reset on clean exit
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("monitor_reconnecting",
                               error=str(e)[:100], wait_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _connect_and_stream(self) -> None:
        logger.info("monitor_connecting", url=_STREAM_URL)
        async with websockets.connect(
            _STREAM_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            logger.info("monitor_connected")
            async for raw_msg in ws:
                if not self._running:
                    return
                try:
                    self._handle_message(raw_msg)
                except Exception as e:
                    logger.warning("monitor_parse_error", error=str(e)[:80])

    def _handle_message(self, raw: str) -> None:
        data = json.loads(raw)
        # !markPrice@arr@1s sends a list
        items = data if isinstance(data, list) else [data]
        for item in items:
            if item.get("e") != "markPriceUpdate":
                continue
            raw_sym = item.get("s", "")
            if raw_sym not in self._watched:
                continue

            rate  = float(item.get("r") or 0)
            price = float(item.get("p") or 0)
            apy   = rate * 3 * 365 * 100
            nft   = float(item.get("T") or 0) / 1000

            base    = raw_sym.replace("USDT", "")
            cc_sym  = f"{base}/USDT:USDT"
            direction = self._watched[raw_sym]

            update = RateUpdate(
                symbol          = base,
                ccxt_symbol     = cc_sym,
                raw_symbol      = raw_sym,
                mark_price      = price,
                funding_rate    = rate,
                apy             = apy,
                next_funding_ts = nft,
                ts              = time.time(),
            )
            self._latest[raw_sym] = update

            if self.on_rate_update:
                try:
                    self.on_rate_update(update)
                except Exception:
                    pass

            # Exit signal: rate effectively gone / reversed sign / below threshold
            # For short_perp: earn when rate > 0 → exit if rate*APY < threshold
            # For long_perp:  earn when rate < 0 → exit if (-rate)*APY < threshold
            effective_apy = apy if direction == "short_perp" else -apy
            if effective_apy < self.exit_apy_threshold:
                logger.warning("exit_signal_ws",
                               symbol=base, effective_apy=round(effective_apy, 1),
                               threshold=self.exit_apy_threshold)
                if self.on_exit_signal:
                    try:
                        self.on_exit_signal(update)
                    except Exception:
                        pass


# ── Standalone test ───────────────────────────────────────────────────────────

async def _demo():
    """Quick demo: watch BTC, SXP, QUICK for 30s."""
    exit_signals = []

    def on_update(u: RateUpdate):
        print(f"  [{u.symbol:6s}] rate={u.funding_rate*100:+.4f}%/8h  "
              f"apy={u.apy:+.1f}%  price=${u.mark_price:,.2f}  "
              f"next_funding={max(0,(u.next_funding_ts-time.time()))/3600:.1f}h")

    def on_exit(u: RateUpdate):
        print(f"  ⚠️  EXIT SIGNAL: {u.symbol} apy={u.apy:+.1f}%")
        exit_signals.append(u.symbol)

    mon = FarmMonitor(exit_apy_threshold=5.0,
                      on_rate_update=on_update,
                      on_exit_signal=on_exit)
    mon.watch("BTC/USDT:USDT",   "BTC",   "short_perp")
    mon.watch("SXP/USDT:USDT",   "SXP",   "short_perp")
    mon.watch("QUICK/USDT:USDT", "QUICK", "short_perp")

    print(f"Connecting to {_STREAM_URL} ...")
    await mon.start()
    await asyncio.sleep(15)
    await mon.stop()
    print(f"\nLatest rates captured:")
    for r in mon.all_rates().values():
        print(f"  {r.symbol}: {r.funding_rate*100:+.4f}%/8h  ${r.mark_price:,.4f}")


if __name__ == "__main__":
    asyncio.run(_demo())
