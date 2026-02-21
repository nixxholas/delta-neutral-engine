"""
farm.py — Delta-neutral funding rate farm.

Strategy: BUY spot + SHORT perp → collect funding when longs pay shorts.
         (Or LONG perp + SHORT spot when rate is negative)

On Binance demo, only the perp leg is executed.
Spot leg is tracked notionally (add real spot when going live).

Usage:
    python farm.py --run          # open best position + monitor
    python farm.py --status       # show current positions + P&L
    python farm.py --close        # close all farm positions
    python farm.py --scan         # just scan, don't open
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import structlog
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from scanner import scan, FundingOpp

load_dotenv()
logger = structlog.get_logger(__name__)
console = Console()

STATE_FILE = Path("/tmp/funding-farm-state.json")

# ── Config ─────────────────────────────────────────────────────────────────────

POSITION_SIZE_USDT  = float(os.getenv("FARM_SIZE_USDT", "500"))    # notional per leg
MIN_ENTRY_APY       = float(os.getenv("FARM_MIN_ENTRY_APY", "15")) # % APY to enter
EXIT_APY_THRESHOLD  = float(os.getenv("FARM_EXIT_APY", "5"))       # % APY to exit
SCAN_INTERVAL_S     = int(os.getenv("FARM_SCAN_INTERVAL", "1800")) # re-scan every 30m
MAX_POSITIONS       = int(os.getenv("FARM_MAX_POSITIONS", "3"))     # concurrent farms


# ── State ──────────────────────────────────────────────────────────────────────

@dataclass
class FarmPosition:
    symbol: str
    ccxt_symbol: str
    direction: str          # "short_perp" or "long_perp"
    perp_order_id: str
    perp_entry_price: float
    perp_size: float        # in base asset
    notional_usdt: float
    entry_rate_8h: float
    entry_apy: float
    entry_ts: float
    funding_collected: float = 0.0
    last_rate: float = 0.0
    last_rate_ts: float = 0.0


def load_state() -> list[FarmPosition]:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text())
            return [FarmPosition(**p) for p in data]
        except Exception:
            pass
    return []


def save_state(positions: list[FarmPosition]) -> None:
    STATE_FILE.write_text(json.dumps([asdict(p) for p in positions], indent=2))


# ── Exchange ───────────────────────────────────────────────────────────────────

def _make_exchange():
    import ccxt.async_support as ccxt
    ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })
    if os.getenv("BINANCE_DEMO", "false").lower() == "true":
        test_urls = ex.urls.get("test", {})
        api_urls = ex.urls.setdefault("api", {})
        for group, url in test_urls.items():
            if "fapi" in group or "dapi" in group:
                api_urls[group] = url
        _orig = ex.fetch
        async def _no_sapi(url, method="GET", headers=None, body=None):
            if "/sapi/" in str(url): return []
            return await _orig(url, method=method, headers=headers, body=body)
        ex.fetch = _no_sapi
    return ex


# ── Core actions ───────────────────────────────────────────────────────────────

async def open_position(opp: FundingOpp) -> Optional[FarmPosition]:
    """Open the perp leg of the basis trade."""
    ex = _make_exchange()
    try:
        await ex.load_markets()
        mkt = ex.market(opp.ccxt_symbol)
        mid_resp = await ex.fetch_ticker(opp.ccxt_symbol)
        mid = float(mid_resp.get("last") or mid_resp.get("markPrice") or 0)
        if not mid:
            logger.error("open_position_no_price", symbol=opp.symbol)
            return None

        size = POSITION_SIZE_USDT / mid
        # Respect min notional and step size
        limits = mkt.get("limits", {})
        prec   = mkt.get("precision", {})
        step   = prec.get("amount") or 0.001
        min_qty = (limits.get("amount") or {}).get("min") or 0.0
        import math
        size = math.floor(size / step) * step
        size = max(size, min_qty)

        # Direction: if short_perp → sell; if long_perp → buy
        side = "sell" if opp.direction == "short_perp" else "buy"
        slippage = 0.001
        price = round(mid * (1 - slippage) if side == "sell" else mid * (1 + slippage), 2)

        logger.info("opening_perp_leg", symbol=opp.symbol, side=side,
                    size=size, price=price, apy=round(opp.apy, 1))

        order = await ex.create_limit_order(
            opp.ccxt_symbol, side, size, price,
            params={"reduceOnly": False}
        )
        order_id = str(order.get("id", ""))
        logger.info("perp_leg_opened", order_id=order_id, symbol=opp.symbol)

        pos = FarmPosition(
            symbol=opp.symbol,
            ccxt_symbol=opp.ccxt_symbol,
            direction=opp.direction,
            perp_order_id=order_id,
            perp_entry_price=price,
            perp_size=size,
            notional_usdt=size * price,
            entry_rate_8h=opp.rate_8h,
            entry_apy=opp.apy,
            entry_ts=time.time(),
            last_rate=opp.rate_8h,
            last_rate_ts=time.time(),
        )
        return pos
    finally:
        await ex.close()


async def close_position(pos: FarmPosition) -> bool:
    """Close the perp leg."""
    ex = _make_exchange()
    try:
        await ex.load_markets()
        side = "buy" if pos.direction == "short_perp" else "sell"
        mid_resp = await ex.fetch_ticker(pos.ccxt_symbol)
        mid = float(mid_resp.get("last") or 0)
        slippage = 0.001
        price = round(mid * (1 + slippage) if side == "buy" else mid * (1 - slippage), 2)
        order = await ex.create_limit_order(
            pos.ccxt_symbol, side, pos.perp_size, price,
            params={"reduceOnly": True}
        )
        logger.info("perp_leg_closed", symbol=pos.symbol, order_id=order.get("id"))
        return True
    except Exception as e:
        logger.error("close_position_failed", symbol=pos.symbol, error=str(e))
        return False
    finally:
        await ex.close()


async def refresh_funding(positions: list[FarmPosition]) -> None:
    """Refresh current funding rates and accrue collected funding."""
    if not positions:
        return
    ex = _make_exchange()
    try:
        for pos in positions:
            try:
                info = await ex.fetch_funding_rate(pos.ccxt_symbol)
                rate = float(info.get("fundingRate") or 0)
                pos.last_rate = rate
                pos.last_rate_ts = time.time()
                # Estimate funding accrued since last update (approximate)
                elapsed_8h = (time.time() - pos.entry_ts) / (8 * 3600)
                direction_sign = -1 if pos.direction == "short_perp" else 1
                pos.funding_collected = pos.notional_usdt * pos.entry_rate_8h * elapsed_8h * direction_sign
            except Exception as e:
                logger.warning("refresh_funding_error", symbol=pos.symbol, error=str(e))
    finally:
        await ex.close()


# ── Display ────────────────────────────────────────────────────────────────────

def show_status(positions: list[FarmPosition]) -> None:
    if not positions:
        console.print("[yellow]No active farm positions.[/yellow]")
        return

    t = Table(title="Active Funding Farm Positions")
    t.add_column("Symbol"); t.add_column("Strategy"); t.add_column("Notional")
    t.add_column("Entry APY"); t.add_column("Current Rate/8h"); t.add_column("Est. Funding Earned")
    t.add_column("Age")

    for p in positions:
        age_h = (time.time() - p.entry_ts) / 3600
        strat = "Short perp" if p.direction == "short_perp" else "Long perp"
        cur_rate = f"{p.last_rate*100:+.4f}%"
        t.add_row(
            p.symbol, strat,
            f"${p.notional_usdt:.0f}",
            f"{p.entry_apy:.1f}%",
            cur_rate,
            f"${p.funding_collected:.4f}",
            f"{age_h:.1f}h",
        )

    console.print(t)


# ── Main loop ──────────────────────────────────────────────────────────────────

async def run_farm() -> None:
    logger.info("farm_starting", size_usdt=POSITION_SIZE_USDT,
                min_apy=MIN_ENTRY_APY, exit_apy=EXIT_APY_THRESHOLD)
    positions = load_state()

    while True:
        # Refresh funding on open positions
        await refresh_funding(positions)

        # Check exits
        to_close = []
        for pos in positions:
            cur_apy = pos.last_rate * 3 * 365 * 100
            if abs(cur_apy) < EXIT_APY_THRESHOLD:
                logger.info("exit_triggered", symbol=pos.symbol,
                            cur_apy=round(cur_apy, 2), threshold=EXIT_APY_THRESHOLD)
                to_close.append(pos)

        for pos in to_close:
            if await close_position(pos):
                positions.remove(pos)
                console.print(f"[red]Closed {pos.symbol} — funding dropped to {pos.last_rate*100*3*365:.1f}% APY[/red]")

        # Scan for new opportunities
        if len(positions) < MAX_POSITIONS:
            console.print(f"\n[cyan]Scanning for funding opportunities (min {MIN_ENTRY_APY}% APY)...[/cyan]")
            opps = await scan(min_apy=MIN_ENTRY_APY, top_n=10)
            existing = {p.symbol for p in positions}

            for opp in opps:
                if len(positions) >= MAX_POSITIONS:
                    break
                if opp.symbol in existing:
                    continue
                if opp.direction != "short_perp":
                    # Skip long_perp for now (requires spot short/borrowing)
                    continue

                console.print(f"[green]Opening {opp.symbol}: {opp.apy:.1f}% APY "
                              f"({opp.rate_8h*100:.4f}%/8h)[/green]")
                pos = await open_position(opp)
                if pos:
                    positions.append(pos)
                    console.print(f"[green]✓ {opp.symbol} position opened "
                                  f"(${pos.notional_usdt:.0f} notional)[/green]")

        save_state(positions)
        show_status(positions)

        console.print(f"\n[dim]Next scan in {SCAN_INTERVAL_S//60}min. "
                      f"Ctrl+C to stop.[/dim]")
        await asyncio.sleep(SCAN_INTERVAL_S)


# ── CLI ────────────────────────────────────────────────────────────────────────

async def main():
    p = argparse.ArgumentParser(description="Funding rate farm")
    p.add_argument("--run",    action="store_true", help="Start farming")
    p.add_argument("--status", action="store_true", help="Show current positions")
    p.add_argument("--close",  action="store_true", help="Close all positions")
    p.add_argument("--scan",   action="store_true", help="Scan only, don't trade")
    args = p.parse_args()

    if args.scan:
        opps = await scan(min_apy=5.0, top_n=25)
        from scanner import print_table
        print_table(opps)
        return

    if args.status:
        positions = load_state()
        await refresh_funding(positions)
        show_status(positions)
        return

    if args.close:
        positions = load_state()
        for pos in positions:
            await close_position(pos)
        save_state([])
        console.print("[red]All positions closed.[/red]")
        return

    if args.run:
        await run_farm()
        return

    p.print_help()


if __name__ == "__main__":
    asyncio.run(main())
