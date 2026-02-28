#!/usr/bin/env python3
"""
cross_arb.py — Cross-exchange funding rate arbitrage.

Strategy:
  For each token, compare Binance USDT-M perp funding vs Hyperliquid perp funding.
  When the differential is large enough, open:
    LONG on the exchange with more-negative rate (earns funding)
    SHORT on the exchange with less-negative (or positive) rate (pays less / earns)

  Both legs are delta-neutral:  long perp + short perp = zero net exposure.
  No spot leg needed.  No borrow cost.

  Net APY = |rate_earn_side| + |rate_other_side| (when opposite signs)
          = |rate_earn_side| - |rate_other_side| (when same sign, earn differential)

Rate convention (both exchanges):
  rate < 0 → longs PAY shorts   → LONG earns
  rate > 0 → shorts PAY longs   → SHORT earns

Usage:
    python3 cross_arb.py --run       # live trading
    python3 cross_arb.py --scan      # scan-only, no trades
    python3 cross_arb.py --status    # show active positions
    python3 cross_arb.py --close SYMBOL  # manually close a position
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import structlog
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()

logger = structlog.get_logger(__name__)
console = Console()

# ── Config ────────────────────────────────────────────────────────────────────

MIN_ARB_APY          = float(os.getenv("CARB_MIN_ARB_APY",        "15.0"))  # min differential to enter
EXIT_ARB_APY         = float(os.getenv("CARB_EXIT_ARB_APY",        "5.0"))  # close when differential < this
POSITION_SIZE_USDT   = float(os.getenv("CARB_POSITION_SIZE_USDT", "300.0")) # per-position notional
MAX_POSITION_SIZE    = float(os.getenv("CARB_MAX_POSITION_SIZE", "1000.0")) # cap per position
MAX_POSITIONS        = int(os.getenv(  "CARB_MAX_POSITIONS",         "30"))
SCAN_INTERVAL_S      = int(os.getenv(  "CARB_SCAN_INTERVAL",       "300"))  # 5 min
MIN_HL_VOL_USD       = float(os.getenv("CARB_MIN_HL_VOL_USD",  "1000000"))  # $1M min HL daily volume
MIN_BIN_VOL_USD      = float(os.getenv("CARB_MIN_BIN_VOL_USD","10000000"))  # $10M min Binance daily vol
PERP_LEVERAGE        = int(os.getenv(  "CARB_LEVERAGE",               "6"))  # 6x leverage (moderate)
SLIPPAGE             = float(os.getenv("CARB_SLIPPAGE",            "0.02"))  # 2% HL market order slippage
RT_FEE_PCT           = 0.0028   # 0.04% perp taker × 2 legs × 2 exchanges (conservative)

STATE_FILE = "/tmp/cross-arb-state.json"
HISTORY_FILE = "/tmp/cross-arb-history.jsonl"
TIMESERIES_FILE = "/tmp/cross-arb-timeseries.jsonl"

BLACKLIST = set(os.getenv("CARB_BLACKLIST", "").split(",")) - {""}

# ── Data ──────────────────────────────────────────────────────────────────────

@dataclass
class ArbOpp:
    symbol:       str
    bin_apy:      float   # Binance annualised APY (negative = longs earn)
    hl_apy:       float   # HL annualised APY      (negative = longs earn)
    net_apy:      float   # expected differential earned (always positive if viable)
    bin_side:     str     # "buy" (long) | "sell" (short) on Binance
    hl_side:      str     # "buy" (long) | "sell" (short) on HL
    bin_vol:      float   # Binance 24h volume
    hl_vol:       float   # HL 24h volume


@dataclass
class ArbPosition:
    symbol:        str
    bin_side:      str    # "buy" | "sell"
    hl_side:       str    # "buy" | "sell"
    bin_size:      float  # contracts on Binance
    hl_size:       float  # contracts on HL
    notional_usdt: float
    entry_bin_apy: float
    entry_hl_apy:  float
    entry_net_apy: float
    entry_ts:      float = field(default_factory=time.time)
    bin_order_id:  str   = ""
    hl_order_id:   str   = ""
    bin_entry_px:  float = 0.0
    hl_entry_px:   float = 0.0
    bin_closed:    bool  = False
    hl_closed:     bool  = False
    # Tracking
    last_bin_apy:  float = 0.0
    last_hl_apy:   float = 0.0
    last_rate_ts:  float = 0.0
    funding_realized_bin: float = 0.0
    funding_realized_hl:  float = 0.0
    needs_close:   bool  = False


# ── State ─────────────────────────────────────────────────────────────────────

def save_state(positions: list[ArbPosition]) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump([asdict(p) for p in positions], f, indent=2)


def load_state() -> list[ArbPosition]:
    if not os.path.exists(STATE_FILE):
        return []
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        return [ArbPosition(**d) for d in data]
    except Exception as e:
        logger.warning("load_state_failed", error=str(e))
        return []


# ── Time-Series Tracking ─────────────────────────────────────────────────────

def log_timeseries_event(event_type: str, data: dict) -> None:
    """Append a time-series event to the JSONL file."""
    try:
        with open(TIMESERIES_FILE, "a") as f:
            f.write(json.dumps({"ts": time.time(), "type": event_type, **data}) + "\n")
    except Exception as e:
        logger.warning("timeseries_log_failed", error=str(e))


def record_position_open(pos: ArbPosition) -> None:
    """Record position open in time-series."""
    log_timeseries_event("position_open", {
        "symbol": pos.symbol,
        "bin_side": pos.bin_side,
        "hl_side": pos.hl_side,
        "notional": pos.notional_usdt,
        "entry_apy": pos.entry_net_apy,
        "entry_ts": pos.entry_ts,
    })


def record_position_close(pos: ArbPosition, exit_apy: float, realized_pnl: float) -> None:
    """Record position close in time-series."""
    log_timeseries_event("position_close", {
        "symbol": pos.symbol,
        "bin_side": pos.bin_side,
        "hl_side": pos.hl_side,
        "notional": pos.notional_usdt,
        "entry_apy": pos.entry_net_apy,
        "exit_apy": exit_apy,
        "entry_ts": pos.entry_ts,
        "close_ts": time.time(),
        "duration_hours": (time.time() - pos.entry_ts) / 3600,
        "realized_pnl": realized_pnl,
    })


def record_hourly_checkpoint(positions: list[ArbPosition], hl_equity: float, bin_equity: float) -> None:
    """Record hourly portfolio snapshot."""
    total_notional = sum(p.notional_usdt for p in positions)
    log_timeseries_event("hourly_checkpoint", {
        "positions_open": len(positions),
        "total_notional": total_notional,
        "hl_equity": hl_equity,
        "bin_equity": bin_equity,
        "total_portfolio": hl_equity + bin_equity,
    })


def load_timeseries_events(event_type: str = None, limit: int = None) -> list[dict]:
    """Load time-series events from file."""
    if not os.path.exists(TIMESERIES_FILE):
        return []
    events = []
    try:
        with open(TIMESERIES_FILE) as f:
            for line in f:
                try:
                    event = json.loads(line.strip())
                    if event_type is None or event.get("type") == event_type:
                        events.append(event)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.warning("timeseries_load_failed", error=str(e))
    
    if limit:
        events = events[-limit:]
    return events


def calculate_historical_stats() -> dict:
    """Calculate historical performance metrics from time-series data."""
    events = load_timeseries_events("position_close")
    if not events:
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "total_realized_pnl": 0.0,
            "avg_pnl_per_trade": 0.0,
            "avg_duration_hours": 0.0,
            "total_notional_traded": 0.0,
        }
    
    winning = [e for e in events if e.get("realized_pnl", 0) > 0]
    losing = [e for e in events if e.get("realized_pnl", 0) < 0]
    
    total_pnl = sum(e.get("realized_pnl", 0) for e in events)
    durations = [e.get("duration_hours", 0) for e in events if e.get("duration_hours")]
    notionals = [e.get("notional", 0) for e in events]
    
    return {
        "total_trades": len(events),
        "winning_trades": len(winning),
        "losing_trades": len(losing),
        "win_rate": len(winning) / len(events) * 100 if events else 0,
        "total_realized_pnl": total_pnl,
        "avg_pnl_per_trade": total_pnl / len(events) if events else 0,
        "avg_duration_hours": sum(durations) / len(durations) if durations else 0,
        "total_notional_traded": sum(notionals),
    }


def show_historical_stats(period: str = "all") -> None:
    """Display historical performance stats."""
    from datetime import datetime, timedelta
    
    events = load_timeseries_events("position_close")
    if not events:
        console.print("[yellow]No closed positions in time-series history.[/yellow]")
        return
    
    # Filter by period
    now = time.time()
    if period == "today":
        start_ts = datetime.now().replace(hour=0, minute=0, second=0).timestamp()
    elif period == "week":
        start_ts = now - 7 * 24 * 3600
    elif period == "month":
        start_ts = now - 30 * 24 * 3600
    else:
        start_ts = 0
    
    filtered = [e for e in events if e.get("close_ts", 0) >= start_ts]
    
    if not filtered:
        console.print(f"[yellow]No closed positions in the last {period}.[/yellow]")
        return
    
    winning = [e for e in filtered if e.get("realized_pnl", 0) > 0]
    losing = [e for e in filtered if e.get("realized_pnl", 0) < 0]
    
    total_pnl = sum(e.get("realized_pnl", 0) for e in filtered)
    durations = [e.get("duration_hours", 0) for e in filtered if e.get("duration_hours")]
    notionals = [e.get("notional", 0) for e in filtered]
    entry_apys = [e.get("entry_apy", 0) for e in filtered]
    
    win_rate = len(winning) / len(filtered) * 100
    avg_dur = sum(durations) / len(durations) if durations else 0
    avg_notional = sum(notionals) / len(notionals) if notionals else 0
    avg_entry_apy = sum(entry_apys) / len(entry_apys) if entry_apys else 0
    
    # Calculate estimated daily return
    total_days = sum(durations) / 24 if durations else 1
    daily_avg = total_pnl / total_days if total_days > 0 else 0
    
    # Total return % (vs total notional traded)
    total_notional = sum(notionals)
    total_return_pct = (total_pnl / total_notional * 100) if total_notional > 0 else 0
    
    t = Table(title=f"📊 Historical Stats ({period})")
    t.add_column("Metric", style="cyan")
    t.add_column("Value", style="green", justify="right")
    
    t.add_row("Total Trades", str(len(filtered)))
    t.add_row("Winning Trades", f"{len(winning)} ({win_rate:.1f}%)")
    t.add_row("Losing Trades", str(len(losing)))
    t.add_row("Total Realized P&L", f"${total_pnl:.4f}")
    t.add_row("Avg P&L per Trade", f"${total_pnl/len(filtered):.4f}")
    t.add_row("Avg Position Duration", f"{avg_dur:.2f} hrs")
    t.add_row("Avg Notional", f"${avg_notional:.0f}")
    t.add_row("Avg Entry APY", f"{avg_entry_apy:.1f}%")
    t.add_row("Total Notional Traded", f"${total_notional:.0f}")
    t.add_row("Total Return %", f"{total_return_pct:.2f}%")
    t.add_row("Est. Daily Avg P&L", f"${daily_avg:.4f}")
    
    console.print(t)
    
    # Recent closes table
    recent = sorted(filtered, key=lambda x: x.get("close_ts", 0), reverse=True)[:10]
    if recent:
        rt = Table(title="Recent Closed Positions")
        rt.add_column("Symbol", style="cyan")
        rt.add_column("Notional", justify="right")
        rt.add_column("Entry APY", justify="right")
        rt.add_column("Exit APY", justify="right")
        rt.add_column("Duration", justify="right")
        rt.add_column("P&L", justify="right", style="green" if total_pnl > 0 else "red")
        
        for e in recent:
            pnl = e.get("realized_pnl", 0)
            color = "green" if pnl > 0 else ("red" if pnl < 0 else "white")
            rt.add_row(
                e.get("symbol", "?"),
                f"${e.get('notional', 0):.0f}",
                f"{e.get('entry_apy', 0):.0f}%",
                f"{e.get('exit_apy', 0):.0f}%",
                f"{e.get('duration_hours', 0):.1f}h",
                f"[{color}]${pnl:.4f}[/{color}]"
            )
        console.print(rt)


# ── Scanner ───────────────────────────────────────────────────────────────────

async def scan_opportunities(
    hl_rates: dict[str, tuple[float, float]],  # {sym: (hr_rate, apy)}
    bin_rates: dict[str, float],               # {sym: apy}
    bin_volumes: dict[str, float],             # {sym: 24h_vol_usd}
    hl_volumes: dict[str, float],              # {sym: 24h_vol_usd}
    min_net_apy: float,
) -> list[ArbOpp]:
    """
    Find all cross-exchange arb opportunities above the APY threshold.
    """
    opps = []
    for sym in set(bin_rates.keys()) & set(hl_rates.keys()):
        if sym in BLACKLIST:
            continue
        b_apy = bin_rates[sym]
        h_apy = hl_rates[sym][1]
        b_vol  = bin_volumes.get(sym, 0.0)
        h_vol  = hl_volumes.get(sym, 0.0)
        if b_vol < MIN_BIN_VOL_USD or h_vol < MIN_HL_VOL_USD:
            continue

        # Determine best arb direction
        # Goal: place LONG where rate < 0 (long earns) and SHORT where rate ≥ 0 or less negative
        if b_apy < 0 and h_apy < 0:
            # Both negative: long the more-negative (earns more), short the less-negative
            if abs(b_apy) >= abs(h_apy):
                net = abs(b_apy) - abs(h_apy)
                bin_side, hl_side = "buy", "sell"   # long Binance, short HL
            else:
                net = abs(h_apy) - abs(b_apy)
                bin_side, hl_side = "sell", "buy"   # short Binance, long HL
        elif b_apy > 0 and h_apy > 0:
            # Both positive: short the more-positive (earns more), long the less-positive
            if b_apy >= h_apy:
                net = b_apy - h_apy
                bin_side, hl_side = "sell", "buy"
            else:
                net = h_apy - b_apy
                bin_side, hl_side = "buy", "sell"
        else:
            # Opposite signs — both sides earn simultaneously (best case)
            net = abs(b_apy) + abs(h_apy)
            bin_side = "buy"  if b_apy < 0 else "sell"
            hl_side  = "sell" if b_apy < 0 else "buy"

        if net >= min_net_apy:
            opps.append(ArbOpp(
                symbol=sym,
                bin_apy=b_apy,
                hl_apy=h_apy,
                net_apy=net,
                bin_side=bin_side,
                hl_side=hl_side,
                bin_vol=b_vol,
                hl_vol=h_vol,
            ))

    opps.sort(key=lambda x: x.net_apy, reverse=True)
    return opps


# ── Fetch Binance data ────────────────────────────────────────────────────────

async def fetch_binance_data() -> tuple[dict[str, float], dict[str, float]]:
    """
    Returns ({symbol: funding_apy_pct}, {symbol: 24h_volume_usd})
    Uses two bulk endpoints (premiumIndex + ticker24h).
    """
    import aiohttp
    rates, volumes = {}, {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex", timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                for r in data:
                    sym = r.get("symbol", "").replace("USDT", "")
                    if sym:
                        try:
                            rates[sym] = float(r["lastFundingRate"]) * 3 * 365 * 100
                        except Exception:
                            pass
            async with session.get(
                "https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                for r in data:
                    sym = r.get("symbol", "").replace("USDT", "")
                    if sym:
                        try:
                            volumes[sym] = float(r.get("quoteVolume", 0))
                        except Exception:
                            pass
    except Exception as e:
        logger.error("fetch_binance_data_failed", error=str(e)[:120])
    return rates, volumes


# ── Open / Close ──────────────────────────────────────────────────────────────

async def open_arb_position(
    opp: ArbOpp,
    size_usdt: float,
) -> Optional[ArbPosition]:
    """
    Open both legs: Binance perp + HL perp.
    Rolls back HL leg if Binance fails, and vice versa.
    Returns ArbPosition on success, None on failure.
    """
    import ccxt.async_support as ccxt
    from hl_client import make_hl_client

    # ── Setup exchanges ──────────────────────────────────────────────────────
    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret   = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    bin_ex   = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })

    try:
        await bin_ex.load_markets()

        # Get mid prices
        bin_sym  = opp.symbol + "/USDT:USDT"
        hl_sym   = opp.symbol

        bin_ticker = await bin_ex.fetch_ticker(bin_sym)
        bin_mid    = float(bin_ticker.get("last") or bin_ticker.get("close") or 0)
        if not bin_mid:
            logger.error("open_arb_no_price", symbol=opp.symbol)
            return None

        # Size in contracts
        bin_mkt  = bin_ex.market(bin_sym)
        step     = float(bin_mkt.get("precision", {}).get("amount") or 1)
        from decimal import Decimal
        bin_size = float(
            (Decimal(str(size_usdt / bin_mid)) // Decimal(str(step))) * Decimal(str(step))
        )
        if bin_size <= 0:
            logger.error("open_arb_invalid_size", symbol=opp.symbol)
            return None

        # Set leverage on both
        try:
            await bin_ex.set_leverage(PERP_LEVERAGE, bin_sym)
        except Exception:
            pass  # may already be set

        hl = make_hl_client()
        hl.set_leverage(hl_sym, PERP_LEVERAGE)
        hl_mid = hl.get_mid(hl_sym)
        if not hl_mid:
            logger.error("open_arb_no_hl_price", symbol=opp.symbol)
            return None
        hl_size = hl.round_size(hl_sym, size_usdt / hl_mid)

        logger.info("opening_arb",
                    symbol=opp.symbol,
                    bin_side=opp.bin_side, bin_size=bin_size, bin_mid=bin_mid,
                    hl_side=opp.hl_side,  hl_size=hl_size,  hl_mid=hl_mid,
                    net_apy=round(opp.net_apy, 1))

        # ── Open Binance leg first ──────────────────────────────────────────
        try:
            bin_order = await bin_ex.create_order(
                bin_sym, "market", opp.bin_side, bin_size,
                params={"reduceOnly": False}
            )
            bin_oid   = str(bin_order.get("id", ""))
            bin_fill  = float(bin_order.get("average") or bin_order.get("price") or bin_mid)
            logger.info("arb_bin_opened", symbol=opp.symbol, side=opp.bin_side,
                        fill=bin_fill, oid=bin_oid)
        except Exception as e:
            logger.error("arb_bin_open_failed", symbol=opp.symbol, error=str(e)[:200])
            return None

        # ── Open HL leg ────────────────────────────────────────────────────
        hl_is_buy = (opp.hl_side == "buy")
        hl_ok, hl_oid, hl_fill = hl.market_open(hl_sym, hl_is_buy, hl_size, slippage=SLIPPAGE)

        if not hl_ok:
            logger.error("arb_hl_open_failed_rolling_back_binance", symbol=opp.symbol)
            # Rollback Binance leg
            rollback_side = "sell" if opp.bin_side == "buy" else "buy"
            try:
                await bin_ex.create_order(
                    bin_sym, "market", rollback_side, bin_size,
                    params={"reduceOnly": True}
                )
                logger.info("arb_bin_rollback_ok", symbol=opp.symbol)
            except Exception as e:
                logger.error("arb_bin_rollback_FAILED_MANUAL_REQUIRED",
                             symbol=opp.symbol, error=str(e)[:200])
            return None

        notional = bin_size * bin_fill
        pos = ArbPosition(
            symbol        = opp.symbol,
            bin_side      = opp.bin_side,
            hl_side       = opp.hl_side,
            bin_size      = bin_size,
            hl_size       = hl_size,
            notional_usdt = notional,
            entry_bin_apy = opp.bin_apy,
            entry_hl_apy  = opp.hl_apy,
            entry_net_apy = opp.net_apy,
            bin_order_id  = bin_oid,
            hl_order_id   = hl_oid,
            bin_entry_px  = bin_fill,
            hl_entry_px   = hl_fill,
            last_bin_apy  = opp.bin_apy,
            last_hl_apy   = opp.hl_apy,
            last_rate_ts  = time.time(),
        )
        # Record time-series event
        record_position_open(pos)
        logger.info("arb_position_opened", symbol=opp.symbol,
                    notional=round(notional, 2), net_apy=round(opp.net_apy, 1))
        return pos

    finally:
        await bin_ex.close()


async def close_arb_position(pos: ArbPosition) -> bool:
    """
    Close both legs. Returns True if fully closed.
    Closes Binance leg first (easier to verify), then HL.
    """
    import ccxt.async_support as ccxt
    from hl_client import make_hl_client

    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret   = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    bin_ex   = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })

    try:
        await bin_ex.load_markets()
        bin_sym = pos.symbol + "/USDT:USDT"

        # ── Close Binance leg ──────────────────────────────────────────────
        if not pos.bin_closed:
            close_side = "sell" if pos.bin_side == "buy" else "buy"
            try:
                # Fetch actual position size first
                positions = await bin_ex.fetch_positions([bin_sym])
                actual_size = 0.0
                for p in positions:
                    if p.get("symbol") == bin_sym:
                        actual_size = abs(float(p.get("contracts") or 0))
                if actual_size < 1e-9:
                    logger.info("arb_bin_already_flat", symbol=pos.symbol)
                    pos.bin_closed = True
                else:
                    order = await bin_ex.create_order(
                        bin_sym, "market", close_side, actual_size,
                        params={"reduceOnly": True}
                    )
                    logger.info("arb_bin_closed", symbol=pos.symbol,
                                fill=order.get("average") or 0)
                    pos.bin_closed = True
            except Exception as e:
                logger.error("arb_bin_close_failed", symbol=pos.symbol, error=str(e)[:200])
                return False

        # ── Close HL leg ───────────────────────────────────────────────────
        if not pos.hl_closed:
            hl = make_hl_client()
            hl_pos = hl.get_position(pos.symbol)
            if not hl_pos or abs(hl_pos.size) < 1e-9:
                logger.info("arb_hl_already_flat", symbol=pos.symbol)
                pos.hl_closed = True
            else:
                ok, fill = hl.market_close(pos.symbol, slippage=SLIPPAGE)
                if ok:
                    pos.hl_closed = True
                    logger.info("arb_hl_closed", symbol=pos.symbol, fill=fill)
                else:
                    logger.error("arb_hl_close_FAILED_will_retry", symbol=pos.symbol)
                    return False

        both_done = pos.bin_closed and pos.hl_closed
        logger.info("arb_position_closed", symbol=pos.symbol, ok=both_done)
        return both_done

    finally:
        await bin_ex.close()


# ── Display ───────────────────────────────────────────────────────────────────

def show_arb_status(positions: list[ArbPosition], hl_equity: float = 0, bin_free: float = 0) -> None:
    if not positions:
        console.print("[yellow]No active cross-arb positions.[/yellow]")
        return

    total_portfolio = hl_equity + bin_free * PERP_LEVERAGE
    
    t = Table(title="⚙️  Cross-Exchange Arb Positions")
    for col, justify in [
        ("Symbol", "left"), 
        ("Notional", "right"), 
        ("% Port", "right"),
        ("Entry APY", "right"),
        ("Live APY", "right"), 
        ("$/day", "right"), 
        ("Tier", "right"),
        ("Age", "right"),
    ]:
        t.add_column(col, justify=justify)

    now = time.time()
    total_notional = 0.0
    total_daily = 0.0
    for pos in positions:
        age_h = (now - pos.entry_ts) / 3600
        age_str = f"{age_h/24:.1f}d" if age_h >= 24 else f"{age_h:.1f}h"

        # Live net APY — unified formula (works for all sign combos)
        bin_earn_sign = 1 if pos.bin_side == "buy" else -1
        hl_earn_sign  = 1 if pos.hl_side == "buy" else -1
        live_bin = pos.last_bin_apy * bin_earn_sign * (-1)
        live_hl  = pos.last_hl_apy  * hl_earn_sign  * (-1)
        live_net = live_bin + live_hl
            
        daily    = pos.notional_usdt * live_net / 100.0 / 365.0
        total_notional += pos.notional_usdt
        total_daily    += daily
        
        # Position as % of portfolio
        port_pct = (pos.notional_usdt / total_portfolio * 100) if total_portfolio > 0 else 0
        
        # APY tier indicator
        if pos.entry_net_apy >= 500:
            tier = "★★★"
        elif pos.entry_net_apy >= 200:
            tier = "★★"
        elif pos.entry_net_apy >= 100:
            tier = "★"
        else:
            tier = "-"
        
        # Color based on live APY
        if live_net > 50:
            color = "green"
        elif live_net > EXIT_ARB_APY:
            color = "bright_green"
        elif live_net > 0:
            color = "yellow"
        else:
            color = "red"

        t.add_row(
            pos.symbol,
            f"${pos.notional_usdt:.0f}",
            f"{port_pct:.0f}%",
            f"{pos.entry_net_apy:.0f}%",
            f"[{color}]{live_net:.0f}%[/{color}]",
            f"${daily:.4f}",
            tier,
            age_str,
        )

    console.print(t)
    
    # Risk summary
    leveraged = total_notional / total_portfolio * 100 if total_portfolio > 0 else 0
    console.print(
        f"[dim]Total notional: ${total_notional:.0f}  |  "
        f"Leverage: {leveraged:.0f}%  |  "
        f"Est. daily: [/dim][bold green]${total_daily:.4f}[/bold green]"
        f"[dim]  |  "
        f"APY: {total_daily/total_notional*365*100:.1f}%[/dim]"
    )


# ── Scan + Print ──────────────────────────────────────────────────────────────

async def run_scan_only(min_apy: float = MIN_ARB_APY) -> None:
    from hl_client import make_hl_client

    console.print(f"[dim]Scanning cross-exchange arb (min {min_apy:.0f}% differential)…[/dim]")
    hl = make_hl_client()

    # Single bulk call: rates + volumes together
    meta, ctxs = hl._info.meta_and_asset_ctxs()
    universe   = meta.get("universe", [])
    hl_rates, hl_vols = {}, {}
    for i, asset in enumerate(universe):
        name = asset.get("name", "")
        if i < len(ctxs):
            rate = float(ctxs[i].get("funding", 0))
            hl_rates[name] = (rate, rate * 24 * 365 * 100)
            hl_vols[name]  = float(ctxs[i].get("dayNtlVlm", 0))

    bin_rates, bin_vols = await fetch_binance_data()
    opps = await scan_opportunities(hl_rates, bin_rates, bin_vols, hl_vols, min_apy)

    t = Table(title=f"Cross-Exchange Arb Scan  [dim](min {min_apy:.0f}% APY)[/dim]")
    for col in ["Symbol", "Binance APY", "HL APY", "Net APY", "Strategy", "Bin Vol", "HL Vol"]:
        t.add_column(col, justify="right" if "APY" in col or "Vol" in col else "left")

    for o in opps[:20]:
        strategy = f"{'long' if o.bin_side == 'buy' else 'short'} Bin + {'long' if o.hl_side == 'buy' else 'short'} HL"
        color = "green" if o.net_apy > 30 else ("yellow" if o.net_apy > 15 else "white")
        t.add_row(
            o.symbol,
            f"{o.bin_apy:+.1f}%",
            f"{o.hl_apy:+.1f}%",
            f"[{color}]{o.net_apy:+.1f}%[/{color}]",
            strategy,
            f"${o.bin_vol/1e6:.0f}M",
            f"${o.hl_vol/1e6:.0f}M",
        )

    console.print(t)
    console.print(f"[dim]  {len(opps)} opportunities ≥ {min_apy:.0f}% APY[/dim]")


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_cross_arb() -> None:
    from hl_client import make_hl_client
    import ccxt.async_support as ccxt

    logger.info("cross_arb_starting",
                min_apy=MIN_ARB_APY, exit_apy=EXIT_ARB_APY,
                size=POSITION_SIZE_USDT, max_pos=MAX_POSITIONS)

    positions: list[ArbPosition] = load_state()
    last_scan_ts = 0.0
    last_checkpoint_ts = 0.0
    hl_equity = 0.0
    bin_equity = 0.0

    while True:
        now = time.time()

        # ── Hourly checkpoint (record portfolio state) ────────────────────────
        if now - last_checkpoint_ts >= 3600:
            try:
                hl = make_hl_client()
                hl_equity = hl.get_equity()
                # Get Binance equity
                key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
                secret = open(key_path).read() if key_path and os.path.exists(key_path) else ""
                bin_ex = ccxt.binanceusdm({
                    "apiKey": os.getenv("BINANCE_API_KEY", ""),
                    "secret": secret,
                    "options": {"defaultType": "future"},
                    "enableRateLimit": True,
                })
                try:
                    await bin_ex.load_markets()
                    bin_bal = await bin_ex.fetch_balance({"type": "future"})
                    bin_equity = float((bin_bal.get("USDT") or {}).get("total", 0))
                finally:
                    await bin_ex.close()
                record_hourly_checkpoint(positions, hl_equity, bin_equity)
                logger.info("hourly_checkpoint_recorded", positions=len(positions), 
                           hl_equity=hl_equity, bin_equity=bin_equity)
            except Exception as e:
                logger.warning("checkpoint_failed", error=str(e)[:120])
            last_checkpoint_ts = now

        # ── 1. Rate refresh (every 10 min regardless of scan cycle) ──────────
        if positions and (now - min((p.last_rate_ts for p in positions), default=0)) > 300:
            try:
                hl = make_hl_client()
                hl_meta2, hl_ctxs2 = hl._info.meta_and_asset_ctxs()
                hl_rates = {}
                for i, asset in enumerate(hl_meta2.get("universe", [])):
                    name = asset.get("name", "")
                    if i < len(hl_ctxs2):
                        rate = float(hl_ctxs2[i].get("funding", 0))
                        hl_rates[name] = (rate, rate * 24 * 365 * 100)
                bin_rates, _ = await fetch_binance_data()
                for pos in positions:
                    if pos.symbol in bin_rates:
                        pos.last_bin_apy = bin_rates[pos.symbol]
                    if pos.symbol in hl_rates:
                        pos.last_hl_apy  = hl_rates[pos.symbol][1]
                    pos.last_rate_ts = now
            except Exception as e:
                logger.warning("rate_refresh_failed", error=str(e)[:120])

        # ── 2. Exit check ─────────────────────────────────────────────────────
        for pos in positions:
            bin_earn_sign = 1 if pos.bin_side == "buy" else -1
            hl_earn_sign  = 1 if pos.hl_side == "buy" else -1
            live_bin = pos.last_bin_apy * bin_earn_sign * (-1)
            live_hl  = pos.last_hl_apy  * hl_earn_sign  * (-1)
            live_net = live_bin + live_hl

            if live_net < EXIT_ARB_APY or pos.needs_close:
                if not pos.needs_close:
                    logger.warning("exit_signal",
                                   symbol=pos.symbol,
                                   live_net=round(live_net, 1),
                                   threshold=EXIT_ARB_APY)
                    pos.needs_close = True
                    console.print(
                        f"[yellow]⚠ {pos.symbol} net APY {live_net:.1f}% < {EXIT_ARB_APY:.0f}% "
                        f"— closing[/yellow]"
                    )
                ok = await close_arb_position(pos)
                if ok:
                    # Calculate exit APY and record close
                    exit_bin = pos.last_bin_apy
                    exit_hl = pos.last_hl_apy
                    # Approximate exit net APY (using last known rates)
                    bin_earn_sign = 1 if pos.bin_side == "buy" else -1
                    hl_earn_sign = 1 if pos.hl_side == "buy" else -1
                    live_bin = exit_bin * bin_earn_sign * (-1)
                    live_hl = exit_hl * hl_earn_sign * (-1)
                    exit_net = live_bin + live_hl
                    
                    # Calculate realized P&L (simplified - from position data)
                    # Using funding_realized fields which track realized funding
                    realized_pnl = pos.funding_realized_hl + pos.funding_realized_bin
                    
                    record_position_close(pos, exit_net, realized_pnl)
                    positions.remove(pos)
                    save_state(positions)
                    console.print(f"[green]✓ {pos.symbol} arb closed[/green]")
                    break

        # ── 3. Scan for new opportunities ────────────────────────────────────
        if now - last_scan_ts >= SCAN_INTERVAL_S:
            console.print(f"\n[dim]Scanning cross-exchange arb (min {MIN_ARB_APY:.0f}% APY)…[/dim]")
            try:
                hl = make_hl_client()
                hl_meta, hl_ctxs = hl._info.meta_and_asset_ctxs()
                hl_rates, hl_vols = {}, {}
                for i, asset in enumerate(hl_meta.get("universe", [])):
                    name = asset.get("name", "")
                    if i < len(hl_ctxs):
                        rate = float(hl_ctxs[i].get("funding", 0))
                        hl_rates[name] = (rate, rate * 24 * 365 * 100)
                        hl_vols[name]  = float(hl_ctxs[i].get("dayNtlVlm", 0))

                bin_rates, bin_vols = await fetch_binance_data()
                opps = await scan_opportunities(
                    hl_rates, bin_rates, bin_vols, hl_vols, MIN_ARB_APY
                )

                # Filter: skip already-open symbols
                existing = {p.symbol for p in positions}
                new_opps = [o for o in opps if o.symbol not in existing
                            and len(positions) < MAX_POSITIONS]

                # Check HL account has enough equity
                hl_equity = hl.get_equity()
                hl_free   = hl.get_withdrawable()
                console.print(
                    f"[dim]  {len(opps)} opps found | "
                    f"HL equity ${hl_equity:.0f} (free ${hl_free:.0f})[/dim]"
                )

                # Fetch Binance futures free balance
                key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
                secret   = open(key_path).read() if key_path and os.path.exists(key_path) else ""
                bin_ex   = ccxt.binanceusdm({
                    "apiKey": os.getenv("BINANCE_API_KEY", ""),
                    "secret": secret,
                    "options": {"defaultType": "future"},
                    "enableRateLimit": True,
                })
                try:
                    await bin_ex.load_markets()
                    bin_bal = await bin_ex.fetch_balance({"type": "future"})
                    bin_free = float((bin_bal.get("USDT") or {}).get("free", 0))
                finally:
                    await bin_ex.close()

                # Size: use min of HL free / bin free / MAX_POSITION_SIZE per position
                per_pos_margin = POSITION_SIZE_USDT / PERP_LEVERAGE
                hl_can_open    = int(hl_free / per_pos_margin) if hl_free > per_pos_margin else 0
                bin_can_open   = int(bin_free / per_pos_margin) if bin_free > per_pos_margin else 0
                console.print(f"[dim]  Can open {min(hl_can_open, bin_can_open)} more positions "
                              f"(HL slots: {hl_can_open}, Bin slots: {bin_can_open})[/dim]")

                for opp in new_opps:
                    if len(positions) >= MAX_POSITIONS:
                        break
                    
                    # APY-tiered sizing - more aggressive but capped
                    if opp.net_apy >= 500:
                        pos_size = POSITION_SIZE_USDT * 3.5
                    elif opp.net_apy >= 200:
                        pos_size = POSITION_SIZE_USDT * 2.5
                    elif opp.net_apy >= 100:
                        pos_size = POSITION_SIZE_USDT * 2.0
                    elif opp.net_apy >= 50:
                        pos_size = POSITION_SIZE_USDT * 1.5
                    else:
                        pos_size = POSITION_SIZE_USDT * 1.0
                    
                    # Cap per position at MAX_POSITION_SIZE (safety)
                    pos_size = min(pos_size, MAX_POSITION_SIZE)
                    
                    # Additional cap: max 15% of portfolio in single token
                    total_portfolio = hl_equity + bin_free * PERP_LEVERAGE
                    max_per_position = total_portfolio * 0.15
                    pos_size = min(pos_size, max_per_position)
                    
                    per_pos_margin = pos_size / PERP_LEVERAGE
                    
                    # Break-even check: fees / net_apy
                    be_days = (RT_FEE_PCT * 365.0) / (opp.net_apy / 100.0) if opp.net_apy > 0 else 999
                    if be_days > 30:
                        console.print(f"[dim]  Skipping {opp.symbol} {opp.net_apy:.0f}% — BE {be_days:.0f}d[/dim]")
                        continue
                    if hl_free < per_pos_margin or bin_free < per_pos_margin:
                        console.print(f"[yellow]  Insufficient margin — HL free ${hl_free:.0f}, Bin free ${bin_free:.0f}[/yellow]")
                        break

                    console.print(
                        f"[green]→ {opp.symbol}  net {opp.net_apy:.0f}% APY  "
                        f"BE={be_days:.1f}d  "
                        f"({'long' if opp.bin_side == 'buy' else 'short'} Bin + "
                        f"{'long' if opp.hl_side == 'buy' else 'short'} HL)[/green]"
                    )
                    pos = await open_arb_position(opp, pos_size)
                    await asyncio.sleep(3.0)
                    if pos:
                        positions.append(pos)
                        hl_free  -= per_pos_margin
                        bin_free -= per_pos_margin
                        save_state(positions)
                        console.print(f"[green]  ✓ Opened ${pos.notional_usdt:.0f} arb on {pos.symbol}[/green]")

            except Exception as e:
                logger.error("scan_error", error=str(e)[:200])

            # Get equity for portfolio calculation and display
            try:
                from hl_client import make_hl_client
                hl = make_hl_client()
                hl_equity = hl.get_equity()
                # Also get Binance equity
                key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
                secret = open(key_path).read() if key_path and os.path.exists(key_path) else ""
                bin_ex = ccxt.binanceusdm({
                    "apiKey": os.getenv("BINANCE_API_KEY", ""),
                    "secret": secret,
                    "options": {"defaultType": "future"},
                    "enableRateLimit": True,
                })
                try:
                    await bin_ex.load_markets()
                    bin_bal = await bin_ex.fetch_balance({"type": "future"})
                    bin_equity = float((bin_bal.get("USDT") or {}).get("total", 0))
                finally:
                    await bin_ex.close()
                show_arb_status(positions, hl_equity, bin_equity)
            except Exception as e:
                logger.warning("equity_fetch_failed", error=str(e)[:120])
                show_arb_status(positions, hl_equity, bin_equity)
            last_scan_ts = time.time()
            next_min = SCAN_INTERVAL_S // 60
            console.print(f"\n[dim]Next scan in {next_min}min[/dim]")

        await asyncio.sleep(30)


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-exchange funding arb")
    parser.add_argument("--run",    action="store_true", help="Run live arb loop")
    parser.add_argument("--scan",   action="store_true", help="Scan and print opportunities")
    parser.add_argument("--status", action="store_true", help="Show active positions")
    parser.add_argument("--close",  type=str,            help="Close position by symbol")
    parser.add_argument("--history", type=str, nargs="?", const="all", 
                        choices=["all", "today", "week", "month"],
                        help="Show historical stats (all/today/week/month)")
    parser.add_argument("--min-apy", type=float, default=MIN_ARB_APY)
    args = parser.parse_args()

    if args.scan:
        await run_scan_only(args.min_apy)
    elif args.status:
        positions = load_state()
        try:
            from hl_client import make_hl_client
            hl = make_hl_client()
            eq = hl.get_equity()
            show_arb_status(positions, eq, 0)
        except:
            show_arb_status(positions, 0, 0)
    elif args.history is not None:
        show_historical_stats(args.history)
    elif args.close:
        positions = load_state()
        sym = args.close.upper()
        to_close = [p for p in positions if p.symbol == sym]
        if not to_close:
            console.print(f"[red]No open position for {sym}[/red]")
            return
        for pos in to_close:
            pos.needs_close = True
            ok = await close_arb_position(pos)
            if ok:
                # Record close event
                realized_pnl = pos.funding_realized_hl + pos.funding_realized_bin
                record_position_close(pos, pos.last_hl_apy, realized_pnl)
                positions.remove(pos)
                save_state(positions)
                console.print(f"[green]✓ {sym} closed[/green]")
            else:
                console.print(f"[red]✗ {sym} close failed — retry with --close again[/red]")
    elif args.run:
        await run_cross_arb()
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
