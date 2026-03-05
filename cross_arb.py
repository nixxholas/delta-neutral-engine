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
MIN_HOLD_HOURS       = float(os.getenv("CARB_MIN_HOLD_HOURS",      "8.0"))  # minimum hold before exit (fee break-even gate)
POSITION_SIZE_USDT   = float(os.getenv("CARB_POSITION_SIZE_USDT", "300.0")) # per-position notional
MAX_POSITION_SIZE    = float(os.getenv("CARB_MAX_POSITION_SIZE", "1000.0")) # cap per position
MAX_POSITIONS        = int(os.getenv(  "CARB_MAX_POSITIONS",         "30"))
SCAN_INTERVAL_S      = int(os.getenv(  "CARB_SCAN_INTERVAL",       "300"))  # 5 min
MIN_HL_VOL_USD       = float(os.getenv("CARB_MIN_HL_VOL_USD",  "1000000"))  # $1M min HL daily volume
MIN_BIN_VOL_USD      = float(os.getenv("CARB_MIN_BIN_VOL_USD","10000000"))  # $10M min Binance daily vol
PERP_LEVERAGE        = int(os.getenv(  "CARB_LEVERAGE",               "6"))  # 6x leverage (moderate)
SLIPPAGE             = float(os.getenv("CARB_SLIPPAGE",            "0.02"))  # 2% HL market order slippage
SLIPPAGE_FORCE_CLOSE = float(os.getenv("CARB_SLIPPAGE_FORCE",       "0.04"))  # 4% slippage for forced closes
RT_FEE_PCT           = 0.0017   # Binance 0.05% + HL 0.035% = 0.085% per way × 2 (open+close) = 0.17%

STATE_FILE = "/tmp/cross-arb-state.json"
ALERTS_FILE = "/tmp/cross-arb-alerts.json"

# Circuit breaker state
_close_failure_timestamps: list[float] = []
CIRCUIT_BREAKER_WINDOW_S = 1800  # 30 minutes
CIRCUIT_BREAKER_THRESHOLD = 3   # 3 failures triggers pause
HISTORY_FILE = "/tmp/cross-arb-history.jsonl"
TIMESERIES_FILE = "/tmp/cross-arb-timeseries.jsonl"

BLACKLIST = set(os.getenv("CARB_BLACKLIST", "").split(",")) - {""}

# Slippage gate threshold (in basis points)
SLIPPAGE_GATE_BPS = 15.0  # 0.15% max combined slippage on entry

# ── Data ──────────────────────────────────────────────────────────────────────

@dataclass
class ArbOpp:
    symbol:       str
    bin_apy:      float   # Binance annualised APY (negative = longs earn)
    hl_apy:       float   # HL annualised APY      (negative = longs earn)
    net_apy:      float   # expected differential earned (adjusted for timing)
    raw_net_apy:  float = 0.0  # Original net APY before timing adjustment
    bin_side:     str = ""  # "buy" (long) | "sell" (short) on Binance
    hl_side:      str = ""  # "buy" (long) | "sell" (short) on HL
    bin_vol:      float = 0.0  # Binance 24h volume
    hl_vol:       float = 0.0  # HL 24h volume


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
    # --- Patch A: Cost tracking ---
    bin_entry_slippage_bps: float = 0.0
    hl_entry_slippage_bps: float = 0.0
    bin_exit_fill: float = 0.0
    hl_exit_fill: float = 0.0
    bin_exit_slippage_bps: float = 0.0
    hl_exit_slippage_bps: float = 0.0
    exit_bin_apy: float = 0.0
    exit_hl_apy: float = 0.0
    exit_net_apy: float = 0.0
    true_pnl: float = 0.0
    # --- Patch C: Close robustness tracking ---
    bin_close_ts:  Optional[float] = None
    hl_close_ts:   Optional[float] = None
    close_failure_count: int = 0


# ── State ─────────────────────────────────────────────────────────────────────

def save_state(positions: list[ArbPosition]) -> None:
    """Save positions to state file atomically (write to temp then rename)."""
    import tempfile
    data = [asdict(p) for p in positions]
    fd, tmp = tempfile.mkstemp(dir='/tmp', suffix='.json')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)  # Atomic on POSIX
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


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


# ── Funding Tracking ─────────────────────────────────────────────────────────

def _update_realized_funding(pos: ArbPosition, current_bin_apy: float, current_hl_apy: float) -> None:
    """
    Update realized funding for a position based on time elapsed and current rates.
    Should be called periodically (e.g., every rate refresh).
    """
    now = time.time()

    if pos.last_rate_ts <= 0:
        pos.last_rate_ts = now
        return

    hours_elapsed = (now - pos.last_rate_ts) / 3600.0
    if hours_elapsed <= 0:
        return

    # Sign convention matches live APY: rate * earn_sign * (-1)
    # e.g. bin_apy=-10%, long (earn_sign=1): -10 * 1 * (-1) = +10% → we earn
    bin_earn_sign = 1 if pos.bin_side == "buy" else -1
    bin_hourly_rate = current_bin_apy / 100.0 / 365.0
    bin_funding_this_interval = bin_hourly_rate * bin_earn_sign * (-1) * pos.notional_usdt * hours_elapsed

    hl_earn_sign = 1 if pos.hl_side == "buy" else -1
    hl_hourly_rate = current_hl_apy / 100.0 / 365.0
    hl_funding_this_interval = hl_hourly_rate * hl_earn_sign * (-1) * pos.notional_usdt * hours_elapsed

    pos.funding_realized_bin += bin_funding_this_interval
    pos.funding_realized_hl += hl_funding_this_interval

    logger.debug("funding_updated",
                  symbol=pos.symbol,
                  hours_elapsed=round(hours_elapsed, 3),
                  bin_rate=current_bin_apy,
                  hl_rate=current_hl_apy,
                  bin_funding=round(bin_funding_this_interval, 4),
                  hl_funding=round(hl_funding_this_interval, 4),
                  total_bin=round(pos.funding_realized_bin, 4),
                  total_hl=round(pos.funding_realized_hl, 4))


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


# ── Smart Entry Config (Patch B) ─────────────────────────────────────────────
RATE_HISTORY_FILE   = "/tmp/cross-arb-rate-history.json"
COOLDOWN_FILE       = "/tmp/cross-arb-cooldowns.json"
MIN_ARB_APY_PERSISTENT = int(os.getenv("CARB_MIN_ARB_PERSISTENT", "2"))  # observations out of 3 that must exceed threshold
MIN_STABILITY_SCORE = float(os.getenv("CARB_MIN_STABILITY_SCORE", "0.3"))  # stability gate (0.0 = disabled)


# ── Rate History (Mean-Reversion Filter) ─────────────────────────────────────

def load_rate_history() -> dict[str, list[dict]]:
    """Load rate history from file."""
    if not os.path.exists(RATE_HISTORY_FILE):
        return {}
    try:
        with open(RATE_HISTORY_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_rate_history(history: dict[str, list[dict]]) -> None:
    """Save rate history to file."""
    with open(RATE_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def record_rates_for_symbols(
    rates: dict[str, tuple[float, float, float]],
) -> None:
    """Record current rates for all symbols in history. Keeps last 6 observations."""
    history = load_rate_history()
    now = time.time()

    for sym, (bin_apy, hl_apy, net_apy) in rates.items():
        if sym not in history:
            history[sym] = []
        history[sym].append({
            "timestamp": now,
            "bin_apy": bin_apy,
            "hl_apy": hl_apy,
            "net_apy": net_apy,
        })
        if len(history[sym]) > 6:
            history[sym] = history[sym][-6:]

    save_rate_history(history)


def check_rate_persistent(history: dict[str, list[dict]], symbol: str, min_apy: float) -> bool:
    """Check if symbol has net_apy >= min_apy in at least 2 of last 3 observations."""
    if symbol not in history or len(history[symbol]) < 3:
        if len(history.get(symbol, [])) >= 1:
            return history[symbol][-1].get("net_apy", 0) >= min_apy
        return False
    recent = history[symbol][-3:]
    qualifying = sum(1 for obs in recent if obs.get("net_apy", 0) >= min_apy)
    return qualifying >= MIN_ARB_APY_PERSISTENT


def get_rate_history_for_symbol(history: dict[str, list[dict]], symbol: str) -> list[float]:
    """Get list of net_apy values for stability calculation."""
    if symbol not in history:
        return []
    return [obs.get("net_apy", 0) for obs in history[symbol]]


def rate_stability_score(history: list[float]) -> float:
    """
    Calculate stability score based on coefficient of variation.
    0.0 = wildly volatile, 1.0 = perfectly stable.
    """
    if len(history) < 2:
        return 0.0
    mean = sum(history) / len(history)
    if mean == 0:
        return 0.0
    variance = sum((x - mean) ** 2 for x in history) / len(history)
    std_dev = variance ** 0.5
    cv = std_dev / abs(mean)
    return max(0, 1 - cv)


# ── Funding Timing Bonus/Penalty ───────────────────────────────────────────

def get_hours_until_funding() -> int:
    """
    Calculate hours until next funding settlement.
    Funding settles at 00:00, 08:00, 16:00 UTC.
    """
    import datetime
    utc_now = datetime.datetime.utcnow()
    current_hour = utc_now.hour
    hours_until = (8 - (current_hour % 8)) % 8
    return hours_until


def apply_funding_timing_bonus(net_apy: float) -> float:
    """
    Apply bonus or penalty to net_apy based on funding timing.
    +5% bonus if 1-4h before funding, -5% penalty if 7-8h until next.
    """
    hours_until = get_hours_until_funding()
    if 1 <= hours_until <= 4:
        return net_apy * 1.05
    elif hours_until >= 7:
        return net_apy * 0.95
    else:
        return net_apy


# ── Cooldown System ──────────────────────────────────────────────────────────

def load_cooldowns() -> dict[str, float]:
    """Load cooldown map from file. {symbol: expiry_timestamp}"""
    if not os.path.exists(COOLDOWN_FILE):
        return {}
    try:
        with open(COOLDOWN_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_cooldowns(cooldowns: dict[str, float]) -> None:
    """Save cooldown map to file."""
    with open(COOLDOWN_FILE, "w") as f:
        json.dump(cooldowns, f, indent=2)


def is_on_cooldown(cooldowns: dict[str, float], symbol: str) -> bool:
    """Check if symbol is currently on cooldown."""
    if symbol not in cooldowns:
        return False
    expiry = cooldowns[symbol]
    if time.time() >= expiry:
        del cooldowns[symbol]
        save_cooldowns(cooldowns)
        return False
    return True


def add_to_cooldown(symbol: str, duration_hours: float = 16.0) -> None:
    """Add symbol to cooldown after position closes due to reversion."""
    cooldowns = load_cooldowns()
    cooldowns[symbol] = time.time() + (duration_hours * 3600)
    save_cooldowns(cooldowns)
    logger.info("symbol_added_to_cooldown", symbol=symbol, duration_hours=duration_hours)


def cleanup_expired_cooldowns() -> None:
    """Remove expired cooldowns from file."""
    cooldowns = load_cooldowns()
    now = time.time()
    expired = [s for s, exp in cooldowns.items() if now >= exp]
    for s in expired:
        del cooldowns[s]
    if expired:
        save_cooldowns(cooldowns)


# ── Close Robustness Helpers (Patch C) ─────────────────────────────────────

def is_circuit_breaker_active() -> bool:
    """Check if circuit breaker is active (too many recent close failures)."""
    global _close_failure_timestamps
    now = time.time()
    _close_failure_timestamps = [
        ts for ts in _close_failure_timestamps
        if now - ts < CIRCUIT_BREAKER_WINDOW_S
    ]
    return len(_close_failure_timestamps) >= CIRCUIT_BREAKER_THRESHOLD


def record_close_failure() -> None:
    """Record a close failure for circuit breaker tracking."""
    global _close_failure_timestamps
    _close_failure_timestamps.append(time.time())


def load_alerts() -> list[dict]:
    """Load existing alerts from file."""
    if not os.path.exists(ALERTS_FILE):
        return []
    try:
        with open(ALERTS_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def save_alerts(alerts: list[dict]) -> None:
    """Save alerts to file."""
    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2)


def add_critical_alert(pos: ArbPosition, reason: str) -> None:
    """Add a critical alert for a persistent half-closed position."""
    alerts = load_alerts()
    alert = {
        "ts": time.time(),
        "symbol": pos.symbol,
        "reason": reason,
        "bin_closed": pos.bin_closed,
        "hl_closed": pos.hl_closed,
        "bin_close_ts": pos.bin_close_ts,
        "hl_close_ts": pos.hl_close_ts,
        "close_failure_count": pos.close_failure_count,
        "notional_usdt": pos.notional_usdt,
    }
    alerts.append(alert)
    alerts = alerts[-100:]
    save_alerts(alerts)
    console.print(f"[red]CRITICAL ALERT: {reason}[/red]")
    console.print(f"   Symbol: {pos.symbol}, Notional: ${pos.notional_usdt:.0f}")
    console.print(f"   Binance closed: {pos.bin_closed}, HL closed: {pos.hl_closed}")
    console.print(f"   Failures: {pos.close_failure_count}")


async def close_leg_with_retry(
    close_fn,
    symbol: str,
    leg_name: str,
    max_retries: int = 3,
):
    """Execute a close function with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            result = await close_fn()
            return result
        except Exception as e:
            err = str(e).lower()
            retryable = (
                'rate limit' in err or
                'timeout' in err or
                '429' in err or
                'temporarily unavailable' in err or
                'service unavailable' in err
            )
            if retryable:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    f"{leg_name}_retry",
                    symbol=symbol,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    wait_seconds=wait,
                    error=str(e)[:100]
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    f"{leg_name}_unretryable",
                    symbol=symbol,
                    error=str(e)[:200]
                )
                raise
    raise Exception(f"{leg_name} failed after {max_retries} retries")


def check_position_health(pos: ArbPosition) -> tuple[bool, str]:
    """Check if a position is in a problematic state."""
    now = time.time()
    if pos.bin_closed != pos.hl_closed:
        half_closed_ts = pos.bin_close_ts if pos.bin_closed else pos.hl_close_ts
        if half_closed_ts is None:
            half_closed_ts = now
        minutes_open = (now - half_closed_ts) / 60
        if minutes_open > 10:
            return False, f"Position half-closed for {minutes_open:.1f} minutes (>10min threshold)"
        else:
            return False, f"Position half-closed ({minutes_open:.1f}m)"
    if pos.close_failure_count >= 5:
        return False, f"Close failed {pos.close_failure_count} times (>=5 threshold)"
    return True, ""


# ── Startup Reconciliation ───────────────────────────────────────────────────

async def reconcile_positions(positions: list[ArbPosition]) -> list[ArbPosition]:
    """On startup, verify actual exchange positions vs state file."""
    import ccxt.async_support as ccxt
    from hl_client import make_hl_client

    logger.info("reconciliation_start", positions=len(positions))

    if not positions:
        return []

    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    bin_ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })

    reconciled = []

    try:
        await bin_ex.load_markets()
        hl = make_hl_client()

        for pos in positions:
            discrepancies = []

            bin_sym = pos.symbol + "/USDT:USDT"
            bin_still_open = False
            try:
                positions_bin = await bin_ex.fetch_positions([bin_sym])
                for p in positions_bin:
                    if p.get("symbol") == bin_sym:
                        actual_size = abs(float(p.get("contracts") or 0))
                        if actual_size > 1e-9:
                            bin_still_open = True
                        if pos.bin_closed and actual_size > 1e-9:
                            discrepancies.append(f"Binance: state=closed but exchange has {actual_size} contracts")
                        elif not pos.bin_closed and actual_size < 1e-9:
                            discrepancies.append(f"Binance: state=open but exchange is flat")
            except Exception as e:
                logger.warning("reconcile_bin_check_failed", symbol=pos.symbol, error=str(e)[:100])

            hl_pos = hl.get_position(pos.symbol)
            hl_still_open = hl_pos and abs(hl_pos.size) > 1e-9

            if pos.hl_closed and hl_still_open:
                discrepancies.append(f"HL: state=closed but exchange has {hl_pos.size} contracts")
            elif not pos.hl_closed and not hl_still_open:
                discrepancies.append(f"HL: state=open but exchange is flat")

            if discrepancies:
                logger.warning(
                    "position_discrepancy",
                    symbol=pos.symbol,
                    discrepancies=discrepancies,
                    state_bin_closed=pos.bin_closed,
                    state_hl_closed=pos.hl_closed,
                    actual_bin_open=bin_still_open,
                    actual_hl_open=hl_still_open
                )
                if not bin_still_open:
                    pos.bin_closed = True
                    if pos.bin_close_ts is None:
                        pos.bin_close_ts = time.time() - 3600
                if not hl_still_open:
                    pos.hl_closed = True
                    if pos.hl_close_ts is None:
                        pos.hl_close_ts = time.time() - 3600
                if pos.bin_closed and pos.hl_closed:
                    logger.info("reconciled_fully_closed", symbol=pos.symbol)
                    continue
                if pos.bin_closed != pos.hl_closed:
                    pos.needs_close = True
                    pos.close_failure_count = max(pos.close_failure_count, 1)
                    logger.warning("reconciled_half_closed", symbol=pos.symbol)
                    # Self-heal: immediately close the orphaned leg
                    orphan_leg = "Binance" if not pos.bin_closed else "HL"
                    console.print(f"[red]🩹 {pos.symbol} half-closed ({orphan_leg} orphaned) — auto-closing now[/red]")
                    try:
                        ok = await close_arb_position(pos)
                        if ok:
                            logger.info("reconciled_auto_closed", symbol=pos.symbol)
                            console.print(f"[green]✓ {pos.symbol} orphaned leg closed[/green]")
                            add_critical_alert(pos, f"Auto-closed orphaned {orphan_leg} leg on startup")
                            continue  # don't add to reconciled — it's fully closed
                        else:
                            logger.error("reconciled_auto_close_failed", symbol=pos.symbol)
                            add_critical_alert(pos, f"Failed to auto-close orphaned {orphan_leg} leg")
                    except Exception as e:
                        logger.error("reconciled_auto_close_error", symbol=pos.symbol, error=str(e)[:200])
                        add_critical_alert(pos, f"Error auto-closing: {str(e)[:100]}")
            else:
                logger.info("reconciled_ok", symbol=pos.symbol)

            reconciled.append(pos)

        save_state(reconciled)
        logger.info("reconciliation_complete", original=len(positions), final=len(reconciled))

    finally:
        await bin_ex.close()

    return reconciled


# ── Scanner ───────────────────────────────────────────────────────────────────

async def scan_opportunities(
    hl_rates: dict[str, tuple[float, float]],  # {sym: (hr_rate, apy)}
    bin_rates: dict[str, float],               # {sym: apy}
    bin_volumes: dict[str, float],             # {sym: 24h_vol_usd}
    hl_volumes: dict[str, float],              # {sym: 24h_vol_usd}
    min_net_apy: float,
    apply_filters: bool = True,
) -> list[ArbOpp]:
    """
    Find all cross-exchange arb opportunities above the APY threshold.
    Applies mean-reversion filter and stability scoring when apply_filters=True.
    """
    rate_history = load_rate_history() if apply_filters else {}
    cooldowns = load_cooldowns() if apply_filters else {}

    opps = []
    for sym in set(bin_rates.keys()) & set(hl_rates.keys()):
        if sym in BLACKLIST:
            continue

        # Check cooldown (don't re-enter within 16h of close)
        if apply_filters and is_on_cooldown(cooldowns, sym):
            logger.debug("symbol_on_cooldown", symbol=sym)
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
            # Apply mean-reversion filter
            if apply_filters and not check_rate_persistent(rate_history, sym, min_net_apy):
                logger.debug("rate_not_persistent", symbol=sym, net_apy=net)
                continue

            # Apply stability score check (optional)
            if apply_filters and MIN_STABILITY_SCORE > 0:
                sym_history = get_rate_history_for_symbol(rate_history, sym)
                stability = rate_stability_score(sym_history)
                if stability < MIN_STABILITY_SCORE:
                    logger.debug("rate_not_stable", symbol=sym, stability=stability)
                    continue

            # Apply funding timing bonus/penalty
            adjusted_net = apply_funding_timing_bonus(net)

            opps.append(ArbOpp(
                symbol=sym,
                bin_apy=b_apy,
                hl_apy=h_apy,
                net_apy=adjusted_net,
                raw_net_apy=net,
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

        # ── Calculate entry slippage BEFORE trading ────────────────────────
        bin_entry_mid = bin_mid
        hl_entry_mid = hl_mid

        # ── Open Binance leg first ──────────────────────────────────────────
        try:
            bin_order = await bin_ex.create_order(
                bin_sym, "market", opp.bin_side, bin_size,
                params={"reduceOnly": False}
            )
            bin_oid   = str(bin_order.get("id", ""))
            bin_fill  = float(bin_order.get("average") or bin_order.get("price") or bin_mid)

            bin_entry_slippage_bps = abs(bin_fill - bin_entry_mid) / bin_entry_mid * 10000 if bin_entry_mid > 0 else 0.0

            logger.info("arb_bin_opened", symbol=opp.symbol, side=opp.bin_side,
                        fill=bin_fill, oid=bin_oid, slippage_bps=round(bin_entry_slippage_bps, 2))
        except Exception as e:
            logger.error("arb_bin_open_failed", symbol=opp.symbol, error=str(e)[:200])
            return None

        # ── Open HL leg ────────────────────────────────────────────────────
        hl_is_buy = (opp.hl_side == "buy")
        hl_ok, hl_oid, hl_fill = hl.market_open(hl_sym, hl_is_buy, hl_size, slippage=SLIPPAGE)

        hl_entry_slippage_bps = abs(hl_fill - hl_entry_mid) / hl_entry_mid * 10000 if hl_entry_mid > 0 else 0.0

        if not hl_ok:
            logger.error("arb_hl_open_failed_rolling_back_binance", symbol=opp.symbol)
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

        # ── Gate: Check combined slippage ───────────────────────────────────
        combined_slippage_bps = bin_entry_slippage_bps + hl_entry_slippage_bps

        if combined_slippage_bps > SLIPPAGE_GATE_BPS:
            logger.warning("arb_slippage_gate_triggered",
                          symbol=opp.symbol,
                          combined_slippage_bps=round(combined_slippage_bps, 2),
                          threshold=SLIPPAGE_GATE_BPS,
                          bin_slippage=round(bin_entry_slippage_bps, 2),
                          hl_slippage=round(hl_entry_slippage_bps, 2))

            rollback_bin_side = "sell" if opp.bin_side == "buy" else "buy"
            try:
                await bin_ex.create_order(
                    bin_sym, "market", rollback_bin_side, bin_size,
                    params={"reduceOnly": True}
                )
            except Exception:
                pass

            rollback_hl_is_buy = (opp.hl_side == "sell")
            try:
                hl.market_open(hl_sym, rollback_hl_is_buy, hl_size, slippage=SLIPPAGE)
            except Exception:
                pass

            console.print(f"[red]⚠ {opp.symbol} slippage too high ({combined_slippage_bps:.2f} bps) — rejected[/red]")
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
            bin_entry_slippage_bps = bin_entry_slippage_bps,
            hl_entry_slippage_bps  = hl_entry_slippage_bps,
        )
        # Record time-series event
        record_position_open(pos)
        logger.info("arb_position_opened", symbol=opp.symbol,
                    notional=round(notional, 2), net_apy=round(opp.net_apy, 1))
        return pos

    finally:
        await bin_ex.close()


async def close_arb_position(
    pos: ArbPosition,
    exit_bin_apy: float = 0.0,
    exit_hl_apy: float = 0.0,
    force_slippage: bool = False,
) -> bool:
    """
    Close both legs. Returns True if fully closed.
    Closes Binance leg first (easier to verify), then HL.

    Includes: slippage tracking (Patch A), retry with backoff (Patch C).
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

    slippage = SLIPPAGE_FORCE_CLOSE if force_slippage else SLIPPAGE

    # Fetch mid prices before closing for slippage calculation (Patch A)
    bin_mid = 0.0
    hl_mid = 0.0
    try:
        bin_ticker = await bin_ex.fetch_ticker(pos.symbol + "/USDT:USDT")
        bin_mid = float(bin_ticker.get("last") or bin_ticker.get("close") or 0)
    except Exception:
        pass

    try:
        hl = make_hl_client()
        hl_mid = hl.get_mid(pos.symbol)
    except Exception:
        pass

    try:
        await bin_ex.load_markets()
        bin_sym = pos.symbol + "/USDT:USDT"

        # ── Close Binance leg ──────────────────────────────────────────────
        if not pos.bin_closed:
            close_side = "sell" if pos.bin_side == "buy" else "buy"

            async def close_bin_leg():
                positions_list = await bin_ex.fetch_positions([bin_sym])
                actual_size = 0.0
                for p in positions_list:
                    if p.get("symbol") == bin_sym:
                        actual_size = abs(float(p.get("contracts") or 0))
                if actual_size < 1e-9:
                    logger.info("arb_bin_already_flat", symbol=pos.symbol)
                    return {"already_closed": True}
                order = await bin_ex.create_order(
                    bin_sym, "market", close_side, actual_size,
                    params={"reduceOnly": True}
                )
                return {"order": order}

            try:
                result = await close_leg_with_retry(
                    close_bin_leg, pos.symbol, "bin", max_retries=3
                )

                if result.get("already_closed"):
                    pos.bin_closed = True
                    pos.bin_exit_fill = 0.0
                else:
                    order = result.get("order", {})
                    bin_fill = float(order.get("average") or order.get("price") or bin_mid or 0)
                    pos.bin_exit_fill = bin_fill
                    if bin_mid > 0:
                        pos.bin_exit_slippage_bps = abs(bin_fill - bin_mid) / bin_mid * 10000
                    logger.info("arb_bin_closed", symbol=pos.symbol,
                                fill=bin_fill, slippage_bps=round(pos.bin_exit_slippage_bps, 2))
                    pos.bin_closed = True

                pos.bin_close_ts = time.time()

            except Exception as e:
                err = str(e).lower()
                if "position not found" in err or "position size is zero" in err:
                    logger.info("arb_bin_already_flat", symbol=pos.symbol)
                    pos.bin_closed = True
                    pos.bin_close_ts = time.time()
                else:
                    logger.error("arb_bin_close_failed", symbol=pos.symbol, error=str(e)[:200])
                    record_close_failure()
                    pos.close_failure_count += 1
                    return False

        # ── Close HL leg ───────────────────────────────────────────────────
        if not pos.hl_closed:
            hl = make_hl_client()
            hl_pos = hl.get_position(pos.symbol)

            if not hl_pos or abs(hl_pos.size) < 1e-9:
                logger.info("arb_hl_already_flat", symbol=pos.symbol)
                pos.hl_closed = True
                pos.hl_close_ts = time.time()
                pos.hl_exit_fill = 0.0
            else:
                def close_hl_leg():
                    return hl.market_close(pos.symbol, slippage=slippage)

                try:
                    loop = asyncio.get_event_loop()
                    ok, hl_fill = await loop.run_in_executor(None, close_hl_leg)

                    if ok:
                        pos.hl_exit_fill = hl_fill
                        if hl_mid > 0:
                            pos.hl_exit_slippage_bps = abs(hl_fill - hl_mid) / hl_mid * 10000
                        pos.hl_closed = True
                        pos.hl_close_ts = time.time()
                        logger.info("arb_hl_closed", symbol=pos.symbol,
                                    fill=hl_fill, slippage_bps=round(pos.hl_exit_slippage_bps, 2))
                    else:
                        raise Exception("HL market_close returned False")

                except Exception as e:
                    err = str(e).lower()
                    if "position already closed" in err or "position not found" in err:
                        logger.info("arb_hl_already_flat", symbol=pos.symbol)
                        pos.hl_closed = True
                        pos.hl_close_ts = time.time()
                    else:
                        logger.error("arb_hl_close_FAILED_will_retry", symbol=pos.symbol, error=str(e)[:200])
                        record_close_failure()
                        pos.close_failure_count += 1
                        return False

        both_done = pos.bin_closed and pos.hl_closed

        # Store exit APYs for P&L calculation (Patch A)
        if both_done and exit_bin_apy != 0.0:
            pos.exit_bin_apy = exit_bin_apy
            pos.exit_hl_apy = exit_hl_apy
            bin_earn_sign = 1 if pos.bin_side == "buy" else -1
            hl_earn_sign = 1 if pos.hl_side == "buy" else -1
            live_bin = exit_bin_apy * bin_earn_sign * (-1)
            live_hl = exit_hl_apy * hl_earn_sign * (-1)
            pos.exit_net_apy = live_bin + live_hl

        if both_done:
            pos.needs_close = False

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

    # --- STARTUP RECONCILIATION (Patch C) ---
    positions = await reconcile_positions(positions)

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
                    # Update realized funding BEFORE updating rates (uses time since last update)
                    if pos.symbol in bin_rates and pos.symbol in hl_rates:
                        _update_realized_funding(pos, bin_rates[pos.symbol], hl_rates[pos.symbol][1])
                    if pos.symbol in bin_rates:
                        pos.last_bin_apy = bin_rates[pos.symbol]
                    if pos.symbol in hl_rates:
                        pos.last_hl_apy  = hl_rates[pos.symbol][1]
                    pos.last_rate_ts = now
            except Exception as e:
                logger.warning("rate_refresh_failed", error=str(e)[:120])

        # ── 2. Exit check ─────────────────────────────────────────────────────
        for pos in list(positions):
            bin_earn_sign = 1 if pos.bin_side == "buy" else -1
            hl_earn_sign  = 1 if pos.hl_side == "buy" else -1
            live_bin = pos.last_bin_apy * bin_earn_sign * (-1)
            live_hl  = pos.last_hl_apy  * hl_earn_sign  * (-1)
            live_net = live_bin + live_hl

            # Fee break-even gate
            import time as _time
            hold_hours = (_time.time() - pos.entry_ts) / 3600
            funding_earned = pos.funding_realized_bin + pos.funding_realized_hl
            fee_cost = pos.notional_usdt * RT_FEE_PCT
            fee_covered = funding_earned >= fee_cost
            emergency_negative = live_net < -10
            prev_funding = getattr(pos, '_prev_funding_total', None)
            current_funding = funding_earned
            if prev_funding is not None and current_funding < prev_funding:
                pos._neg_funding_intervals = getattr(pos, '_neg_funding_intervals', 0) + 1
            else:
                pos._neg_funding_intervals = 0
            pos._prev_funding_total = current_funding
            sustained_bleed = getattr(pos, '_neg_funding_intervals', 0) >= 3 and live_net < EXIT_ARB_APY

            should_exit = False
            if pos.needs_close or emergency_negative:
                should_exit = True
            elif sustained_bleed:
                should_exit = True
                if not hasattr(pos, '_bleed_logged'):
                    console.print(
                        f"[red]{pos.symbol} sustained funding bleed ({pos._neg_funding_intervals} "
                        f"consecutive declines, net APY {live_net:.1f}%) — closing[/red]"
                    )
                    pos._bleed_logged = True
            elif live_net < EXIT_ARB_APY:
                if fee_covered:
                    should_exit = True
                elif hold_hours >= MIN_HOLD_HOURS:
                    should_exit = True
                else:
                    if not hasattr(pos, '_fee_gate_logged'):
                        console.print(
                            f"[dim]{pos.symbol} net APY {live_net:.1f}% < {EXIT_ARB_APY:.0f}% "
                            f"but funding ${funding_earned:.4f} < fees ${fee_cost:.4f} "
                            f"— holding (min {MIN_HOLD_HOURS}h, at {hold_hours:.1f}h)[/dim]"
                        )
                        pos._fee_gate_logged = True

            # --- Patch C: Check position health (half-closed detection) ---
            is_healthy, health_msg = check_position_health(pos)
            if not is_healthy:
                if not pos.needs_close:
                    pos.needs_close = True
                    logger.warning("position_unhealthy", symbol=pos.symbol, reason=health_msg)

                    now_ts = time.time()
                    half_closed = pos.bin_closed != pos.hl_closed
                    if half_closed:
                        half_ts = pos.bin_close_ts if pos.bin_closed else pos.hl_close_ts
                        if half_ts and (now_ts - half_ts) > 600:
                            add_critical_alert(pos, f"Persistent half-closed: {health_msg}")
                            console.print(
                                f"[yellow]Force-closing {pos.symbol} with elevated slippage[/yellow]"
                            )
                            ok = await close_arb_position(pos, force_slippage=True)
                            if ok:
                                positions.remove(pos)
                                save_state(positions)
                                console.print(f"[green]{pos.symbol} force-closed[/green]")
                                continue
                    elif pos.close_failure_count >= 5:
                        add_critical_alert(pos, f"Excessive close failures: {pos.close_failure_count}")

            if should_exit or not is_healthy:
                if not pos.needs_close and is_healthy:
                    logger.warning("exit_signal",
                                   symbol=pos.symbol,
                                   live_net=round(live_net, 1),
                                   threshold=EXIT_ARB_APY,
                                   hold_hours=round(hold_hours, 1),
                                   funding_earned=round(funding_earned, 4),
                                   fee_cost=round(fee_cost, 4),
                                   fee_covered=fee_covered)
                    pos.needs_close = True
                    console.print(
                        f"[yellow]{pos.symbol} net APY {live_net:.1f}% < {EXIT_ARB_APY:.0f}% "
                        f"— closing (held {hold_hours:.1f}h, funding ${funding_earned:.4f} vs fees ${fee_cost:.4f})[/yellow]"
                    )
                ok = await close_arb_position(pos)
                if ok:
                    # Calculate exit APY and record close
                    exit_bin = pos.last_bin_apy
                    exit_hl = pos.last_hl_apy
                    bin_earn_sign = 1 if pos.bin_side == "buy" else -1
                    hl_earn_sign = 1 if pos.hl_side == "buy" else -1
                    live_bin = exit_bin * bin_earn_sign * (-1)
                    live_hl = exit_hl * hl_earn_sign * (-1)
                    exit_net = live_bin + live_hl

                    # === TRUE P&L CALCULATION (Patch A) ===
                    funding_earned = pos.funding_realized_hl + pos.funding_realized_bin

                    entry_slippage_cost = (
                        (pos.bin_entry_slippage_bps / 10000) * pos.notional_usdt +
                        (pos.hl_entry_slippage_bps / 10000) * pos.notional_usdt
                    )
                    exit_slippage_cost = (
                        (pos.bin_exit_slippage_bps / 10000) * pos.notional_usdt +
                        (pos.hl_exit_slippage_bps / 10000) * pos.notional_usdt
                    )
                    total_slippage_cost = entry_slippage_cost + exit_slippage_cost

                    entry_basis = pos.bin_entry_px - pos.hl_entry_px
                    exit_basis = pos.bin_exit_fill - pos.hl_exit_fill if pos.bin_exit_fill > 0 and pos.hl_exit_fill > 0 else 0.0
                    basis_change = exit_basis - entry_basis
                    basis_cost = basis_change * pos.notional_usdt / pos.bin_entry_px if pos.bin_entry_px > 0 else 0.0

                    fee_cost_close = pos.notional_usdt * RT_FEE_PCT

                    true_pnl = funding_earned - fee_cost_close - total_slippage_cost - basis_cost
                    pos.true_pnl = true_pnl

                    logger.info("true_pnl_calculation",
                               symbol=pos.symbol,
                               funding_earned=round(funding_earned, 4),
                               entry_slippage=round(entry_slippage_cost, 4),
                               exit_slippage=round(exit_slippage_cost, 4),
                               basis_change=round(basis_change, 4),
                               basis_cost=round(basis_cost, 4),
                               fee_cost=round(fee_cost_close, 4),
                               true_pnl=round(true_pnl, 4))

                    record_position_close(pos, exit_net, true_pnl)

                    # If closed due to APY drop, add to cooldown (Patch B)
                    if live_net < EXIT_ARB_APY and not getattr(pos, '_manual_close', False):
                        add_to_cooldown(pos.symbol)
                        console.print(f"[dim]{pos.symbol} added to cooldown (16h)[/dim]")

                    positions.remove(pos)
                    save_state(positions)
                    console.print(f"[green]{pos.symbol} arb closed — true P&L: ${true_pnl:.4f}[/green]")
                    break

        # ── 3. Scan for new opportunities ────────────────────────────────────
        # Check circuit breaker before scanning (Patch C)
        if is_circuit_breaker_active():
            if not hasattr(run_cross_arb, '_circuit_breaker_logged'):
                logger.warning("circuit_breaker_active",
                               failures=len(_close_failure_timestamps),
                               window_s=CIRCUIT_BREAKER_WINDOW_S)
                console.print(
                    f"[red]CIRCUIT BREAKER ACTIVE — Too many close failures "
                    f"({len(_close_failure_timestamps)} in last {CIRCUIT_BREAKER_WINDOW_S//60}min)[/red]"
                )
                run_cross_arb._circuit_breaker_logged = True
        else:
            run_cross_arb._circuit_breaker_logged = False

        if now - last_scan_ts >= SCAN_INTERVAL_S and not is_circuit_breaker_active():
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

                # Record ALL rates for history tracking (Patch B)
                all_rates = {}
                for sym in set(bin_rates.keys()) & set(hl_rates.keys()):
                    if sym in BLACKLIST:
                        continue
                    b_apy = bin_rates[sym]
                    h_apy = hl_rates[sym][1]
                    b_vol = bin_vols.get(sym, 0.0)
                    h_vol = hl_vols.get(sym, 0.0)
                    if b_vol < MIN_BIN_VOL_USD or h_vol < MIN_HL_VOL_USD:
                        continue
                    if b_apy < 0 and h_apy < 0:
                        net = abs(b_apy) - abs(h_apy) if abs(b_apy) >= abs(h_apy) else abs(h_apy) - abs(b_apy)
                    elif b_apy > 0 and h_apy > 0:
                        net = b_apy - h_apy if b_apy >= h_apy else h_apy - b_apy
                    else:
                        net = abs(b_apy) + abs(h_apy)
                    all_rates[sym] = (b_apy, h_apy, net)
                record_rates_for_symbols(all_rates)

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
