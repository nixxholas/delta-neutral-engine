"""
farm.py — Delta-neutral funding rate farm.

Strategy A (positive funding): BUY spot + SHORT perp → collect funding from longs
Strategy B (negative funding): LONG perp + SELL spot  → collect funding from shorts

Both legs execute simultaneously for true delta neutrality.
Spot leg uses Binance spot API; perp leg uses Binance USDT-M futures API.

Usage:
    python farm.py --run          # start farming
    python farm.py --status       # show positions + P&L
    python farm.py --close        # close all positions
    python farm.py --scan         # scan rates, don't trade
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional

import structlog
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from scanner import scan, FundingOpp

_ENV = Path(__file__).parent / ".env"
load_dotenv(_ENV)
logger    = structlog.get_logger(__name__)
console   = Console()
STATE_FILE = Path("/tmp/funding-farm-state.json")

# ── Retry / rate-limit helpers ─────────────────────────────────────────────────

async def _with_retry(coro_fn, *, label="op", max_attempts=5, base_delay=2.0):
    """
    Execute coro_fn() with exponential backoff.
    Handles Binance rate limits (418 IP ban, 429, -1000/-1003/-1015 order limits).
    """
    import ccxt

    _RATE_ERRS = (ccxt.RateLimitExceeded, ccxt.DDoSProtection)

    def _is_rate_limited(e: Exception) -> bool:
        if isinstance(e, _RATE_ERRS):
            return True
        msg = str(e).lower()
        # Binance -1000 "Global Order rate limitation exceeded"
        # Binance -1015 "Too many new orders"
        # Binance -1003 "Way too many requests"
        return any(x in msg for x in [
            "rate limit", "rate limitation", "too many orders",
            "too many requests", "global order", "-1000", "-1003", "-1015",
        ])

    def _is_ban(e: Exception) -> bool:
        msg = str(e).lower()
        return "418" in str(e) or "banned" in msg or "ip banned" in msg

    for attempt in range(1, max_attempts + 1):
        try:
            return await coro_fn()
        except Exception as e:
            if not (_is_rate_limited(e) or isinstance(e, ccxt.NetworkError)):
                raise   # non-retriable — bubble up immediately

            wait = base_delay * (2 ** (attempt - 1))
            if _is_ban(e):
                wait = max(wait, 60.0)   # Binance IP bans last ≥ 60s
            logger.warning("retry_backoff", label=label, attempt=attempt,
                           wait_s=round(wait, 1), ban=_is_ban(e), error=str(e)[:100])
            if attempt == max_attempts:
                raise
            await asyncio.sleep(wait)

# ── Config ─────────────────────────────────────────────────────────────────────
POSITION_SIZE_USDT = float(os.getenv("FARM_SIZE_USDT",        "500"))
MIN_ENTRY_APY      = float(os.getenv("FARM_MIN_ENTRY_APY",    "15"))
EXIT_APY_THRESHOLD = float(os.getenv("FARM_EXIT_APY",         "5"))
SCAN_INTERVAL_S    = int(  os.getenv("FARM_SCAN_INTERVAL",    "1800"))
MAX_POSITIONS      = int(  os.getenv("FARM_MAX_POSITIONS",    "3"))

# Spot exchange credentials (defaults to same keys as futures — works on mainnet)
SPOT_API_KEY    = os.getenv("BINANCE_SPOT_API_KEY")    or os.getenv("BINANCE_API_KEY",    "")
SPOT_API_SECRET = os.getenv("BINANCE_SPOT_API_SECRET") or os.getenv("BINANCE_API_SECRET", "")
SPOT_TESTNET    = os.getenv("BINANCE_SPOT_TESTNET",    "false").lower() == "true"


# ── Position state ────────────────────────────────────────────────────────────

@dataclass
class FarmPosition:
    symbol:            str
    ccxt_symbol:       str     # perp  e.g. BTC/USDT:USDT
    spot_symbol:       str     # spot  e.g. BTC/USDT
    direction:         str     # "short_perp" | "long_perp"
    perp_order_id:     str
    spot_order_id:     str
    perp_entry_price:  float
    spot_entry_price:  float
    size:              float   # base asset quantity (same both legs)
    notional_usdt:     float
    entry_rate_8h:     float
    entry_apy:         float
    entry_ts:          float
    spot_leg_live:     bool    = False   # False = notional only
    funding_collected: float   = 0.0
    last_rate:         float   = 0.0
    last_rate_ts:      float   = 0.0


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


# ── Exchange factories ────────────────────────────────────────────────────────

def _make_perp_exchange():
    import ccxt.async_support as ccxt
    ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
        "rateLimit": 100,       # 10 req/s — well under Binance 1200/min weight limit
    })
    if os.getenv("BINANCE_DEMO", "false").lower() == "true":
        test_urls = ex.urls.get("test", {})
        api_urls  = ex.urls.setdefault("api", {})
        for group, url in test_urls.items():
            if "fapi" in group or "dapi" in group:
                api_urls[group] = url
        _orig = ex.fetch
        async def _no_sapi(url, m="GET", h=None, b=None):
            if "/sapi/" in str(url): return []
            return await _orig(url, method=m, headers=h, body=b)
        ex.fetch = _no_sapi
    return ex


def _make_spot_exchange():
    import ccxt.async_support as ccxt

    key_path = os.getenv("BINANCE_SPOT_PRIVATE_KEY_PATH", "")
    config: dict = {
        "apiKey": SPOT_API_KEY,
        "options": {"defaultType": "spot"},
        "enableRateLimit": True,
    }

    if key_path and os.path.exists(key_path):
        # Ed25519 signing: CCXT detects PEM by checking if secret contains "PRIVATE KEY"
        with open(key_path, "r") as f:
            config["secret"] = f.read()
    else:
        config["secret"] = SPOT_API_SECRET

    ex = ccxt.binance(config)
    if SPOT_TESTNET:
        ex.set_sandbox_mode(True)
    return ex


# ── Sizing helpers ────────────────────────────────────────────────────────────

def _calc_size(market: dict, mid: float, usdt: float) -> Optional[float]:
    prec    = market.get("precision", {})
    limits  = market.get("limits", {})
    step    = float(prec.get("amount") or 0.001)
    min_qty = float((limits.get("amount") or {}).get("min") or 0.0)
    max_qty = float((limits.get("amount") or {}).get("max") or float("inf"))
    size    = math.floor((usdt / mid) / step) * step
    size    = max(size, min_qty)
    size    = min(size, max_qty)
    if size <= 0 or size * mid > usdt * 3:
        return None
    return size


async def _get_mid(ex, symbol: str) -> Optional[float]:
    try:
        t = await ex.fetch_ticker(symbol)
        return (
            float(t.get("markPrice") or 0) or
            float(t.get("last")      or 0) or
            float(t.get("close")     or 0) or
            float(t.get("bid")       or 0)
        ) or None
    except Exception:
        return None


# ── Open both legs ────────────────────────────────────────────────────────────

async def open_position(opp: FundingOpp) -> Optional[FarmPosition]:
    """Open perp + spot legs simultaneously (asyncio.gather)."""
    spot_symbol = opp.symbol + "/USDT"

    perp_ex = _make_perp_exchange()
    spot_ex = _make_spot_exchange()

    try:
        # Load perp markets (mandatory); spot markets (optional)
        await perp_ex.load_markets()
        spot_markets_ok = False
        try:
            await spot_ex.load_markets()
            spot_markets_ok = True
        except Exception as e:
            logger.warning("spot_markets_unavailable", error=str(e)[:120])

        # Get mid from perp
        mid = await _get_mid(perp_ex, opp.ccxt_symbol)
        if not mid:
            logger.error("open_no_price", symbol=opp.symbol)
            return None

        # Calc perp size
        perp_mkt  = perp_ex.market(opp.ccxt_symbol)
        perp_size = _calc_size(perp_mkt, mid, POSITION_SIZE_USDT)
        if not perp_size:
            logger.warning("open_size_invalid", symbol=opp.symbol, mid=mid)
            return None

        perp_side = "sell" if opp.direction == "short_perp" else "buy"
        spot_side = "buy"  if opp.direction == "short_perp" else "sell"

        # ── Enforce delta neutrality: spot MUST be available before any perp opens ──
        if not spot_markets_ok:
            logger.warning("spot_unavailable_skipping", symbol=opp.symbol,
                           note="No spot API — will not open naked perp. "
                                "Set BINANCE_SPOT_API_KEY in .env to enable.")
            return None

        logger.info("opening_both_legs", symbol=opp.symbol,
                    perp_side=perp_side, spot_side=spot_side,
                    size=perp_size, mid=mid, apy=round(opp.apy, 1))

        # Open perp first, then spot. If spot fails, roll back perp.
        try:
            perp_id, perp_fill = await _open_perp(perp_ex, opp.ccxt_symbol, perp_side, perp_size)
        except Exception as e:
            logger.error("perp_leg_failed", symbol=opp.symbol, error=str(e)[:200])
            return None

        spot_id   = ""
        spot_fill = mid
        spot_live = False
        try:
            spot_id, spot_fill = await _open_spot(
                spot_ex, spot_symbol, spot_side, perp_size, mid)
            spot_live = True
        except Exception as e:
            logger.error("spot_leg_failed_rolling_back_perp",
                         symbol=opp.symbol, error=str(e)[:200])
            await _rollback_perp(perp_ex, opp.ccxt_symbol, perp_side, perp_size, mid)
            return None

        pos = FarmPosition(
            symbol           = opp.symbol,
            ccxt_symbol      = opp.ccxt_symbol,
            spot_symbol      = spot_symbol,
            direction        = opp.direction,
            perp_order_id    = perp_id,
            spot_order_id    = spot_id,
            perp_entry_price = perp_fill,
            spot_entry_price = spot_fill,
            size             = perp_size,
            notional_usdt    = perp_size * perp_fill,
            entry_rate_8h    = opp.rate_8h,
            entry_apy        = opp.apy,
            entry_ts         = time.time(),
            spot_leg_live    = spot_live,
            last_rate        = opp.rate_8h,
            last_rate_ts     = time.time(),
        )

        legs = "perp+spot" if spot_live else "perp-only (spot failed)"
        logger.info("position_opened", symbol=opp.symbol, legs=legs,
                    notional=round(perp_size * perp_fill, 2), apy=round(opp.apy, 1))
        return pos

    finally:
        await asyncio.gather(perp_ex.close(), spot_ex.close(), return_exceptions=True)


async def _rollback_perp(ex, symbol: str, opened_side: str, size: float, ref_mid: float) -> None:
    """
    Reverse a perp leg after spot fails. Tries multiple strategies to handle
    testnet PERCENT_PRICE filter and max-quantity limits.
    """
    rollback_side = "buy" if opened_side == "sell" else "sell"
    mkt     = ex.market(symbol)
    max_qty = float((mkt.get("limits") or {}).get("amount", {}).get("max") or size)
    step    = float((mkt.get("precision") or {}).get("amount") or 1.0)

    # Strategy 1: simple market reduceOnly
    try:
        await ex.create_order(symbol, "market", rollback_side, size,
                              params={"reduceOnly": True})
        logger.info("perp_rolled_back", symbol=symbol, method="market")
        return
    except Exception as e:
        logger.warning("rollback_market_failed", symbol=symbol, error=str(e)[:120])

    # Strategy 2: limit order at mark price ± small ticks, chunked if needed
    try:
        ticker = await ex.fetch_ticker(symbol)
        mark   = float(ticker.get("last") or ticker.get("close") or ref_mid)
        remaining = size
        chunk_num = 0
        while remaining > step * 0.5:
            chunk      = math.floor(min(remaining, max_qty) / step) * step
            chunk_num += 1
            placed     = False
            for mult in [0.998, 0.995, 0.990, 0.980, 0.970]:
                lp = mark * mult if rollback_side == "buy" else mark / mult
                lp = round(lp, 8)
                try:
                    await ex.create_order(symbol, "limit", rollback_side, chunk, lp,
                                          params={"reduceOnly": True, "timeInForce": "GTC"})
                    logger.info("perp_rolled_back", symbol=symbol,
                                method=f"limit@{lp:.7f}", chunk=chunk_num)
                    placed = True
                    break
                except Exception:
                    continue
            if not placed:
                raise RuntimeError(f"all limit attempts failed for chunk {chunk_num}")
            remaining -= chunk
    except Exception as rb_err:
        logger.error("rollback_failed_MANUAL_ACTION_REQUIRED",
                     symbol=symbol, size=size, error=str(rb_err)[:200])


async def _open_perp(ex, symbol: str, side: str, size: float):
    order = await _with_retry(
        lambda: ex.create_order(symbol, "market", side, size, params={"reduceOnly": False}),
        label=f"open_perp:{symbol}:{side}")
    fill  = float(order.get("average") or order.get("price") or 0)
    return str(order["id"]), fill


async def _open_spot(ex, symbol: str, side: str, perp_size: float, perp_mid: float):
    """
    Open spot leg. Uses the actual spot price for sizing (not perp mark price)
    so notional matches in USD terms — critical for delta neutrality.
    """
    try:
        mkt = ex.market(symbol)
        # Get spot price independently
        spot_ticker = await ex.fetch_ticker(symbol)
        spot_mid = (
            float(spot_ticker.get("last")  or 0) or
            float(spot_ticker.get("close") or 0) or
            float(spot_ticker.get("bid")   or 0) or
            perp_mid  # last resort fallback
        )
        target_usdt = perp_size * perp_mid   # dollar notional to match
        adj = _calc_size(mkt, spot_mid, target_usdt)
        if not adj:
            raise ValueError(f"invalid spot size for {symbol} "
                             f"(spot_mid={spot_mid}, target=${target_usdt:.2f})")
        order = await _with_retry(
            lambda: ex.create_order(symbol, "market", side, adj),
            label=f"open_spot:{symbol}:{side}")
        fill  = float(order.get("average") or order.get("price") or spot_mid)
        return str(order["id"]), fill
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"spot open failed: {e}") from e


# ── Close both legs ───────────────────────────────────────────────────────────

async def close_position(pos: FarmPosition) -> bool:
    perp_ex = _make_perp_exchange()
    spot_ex = _make_spot_exchange()
    try:
        await perp_ex.load_markets()
        spot_ok = False
        try:
            await spot_ex.load_markets()
            spot_ok = True
        except Exception:
            pass
        perp_close_side = "buy"  if pos.direction == "short_perp" else "sell"
        spot_close_side = "sell" if pos.direction == "short_perp" else "buy"

        tasks = [_close_perp(perp_ex, pos.ccxt_symbol, perp_close_side, pos.size)]
        if pos.spot_leg_live and spot_ok:
            tasks.append(_close_spot(spot_ex, pos.spot_symbol, spot_close_side, pos.size))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = not any(isinstance(r, Exception) for r in results)
        logger.info("position_closed", symbol=pos.symbol, ok=ok)
        return ok
    finally:
        await asyncio.gather(perp_ex.close(), spot_ex.close(), return_exceptions=True)


async def _close_perp(ex, symbol, side, size):
    order = await _with_retry(
        lambda: ex.create_order(symbol, "market", side, size, params={"reduceOnly": True}),
        label=f"close_perp:{symbol}:{side}")
    return order


async def _close_spot(ex, symbol, side, size):
    try:
        mkt = ex.market(symbol)
        adj = _calc_size(mkt, 1.0, size) or size
        order = await _with_retry(
            lambda: ex.create_order(symbol, "market", side, size),
            label=f"close_spot:{symbol}:{side}")
        return order
    except Exception as e:
        raise RuntimeError(f"spot close failed: {e}") from e


# ── Funding refresh ───────────────────────────────────────────────────────────

async def refresh_funding(positions: list[FarmPosition]) -> None:
    if not positions:
        return
    ex = _make_perp_exchange()
    try:
        for pos in positions:
            try:
                info = await ex.fetch_funding_rate(pos.ccxt_symbol)
                rate = float(info.get("fundingRate") or 0)
                pos.last_rate    = rate
                pos.last_rate_ts = time.time()
                # Approximate accrued funding (signed: short_perp earns positive rate)
                elapsed_8h = (time.time() - pos.entry_ts) / (8 * 3600)
                sign = 1 if pos.direction == "short_perp" else -1
                pos.funding_collected = pos.notional_usdt * pos.entry_rate_8h * elapsed_8h * sign
            except Exception as e:
                logger.warning("refresh_funding_error", symbol=pos.symbol, error=str(e))
    finally:
        await ex.close()


# ── Display ───────────────────────────────────────────────────────────────────

def show_status(positions: list[FarmPosition]) -> None:
    if not positions:
        console.print("[yellow]No active farm positions.[/yellow]")
        return
    t = Table(title="Active Funding Farm Positions")
    for col in ["Symbol", "Strategy", "Notional", "Legs", "Entry APY",
                "Cur Rate/8h", "Est. Funding Earned", "Age"]:
        t.add_column(col)
    for p in positions:
        age_h  = (time.time() - p.entry_ts) / 3600
        strat  = "Short perp" if p.direction == "short_perp" else "Long perp"
        legs   = "perp+spot" if p.spot_leg_live else "perp-only"
        t.add_row(
            p.symbol, strat, f"${p.notional_usdt:.0f}", legs,
            f"{p.entry_apy:.1f}%", f"{p.last_rate*100:+.4f}%",
            f"${p.funding_collected:.4f}", f"{age_h:.1f}h",
        )
    console.print(t)


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_farm() -> None:
    from monitor import FarmMonitor, RateUpdate

    logger.info("farm_starting", size_usdt=POSITION_SIZE_USDT,
                min_apy=MIN_ENTRY_APY, exit_apy=EXIT_APY_THRESHOLD,
                spot_testnet=SPOT_TESTNET)
    positions = load_state()

    # WS exit signals arrive here (non-blocking)
    exit_queue: asyncio.Queue = asyncio.Queue()

    def _on_rate_update(u: RateUpdate) -> None:
        for pos in positions:
            if pos.ccxt_symbol == u.ccxt_symbol:
                pos.last_rate    = u.funding_rate
                pos.last_rate_ts = u.ts

    def _on_exit_signal(u: RateUpdate) -> None:
        logger.warning("ws_exit_signal", symbol=u.symbol,
                       apy=round(u.apy, 1), threshold=EXIT_APY_THRESHOLD)
        exit_queue.put_nowait(u.ccxt_symbol)

    # Start WS monitor — single connection, all symbols at 1s
    monitor = FarmMonitor(
        exit_apy_threshold = EXIT_APY_THRESHOLD,
        on_rate_update     = _on_rate_update,
        on_exit_signal     = _on_exit_signal,
    )
    for pos in positions:
        monitor.watch(pos.ccxt_symbol, pos.symbol, pos.direction)
    await monitor.start()

    last_scan_ts = 0.0   # force scan on first loop iteration

    try:
        while True:
            # ── 1. Process WS exit signals immediately ─────────────────────
            pending_exits: set = set()
            while not exit_queue.empty():
                try:
                    pending_exits.add(exit_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            to_close = [p for p in positions
                        if p.ccxt_symbol in pending_exits
                        or abs(p.last_rate * 3 * 365 * 100) < EXIT_APY_THRESHOLD]
            for pos in to_close:
                eff = (pos.last_rate if pos.direction == "short_perp"
                       else -pos.last_rate) * 3 * 365 * 100
                console.print(f"[red]Closing {pos.symbol} — "
                              f"rate {eff:.1f}% APY < threshold[/red]")
                if await close_position(pos):
                    monitor.unwatch(pos.ccxt_symbol)
                    positions.remove(pos)

            # ── 2. Scan for new positions (rate-limited) ───────────────────
            now = time.time()
            if len(positions) < MAX_POSITIONS and (now - last_scan_ts) >= SCAN_INTERVAL_S:
                console.print(f"\n[cyan]Scanning (min {MIN_ENTRY_APY}% APY)…[/cyan]")
                opps     = await scan(min_apy=MIN_ENTRY_APY, top_n=30)
                existing = {p.symbol for p in positions}

                # Pre-filter: spot market exists + sufficient balance for leg
                spot_ex_check = _make_spot_exchange()
                spot_symbols: set  = set()
                spot_balances: dict = {}
                try:
                    await spot_ex_check.load_markets()
                    spot_symbols = set(spot_ex_check.markets.keys())
                    bal = await spot_ex_check.fetch_balance()
                    spot_balances = {k: float(v.get("free", 0))
                                     for k, v in bal.items()
                                     if isinstance(v, dict)}
                except Exception as e:
                    logger.warning("spot_prefilter_failed", error=str(e)[:100])
                finally:
                    await spot_ex_check.close()

                spot_usdt = spot_balances.get("USDT", 0.0)

                def _can_execute(opp: FundingOpp) -> bool:
                    if f"{opp.symbol}/USDT" not in spot_symbols:
                        return False
                    if opp.direction == "short_perp":
                        return spot_usdt >= POSITION_SIZE_USDT * 0.9
                    else:
                        held      = spot_balances.get(opp.symbol, 0.0)
                        held_usdt = held * (opp.mid_price or 1.0)
                        return held_usdt >= POSITION_SIZE_USDT * 0.9

                opps = [o for o in opps if _can_execute(o)]
                console.print(f"[dim]  {len(opps)} opps pass spot pre-filter "
                              f"(USDT=${spot_usdt:.0f})[/dim]")

                for opp in opps:
                    if len(positions) >= MAX_POSITIONS:
                        break
                    if opp.symbol in existing:
                        continue
                    console.print(f"[green]→ {opp.symbol} {opp.apy:.0f}% APY "
                                  f"({opp.rate_8h*100:+.4f}%/8h)[/green]")
                    try:
                        pos = await open_position(opp)
                    except Exception as e:
                        logger.warning("open_error", symbol=opp.symbol, error=str(e)[:150])
                        pos = None
                    await asyncio.sleep(5.0)   # throttle between attempts
                    if pos:
                        positions.append(pos)
                        monitor.watch(pos.ccxt_symbol, pos.symbol, pos.direction)
                        existing.add(pos.symbol)
                        legs = "perp+spot ✓" if pos.spot_leg_live else "perp-only"
                        console.print(f"[green]  Opened ${pos.notional_usdt:.0f} "
                                      f"— {legs}[/green]")

                last_scan_ts = time.time()
                save_state(positions)
                show_status(positions)
                next_min = SCAN_INTERVAL_S // 60
                console.print(f"\n[dim]Next scan in {next_min}min "
                              f"| WS monitor: {len(monitor._watched)} symbols[/dim]")

            # ── 3. Short sleep — WS drives real-time exits, this paces scan ─
            await asyncio.sleep(30)

    finally:
        await monitor.stop()
        logger.info("farm_stopped")


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run",    action="store_true")
    p.add_argument("--status", action="store_true")
    p.add_argument("--close",  action="store_true")
    p.add_argument("--scan",   action="store_true")
    args = p.parse_args()

    if args.scan:
        opps = await scan(min_apy=5.0, top_n=25)
        from scanner import print_table; print_table(opps); return

    if args.status:
        pos = load_state(); await refresh_funding(pos); show_status(pos); return

    if args.close:
        pos = load_state()
        for p in pos: await close_position(p)
        save_state([])
        console.print("[red]All positions closed.[/red]"); return

    if args.run:
        await run_farm(); return

    p.print_help()


if __name__ == "__main__":
    asyncio.run(main())
