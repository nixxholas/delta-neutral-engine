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
POSITION_SIZE_USDT = float(os.getenv("FARM_SIZE_USDT",        "50"))   # min per position
MAX_POSITION_SIZE  = float(os.getenv("FARM_MAX_SIZE_USDT",    "0"))    # 0 = no cap
MIN_ENTRY_APY      = float(os.getenv("FARM_MIN_ENTRY_APY",    "15"))
EXIT_APY_THRESHOLD = float(os.getenv("FARM_EXIT_APY",         "5"))
SCAN_INTERVAL_S    = int(  os.getenv("FARM_SCAN_INTERVAL",    "1800"))
MAX_POSITIONS      = int(  os.getenv("FARM_MAX_POSITIONS",    "3"))
MIN_VOLUME_USD     = float(os.getenv("FARM_MIN_VOLUME_USD",   "0"))
BLACKLIST: set     = {s.strip().upper()
                      for s in os.getenv("FARM_BLACKLIST", "").split(",") if s.strip()}


def _calc_effective_size(available_usdt: float, free_slots: int) -> float:
    """
    Progressive capital deployment: divide available capital across remaining slots.
    Ensures full capital efficiency — idle USDT trends toward zero as slots fill.

    available_usdt  : capital available for new positions (margin or spot USDT)
    free_slots      : MAX_POSITIONS - current open positions
    Returns         : per-position size in USDT, bounded by [POSITION_SIZE_USDT, MAX_POSITION_SIZE]
    """
    if free_slots <= 0:
        return POSITION_SIZE_USDT
    # Divide by free_slots (not free_slots+1) — deploy fully across available slots
    ideal = available_usdt / free_slots
    size  = max(POSITION_SIZE_USDT, ideal)
    if MAX_POSITION_SIZE > 0:
        size = min(MAX_POSITION_SIZE, size)
    return math.floor(size)   # round down to whole USDT

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
    needs_close:       bool    = False   # True = must close regardless of current rate
    perp_closed:       bool    = False   # True = perp leg confirmed closed
    spot_closed:       bool    = False   # True = spot leg confirmed closed
    spot_is_margin:    bool    = False   # True = spot leg used cross-margin (long_perp)


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
    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    if key_path and os.path.exists(key_path):
        with open(key_path) as f:
            secret = f.read()
    else:
        secret = os.getenv("BINANCE_API_SECRET", "")
    ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
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

    # Ed25519 key: prefer SPOT-specific path, fall back to shared BINANCE_PRIVATE_KEY_PATH
    key_path = (os.getenv("BINANCE_SPOT_PRIVATE_KEY_PATH", "") or
                os.getenv("BINANCE_PRIVATE_KEY_PATH", ""))
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


def _make_margin_exchange():
    """Cross-margin exchange for long_perp spot leg (borrow+sell / buy+repay)."""
    import ccxt.async_support as ccxt
    key_path = (os.getenv("BINANCE_SPOT_PRIVATE_KEY_PATH", "") or
                os.getenv("BINANCE_PRIVATE_KEY_PATH", ""))
    config: dict = {
        "apiKey": SPOT_API_KEY,
        "options": {"defaultType": "margin"},
        "enableRateLimit": True,
    }
    if key_path and os.path.exists(key_path):
        with open(key_path) as f:
            config["secret"] = f.read()
    else:
        config["secret"] = SPOT_API_SECRET
    ex = ccxt.binance(config)
    if SPOT_TESTNET:
        ex.set_sandbox_mode(True)
    return ex


async def _fetch_margin_usdt() -> float:
    """Return free USDT in cross-margin wallet."""
    ex = _make_spot_exchange()
    try:
        await ex.load_markets()
        resp = await ex.sapi_get_margin_account()
        for asset in resp.get("userAssets", []):
            if asset["asset"] == "USDT":
                return float(asset.get("free") or 0)
        return 0.0
    except Exception as e:
        logger.warning("fetch_margin_usdt_failed", error=str(e)[:120])
        return 0.0
    finally:
        await ex.close()


async def _repay_margin_loan(ex, asset: str, qty: float) -> None:
    """Repay a cross-margin loan. Called on error cleanup to avoid dangling borrows."""
    try:
        await ex.sapi_post_margin_repay({"asset": asset, "amount": str(qty)})
        logger.info("margin_loan_repaid_cleanup", asset=asset, qty=qty)
    except Exception as e:
        logger.error("margin_loan_repay_failed_MANUAL_REQUIRED",
                     asset=asset, qty=qty, error=str(e)[:120])


async def _open_spot_margin_short(
    ex,           # margin exchange (defaultType=margin)
    symbol: str,  # e.g. "XRP/USDT"
    qty: float,
    ref_mid: float,
) -> tuple[str, float]:
    """
    Borrow base asset from cross-margin pool and sell it (short-sell).
    Used for long_perp direction: long perp + short spot = delta neutral.
    Returns (order_id, fill_price).
    If sell fails after borrow succeeds, repays the loan before raising.
    """
    base = symbol.split("/")[0]
    mkt  = ex.market(symbol)
    # Use exchange precision formatter to avoid floating-point string issues
    step      = float((mkt.get("precision") or {}).get("amount") or 0.001)
    loan_qty  = math.floor(qty / step) * step
    loan_qty  = float(ex.amount_to_precision(symbol, loan_qty))

    # Step 1: borrow the token (cross-margin, no isIsolated needed for cross)
    await _with_retry(
        lambda: ex.sapi_post_margin_loan({"asset": base, "amount": str(loan_qty)}),
        label=f"margin_borrow:{symbol}")
    logger.info("margin_borrowed", symbol=symbol, qty=loan_qty)

    # Step 2: sell via cross-margin using SAPI endpoint directly
    # (ccxt's create_order with defaultType=margin routes incorrectly for cross-margin sells)
    binance_sym = symbol.replace("/", "")  # e.g. XRP/USDT → XRPUSDT
    try:
        order = await _with_retry(
            lambda: ex.sapi_post_margin_order({
                "symbol":   binance_sym,
                "side":     "SELL",
                "type":     "MARKET",
                "quantity": str(loan_qty),
            }),
            label=f"margin_sell:{symbol}")
    except Exception as e:
        # Borrow succeeded but sell failed — repay immediately to avoid dangling loan
        logger.error("margin_sell_failed_repaying_loan", symbol=symbol, error=str(e)[:140])
        await _repay_margin_loan(ex, base, loan_qty)
        raise

    fills  = order.get("fills") or []
    fill   = (sum(float(f["price"]) * float(f["qty"]) for f in fills) /
              sum(float(f["qty"]) for f in fills)) if fills else float(
              order.get("price") or ref_mid)
    logger.info("margin_sold", symbol=symbol, qty=loan_qty, fill=fill,
                order_id=order.get("orderId"))
    return str(order.get("orderId", "")), fill


async def _close_spot_margin_short(
    ex,
    symbol: str,
    stored_size: float,
    ref_mid: float,
) -> bool:
    """
    Buy back borrowed base asset and auto-repay cross-margin loan.
    """
    base = symbol.split("/")[0]
    mkt  = ex.market(symbol)
    step = float((mkt.get("precision") or {}).get("amount") or 0.001)
    try:
        # Check current borrowed amount — buy back exactly what we owe
        resp = await ex.sapi_get_margin_asset({"asset": base})
        borrowed = float(resp.get("borrowed") or resp.get("netAsset") or stored_size)
        if borrowed < step * 0.5:
            logger.info("margin_short_already_repaid", symbol=symbol)
            return True
        buy_qty = math.floor(min(borrowed, stored_size * 1.05) / step) * step

        # Get current spot price for reference
        ticker = await ex.fetch_ticker(symbol)
        ref = float(ticker.get("last") or ticker.get("close") or ref_mid)

        binance_sym = symbol.replace("/", "")
        order = await _with_retry(
            lambda: ex.sapi_post_margin_order({
                "symbol":         binance_sym,
                "side":           "BUY",
                "type":           "MARKET",
                "quantity":       str(buy_qty),
                "sideEffectType": "AUTO_REPAY",
            }),
            label=f"margin_repay:{symbol}")
        logger.info("margin_repaid", symbol=symbol, qty=buy_qty,
                    fill=order.get("average") or ref)
        return True
    except Exception as e:
        logger.error("margin_close_failed", symbol=symbol, error=str(e)[:200])
        return False


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

async def open_position(opp: FundingOpp, size_usdt: Optional[float] = None) -> Optional[FarmPosition]:
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

        effective_usdt = size_usdt if size_usdt else POSITION_SIZE_USDT

        # Calc perp size
        perp_mkt  = perp_ex.market(opp.ccxt_symbol)
        perp_size = _calc_size(perp_mkt, mid, effective_usdt)
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

        use_margin = (opp.direction == "long_perp") and not SPOT_TESTNET
        logger.info("opening_both_legs", symbol=opp.symbol,
                    perp_side=perp_side, spot_side=spot_side,
                    size=perp_size, mid=mid, apy=round(opp.apy, 1),
                    spot_mode="cross_margin" if use_margin else "spot")

        # Open perp first, then spot. If spot fails, roll back perp.
        try:
            perp_id, perp_fill = await _open_perp(perp_ex, opp.ccxt_symbol, perp_side, perp_size)
        except Exception as e:
            logger.error("perp_leg_failed", symbol=opp.symbol, error=str(e)[:200])
            return None

        spot_id      = ""
        spot_fill    = mid
        spot_live    = False
        spot_margin  = False
        try:
            if use_margin:
                margin_ex = _make_margin_exchange()
                try:
                    await margin_ex.load_markets()
                    spot_id, spot_fill = await _open_spot_margin_short(
                        margin_ex, spot_symbol, perp_size, mid)
                    spot_margin = True
                finally:
                    await margin_ex.close()
            else:
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
            spot_is_margin   = spot_margin,
            last_rate        = opp.rate_8h,
            last_rate_ts     = time.time(),
        )

        legs = ("perp+margin_short" if spot_margin
                else "perp+spot" if spot_live
                else "perp-only (spot failed)")
        logger.info("position_opened", symbol=opp.symbol, legs=legs,
                    notional=round(perp_size * perp_fill, 2), apy=round(opp.apy, 1))
        return pos

    finally:
        await asyncio.gather(perp_ex.close(), spot_ex.close(), return_exceptions=True)


async def _fetch_actual_perp_size(ex, symbol: str) -> float:
    """Fetch actual open position size from exchange (contracts). Returns 0 if no position."""
    try:
        positions = await ex.fetch_positions([symbol])
        for p in positions:
            if p.get("symbol") == symbol:
                return abs(float(p.get("contracts") or p.get("contractSize") or 0))
        return 0.0
    except Exception as e:
        logger.warning("fetch_actual_perp_size_failed", symbol=symbol, error=str(e)[:120])
        return 0.0


async def _close_perp_robust(
    ex,
    symbol: str,
    close_side: str,   # "buy" to close short, "sell" to close long
    stored_size: float,
    ref_mid: float,
    label: str = "close",
) -> bool:
    """
    Close (or rollback) a perp position robustly.

    Strategy:
      1. Fetch actual position size from exchange — if already 0, we're done.
      2. Market reduceOnly with actual size.
      3. On failure: fall back to chunked limit orders at mark ± ticks.
      4. Verify position is 0 after all attempts.

    Returns True if position confirmed closed (or was already closed).
    """
    mkt      = ex.market(symbol)
    max_qty  = float((mkt.get("limits") or {}).get("amount", {}).get("max") or stored_size)
    step     = float((mkt.get("precision") or {}).get("amount") or 1.0)

    # Step 1: fetch actual size — if 0 already, we're done
    actual = await _fetch_actual_perp_size(ex, symbol)
    if actual < step * 0.5:
        logger.info(f"perp_{label}_already_flat", symbol=symbol)
        return True

    # Use actual size (not stored) to avoid -2022 size mismatches
    remaining = actual
    logger.info(f"perp_{label}_start", symbol=symbol,
                actual_size=actual, stored_size=stored_size, side=close_side)

    # Step 2: market reduceOnly
    try:
        await ex.create_order(symbol, "market", close_side, remaining,
                              params={"reduceOnly": True})
        logger.info(f"perp_{label}_ok", symbol=symbol, method="market")
        # Verify
        post = await _fetch_actual_perp_size(ex, symbol)
        if post < step * 0.5:
            return True
        remaining = post  # partially closed — fall through to limit orders
    except Exception as e:
        logger.warning(f"perp_{label}_market_failed", symbol=symbol, error=str(e)[:140])

    # Step 3: limit order fallback, chunked
    try:
        ticker = await ex.fetch_ticker(symbol)
        mark   = float(ticker.get("last") or ticker.get("close") or ref_mid or 1.0)
        chunk_num = 0
        while remaining > step * 0.5:
            chunk      = math.floor(min(remaining, max_qty) / step) * step
            chunk_num += 1
            placed     = False
            for mult in [0.998, 0.995, 0.990, 0.980, 0.970, 0.950]:
                lp = mark * mult if close_side == "buy" else mark / mult
                lp = round(lp, 8)
                try:
                    await ex.create_order(symbol, "limit", close_side, chunk, lp,
                                          params={"reduceOnly": True, "timeInForce": "GTC"})
                    logger.info(f"perp_{label}_ok", symbol=symbol,
                                method=f"limit@{lp:.7f}", chunk=chunk_num)
                    placed = True
                    break
                except Exception as le:
                    logger.warning(f"perp_{label}_limit_attempt_failed",
                                   symbol=symbol, mult=mult, error=str(le)[:80])
                    continue
            if not placed:
                logger.error(f"perp_{label}_all_attempts_failed_MANUAL_REQUIRED",
                             symbol=symbol, remaining=remaining)
                return False
            remaining -= chunk
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.error(f"perp_{label}_limit_phase_error_MANUAL_REQUIRED",
                     symbol=symbol, error=str(e)[:200])
        return False

    # Final verify
    post = await _fetch_actual_perp_size(ex, symbol)
    if post < step * 0.5:
        logger.info(f"perp_{label}_verified_flat", symbol=symbol)
        return True
    # Limit orders placed but may be pending fill (GTC) — treat as in-progress
    logger.warning(f"perp_{label}_limit_orders_pending", symbol=symbol, remaining=post)
    return False


async def _rollback_perp(ex, symbol: str, opened_side: str, size: float, ref_mid: float) -> None:
    """Reverse a perp leg after spot open fails."""
    rollback_side = "buy" if opened_side == "sell" else "sell"
    ok = await _close_perp_robust(ex, symbol, rollback_side, size, ref_mid, label="rollback")
    if not ok:
        logger.error("rollback_failed_MANUAL_ACTION_REQUIRED", symbol=symbol, size=size)


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
    """
    Close both legs of a position robustly.

    Strategy:
    - Close perp first (via _close_perp_robust — fetches actual size, multi-fallback).
    - Then close spot (via _close_spot_robust — fetches actual balance, robust).
    - Track per-leg state on pos.perp_closed / pos.spot_closed so retries skip done legs.
    - Returns True only if BOTH legs confirmed closed.

    IMPORTANT: If perp closes but spot fails, the position is delta-exposed.
    The main loop will retry on next tick due to pos.needs_close=True.
    A CRITICAL log is emitted so the operator knows immediately.
    """
    perp_ex = _make_perp_exchange()
    spot_ex = _make_spot_exchange()
    try:
        await perp_ex.load_markets()

        perp_close_side = "buy"  if pos.direction == "short_perp" else "sell"
        spot_close_side = "sell" if pos.direction == "short_perp" else "buy"

        # ── Perp leg ──────────────────────────────────────────────────────────
        if not pos.perp_closed:
            # Get current mark price for fallback reference
            ticker  = await perp_ex.fetch_ticker(pos.ccxt_symbol)
            ref_mid = float(ticker.get("last") or ticker.get("close") or pos.perp_entry_price)

            perp_ok = await _close_perp_robust(
                perp_ex, pos.ccxt_symbol, perp_close_side, pos.size, ref_mid)
            if perp_ok:
                pos.perp_closed = True
                logger.info("perp_leg_closed", symbol=pos.symbol)
            else:
                logger.error("perp_leg_close_FAILED_will_retry", symbol=pos.symbol)
                return False   # Don't touch spot until perp is confirmed closed

        # ── Spot leg ──────────────────────────────────────────────────────────
        if pos.spot_leg_live and not pos.spot_closed:
            if pos.spot_is_margin:
                # long_perp: buy back borrowed token + auto-repay margin loan
                margin_ex = _make_margin_exchange()
                try:
                    await margin_ex.load_markets()
                    ticker_spot = await margin_ex.fetch_ticker(pos.spot_symbol)
                    ref_spot = (float(ticker_spot.get("last") or 0) or
                                float(ticker_spot.get("close") or 0) or pos.spot_entry_price)
                    spot_ok = await _close_spot_margin_short(
                        margin_ex, pos.spot_symbol, pos.size, ref_spot)
                except Exception as e:
                    logger.error("margin_close_exception", symbol=pos.symbol, error=str(e)[:200])
                    spot_ok = False
                finally:
                    await margin_ex.close()
            else:
                # short_perp: sell back spot tokens we hold
                spot_markets_ok = False
                try:
                    await spot_ex.load_markets()
                    spot_markets_ok = True
                except Exception as e:
                    logger.error("spot_markets_unavailable_on_close", symbol=pos.symbol,
                                 error=str(e)[:120])

                if not spot_markets_ok:
                    logger.error(
                        "CRITICAL_delta_exposed_spot_leg_cannot_reach",
                        symbol=pos.symbol,
                        note="Perp closed but spot exchange unreachable — "
                             "manual spot close required!")
                    return False

                ticker_spot = await spot_ex.fetch_ticker(pos.spot_symbol)
                ref_spot = (float(ticker_spot.get("last") or 0) or
                            float(ticker_spot.get("close") or 0) or pos.spot_entry_price)
                spot_ok = await _close_spot_robust(
                    spot_ex, pos.spot_symbol, spot_close_side, pos.size, ref_spot)

            if spot_ok:
                pos.spot_closed = True
                logger.info("spot_leg_closed", symbol=pos.symbol,
                            mode="margin" if pos.spot_is_margin else "spot")
            else:
                logger.error(
                    "CRITICAL_delta_exposed_spot_close_failed",
                    symbol=pos.symbol,
                    note="Perp closed, spot close FAILED — retrying next tick")
                return False

        both_done = pos.perp_closed and (pos.spot_closed or not pos.spot_leg_live)
        logger.info("position_closed", symbol=pos.symbol, ok=both_done,
                    perp_done=pos.perp_closed, spot_done=pos.spot_closed)
        return both_done

    finally:
        await asyncio.gather(perp_ex.close(), spot_ex.close(), return_exceptions=True)


async def _fetch_actual_spot_balance(ex, asset: str) -> float:
    """Fetch actual free balance of an asset on spot exchange."""
    try:
        bal = await ex.fetch_balance()
        return float((bal.get(asset) or {}).get("free") or 0)
    except Exception as e:
        logger.warning("fetch_spot_balance_failed", asset=asset, error=str(e)[:120])
        return 0.0


async def _close_spot_robust(
    ex,
    symbol: str,    # e.g. "SXP/USDT"
    side: str,      # "sell" for short_perp unwind, "buy" for long_perp unwind
    stored_size: float,
    ref_mid: float,
) -> bool:
    """
    Close spot leg robustly.

    For 'sell' (unwinding spot buy): check actual token balance, sell what we have.
    For 'buy'  (unwinding spot short): buy back with USDT; use stored_size as reference.
    Returns True if order placed (market fills assumed instant).
    """
    base = symbol.split("/")[0]
    mkt  = ex.market(symbol)
    step = float((mkt.get("precision") or {}).get("amount") or 0.001)
    try:
        if side == "sell":
            # Selling token back to USDT — use actual balance, not stored
            actual = await _fetch_actual_spot_balance(ex, base)
            if actual < step * 0.5:
                logger.info("spot_close_already_flat", symbol=symbol, side=side)
                return True
            # Don't over-sell beyond stored_size (guard against stale balance data)
            sell_qty = math.floor(min(actual, stored_size * 1.02) / step) * step
            logger.info("spot_close_start", symbol=symbol, side=side,
                        actual_balance=actual, sell_qty=sell_qty)
            await _with_retry(
                lambda: ex.create_order(symbol, "market", "sell", sell_qty),
                label=f"close_spot:{symbol}:sell")
        else:
            # Buying back spot short — use stored_size (USDT denominated)
            # Re-derive qty from current price
            spot_ticker = await ex.fetch_ticker(symbol)
            spot_mid = (float(spot_ticker.get("last") or 0) or
                        float(spot_ticker.get("close") or 0) or ref_mid or 1.0)
            buy_qty = math.floor(min(stored_size, stored_size * 1.02) / step) * step
            logger.info("spot_close_start", symbol=symbol, side=side,
                        spot_mid=spot_mid, buy_qty=buy_qty)
            await _with_retry(
                lambda: ex.create_order(symbol, "market", "buy", buy_qty),
                label=f"close_spot:{symbol}:buy")
        logger.info("spot_close_ok", symbol=symbol, side=side)
        return True
    except Exception as e:
        logger.error("spot_close_failed", symbol=symbol, side=side, error=str(e)[:200])
        return False


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


async def _reconcile_on_startup(tracked: list) -> None:
    """
    On startup: two-pass reconciliation.

    Pass 1 — Naked positions (not in state):
        Any open perp position not tracked in state → close immediately.
        These come from failed rollbacks, crashed sessions, etc.

    Pass 2 — Size drift for tracked positions:
        For each tracked position, fetch actual exchange size.
        If it differs from stored pos.size by >5%, update pos.size to actual.
        If actual is 0 (already flat), mark pos.perp_closed = True.
        This prevents -2022 errors from stale size in state.
    """
    tracked_map = {p.ccxt_symbol: p for p in tracked}
    perp = _make_perp_exchange()
    try:
        await perp.load_markets()
        all_positions = await perp.fetch_positions()
        open_pos = {p["symbol"]: p for p in all_positions
                    if abs(float(p.get("contracts", 0))) > 0}

        # ── Pass 1: Naked ────────────────────────────────────────────────────
        naked = {sym: p for sym, p in open_pos.items() if sym not in tracked_map}
        if naked:
            logger.warning("naked_positions_detected_on_startup", count=len(naked),
                           symbols=list(naked.keys()))
            console.print(f"[yellow]⚠️  {len(naked)} naked perp position(s) detected — "
                          f"closing now...[/yellow]")
            for sym, ex_pos in naked.items():
                side       = ex_pos.get("side", "")
                contracts  = float(ex_pos.get("contracts", 0))
                close_side = "buy" if side == "short" else "sell"
                ticker     = await perp.fetch_ticker(sym)
                ref_mid    = float(ticker.get("last") or ticker.get("close") or 0)
                ok = await _close_perp_robust(perp, sym, close_side, contracts, ref_mid,
                                              label="startup_naked_close")
                console.print(
                    f"[{'green' if ok else 'red'}]  "
                    f"{'Closed' if ok else 'FAILED to close'} "
                    f"{side} {contracts:.0f} {sym}[/]")

        # ── Pass 2: Size drift for tracked positions ─────────────────────────
        size_issues = []
        for ccxt_sym, pos in tracked_map.items():
            ex_pos = open_pos.get(ccxt_sym)
            if ex_pos is None:
                actual = 0.0
            else:
                actual = abs(float(ex_pos.get("contracts", 0)))

            if actual < 1e-9:
                if not pos.perp_closed:
                    logger.info("startup_perp_already_flat_in_exchange",
                                symbol=pos.symbol, stored_size=pos.size)
                    pos.perp_closed = True
                    size_issues.append(f"{pos.symbol}: perp already flat (marking closed)")
            elif abs(actual - pos.size) / max(pos.size, 1e-9) > 0.05:
                logger.warning("startup_size_drift_corrected",
                               symbol=pos.symbol, stored=pos.size, actual=actual,
                               drift_pct=round((actual - pos.size) / pos.size * 100, 2))
                size_issues.append(
                    f"{pos.symbol}: size {pos.size:.2f} → {actual:.2f} (drift corrected)")
                pos.size = actual

        if not naked and not size_issues:
            logger.info("startup_reconcile_clean", tracked=len(tracked))
        elif size_issues:
            console.print(f"[yellow]Size drift corrections:[/yellow]")
            for issue in size_issues:
                console.print(f"[yellow]  • {issue}[/yellow]")

    finally:
        await perp.close()

    # ── Cross-margin loan cleanup ─────────────────────────────────────────────
    # Repay any dangling loans not covered by active tracked positions.
    # Arises from failed open attempts where borrow succeeded but sell failed.
    if not SPOT_TESTNET:
        active_borrows = {p.symbol for p in tracked if p.spot_is_margin and p.spot_leg_live}
        spot = _make_spot_exchange()
        try:
            await spot.load_markets()
            resp = await spot.sapi_get_margin_account()
            dangling = [(a["asset"], float(a["borrowed"]))
                        for a in resp.get("userAssets", [])
                        if float(a.get("borrowed") or 0) > 1e-6
                        and a["asset"] not in active_borrows]
            if dangling:
                logger.warning("startup_dangling_margin_loans", loans=dangling)
                console.print(f"[yellow]⚠️  Repaying {len(dangling)} dangling margin loan(s)…[/yellow]")
                for asset, amount in dangling:
                    free = float(next(
                        (a["free"] for a in resp["userAssets"] if a["asset"] == asset), 0))
                    repay = min(amount, free)
                    if repay > 1e-8:
                        await spot.sapi_post_margin_repay({"asset": asset, "amount": str(repay)})
                        console.print(f"[yellow]  ✓ Repaid {repay:.6f} {asset}[/yellow]")
        except Exception as e:
            logger.warning("startup_margin_cleanup_failed", error=str(e)[:120])
        finally:
            await spot.close()


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

    # ── Startup: reconcile exchange state vs local state ─────────────────────
    await _reconcile_on_startup(positions)
    save_state(positions)   # persist any size drift corrections immediately

    last_scan_ts = 0.0   # force scan on first loop iteration

    try:
        while True:
            # ── 1. Process WS exit signals + retry any pending closes ──────
            pending_exits: set = set()
            while not exit_queue.empty():
                try:
                    pending_exits.add(exit_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            # Mark positions that should close (new signal or rate fallen below threshold)
            for pos in positions:
                eff = (pos.last_rate if pos.direction == "short_perp"
                       else -pos.last_rate) * 3 * 365 * 100
                if pos.ccxt_symbol in pending_exits or abs(eff) < EXIT_APY_THRESHOLD:
                    if not pos.needs_close:
                        pos.needs_close = True
                        console.print(f"[red]Exit triggered: {pos.symbol} "
                                      f"({eff:.1f}% APY < {EXIT_APY_THRESHOLD}% threshold)[/red]")

            # Attempt close for all positions flagged (includes retries from prior failures)
            to_close = [p for p in positions if p.needs_close]
            closed_positions = []
            for pos in to_close:
                console.print(f"[red]Closing {pos.symbol} "
                              f"{'(retry)' if pos.perp_closed or pos.spot_closed else ''}[/red]")
                try:
                    ok = await close_position(pos)
                except Exception as e:
                    logger.error("close_position_exception", symbol=pos.symbol,
                                 error=str(e)[:200])
                    ok = False

                if ok:
                    monitor.unwatch(pos.ccxt_symbol)
                    closed_positions.append(pos)
                else:
                    logger.warning("close_will_retry_next_tick", symbol=pos.symbol,
                                   perp_done=pos.perp_closed, spot_done=pos.spot_closed)

            for pos in closed_positions:
                positions.remove(pos)

            if to_close:
                save_state(positions)

            # ── 2. Scan for new positions (rate-limited) ───────────────────
            now = time.time()
            if len(positions) < MAX_POSITIONS and (now - last_scan_ts) >= SCAN_INTERVAL_S:
                console.print(f"\n[cyan]Scanning (min {MIN_ENTRY_APY}% APY)…[/cyan]")
                opps     = await scan(min_apy=MIN_ENTRY_APY, top_n=200)
                # Volume floor + blacklist
                before = len(opps)
                opps = [o for o in opps
                        if o.volume_24h_usdt >= MIN_VOLUME_USD
                        and o.symbol not in BLACKLIST]
                if len(opps) < before:
                    console.print(f"[dim]  {before - len(opps)} filtered by volume/blacklist "
                                  f"(floor=${MIN_VOLUME_USD/1e6:.0f}M)[/dim]")
                existing = {p.symbol for p in positions}

                # Pre-filter: spot market exists + sufficient balance + borrow liquidity
                spot_ex_check = _make_spot_exchange()
                spot_symbols:   set  = set()
                spot_balances:  dict = {}
                borrowable_set: set  = set()   # symbols with borrow liquidity (long_perp)
                margin_usdt = 0.0
                try:
                    await spot_ex_check.load_markets()
                    spot_symbols = set(spot_ex_check.markets.keys())
                    bal = await spot_ex_check.fetch_balance()
                    spot_balances = {k: float(v.get("free", 0))
                                     for k, v in bal.items()
                                     if isinstance(v, dict)}

                    # Margin USDT for long_perp collateral check
                    if not SPOT_TESTNET:
                        try:
                            resp = await spot_ex_check.sapi_get_margin_account()
                            for a in resp.get("userAssets", []):
                                if a["asset"] == "USDT":
                                    margin_usdt = float(a.get("free") or 0)
                        except Exception:
                            pass

                    # Borrow liquidity pre-check for long_perp on mainnet
                    if not SPOT_TESTNET:
                        for o in opps:
                            if o.direction != "long_perp":
                                borrowable_set.add(o.symbol)
                                continue
                            if f"{o.symbol}/USDT" not in spot_symbols:
                                continue
                            try:
                                info  = await spot_ex_check.sapi_get_margin_maxborrowable(
                                    {"asset": o.symbol})
                                max_b  = float(info.get("amount") or 0)
                                needed = POSITION_SIZE_USDT / (o.mid_price or 1.0)
                                if max_b >= needed:
                                    borrowable_set.add(o.symbol)
                                    logger.debug("long_perp_borrow_ok",
                                                 symbol=o.symbol, max_borrow=round(max_b, 2))
                                else:
                                    logger.info("long_perp_borrow_unavailable",
                                                symbol=o.symbol, max_borrow=round(max_b, 2),
                                                needed=round(needed, 2))
                            except Exception as be:
                                # -3045 or any error = not borrowable right now; skip
                                logger.info("long_perp_borrow_check_failed",
                                            symbol=o.symbol, error=str(be)[:80])
                            await asyncio.sleep(0.05)
                    else:
                        borrowable_set = {o.symbol for o in opps}  # testnet: skip check

                except Exception as e:
                    logger.warning("spot_prefilter_failed", error=str(e)[:100])
                    borrowable_set = {o.symbol for o in opps}
                finally:
                    await spot_ex_check.close()

                spot_usdt = spot_balances.get("USDT", 0.0)

                def _can_execute(opp: FundingOpp) -> bool:
                    if f"{opp.symbol}/USDT" not in spot_symbols:
                        return False
                    if opp.symbol not in borrowable_set:
                        return False
                    if opp.direction == "short_perp":
                        return spot_usdt >= POSITION_SIZE_USDT * 1.05
                    else:
                        if SPOT_TESTNET:
                            held  = spot_balances.get(opp.symbol, 0.0)
                            return held * (opp.mid_price or 1.0) >= POSITION_SIZE_USDT
                        else:
                            # Need margin USDT as collateral (Binance ~10x margin ratio)
                            return margin_usdt >= POSITION_SIZE_USDT * 0.5

                opps = [o for o in opps if _can_execute(o)]
                console.print(f"[dim]  {len(opps)} opps pass filter "
                              f"(spot=${spot_usdt:.0f}, margin=${margin_usdt:.0f})[/dim]")

                for opp in opps:
                    if len(positions) >= MAX_POSITIONS:
                        break
                    if opp.symbol in existing:
                        continue

                    # Dynamic position sizing: deploy available capital across remaining slots
                    free_slots   = MAX_POSITIONS - len(positions)
                    budget       = margin_usdt if opp.direction == "long_perp" else spot_usdt
                    eff_size     = _calc_effective_size(budget, free_slots)
                    console.print(f"[green]→ {opp.symbol} {opp.apy:.0f}% APY "
                                  f"({opp.rate_8h*100:+.4f}%/8h) "
                                  f"[dim]size=${eff_size:.0f}[/dim][/green]")
                    try:
                        pos = await open_position(opp, size_usdt=eff_size)
                    except Exception as e:
                        logger.warning("open_error", symbol=opp.symbol, error=str(e)[:150])
                        pos = None
                    await asyncio.sleep(5.0)   # throttle between attempts
                    if pos:
                        positions.append(pos)
                        monitor.watch(pos.ccxt_symbol, pos.symbol, pos.direction)
                        existing.add(pos.symbol)
                        # Update margin_usdt estimate (proceeds added from short sell)
                        if opp.direction == "long_perp":
                            margin_usdt += pos.notional_usdt
                        else:
                            spot_usdt   -= pos.notional_usdt
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


# ── Spot balance display ──────────────────────────────────────────────────────

async def fetch_spot_data(positions: list[FarmPosition]) -> tuple[float, list[dict]]:
    """Return (usdt_free, per-position spot data) for display."""
    spot_ex = _make_spot_exchange()
    try:
        await spot_ex.load_markets()
        bal = await spot_ex.fetch_balance()
        usdt = float((bal.get("USDT") or {}).get("free") or 0)
        rows = []
        for pos in positions:
            b = float((bal.get(pos.symbol) or {}).get("free") or 0)
            try:
                t = await spot_ex.fetch_ticker(pos.spot_symbol)
                price = float(t.get("last") or t.get("close") or pos.spot_entry_price)
            except Exception:
                price = pos.spot_entry_price
            usd = b * price
            if pos.direction == "short_perp":
                hedge_desc = f"Hold ≥{pos.size:.0f}"
                ok = b >= pos.size * 0.95
            else:
                hedge_desc = f"Sold {pos.size:.0f}"
                ok = True
            rows.append(dict(
                sym=pos.symbol, balance=b, usd=usd, price=price,
                direction=pos.direction, hedge=hedge_desc, ok=ok,
                apy=pos.entry_apy, earned=pos.funding_collected,
                age_h=(time.time() - pos.entry_ts) / 3600,
                rate=pos.last_rate, size=pos.size,
            ))
        return usdt, rows
    finally:
        await spot_ex.close()


def show_balance(usdt: float, rows: list[dict]) -> None:
    t = Table(title="Spot Hedge Balances")
    for col in ["Asset", "Balance", "~USD", "Direction", "Hedge", "Status"]:
        t.add_column(col)
    for r in rows:
        status = "[green]✓ OK[/green]" if r["ok"] else "[red]⚠ UNDER[/red]"
        t.add_row(
            r["sym"], f"{r['balance']:,.1f}", f"${r['usd']:,.2f}",
            r["direction"], r["hedge"], status,
        )
    console.print(t)
    console.print(f"\n[cyan]USDT available:[/cyan] ${usdt:,.2f}")


def generate_dashboard_html(usdt: float, rows: list[dict]) -> str:
    import json as _json
    ts = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_earned = sum(r["earned"] for r in rows)
    total_notional = len(rows) * 500

    rows_json = _json.dumps(rows)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="60">
<title>Funding Farm Dashboard</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0b0e1a;color:#e0e6f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:28px;min-width:900px}}
  h1{{font-size:20px;font-weight:700;color:#7dd3fc;margin-bottom:4px}}
  .sub{{font-size:12px;color:#475569;margin-bottom:24px}}
  .cards{{display:flex;gap:14px;margin-bottom:28px;flex-wrap:wrap}}
  .card{{background:#141926;border:1px solid #1e2740;border-radius:10px;padding:16px 22px;min-width:150px}}
  .cl{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:5px}}
  .cv{{font-size:24px;font-weight:700}}
  .green{{color:#4ade80}} .blue{{color:#7dd3fc}} .yellow{{color:#fbbf24}}
  h2{{font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}}
  table{{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:28px}}
  thead th{{background:#141926;color:#64748b;font-weight:500;text-align:left;padding:9px 12px;border-bottom:1px solid #1e2740;font-size:11px;text-transform:uppercase;letter-spacing:.4px}}
  tr{{border-bottom:1px solid #11151f}}
  tr:hover{{background:#0f1420}}
  td{{padding:9px 12px;vertical-align:middle}}
  .sym{{font-weight:700;font-size:14px;letter-spacing:.3px}}
  .mono{{font-family:'SF Mono','Consolas',monospace;font-size:12px}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}}
  .short{{background:#7f1d1d22;color:#f87171;border:1px solid #7f1d1d55}}
  .long{{background:#14532d22;color:#4ade80;border:1px solid #14532d55}}
  .ok{{color:#4ade80;font-weight:600}} .warn{{color:#f87171;font-weight:600}}
  .pos-rate{{font-family:'SF Mono','Consolas',monospace;font-size:12px}}
  hr{{border:none;border-top:1px solid #1e2740;margin:0 0 24px 0}}
</style>
</head>
<body>
<h1>⚙️ Funding Farm</h1>
<p class="sub">Last updated: {ts} &nbsp;·&nbsp; Auto-refreshes every 60s</p>

<div class="cards">
  <div class="card"><div class="cl">USDT Free</div><div class="cv blue">${usdt:,.2f}</div></div>
  <div class="card"><div class="cl">Open Positions</div><div class="cv">{len(rows)}</div></div>
  <div class="card"><div class="cl">Est. Funding Earned</div><div class="cv green">${total_earned:,.4f}</div></div>
  <div class="card"><div class="cl">Total Notional</div><div class="cv yellow">${total_notional:,}</div></div>
</div>

<h2>Perp Positions</h2>
<table>
<thead><tr><th>Symbol</th><th>Direction</th><th>Notional</th><th>Age</th><th>Entry APY</th><th>Rate/8h</th><th>Est. Earned</th></tr></thead>
<tbody id="perp-body"></tbody>
</table>

<hr>
<h2>Spot Hedge Legs</h2>
<table>
<thead><tr><th>Asset</th><th>Balance</th><th>~USD Value</th><th>Hedge Type</th><th>Required</th><th>Status</th></tr></thead>
<tbody id="spot-body"></tbody>
</table>

<script>
const rows = {rows_json};
const pb = document.getElementById("perp-body");
const sb = document.getElementById("spot-body");
rows.forEach(function(r) {{
  const dir = r.direction === "short_perp";
  const age = r.age_h >= 24 ? (r.age_h/24).toFixed(1)+"d" : r.age_h.toFixed(1)+"h";
  const rc  = r.rate >= 0 ? "#4ade80" : "#f87171";
  const bdg = dir ? "short" : "long";
  const dlabel = dir ? "SHORT perp" : "LONG perp";
  pb.innerHTML +=
    "<tr>" +
    "<td class='sym'>" + r.sym + "</td>" +
    "<td><span class='badge " + bdg + "'>" + dlabel + "</span></td>" +
    "<td class='mono'>$500</td>" +
    "<td class='mono'>" + age + "</td>" +
    "<td class='mono'>" + r.apy.toFixed(0) + "%</td>" +
    "<td class='mono pos-rate' style='color:" + rc + "'>" + (r.rate*100).toFixed(4) + "%</td>" +
    "<td class='mono green'>$" + r.earned.toFixed(4) + "</td>" +
    "</tr>";
  const hedgeType = dir ? "Hold tokens" : "Sell tokens short";
  const stCls = r.ok ? "ok" : "warn";
  const stTxt = r.ok ? "&#10003; Hedged" : "&#9888; CHECK";
  sb.innerHTML +=
    "<tr>" +
    "<td class='sym'>" + r.sym + "</td>" +
    "<td class='mono'>" + r.balance.toLocaleString() + "</td>" +
    "<td class='mono'>$" + r.usd.toFixed(2) + "</td>" +
    "<td>" + hedgeType + "</td>" +
    "<td class='mono'>" + r.hedge + "</td>" +
    "<td class='" + stCls + "'>" + stTxt + "</td>" +
    "</tr>";
}});
</script>
</body>
</html>"""


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run",       action="store_true")
    p.add_argument("--status",    action="store_true")
    p.add_argument("--close",     action="store_true")
    p.add_argument("--scan",      action="store_true")
    p.add_argument("--balance",   action="store_true", help="Show spot hedge balances")
    p.add_argument("--dashboard", action="store_true", help="Open HTML dashboard in browser")
    args = p.parse_args()

    if args.scan:
        opps = await scan(min_apy=5.0, top_n=25)
        from scanner import print_table; print_table(opps); return

    if args.status:
        pos = load_state(); await refresh_funding(pos); show_status(pos); return

    if args.balance:
        pos = load_state()
        usdt, rows = await fetch_spot_data(pos)
        show_balance(usdt, rows)
        return

    if args.dashboard:
        import webbrowser
        pos = load_state()
        await refresh_funding(pos)
        usdt, rows = await fetch_spot_data(pos)
        html = generate_dashboard_html(usdt, rows)
        out = Path("/tmp/farm-dashboard.html")
        out.write_text(html)
        console.print(f"[cyan]Dashboard written → {out}[/cyan]")
        webbrowser.open(f"file://{out}")
        return

    if args.close:
        pos = load_state()
        remaining = []
        for p in pos:
            p.needs_close = True
            ok = await close_position(p)
            if ok:
                console.print(f"[green]✓ Closed {p.symbol}[/green]")
            else:
                console.print(f"[red]✗ {p.symbol} close FAILED — "
                              f"perp={'done' if p.perp_closed else 'OPEN'} "
                              f"spot={'done' if p.spot_closed else 'OPEN'}[/red]")
                remaining.append(p)
        save_state(remaining)
        if not remaining:
            console.print("[green]All positions closed.[/green]")
        else:
            console.print(f"[red]{len(remaining)} position(s) NOT closed — "
                          f"re-run --close to retry.[/red]")
        return

    if args.run:
        await run_farm(); return

    p.print_help()


if __name__ == "__main__":
    asyncio.run(main())
