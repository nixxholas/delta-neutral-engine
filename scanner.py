"""
scanner.py — Scan all Binance USDT-M perps for funding rate opportunities.

Uses three bulk API calls total:
  1. /fapi/v1/premiumIndex  — all perp funding rates
  2. /fapi/v1/ticker/24hr   — perp 24h volumes + prices
  3. /api/v3/ticker/24hr    — spot 24h volumes + bid/ask spreads

For long_perp candidates also checks cross-margin borrow availability
(filters -3045 lending-pool exhausted early in the scan).
"""

from __future__ import annotations

import asyncio
import argparse
import os
import time
from dataclasses import dataclass, field
from typing import List, Optional

import ccxt.async_support as ccxt
from dotenv import load_dotenv

load_dotenv("/Users/nicholas/workspace/funding-farm/.env")

# ── Scan filters ──────────────────────────────────────────────────────────────
MIN_PERP_VOL_USDT  = float(os.getenv("FARM_MIN_VOLUME_USD",    "100000000"))  # $100M default
MIN_SPOT_VOL_USDT  = float(os.getenv("FARM_MIN_SPOT_VOL_USD",  "10000000"))   # $10M default
MAX_SPOT_SPREAD_PCT= float(os.getenv("FARM_MAX_SPOT_SPREAD",   "0.30"))       # 0.30% max
MAX_RATE_8H        = 0.10   # 10%/8h cap — above this is almost certainly anomalous

BLACKLIST: set = {s.strip().upper()
                  for s in os.getenv("FARM_BLACKLIST", "").split(",") if s.strip()}


@dataclass
class FundingOpp:
    symbol:          str
    ccxt_symbol:     str
    rate_8h:         float
    apy:             float
    direction:       str           # "short_perp" | "long_perp"
    perp_vol_24h:    float
    spot_vol_24h:    float
    spot_spread_pct: float         # bid-ask spread on spot (proxy for slippage)
    next_funding_ts: Optional[float] = None
    mid_price:       float = 0.0
    borrow_ok:       bool  = True  # False if -3045 (lending pool exhausted)
    max_borrow:      float = 0.0   # max borrowable quantity (long_perp only)


def _make_perp_ex() -> ccxt.binanceusdm:
    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret   = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    return ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
        "rateLimit": 100,
    })


def _make_spot_ex() -> ccxt.binance:
    key_path = os.getenv("BINANCE_SPOT_PRIVATE_KEY_PATH") or os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret   = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    return ccxt.binance({
        "apiKey": os.getenv("BINANCE_SPOT_API_KEY") or os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "spot"},
        "enableRateLimit": True,
        "rateLimit": 100,
    })


async def scan(
    min_apy:       float = 0.0,
    top_n:         int   = 200,
    check_borrow:  bool  = True,
) -> List[FundingOpp]:
    """
    Returns opportunities sorted by |APY| descending.
    Each candidate has passed BOTH perp AND spot liquidity checks.
    For long_perp: also checks cross-margin borrow availability.
    """
    perp_ex = _make_perp_ex()
    spot_ex  = _make_spot_ex()

    try:
        await asyncio.gather(perp_ex.load_markets(), spot_ex.load_markets())

        # ── 1. Bulk perp funding rates + interval metadata ────────────────────
        rates_raw, funding_info_raw = await asyncio.gather(
            perp_ex.fapiPublicGetPremiumIndex({}),
            perp_ex.fapiPublicGetFundingInfo({}),
        )
        # {symbolUSDT → settlement hours} e.g. AZTECUSDT → 1, EIGENUSDT → 4
        funding_interval: dict[str, int] = {
            r["symbol"]: int(r.get("fundingIntervalHours", 8))
            for r in funding_info_raw
        }

        # ── 2. Bulk perp 24h volumes + prices ─────────────────────────────────
        perp_vol, perp_price = await _bulk_perp_volumes(perp_ex)

        # ── 3. Bulk spot 24h volumes + spreads ────────────────────────────────
        spot_vol, spot_spread = await _bulk_spot_data(spot_ex)

        # ── 4. Build candidates ────────────────────────────────────────────────
        spot_symbols = set(spot_ex.markets.keys())
        candidates: List[FundingOpp] = []

        for entry in rates_raw:
            sym_raw = entry.get("symbol", "")
            if not sym_raw.endswith("USDT"):
                continue
            base    = sym_raw[:-4]
            rate    = float(entry.get("lastFundingRate") or 0)
            next_ts = float(entry.get("nextFundingTime") or 0) / 1000

            # Hard filters
            if abs(rate) > MAX_RATE_8H:
                continue
            if base in BLACKLIST:
                continue

            # Map to ccxt symbol
            ccxt_sym = f"{base}/USDT:USDT"
            if ccxt_sym not in perp_ex.markets:
                continue
            mkt = perp_ex.markets[ccxt_sym]
            if not (mkt.get("linear") and mkt.get("swap")):
                continue

            # Correct APY using actual settlement interval (not hardcoded 8h/3×)
            interval_h = funding_interval.get(sym_raw, 8)
            apy = rate * (24 / interval_h) * 365 * 100
            if abs(apy) < min_apy:
                continue

            # ── Liquidity gates ──────────────────────────────────────────────
            pv = perp_vol.get(sym_raw, 0.0)
            if pv < MIN_PERP_VOL_USDT:
                continue

            spot_sym = f"{base}/USDT"
            if spot_sym not in spot_symbols:
                continue   # no spot market on Binance

            sv = spot_vol.get(base, 0.0)
            if sv < MIN_SPOT_VOL_USDT:
                continue

            sp = spot_spread.get(base, 999.0)
            if sp > MAX_SPOT_SPREAD_PCT:
                continue   # too wide — slippage will eat returns

            candidates.append(FundingOpp(
                symbol          = base,
                ccxt_symbol     = ccxt_sym,
                rate_8h         = rate,
                apy             = apy,
                direction       = "short_perp" if rate >= 0 else "long_perp",
                perp_vol_24h    = pv,
                spot_vol_24h    = sv,
                spot_spread_pct = sp,
                next_funding_ts = next_ts or None,
                mid_price       = perp_price.get(sym_raw, 0.0),
                borrow_ok       = True,
            ))

        # ── 5. Borrow check for long_perp (parallel) ─────────────────────────
        if check_borrow:
            lp = [c for c in candidates if c.direction == "long_perp"]
            if lp:
                await _check_borrow_batch(spot_ex, lp)

        candidates.sort(key=lambda x: abs(x.apy), reverse=True)
        return candidates[:top_n]

    finally:
        await asyncio.gather(perp_ex.close(), spot_ex.close())


async def _bulk_perp_volumes(ex: ccxt.binanceusdm):
    """Returns ({rawSym: quoteVol}, {rawSym: lastPrice})."""
    try:
        tickers = await ex.fetch_tickers()
        vols, prices = {}, {}
        for sym, t in tickers.items():
            raw            = sym.replace("/USDT:USDT", "USDT")
            vols[raw]      = float(t.get("quoteVolume") or t.get("baseVolume") or 0)
            prices[raw]    = float(t.get("last") or t.get("close") or 0)
        return vols, prices
    except Exception:
        return {}, {}


async def _bulk_spot_data(ex: ccxt.binance):
    """
    Returns ({base: quoteVol}, {base: spread_pct}) for all BASE/USDT spot pairs.
    Single call via fetch_tickers().
    """
    try:
        tickers = await ex.fetch_tickers()
        vols, spreads = {}, {}
        for sym, t in tickers.items():
            if not sym.endswith("/USDT") or ":" in sym:
                continue
            base = sym.replace("/USDT", "")
            vols[base] = float(t.get("quoteVolume") or 0)
            bid = float(t.get("bid") or 0)
            ask = float(t.get("ask") or 0)
            if bid > 0 and ask > 0:
                spreads[base] = (ask - bid) / bid * 100
            else:
                spreads[base] = 999.0
        return vols, spreads
    except Exception:
        return {}, {}


async def _check_borrow_batch(spot_ex: ccxt.binance, candidates: List[FundingOpp]) -> None:
    """
    Check cross-margin borrow availability for long_perp candidates.
    Sets candidate.borrow_ok=False and candidate.max_borrow=0 if -3045
    (lending pool exhausted) or any other error.
    Runs concurrently with a small semaphore to avoid rate limits.
    """
    sem = asyncio.Semaphore(5)   # max 5 concurrent borrow checks

    async def _check_one(opp: FundingOpp) -> None:
        async with sem:
            try:
                result = await spot_ex.sapiGetMarginMaxBorrowable(
                    params={"asset": opp.symbol, "isolatedSymbol": None}
                )
                mb = float((result or {}).get("amount", 0))
                opp.max_borrow = mb
                opp.borrow_ok  = mb > 0
            except Exception as e:
                msg = str(e)
                if "-3045" in msg or "does not have enough" in msg.lower():
                    opp.borrow_ok  = False
                    opp.max_borrow = 0.0
                else:
                    # Unknown error — be conservative, exclude
                    opp.borrow_ok  = False
                    opp.max_borrow = 0.0
            await asyncio.sleep(0.05)

    await asyncio.gather(*[_check_one(o) for o in candidates])


def print_table(opps: List[FundingOpp], show_borrow: bool = True) -> None:
    print(f"\n{'Token':<7} {'APY':>9}  {'Dir':<12} {'Perp$24h':>9}  {'Spot$24h':>9}  {'Spread':>7}  {'Borrow':>8}  {'Price':>10}")
    print("─" * 95)
    for o in opps:
        apy_abs = abs(o.apy)
        flag    = " 🔥" if apy_abs > 50 else (" ⚠️" if apy_abs < 10 else "")
        borrow  = f"{o.max_borrow:.0f}" if o.borrow_ok and o.max_borrow > 0 else ("N/A" if not o.borrow_ok else "—")
        borrow_col = borrow if o.direction == "long_perp" else "—"
        print(
            f"{o.symbol:<7} {o.apy:>8.1f}%  {o.direction:<12} "
            f"${o.perp_vol_24h/1e6:>6.0f}M   ${o.spot_vol_24h/1e6:>6.0f}M   "
            f"{o.spot_spread_pct:>5.3f}%  {borrow_col:>8}  "
            f"${o.mid_price:>10.4f}{flag}"
        )
    print(f"\n{len(opps)} opportunities (perp>${MIN_PERP_VOL_USDT/1e6:.0f}M, spot>${MIN_SPOT_VOL_USDT/1e6:.0f}M, spread<{MAX_SPOT_SPREAD_PCT:.2f}%)")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--min",    type=float, default=5.0,  help="Min |APY|%%")
    p.add_argument("--top",    type=int,   default=30,   help="Max results")
    p.add_argument("--no-borrow", action="store_true",   help="Skip borrow check")
    args = p.parse_args()
    opps = asyncio.run(scan(min_apy=args.min, top_n=args.top, check_borrow=not args.no_borrow))
    print_table(opps)
