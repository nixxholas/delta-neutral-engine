"""
scanner.py — Scan all Binance USDT-M perps for funding rate opportunities.

Optimised:
- Single bulk call to /fapi/v1/premiumIndex (replaces 640 individual requests)
- Single bulk call to /fapi/v1/ticker/24hr for volume
- Filters illiquid / anomalous-rate tokens before returning
"""

from __future__ import annotations

import asyncio
import argparse
import os
import time
from dataclasses import dataclass
from typing import List, Optional

import ccxt.async_support as ccxt
from dotenv import load_dotenv

load_dotenv("/Users/nicholas/workspace/funding-farm/.env")

MIN_VOLUME_USDT = 5_000_000   # $5M 24h volume minimum
MAX_RATE_8H     = 0.10        # 10%/8h cap — above this is almost certainly testnet noise


@dataclass
class FundingOpp:
    symbol: str
    ccxt_symbol: str
    rate_8h: float
    apy: float
    direction: str           # "short_perp" | "long_perp"
    volume_24h_usdt: float
    next_funding_ts: Optional[float] = None


def _make_ex() -> ccxt.binanceusdm:
    ex = ccxt.binanceusdm({
        "apiKey":  os.getenv("BINANCE_API_KEY", ""),
        "secret":  os.getenv("BINANCE_API_SECRET", ""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
        "rateLimit": 100,       # ms between requests (conservative)
    })
    if os.getenv("BINANCE_DEMO", "false").lower() == "true":
        test_urls = ex.urls.get("test", {})
        api_urls  = ex.urls.setdefault("api", {})
        for group, url in test_urls.items():
            if "fapi" in group or "dapi" in group:
                api_urls[group] = url
        _orig = ex.fetch
        async def _no_sapi(url, method="GET", headers=None, body=None):
            if "/sapi/" in str(url): return []
            return await _orig(url, method=method, headers=headers, body=body)
        ex.fetch = _no_sapi
    return ex


async def scan(min_apy: float = 0.0, top_n: int = 50) -> List[FundingOpp]:
    """
    Returns opportunities sorted by |APY| descending.
    Uses two bulk API calls total (vs 640+ before).
    """
    ex = _make_ex()
    try:
        await ex.load_markets()

        # ── 1. Bulk funding rates (single request) ────────────────────────────
        # /fapi/v1/premiumIndex returns all symbols at once
        raw_rates = await _bulk_funding_rates(ex)

        # ── 2. Bulk 24h volumes (single request) ──────────────────────────────
        raw_vols  = await _bulk_volumes(ex)

        # ── 3. Build opportunities ─────────────────────────────────────────────
        results: List[FundingOpp] = []
        for entry in raw_rates:
            sym_raw  = entry.get("symbol", "")          # e.g. "BTCUSDT"
            rate     = float(entry.get("lastFundingRate") or 0)
            next_ts  = float(entry.get("nextFundingTime") or 0) / 1000

            if abs(rate) > MAX_RATE_8H:
                continue

            # Map to ccxt symbol (BTCUSDT → BTC/USDT:USDT)
            ccxt_sym = sym_raw.replace("USDT", "/USDT:USDT") if sym_raw.endswith("USDT") else None
            if not ccxt_sym or ccxt_sym not in ex.markets:
                continue
            mkt = ex.markets[ccxt_sym]
            if not (mkt.get("linear") and mkt.get("swap")):
                continue

            base   = mkt.get("base", sym_raw.replace("USDT", ""))
            vol    = raw_vols.get(sym_raw, 0.0)
            if vol < MIN_VOLUME_USDT:
                continue

            apy = rate * 3 * 365 * 100
            if abs(apy) < min_apy:
                continue

            results.append(FundingOpp(
                symbol          = base,
                ccxt_symbol     = ccxt_sym,
                rate_8h         = rate,
                apy             = apy,
                direction       = "short_perp" if rate >= 0 else "long_perp",
                volume_24h_usdt = vol,
                next_funding_ts = next_ts or None,
            ))

        results.sort(key=lambda x: abs(x.apy), reverse=True)
        return results[:top_n]
    finally:
        await ex.close()


async def _bulk_funding_rates(ex: ccxt.binanceusdm) -> list:
    """
    Fetch all funding rates in a single API call.
    Returns raw list of {symbol, lastFundingRate, nextFundingTime, ...}.
    """
    try:
        # ccxt method for /fapi/v1/premiumIndex (no symbol = all symbols)
        data = await ex.fapiPublicGetPremiumIndex({})
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        # Fallback: fetch_funding_rates if available
        try:
            rates = await ex.fetch_funding_rates()
            return [
                {"symbol": k.replace("/USDT:USDT", "USDT"),
                 "lastFundingRate": v.get("fundingRate", 0),
                 "nextFundingTime": (v.get("fundingDatetime") or 0)}
                for k, v in rates.items()
            ]
        except Exception:
            return []


async def _bulk_volumes(ex: ccxt.binanceusdm) -> dict:
    """
    Returns {rawSymbol: quoteVolume} for all USDT-M perps.
    Single call to /fapi/v1/ticker/24hr.
    """
    try:
        tickers = await ex.fetch_tickers()
        result = {}
        for sym, t in tickers.items():
            raw = sym.replace("/USDT:USDT", "USDT")
            vol = float(t.get("quoteVolume") or t.get("baseVolume") or 0)
            result[raw] = vol
        return result
    except Exception:
        return {}


def print_table(opps: List[FundingOpp]) -> None:
    next_label = ""
    print(f"\n{'Token':<8} {'Rate/8h':>9} {'APY':>10}  {'Strategy':<35} {'Vol $24h'}")
    print("─" * 85)
    for o in opps:
        strat = "BUY spot + SHORT perp" if o.direction == "short_perp" else "LONG perp + SHORT spot"
        earn  = "longs pay you" if o.direction == "short_perp" else "shorts pay you"
        sign  = "+" if o.rate_8h > 0 else ""
        vol_m = o.volume_24h_usdt / 1e6
        next_str = ""
        if o.next_funding_ts:
            mins = (o.next_funding_ts - time.time()) / 60
            if 0 < mins < 480:
                next_str = f"  next={mins:.0f}min"
        print(f"{o.symbol:<8} {sign}{o.rate_8h*100:>8.4f}%  {o.apy:>8.1f}%  {strat:<35} ← {earn}  ${vol_m:.0f}M{next_str}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--min",  type=float, default=10.0)
    p.add_argument("--top",  type=int,   default=20)
    args = p.parse_args()
    opps = asyncio.run(scan(min_apy=args.min, top_n=args.top))
    print_table(opps)
    print(f"\n{len(opps)} liquid opportunities above {args.min}% APY")
