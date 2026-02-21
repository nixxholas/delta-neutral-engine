"""
scanner.py — Scan all Binance USDT-M perps for funding rate opportunities.

Filters out illiquid/low-volume tokens to avoid manipulated rates.
Returns ranked list sorted by |APY|, both directions.
"""

from __future__ import annotations

import asyncio
import argparse
from dataclasses import dataclass
from typing import List, Optional

import ccxt.async_support as ccxt


@dataclass
class FundingOpp:
    symbol: str
    ccxt_symbol: str
    rate_8h: float
    apy: float
    direction: str          # "short_perp" (pos funding) | "long_perp" (neg funding)
    volume_24h_usdt: float  # 24h volume for liquidity check
    next_funding_ts: Optional[float] = None


MIN_VOLUME_USDT = 5_000_000   # $5M 24h volume minimum — filters out illiquid tokens
MAX_RATE_8H     = 0.10        # 10% per 8h cap — above this is almost certainly anomalous


async def scan(min_apy: float = 0.0, top_n: int = 30) -> List[FundingOpp]:
    import os; from dotenv import load_dotenv; load_dotenv()

    ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
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

    try:
        await ex.load_markets()
        linear_swaps = [
            m for m in ex.markets.values()
            if m.get("linear") and m.get("swap") and m.get("settle") == "USDT"
        ]

        # Fetch tickers in bulk for volume data
        try:
            tickers = await ex.fetch_tickers()
        except Exception:
            tickers = {}

        results = []
        BATCH = 20
        for i in range(0, len(linear_swaps), BATCH):
            batch = linear_swaps[i:i + BATCH]
            tasks = [_fetch_opp(ex, m, tickers) for m in batch]
            batch_res = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_res:
                if isinstance(r, FundingOpp):
                    results.append(r)

        # Filter: liquidity + sanity cap on rate
        results = [
            r for r in results
            if r.volume_24h_usdt >= MIN_VOLUME_USDT
            and abs(r.rate_8h) <= MAX_RATE_8H
        ]

        results.sort(key=lambda x: abs(x.apy), reverse=True)
        return [r for r in results if abs(r.apy) >= min_apy][:top_n]
    finally:
        await ex.close()


async def _fetch_opp(ex, market: dict, tickers: dict) -> Optional[FundingOpp]:
    try:
        sym = market["symbol"]
        info = await ex.fetch_funding_rate(sym)
        rate = float(info.get("fundingRate") or 0)
        apy  = rate * 3 * 365 * 100

        # Volume from bulk tickers
        ticker = tickers.get(sym, {})
        vol = float(ticker.get("quoteVolume") or ticker.get("baseVolume") or 0)
        # Fallback: fetch individually
        if vol == 0:
            try:
                t = await ex.fetch_ticker(sym)
                vol = float(t.get("quoteVolume") or 0)
            except Exception:
                pass

        return FundingOpp(
            symbol=market.get("base", sym.split("/")[0]),
            ccxt_symbol=sym,
            rate_8h=rate,
            apy=apy,
            direction="short_perp" if rate >= 0 else "long_perp",
            volume_24h_usdt=vol,
        )
    except Exception:
        return None


def print_table(opps: List[FundingOpp]) -> None:
    print(f"\n{'Token':<8} {'Rate/8h':>9} {'APY':>10}  {'Strategy':<35} {'Vol $24h'}")
    print("─" * 85)
    for o in opps:
        strat = "BUY spot + SHORT perp" if o.direction == "short_perp" else "LONG perp + SHORT spot"
        earn  = "longs pay you" if o.direction == "short_perp" else "shorts pay you"
        sign  = "+" if o.rate_8h > 0 else ""
        vol_m = o.volume_24h_usdt / 1e6
        print(f"{o.symbol:<8} {sign}{o.rate_8h*100:>8.4f}%  {o.apy:>8.1f}%  {strat:<35} ← {earn}  ${vol_m:.0f}M")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--min",  type=float, default=10.0)
    p.add_argument("--top",  type=int,   default=20)
    args = p.parse_args()
    opps = asyncio.run(scan(min_apy=args.min, top_n=args.top))
    print_table(opps)
    print(f"\n{len(opps)} liquid opportunities above {args.min}% APY")
