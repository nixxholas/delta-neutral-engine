"""
scanner.py — Scan all Binance USDT-M perps for funding rates.

Returns ranked list of opportunities sorted by |APY|.
Usage:
    python scanner.py              # print top 20
    python scanner.py --min 20     # only APY > 20%
"""

from __future__ import annotations

import asyncio
import argparse
from dataclasses import dataclass
from typing import List, Optional

import ccxt.async_support as ccxt


@dataclass
class FundingOpp:
    symbol: str          # e.g. BTC
    ccxt_symbol: str     # e.g. BTC/USDT:USDT
    rate_8h: float       # funding rate per 8h
    apy: float           # annualized %
    direction: str       # "long_perp" (shorts pay longs) or "short_perp" (longs pay shorts)
    next_funding_ts: Optional[float] = None


async def scan(min_apy: float = 0.0, top_n: int = 30) -> List[FundingOpp]:
    """Scan all USDT-M perps and return opportunities sorted by |APY|."""
    import os; from dotenv import load_dotenv; load_dotenv()

    ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })

    # Route to testnet if demo
    if os.getenv("BINANCE_DEMO", "false").lower() == "true":
        _TESTNET = "https://testnet.binancefuture.com"
        test_urls = ex.urls.get("test", {})
        api_urls = ex.urls.setdefault("api", {})
        for group, url in test_urls.items():
            if "fapi" in group or "dapi" in group:
                api_urls[group] = url
        _orig_fetch = ex.fetch
        async def _fetch_no_sapi(url, method="GET", headers=None, body=None):
            if "/sapi/" in str(url):
                return []
            return await _orig_fetch(url, method=method, headers=headers, body=body)
        ex.fetch = _fetch_no_sapi

    try:
        await ex.load_markets()
        linear_swaps = [
            m for m in ex.markets.values()
            if m.get("linear") and m.get("swap") and m.get("settle") == "USDT"
        ]

        results = []
        # Fetch in parallel batches of 20
        BATCH = 20
        for i in range(0, len(linear_swaps), BATCH):
            batch = linear_swaps[i:i+BATCH]
            coros = [_fetch_rate(ex, m) for m in batch]
            batch_results = await asyncio.gather(*coros, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, FundingOpp):
                    results.append(r)

        results.sort(key=lambda x: abs(x.apy), reverse=True)
        filtered = [r for r in results if abs(r.apy) >= min_apy]
        return filtered[:top_n]
    finally:
        await ex.close()


async def _fetch_rate(ex, market: dict) -> Optional[FundingOpp]:
    try:
        info = await ex.fetch_funding_rate(market["symbol"])
        rate = float(info.get("fundingRate") or 0)
        apy = rate * 3 * 365 * 100
        direction = "short_perp" if rate > 0 else "long_perp"
        base = market.get("base", market["symbol"].split("/")[0])
        return FundingOpp(
            symbol=base,
            ccxt_symbol=market["symbol"],
            rate_8h=rate,
            apy=apy,
            direction=direction,
            next_funding_ts=info.get("nextFundingDatetime"),
        )
    except Exception:
        return None


def print_table(opps: List[FundingOpp]) -> None:
    print(f"\n{'Token':<8} {'Rate/8h':>9} {'APY':>10}  {'Strategy':<35} {'Earn via'}")
    print("─" * 80)
    for o in opps:
        if o.direction == "short_perp":
            strat = "BUY spot + SHORT perp"
            earn = "longs pay you"
        else:
            strat = "SHORT spot + LONG perp"
            earn = "shorts pay you"
        sign = "+" if o.rate_8h > 0 else ""
        print(f"{o.symbol:<8} {sign}{o.rate_8h*100:>8.4f}%  {o.apy:>8.1f}%  {strat:<35} ← {earn}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--min", type=float, default=10.0, help="Minimum |APY|% to show")
    p.add_argument("--top", type=int, default=20, help="Max results")
    args = p.parse_args()

    opps = asyncio.run(scan(min_apy=args.min, top_n=args.top))
    print_table(opps)
    print(f"\n{len(opps)} opportunities above {args.min}% APY")
