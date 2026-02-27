#!/usr/bin/env python3
"""
reporter.py — Mechanical portfolio status reporter.

Fetches live rates from Binance + HL, reads state file,
computes live APY / drift / realized funding, scans top opps,
and POSTs a structured report directly to Telegram.

No AI in the loop. Pure deterministic code.

Usage:
    python3 reporter.py
    # or via cron / launchd every 30 min
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

ROOT      = Path(__file__).parent
ENV_FILE  = ROOT / ".env"

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = Path("/tmp/cross-arb-state.json")
LOG_FILE   = Path("/tmp/cross-arb.log")

EXIT_APY   = 5.0   # must match CARB_EXIT_ARB_APY
SCAN_OPPS  = 5     # top N opportunities to show in report

# ── Env loader ────────────────────────────────────────────────────────────────

def load_env() -> None:
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

# ── Process check ─────────────────────────────────────────────────────────────

def find_pid(pattern: str) -> int | None:
    try:
        out = subprocess.check_output(["pgrep", "-f", pattern], text=True).strip()
        pids = [int(x) for x in out.splitlines() if x.strip().isdigit()]
        return pids[0] if pids else None
    except subprocess.CalledProcessError:
        return None

# ── State ─────────────────────────────────────────────────────────────────────

def load_state() -> list[dict]:
    try:
        data = json.loads(STATE_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []

# ── Binance rates ─────────────────────────────────────────────────────────────

async def fetch_binance_rates() -> dict[str, float]:
    """Returns {symbol: annualised_apy_pct}. Binance rate × 3 × 365."""
    import aiohttp
    rates: dict[str, float] = {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                for r in await resp.json():
                    sym = r.get("symbol", "").replace("USDT", "")
                    if sym:
                        try:
                            rates[sym] = float(r["lastFundingRate"]) * 3 * 365 * 100
                        except Exception:
                            pass
    except Exception:
        pass
    return rates


# ── Binance account snapshot ──────────────────────────────────────────────────

async def fetch_binance_account() -> dict:
    """
    Returns Binance Futures account snapshot:
      wallet_balance, available_balance, unrealized_pnl,
      initial_margin (in use), positions_count
    """
    result = {
        "wallet_balance":   0.0,
        "available_balance": 0.0,
        "unrealized_pnl":   0.0,
        "initial_margin":   0.0,
        "positions_count":  0,
        "error":            None,
    }
    try:
        import ccxt.async_support as ccxt
        key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
        api_key  = os.getenv("BINANCE_API_KEY", "")
        if not key_path or not api_key:
            result["error"] = "missing credentials"
            return result

        # ccxt handles Ed25519 PEM natively — pass raw PEM as secret
        secret = open(key_path).read() if os.path.exists(key_path) else ""
        ex = ccxt.binanceusdm({
            "apiKey":          api_key,
            "secret":          secret,
            "options":         {"defaultType": "future"},
            "enableRateLimit": True,
        })
        try:
            bal = await ex.fetch_balance({"type": "future"})
            info = bal.get("info", {})
            result["wallet_balance"]    = float(info.get("totalWalletBalance",    0))
            result["available_balance"] = float(info.get("availableBalance",      0))
            result["unrealized_pnl"]    = float(info.get("totalUnrealizedProfit", 0))
            result["initial_margin"]    = float(info.get("totalInitialMargin",    0))
            # Count active positions from the positions list
            positions = info.get("positions", [])
            result["positions_count"] = sum(
                1 for p in positions if abs(float(p.get("positionAmt", 0))) > 1e-9
            )
        finally:
            await ex.close()
    except Exception as e:
        result["error"] = str(e)[:120]
    return result

# ── HL rates + realized ───────────────────────────────────────────────────────

def fetch_hl_data(public_addr: str) -> tuple[
    dict[str, tuple[float, float]],
    dict[str, float],
    dict,
]:
    """
    Returns:
      hl_rates:    {symbol: (hourly_rate, apy_pct)}   HL rate × 24 × 365
      hl_realized: {symbol: cum_funding_since_open}
      hl_account:  {equity, withdrawable, margin_used, unrealized_pnl, positions_count}
    """
    hl_account = {
        "equity":           0.0,
        "withdrawable":     0.0,
        "margin_used":      0.0,
        "unrealized_pnl":   0.0,
        "positions_count":  0,
    }
    try:
        from hyperliquid.info import Info
        info = Info("https://api.hyperliquid.xyz", skip_ws=True)

        # Rates
        meta, ctxs = info.meta_and_asset_ctxs()
        hl_rates: dict[str, tuple[float, float]] = {}
        for i, asset in enumerate(meta.get("universe", [])):
            name = asset.get("name", "")
            if i < len(ctxs):
                rate = float(ctxs[i].get("funding", 0))
                hl_rates[name] = (rate, rate * 24 * 365 * 100)

        # Account + realized
        hl_realized: dict[str, float] = {}
        if public_addr:
            state = info.user_state(public_addr)
            summary = state.get("crossMarginSummary", {})
            hl_account["equity"]        = float(summary.get("accountValue", "0"))
            hl_account["margin_used"]   = float(summary.get("totalMarginUsed", "0"))
            hl_account["withdrawable"]  = float(state.get("withdrawable", "0"))

            unrealized = 0.0
            active = 0
            for pos in state.get("assetPositions", []):
                p = pos.get("position", {})
                coin = p.get("coin", "")
                cf = p.get("cumFunding", {})
                hl_realized[coin] = float(cf.get("sinceOpen", "0"))
                upnl = float(p.get("unrealizedPnl", "0"))
                sz   = float(p.get("szi", "0"))
                unrealized += upnl
                if abs(sz) > 1e-9:
                    active += 1

            hl_account["unrealized_pnl"]  = unrealized
            hl_account["positions_count"] = active

        return hl_rates, hl_realized, hl_account
    except Exception as e:
        print(f"[reporter] HL fetch error: {e}", file=sys.stderr)
        return {}, {}, hl_account

# ── Opportunity scanner ───────────────────────────────────────────────────────

def scan_opportunities(
    bin_rates: dict[str, float],
    hl_rates: dict[str, tuple[float, float]],
    held_symbols: set[str],
    blacklist: set[str],
    min_apy: float = 15.0,
    min_vol: float = 1e8,
) -> list[dict]:
    opps = []
    for sym in set(bin_rates) & set(hl_rates):
        if sym in held_symbols or sym in blacklist:
            continue
        b = bin_rates[sym]
        h = hl_rates[sym][1]
        if b < 0 and h < 0:
            if abs(b) >= abs(h):
                net, bside, hside = abs(b) - abs(h), "buy", "sell"
            else:
                net, bside, hside = abs(h) - abs(b), "sell", "buy"
        elif b > 0 and h > 0:
            if b >= h:
                net, bside, hside = b - h, "sell", "buy"
            else:
                net, bside, hside = h - b, "buy", "sell"
        else:
            net = abs(b) + abs(h)
            bside = "buy" if b < 0 else "sell"
            hside = "sell" if b < 0 else "buy"
        if net >= min_apy:
            opps.append({"symbol": sym, "net_apy": round(net, 1),
                         "bin_side": bside, "hl_side": hside,
                         "bin_apy": round(b, 1), "hl_apy": round(h, 1)})
    opps.sort(key=lambda x: x["net_apy"], reverse=True)
    return opps[:SCAN_OPPS]

# ── Formatting ────────────────────────────────────────────────────────────────

def fmt_sign(val: float) -> str:
    return f"+{val:.1f}" if val >= 0 else f"{val:.1f}"

def fmt_flag(live: float, entry: float) -> str:
    if live < 0:           return "🔴"
    if live < EXIT_APY:    return "🚨"
    drift_pct = (live - entry) / abs(entry) * 100 if entry else 0
    if drift_pct < -50:    return "⚠️"
    if live > entry:       return "✅"
    return "  "

def recent_errors(log: Path, n: int = 3) -> list[str]:
    if not log.exists():
        return []
    try:
        lines = subprocess.check_output(
            ["tail", "-n", "60", str(log)], text=True
        ).splitlines()
        flagged = [
            l.strip() for l in lines
            if any(k in l.lower() for k in ("error", "exception", "traceback", "failed", "critical"))
        ]
        return flagged[-n:]
    except Exception:
        return []

# ── Build message ─────────────────────────────────────────────────────────────

def build_report(
    pid: int | None,
    positions: list[dict],
    bin_rates: dict[str, float],
    hl_rates: dict[str, tuple[float, float]],
    hl_realized: dict[str, float],
    hl_account: dict,
    bin_account: dict,
    opps: list[dict],
    errors: list[str],
) -> str:
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines: list[str] = []

    # Header
    status = f"✅ PID {pid}" if pid else "🔴 DEAD"
    lines.append(f"⚙️ <b>Cross-Arb Report</b> — {now}  |  Bot: {status}")

    # ── Exchange Inventory ────────────────────────────────────────────────────
    lines.append("\n<b>📊 Exchange Inventory</b>")

    # Binance Futures
    if bin_account.get("error"):
        lines.append(f"  Binance Futures: ⚠️ {bin_account['error']}")
    else:
        bin_total    = bin_account["wallet_balance"]
        bin_free     = bin_account["available_balance"]
        bin_used     = bin_account["initial_margin"]
        bin_upnl     = bin_account["unrealized_pnl"]
        bin_n        = bin_account["positions_count"]
        upnl_str     = f"{fmt_sign(bin_upnl)}" if bin_upnl else "—"
        lines.append(f"  <b>Binance Futures</b>")
        lines.append(f"    Balance: ${bin_total:.2f}   Free: ${bin_free:.2f}   Margin: ${bin_used:.2f}")
        lines.append(f"    Unrealized PnL: ${upnl_str}   Positions: {bin_n}")

    # Hyperliquid
    hl_eq    = hl_account["equity"]
    hl_free  = hl_account["withdrawable"]
        
    hl_used  = hl_account["margin_used"]
    hl_upnl  = hl_account["unrealized_pnl"]
    hl_n     = hl_account["positions_count"]
    hl_upnl_str = f"{fmt_sign(hl_upnl)}" if hl_upnl else "—"
    lines.append(f"  <b>Hyperliquid</b>")
    lines.append(f"    Equity: ${hl_eq:.2f}   Free: ${hl_free:.2f}   Margin: ${hl_used:.2f}")
    lines.append(f"    Unrealized PnL: ${hl_upnl_str}   Positions: {hl_n}")

    # Combined capital
    total_capital = bin_total + hl_eq if not bin_account.get("error") else hl_eq
    total_upnl    = bin_upnl + hl_upnl if not bin_account.get("error") else hl_upnl
    lines.append(f"\n  <b>Total Capital: ${total_capital:.2f}</b>  |  Combined uPnL: ${fmt_sign(total_upnl)}")

    # ── Positions ─────────────────────────────────────────────────────────────
    if not positions:
        lines.append("\nNo open positions.")
    else:
        total_daily = 0.0
        total_realized = 0.0
        lines.append("\n<b>Positions:</b>")
        alerts: list[str] = []

        for p in positions:
            sym        = p["symbol"]
            entry_apy  = p.get("entry_net_apy", 0.0)
            notl       = p.get("notional_usdt", 100.0)
            b_sign     = 1 if p.get("bin_side") == "buy" else -1
            h_sign     = 1 if p.get("hl_side")  == "buy" else -1
            b_live     = bin_rates.get(sym, 0.0)
            h_live     = hl_rates.get(sym, (0.0, 0.0))[1]
            live_net   = b_live * b_sign * (-1) + h_live * h_sign * (-1)
            daily      = notl * live_net / 100 / 365
            realized   = hl_realized.get(sym, 0.0)
            drift      = live_net - entry_apy
            age_h      = (time.time() - p.get("entry_ts", time.time())) / 3600
            flag       = fmt_flag(live_net, entry_apy)
            total_daily    += daily
            total_realized += realized

            lines.append(
                f"  {flag} <b>{sym}</b>  {live_net:.1f}% live "
                f"({fmt_sign(drift)}pp)  ${daily:.3f}/day  "
                f"realized ${realized:.3f}  {age_h:.1f}h"
            )

            if live_net < 0:
                alerts.append(f"🔴 {sym} NEGATIVE ({live_net:.1f}%) — close now")
            elif live_net < EXIT_APY:
                alerts.append(f"🚨 {sym} near exit ({live_net:.1f}%)")

        lines.append(f"\n  <b>Total: ${total_daily:.3f}/day</b>  |  HL realized: ${total_realized:.3f}")

        if alerts:
            lines.append("\n<b>⚠️ Alerts:</b>")
            for a in alerts:
                lines.append(f"  {a}")

    # Opportunities
    if opps:
        lines.append("\n<b>Top unused opps:</b>")
        for o in opps:
            lines.append(
                f"  {o['symbol']}  {o['net_apy']:.1f}%  "
                f"Bin={o['bin_side']} HL={o['hl_side']}"
            )

    # Errors
    if errors:
        lines.append("\n<b>Recent errors:</b>")
        for e in errors:
            lines.append(f"  • {e[:100]}")

    return "\n".join(lines)

# ── Telegram send ─────────────────────────────────────────────────────────────

def send_telegram(text: str) -> bool:
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[reporter] Telegram failed: {e}", file=sys.stderr)
        return False

# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    load_env()

    public_addr = os.getenv("HL_PUBLIC_ADDRESS", "")
    blacklist   = set(os.getenv("CARB_BLACKLIST", "").split(",")) - {""}

    pid       = find_pid("cross_arb.py --run")
    positions = load_state()
    errors    = recent_errors(LOG_FILE)

    held_syms = {p["symbol"] for p in positions}

    # Fetch live data concurrently
    bin_rates_task   = asyncio.create_task(fetch_binance_rates())
    bin_account_task = asyncio.create_task(fetch_binance_account())

    loop = asyncio.get_event_loop()
    hl_rates, hl_realized, hl_account = await loop.run_in_executor(
        None, fetch_hl_data, public_addr
    )
    bin_rates   = await bin_rates_task
    bin_account = await bin_account_task

    opps = scan_opportunities(bin_rates, hl_rates, held_syms, blacklist)

    report = build_report(
        pid, positions, bin_rates, hl_rates,
        hl_realized, hl_account, bin_account, opps, errors,
    )

    ok = send_telegram(report)
    if not ok:
        print(report)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
