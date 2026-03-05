#!/usr/bin/env python3
"""
publisher.py — Dashboard data publisher (persistent daemon).

Runs as a long-lived daemon via launchd (KeepAlive). Loops every INTERVAL
seconds, fetching live exchange data and pushing a dashboard JSON blob to
Upstash Redis or the local fallback file.

Stability design:
  - Each data section (Binance rates, HL rates, Binance account, HL account)
    is fetched independently. A failure in one section does NOT kill the publish.
    Last-known-good values are used as fallback instead.
  - File writes are atomic (temp file + os.replace) — no partial writes.
  - SIGTERM/SIGINT caught cleanly for graceful shutdown.
  - All exceptions in the main loop are caught and logged; the daemon never exits
    due to a runtime error.
  - On startup, last-known-good state is seeded from the fallback file so the
    first publish is always valid even before any API calls succeed.
  - Python stdout/stderr are unbuffered (-u flag in plist).

Architecture:
    Mac mini (this) → Upstash Redis  → Vercel API routes → Browser
                    → /tmp fallback  → local dev server  → Browser

Env vars (add to .env):
    UPSTASH_REDIS_REST_URL    e.g. https://xxx.upstash.io
    UPSTASH_REDIS_REST_TOKEN  REST token from Upstash console
    BINANCE_API_KEY
    BINANCE_PRIVATE_KEY_PATH
    HL_PUBLIC_ADDRESS

State files:
    /tmp/cross-arb-state.json      — bot positions
    /tmp/cross-arb-history.jsonl   — open/close event log
    /tmp/cross-arb-dashboard.json  — last published payload (fallback + dev)
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# ── Config ────────────────────────────────────────────────────────────────────

STATE_FILE    = Path("/tmp/cross-arb-state.json")
HISTORY_FILE  = Path("/tmp/cross-arb-history.jsonl")
TIMESERIES_FILE = Path("/tmp/cross-arb-timeseries.jsonl")
FALLBACK_FILE = Path("/tmp/cross-arb-dashboard.json")
ENV_FILE      = Path("/Users/nicholas/workspace/funding-farm/.env")
SGT           = timezone(timedelta(hours=8))

EXIT_APY   = float(os.getenv("CARB_EXIT_ARB_APY",        "5.0"))
MAX_POS    = int(os.getenv("CARB_MAX_POSITIONS",         "20"))
REDIS_TTL  = 90    # seconds — frontend treats data older than this as stale
INTERVAL   = 30    # default loop interval

# ── Graceful shutdown ─────────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig: int, _frame: Any) -> None:
    global _shutdown
    print(f"[pub] received signal {sig}, shutting down gracefully", flush=True)
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)

# ── Env loading ───────────────────────────────────────────────────────────────

def load_env() -> None:
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k.strip(), v)

# ── State files ───────────────────────────────────────────────────────────────

def load_positions() -> list[dict]:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return []

def load_history(max_events: int = 100) -> list[dict]:
    """Load open/close events from timeseries file (primary) or history file (legacy)."""
    events: list[dict] = []
    # Primary: read from timeseries JSONL (has position_open/position_close events)
    source = TIMESERIES_FILE if TIMESERIES_FILE.exists() else HISTORY_FILE
    try:
        for line in source.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                etype = ev.get("type", "")
                # Normalize timeseries events to history format
                if etype in ("position_open", "open"):
                    events.append({
                        "type": "open",
                        "ts": ev.get("ts", 0),
                        "symbol": ev.get("symbol", ""),
                        "notional": ev.get("notional", 0),
                        "entry_apy": ev.get("entry_apy", 0),
                        "bin_side": ev.get("bin_side", ""),
                        "hl_side": ev.get("hl_side", ""),
                    })
                elif etype in ("position_close", "close"):
                    events.append({
                        "type": "close",
                        "ts": ev.get("ts", 0),
                        "symbol": ev.get("symbol", ""),
                        "notional": ev.get("notional", 0),
                        "entry_apy": ev.get("entry_apy", 0),
                        "exit_apy": ev.get("exit_apy", 0),
                        "realized_total": ev.get("realized_pnl", ev.get("realized_total", 0)),
                        "age_hours": ev.get("age_hours", 0),
                    })
            except Exception:
                pass
    except Exception:
        pass
    return sorted(events, key=lambda e: e.get("ts", 0), reverse=True)[:max_events]


def load_timeseries(max_events: int = 200) -> list[dict]:
    """Load timeseries events for historical stats."""
    events: list[dict] = []
    try:
        for line in TIMESERIES_FILE.read_text().splitlines():
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    return sorted(events, key=lambda e: e.get("ts", 0), reverse=True)[:max_events]


# ── Last-known-good cache (in-process, keyed by section) ─────────────────────

_lkg: dict[str, Any] = {
    "bin_rates":   {},
    "hl_rates":    {},
    "bin_account": {"wallet_balance": 0, "available_balance": 0,
                    "unrealized_pnl": 0, "initial_margin": 0},
    "hl_account":  {"equity": 0, "withdrawable": 0, "margin_used": 0,
                    "unrealized_pnl": 0, "position_count": 0},
}

def _update_lkg(key: str, value: Any) -> Any:
    """Update last-known-good cache and return value."""
    _lkg[key] = value
    return value

# ── Live rate fetchers (each fails independently) ─────────────────────────────

async def fetch_bin_rates() -> dict[str, float]:
    """Binance perpetual funding rates, annualised. Falls back to LKG on error."""
    try:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                data = await r.json()
                rates = {
                    rec["symbol"].replace("USDT", ""): float(rec["lastFundingRate"]) * 3 * 365 * 100
                    for rec in data
                    if rec.get("symbol", "").endswith("USDT")
                }
                return _update_lkg("bin_rates", rates)
    except Exception as e:
        print(f"[pub] bin_rates fetch failed (using LKG): {e}", flush=True)
        return _lkg["bin_rates"]


async def fetch_hl_rates() -> dict[str, float]:
    """HL perpetual funding rates, annualised. Falls back to LKG on error."""
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch_hl_rates_sync)
    except Exception as e:
        print(f"[pub] hl_rates fetch failed (using LKG): {e}", flush=True)
        return _lkg["hl_rates"]

def _fetch_hl_rates_sync() -> dict[str, float]:
    from hyperliquid.info import Info
    info = Info("https://api.hyperliquid.xyz", skip_ws=True)
    meta, ctxs = info.meta_and_asset_ctxs()
    rates: dict[str, float] = {}
    for i, asset in enumerate(meta.get("universe", [])):
        if i < len(ctxs):
            rates[asset["name"]] = float(ctxs[i].get("funding", 0)) * 24 * 365 * 100
    return _update_lkg("hl_rates", rates)


async def fetch_hl_account() -> dict:
    """HL account state. Falls back to LKG on error."""
    public_addr = os.getenv("HL_PUBLIC_ADDRESS", "")
    if not public_addr:
        return _lkg["hl_account"]
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _fetch_hl_account_sync, public_addr)
    except Exception as e:
        print(f"[pub] hl_account fetch failed (using LKG): {e}", flush=True)
        return _lkg["hl_account"]

def _fetch_hl_account_sync(public_addr: str) -> dict:
    from hyperliquid.info import Info
    info = Info("https://api.hyperliquid.xyz", skip_ws=True)
    state = info.user_state(public_addr)
    summary = state.get("crossMarginSummary", {})
    pnl, count = 0.0, 0
    for pos in state.get("assetPositions", []):
        p = pos.get("position", {})
        if abs(float(p.get("szi", "0"))) > 1e-9:
            pnl += float(p.get("unrealizedPnl", "0"))
            count += 1
    result = {
        "equity":        float(summary.get("accountValue",    0)),
        "withdrawable":  float(state.get("withdrawable",      0)),
        "margin_used":   float(summary.get("totalMarginUsed", 0)),
        "unrealized_pnl": pnl,
        "position_count": count,
    }
    return _update_lkg("hl_account", result)


async def fetch_bin_account() -> dict:
    """Binance futures account. Falls back to LKG on error."""
    try:
        import ccxt.async_support as ccxt
        key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
        api_key  = os.getenv("BINANCE_API_KEY", "")
        if not key_path or not api_key:
            return _lkg["bin_account"]
        secret = open(key_path).read()
        ex = ccxt.binanceusdm({
            "apiKey": api_key, "secret": secret,
            "options": {"defaultType": "future"}, "enableRateLimit": True,
        })
        try:
            bal  = await ex.fetch_balance({"type": "future"})
            info = bal.get("info", {})
            result = {
                "wallet_balance":    float(info.get("totalWalletBalance",    0)),
                "available_balance": float(info.get("availableBalance",      0)),
                "unrealized_pnl":    float(info.get("totalUnrealizedProfit", 0)),
                "initial_margin":    float(info.get("totalInitialMargin",    0)),
            }
            return _update_lkg("bin_account", result)
        finally:
            await ex.close()
    except Exception as e:
        print(f"[pub] bin_account fetch failed (using LKG): {e}", flush=True)
        return _lkg["bin_account"]

# ── Enrichment ────────────────────────────────────────────────────────────────

def compute_live_apy(pos: dict, bin_rates: dict[str, float], hl_rates: dict[str, float]) -> float:
    sym      = pos["symbol"]
    bin_sign = 1 if pos["bin_side"] == "buy" else -1
    hl_sign  = 1 if pos["hl_side"]  == "buy" else -1
    b = bin_rates.get(sym, pos.get("last_bin_apy", 0.0))
    h = hl_rates.get(sym,  pos.get("last_hl_apy",  0.0))
    return (b * bin_sign * -1) + (h * hl_sign * -1)


def compute_accruing(pos: dict, live_bin_apy: float, live_hl_apy: float) -> float:
    now   = datetime.now(timezone.utc)
    utc_m, utc_s = now.minute, now.second

    bin_sign = 1 if pos["bin_side"] == "buy" else -1
    hl_sign  = 1 if pos["hl_side"]  == "buy" else -1
    notional = pos["notional_usdt"]

    hl_fraction   = (utc_m * 60 + utc_s) / 3600
    hl_hourly_rate = live_hl_apy / (24 * 365 * 100)
    hl_accruing    = notional * hl_hourly_rate * hl_sign * -1 * hl_fraction

    hours_into_period = (now.hour % 8) + utc_m / 60 + utc_s / 3600
    bin_8h_rate  = live_bin_apy / (3 * 365 * 100)
    bin_accruing = notional * bin_8h_rate * bin_sign * -1 * (hours_into_period / 8)

    return hl_accruing + bin_accruing


def position_status(live_apy: float, total_pnl: float, age_hours: float,
                    needs_close: bool) -> str:
    if needs_close:          return "closing"
    if live_apy < 0:         return "inverted"
    if live_apy < EXIT_APY:  return "near_exit"
    if total_pnl < -0.05 and age_hours > 4 and live_apy < 20:
        return "bleeding"
    return "live"


def enrich_positions(positions: list[dict], bin_rates: dict[str, float],
                     hl_rates: dict[str, float]) -> tuple[list[dict], list[dict]]:
    enriched, alerts = [], []
    now_ts = time.time()

    for pos in positions:
        try:
            sym      = pos["symbol"]
            notional = pos["notional_usdt"]
            age_h    = (now_ts - pos["entry_ts"]) / 3600
            live_bin = bin_rates.get(sym, pos.get("last_bin_apy", 0.0))
            live_hl  = hl_rates.get(sym,  pos.get("last_hl_apy",  0.0))
            live_apy = compute_live_apy(pos, bin_rates, hl_rates)
            drift    = live_apy - pos.get("entry_net_apy", 0.0)
            daily    = notional * live_apy / 100 / 365
            realized = (pos.get("funding_realized_bin", 0.0) +
                        pos.get("funding_realized_hl",  0.0))
            accruing = compute_accruing(pos, live_bin, live_hl)
            total_pnl = realized + accruing
            status   = position_status(live_apy, total_pnl, age_h,
                                       pos.get("needs_close", False))

            enriched.append({
                **pos,
                "live_net_apy":   round(live_apy, 2),
                "drift_pp":       round(drift, 2),
                "daily_usd":      round(daily, 4),
                "realized_total": round(realized, 4),
                "accruing_usd":   round(accruing, 4),
                "total_pnl":      round(total_pnl, 4),
                "age_hours":      round(age_h, 2),
                "status":         status,
            })

            if status in ("inverted", "near_exit", "bleeding"):
                daily_loss = notional * abs(live_apy) / 100 / 365
                alerts.append({
                    "symbol":    sym,
                    "type":      status,
                    "live_apy":  round(live_apy, 1),
                    "total_pnl": round(total_pnl, 4),
                    "message":   (
                        f"{sym} inverted at {live_apy:.1f}% APY — losing ${daily_loss:.3f}/day"
                        if status == "inverted" else
                        f"{sym} near exit floor at {live_apy:.1f}% APY"
                        if status == "near_exit" else
                        f"{sym} bleeding — P&L ${total_pnl:.3f}, APY {live_apy:.1f}%"
                    ),
                })
        except Exception as e:
            print(f"[pub] enrich error for {pos.get('symbol','?')}: {e}", flush=True)

    # Sort: inverted/bleeding first, then by daily desc
    order = {"inverted": 0, "bleeding": 1, "near_exit": 2, "closing": 3, "live": 4}
    enriched.sort(key=lambda p: (order.get(p["status"], 9), -p["daily_usd"]))
    return enriched, alerts


def build_summary(positions: list[dict]) -> dict:
    if not positions:
        return {"position_count": 0, "max_positions": MAX_POS,
                "total_notional": 0.0, "daily_usd_total": 0.0,
                "realized_total": 0.0, "accruing_total": 0.0,
                "total_pnl": 0.0, "weighted_apy": 0.0}
    total_notional = sum(p["notional_usdt"]  for p in positions)
    return {
        "position_count":  len(positions),
        "max_positions":   MAX_POS,
        "total_notional":  round(total_notional, 2),
        "daily_usd_total": round(sum(p["daily_usd"]      for p in positions), 4),
        "realized_total":  round(sum(p["realized_total"] for p in positions), 4),
        "accruing_total":  round(sum(p["accruing_usd"]   for p in positions), 4),
        "total_pnl":       round(sum(p["total_pnl"]      for p in positions), 4),
        "weighted_apy":    round(
            sum(p["live_net_apy"] * p["notional_usdt"] for p in positions) / total_notional, 2
        ) if total_notional > 0 else 0.0,
    }


def build_cumulative_pnl(events: list[dict]) -> list[dict]:
    closes   = sorted([e for e in events if e.get("type") == "close"],
                      key=lambda e: e.get("ts", 0))
    running, result = 0.0, []
    for e in closes:
        running += e.get("realized_total", 0.0)
        result.append({
            "ts":    e["ts"],
            "value": round(running, 4),
            "label": datetime.fromtimestamp(e["ts"], tz=SGT).strftime("%b %d %H:%M"),
        })
    return result


def is_bot_alive() -> bool:
    import subprocess
    return subprocess.run(["pgrep", "-f", "cross_arb.py --run"],
                          capture_output=True).returncode == 0

# ── Upstash publish ───────────────────────────────────────────────────────────

def publish_to_redis(payload: dict) -> bool:
    url   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
    token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
    if not url or not token:
        return False
    value = json.dumps(payload, separators=(",", ":"))
    req   = urllib.request.Request(
        f"{url}/set/cross-arb:dashboard?ex={REDIS_TTL}",
        data=value.encode(),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()).get("result") == "OK"
    except Exception as e:
        print(f"[pub] Redis publish failed: {e}", flush=True)
        return False


def publish_to_timescale(history: list[dict], summary: dict, inventory: dict) -> bool:
    """Sync position events and portfolio snapshot to TimescaleDB."""
    db_url = os.getenv("TIMESCALE_DB", "")
    if not db_url:
        return False
    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Upsert position events (open + close)
        for ev in history:
            if ev.get("type") == "open":
                cur.execute("""
                    INSERT INTO arb_positions (symbol, opened_at, bin_side, hl_side, notional, entry_apy)
                    VALUES (%s, to_timestamp(%s), %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    ev["symbol"],
                    ev["ts"],
                    ev.get("bin_side", ""),
                    ev.get("hl_side", ""),
                    ev.get("notional", 0),
                    ev.get("entry_apy", 0),
                ))
            elif ev.get("type") == "close":
                # Update existing open row with close data
                cur.execute("""
                    UPDATE arb_positions
                    SET closed_at = to_timestamp(%s),
                        exit_apy = %s,
                        realized_total = %s,
                        age_hours = %s
                    WHERE symbol = %s
                      AND closed_at IS NULL
                      AND opened_at = (
                          SELECT MAX(opened_at) FROM arb_positions
                          WHERE symbol = %s AND closed_at IS NULL
                      )
                """, (
                    ev["ts"],
                    ev.get("exit_apy", 0),
                    ev.get("realized_total", 0),
                    ev.get("age_hours", 0),
                    ev["symbol"],
                    ev["symbol"],
                ))

        # Portfolio snapshot
        inv = inventory or {}
        bin_bal = inv.get("binance", {}).get("wallet_balance", 0)
        hl_eq = inv.get("hl", {}).get("equity", 0)
        cur.execute("""
            INSERT INTO arb_portfolio_snapshots
                (ts, position_count, total_notional, weighted_apy, daily_usd, total_realized, hl_equity, bin_balance, leverage)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            summary.get("position_count", 0),
            summary.get("total_notional", 0),
            summary.get("weighted_apy", 0),
            summary.get("daily_usd_total", 0),
            summary.get("realized_total", 0),
            hl_eq,
            bin_bal,
            summary.get("total_notional", 0) / (bin_bal + hl_eq) if (bin_bal + hl_eq) > 0 else 0,
        ))

        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"[pub] Timescale sync failed: {e}", flush=True)
        return False


def publish_to_file(payload: dict) -> None:
    """Atomic write to fallback file (temp + os.replace)."""
    import tempfile
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=FALLBACK_FILE.parent,
            prefix=".pub-", suffix=".json", delete=False
        ) as tmp:
            json.dump(payload, tmp, indent=2)
            tmp_path = tmp.name
        os.replace(tmp_path, FALLBACK_FILE)
    except Exception as e:
        print(f"[pub] file write failed: {e}", flush=True)

# ── Seed LKG from existing fallback (startup) ─────────────────────────────────

def seed_lkg_from_fallback() -> None:
    """On startup, seed last-known-good values from the existing fallback file."""
    try:
        if not FALLBACK_FILE.exists():
            return
        d = json.loads(FALLBACK_FILE.read_text())
        inv = d.get("inventory", {})
        if inv.get("binance"):
            _lkg["bin_account"] = inv["binance"]
        if inv.get("hl"):
            _lkg["hl_account"]  = inv["hl"]
        # Reconstruct LKG rates from last position data
        if d.get("positions"):
            for p in d["positions"]:
                sym = p.get("symbol", "")
                if sym:
                    if p.get("last_bin_apy"):
                        _lkg["bin_rates"][sym] = p["last_bin_apy"]
                    if p.get("last_hl_apy"):
                        _lkg["hl_rates"][sym]  = p["last_hl_apy"]
        print("[pub] seeded LKG from fallback file", flush=True)
    except Exception as e:
        print(f"[pub] LKG seed failed (non-fatal): {e}", flush=True)

# ── One publish cycle ─────────────────────────────────────────────────────────

async def run_once() -> None:
    # Fetch all sections concurrently; each section handles its own errors
    bin_rates_task   = asyncio.create_task(fetch_bin_rates())
    hl_rates_task    = asyncio.create_task(fetch_hl_rates())
    bin_account_task = asyncio.create_task(fetch_bin_account())
    hl_account_task  = asyncio.create_task(fetch_hl_account())

    bin_rates, hl_rates, bin_account, hl_account = await asyncio.gather(
        bin_rates_task, hl_rates_task, bin_account_task, hl_account_task
    )

    raw_positions       = load_positions()
    history             = load_history(max_events=100)
    timeseries          = load_timeseries(max_events=200)
    positions, alerts   = enrich_positions(raw_positions, bin_rates, hl_rates)
    summary             = build_summary(positions)
    cumulative          = build_cumulative_pnl(history)

    payload = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "generated_ts":   time.time(),
        "bot_alive":      is_bot_alive(),
        "summary":        summary,
        "positions":      positions,
        "alerts":         alerts,
        "inventory": {
            "binance":       bin_account,
            "hl":            hl_account,
            "total_capital": round(
                bin_account.get("wallet_balance", 0) + hl_account.get("equity", 0), 2
            ),
        },
        "history":         history[:50],
        "cumulative_pnl":  cumulative,
        "timeseries":      timeseries,
    }

    redis_ok = publish_to_redis(payload)
    publish_to_file(payload)   # always write fallback (for local dev + redundancy)
    publish_to_timescale(history, summary, payload.get("inventory", {}))

    if redis_ok:
        print(f"[pub] published → Redis + file | {len(positions)} pos | "
              f"${summary['daily_usd_total']:.2f}/day", flush=True)
    else:
        print(f"[pub] published → file only   | {len(positions)} pos | "
              f"${summary['daily_usd_total']:.2f}/day", flush=True)

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--once",     action="store_true", help="Run one cycle and exit")
    ap.add_argument("--interval", type=int, default=INTERVAL, help="Loop interval (default: 30s)")
    args = ap.parse_args()

    load_env()

    if args.once:
        asyncio.run(run_once())
        return

    # Persistent daemon loop
    seed_lkg_from_fallback()
    print(f"[pub] daemon started | interval={args.interval}s", flush=True)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while not _shutdown:
        t0 = time.monotonic()
        try:
            loop.run_until_complete(run_once())
        except Exception as e:
            # Last-resort catch — log and keep going
            print(f"[pub] unexpected error (continuing): {e}", flush=True)

        elapsed = time.monotonic() - t0
        sleep_s = max(0.0, args.interval - elapsed)

        # Sleep in short chunks to honour SIGTERM promptly
        slept = 0.0
        while slept < sleep_s and not _shutdown:
            time.sleep(min(1.0, sleep_s - slept))
            slept += 1.0

    loop.close()
    print("[pub] daemon stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
