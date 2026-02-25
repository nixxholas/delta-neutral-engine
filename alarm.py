#!/usr/bin/env python3
"""
alarm.py — Real-time rate alarm system for cross-arb positions.

Checks every 5 minutes (run via cron). Fires Telegram alerts only when
something needs attention. Checks both Binance AND HL rate histories.

Alarms fired:
  🔴 CRITICAL  — live net APY gone negative (losing money now)
  🚨 WARNING   — live net APY below exit floor (5%)
  ⚠️  UNSTABLE  — rate has flipped direction ≥2 times in last 4h on either exchange
                  (PROMPT-style whipsaw trap)
  📉 DECAY     — live net APY has collapsed >60% from entry in under 2h

Silences repeat alerts for the same symbol within RESOUND_MINUTES to avoid spam.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

ROOT       = Path(__file__).parent
ENV_FILE   = ROOT / ".env"
STATE_FILE = Path("/tmp/cross-arb-state.json")
ALERT_FILE = Path("/tmp/cross-arb-alerts.json")  # tracks last alert per symbol

BOT_TOKEN = "8236078404:AAGyY0RwES8fWJ1w2pF5Z3Bh7PV5P3HkBH8"
CHAT_ID   = "25836422"

EXIT_APY         = 5.0    # below this → WARNING
DECAY_THRESHOLD  = 0.60   # >60% of entry APY lost → DECAY alarm
RESOUND_MINUTES  = 20     # don't repeat same alarm for same symbol within N min
HL_HISTORY_HOURS  = 2     # hours of HL rate history (last 2 hourly settlements)
BIN_HISTORY_LIMIT = 3     # last 3 Binance settlements (~24h). Recent flip = last 2 opposite.

# Engineering trigger — run the engineer cron on demand when patterns indicate
# a code-level fix is needed (not just a one-off rate move)
ENGINEER_CRON_ID = "148babfd-c8ae-4f78-be87-72f48019c01d"
ENGINEER_ALARM_THRESHOLD = 3   # trigger engineer after N alarms in one run
ENGINEER_COOLDOWN_HOURS  = 2   # don't trigger more than once per N hours

# ── Env ───────────────────────────────────────────────────────────────────────

def load_env() -> None:
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

# ── Alert dedup ───────────────────────────────────────────────────────────────

def load_alert_state() -> dict[str, float]:
    """Returns {symbol+kind: last_fired_ts}."""
    try:
        return json.loads(ALERT_FILE.read_text())
    except Exception:
        return {}

def save_alert_state(state: dict[str, float]) -> None:
    ALERT_FILE.write_text(json.dumps(state))

def should_fire(key: str, alert_state: dict[str, float]) -> bool:
    last = alert_state.get(key, 0.0)
    return (time.time() - last) > RESOUND_MINUTES * 60

# ── State ─────────────────────────────────────────────────────────────────────

def load_positions() -> list[dict]:
    try:
        data = json.loads(STATE_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []

# ── Binance live rates ────────────────────────────────────────────────────────

async def fetch_binance_live(symbols: list[str]) -> dict[str, float]:
    """Returns {symbol: annualised_apy_pct} for given symbols."""
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
                    if sym in symbols:
                        try:
                            rates[sym] = float(r["lastFundingRate"]) * 3 * 365 * 100
                        except Exception:
                            pass
    except Exception as e:
        print(f"[alarm] Binance live fetch error: {e}", file=sys.stderr)
    return rates

# ── Binance rate history ──────────────────────────────────────────────────────

async def fetch_binance_history(symbols: list[str]) -> dict[str, list[float]]:
    """
    Returns {symbol: [rate_pct, ...]} — last BIN_HISTORY_LIMIT settlements.
    Each rate is annualised APY %, ordered oldest → newest.
    """
    import aiohttp
    history: dict[str, list[float]] = {}
    async with aiohttp.ClientSession() as session:
        for sym in symbols:
            try:
                url = (
                    f"https://fapi.binance.com/fapi/v1/fundingRate"
                    f"?symbol={sym}USDT&limit={BIN_HISTORY_LIMIT}"
                )
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    data = await resp.json()
                    if isinstance(data, list):
                        history[sym] = [
                            float(r["fundingRate"]) * 3 * 365 * 100
                            for r in data
                        ]
            except Exception as e:
                print(f"[alarm] Binance history {sym}: {e}", file=sys.stderr)
    return history

# ── HL live rates + history ───────────────────────────────────────────────────

def fetch_hl_live_and_history(
    symbols: list[str],
    hours: int = HL_HISTORY_HOURS,
) -> tuple[dict[str, float], dict[str, list[float]]]:
    """
    Returns:
      live:    {symbol: apy_pct}
      history: {symbol: [apy_pct, ...]} oldest → newest, last `hours` hourly payments
    """
    import urllib.request as ur
    hl_url = "https://api.hyperliquid.xyz/info"
    live: dict[str, float] = {}
    history: dict[str, list[float]] = {}

    def _post(payload: dict) -> dict | list:
        data = json.dumps(payload).encode()
        req  = ur.Request(hl_url, data=data,
                          headers={"Content-Type": "application/json"})
        with ur.urlopen(req, timeout=10) as r:
            return json.loads(r.read())

    try:
        # Live rates — bulk
        meta_resp = _post({"type": "metaAndAssetCtxs"})
        if isinstance(meta_resp, list) and len(meta_resp) == 2:
            meta, ctxs = meta_resp
            for i, asset in enumerate(meta.get("universe", [])):
                name = asset.get("name", "")
                if name in symbols and i < len(ctxs):
                    rate = float(ctxs[i].get("funding", 0))
                    live[name] = rate * 24 * 365 * 100
    except Exception as e:
        print(f"[alarm] HL live fetch error: {e}", file=sys.stderr)

    # History per symbol
    start_ms = int((time.time() - hours * 3600) * 1000)
    for sym in symbols:
        try:
            resp = _post({"type": "fundingHistory", "coin": sym,
                          "startTime": start_ms})
            if isinstance(resp, list):
                history[sym] = [
                    float(r.get("fundingRate", 0)) * 24 * 365 * 100
                    for r in resp
                ]
        except Exception as e:
            print(f"[alarm] HL history {sym}: {e}", file=sys.stderr)

    return live, history

# ── Stability check ───────────────────────────────────────────────────────────

def count_direction_flips(rates: list[float]) -> int:
    """Count how many times the rate sign changed."""
    if len(rates) < 2:
        return 0
    flips = 0
    for i in range(1, len(rates)):
        if rates[i - 1] != 0 and rates[i] != 0:
            if (rates[i - 1] > 0) != (rates[i] > 0):
                flips += 1
    return flips

# ── Engineer trigger ─────────────────────────────────────────────────────────

ENGINEER_TRIGGER_FILE = Path("/tmp/needs-engineering.json")

def trigger_engineer(reason: str, alert_state: dict[str, float]) -> bool:
    """
    Signal the engineer cron by writing a trigger file.
    The engineer cron checks this file on every run and acts if present.
    Respects a cooldown so it doesn't spam on every alarm cycle.
    Returns True if trigger was written.
    """
    key = "__engineer_last_triggered__"
    last = alert_state.get(key, 0.0)
    if (time.time() - last) < ENGINEER_COOLDOWN_HOURS * 3600:
        return False  # still in cooldown
    try:
        ENGINEER_TRIGGER_FILE.write_text(json.dumps({
            "reason":    reason,
            "triggered": time.time(),
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }))
        alert_state[key] = time.time()
        print(f"[alarm] Engineer trigger written: {reason}", file=sys.stderr)
        return True
    except Exception as e:
        print(f"[alarm] Engineer trigger failed: {e}", file=sys.stderr)
        return False


# ── Telegram ──────────────────────────────────────────────────────────────────

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
        print(f"[alarm] Telegram failed: {e}", file=sys.stderr)
        return False

# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    load_env()

    positions = load_positions()
    if not positions:
        return  # nothing to watch

    alert_state = load_alert_state()
    symbols     = [p["symbol"] for p in positions]
    alerts:  list[str] = []
    fired_keys: list[str] = []

    # Fetch all data concurrently
    bin_live_task = asyncio.create_task(fetch_binance_live(symbols))
    bin_hist_task = asyncio.create_task(fetch_binance_history(symbols))
    hl_live, hl_hist = await asyncio.get_event_loop().run_in_executor(
        None, fetch_hl_live_and_history, symbols, HL_HISTORY_HOURS
    )
    bin_live = await bin_live_task
    bin_hist = await bin_hist_task

    now = datetime.now(timezone.utc).strftime("%H:%M UTC")

    for pos in positions:
        sym      = pos["symbol"]
        entry    = pos.get("entry_net_apy", 0.0)
        notl     = pos.get("notional_usdt", 100.0)
        b_sign   = 1 if pos.get("bin_side") == "buy" else -1
        h_sign   = 1 if pos.get("hl_side")  == "buy" else -1
        age_h    = (time.time() - pos.get("entry_ts", time.time())) / 3600

        b_live   = bin_live.get(sym, 0.0)
        h_live_v = hl_live.get(sym, 0.0)
        live_net = b_live * b_sign * (-1) + h_live_v * h_sign * (-1)
        daily    = notl * live_net / 100 / 365

        # ── CRITICAL: negative APY ────────────────────────────────────────────
        key = f"{sym}:negative"
        if live_net < 0 and should_fire(key, alert_state):
            alerts.append(
                f"🔴 <b>CRITICAL — {sym} NEGATIVE</b>\n"
                f"  Live net: {live_net:.1f}% (${daily:.4f}/day — LOSING)\n"
                f"  Bin: {b_live:.1f}%  HL: {h_live_v:.1f}%  Age: {age_h:.1f}h\n"
                f"  → <code>python3 cross_arb.py --close {sym}</code>"
            )
            fired_keys.append(key)

        # ── WARNING: near exit floor ──────────────────────────────────────────
        elif 0 <= live_net < EXIT_APY:
            key = f"{sym}:exit_floor"
            if should_fire(key, alert_state):
                alerts.append(
                    f"🚨 <b>WARNING — {sym} near exit</b>\n"
                    f"  Live net: {live_net:.1f}% (below {EXIT_APY:.0f}% floor)\n"
                    f"  Bin: {b_live:.1f}%  HL: {h_live_v:.1f}%  Age: {age_h:.1f}h"
                )
                fired_keys.append(key)

        # ── WARNING: significant decay ────────────────────────────────────────
        if entry > 0 and live_net < entry * (1 - DECAY_THRESHOLD) and age_h < 2.0:
            key = f"{sym}:decay"
            if should_fire(key, alert_state):
                pct = (entry - live_net) / entry * 100
                alerts.append(
                    f"📉 <b>DECAY — {sym} lost {pct:.0f}% of entry APY in {age_h:.1f}h</b>\n"
                    f"  Entry: {entry:.1f}%  Live: {live_net:.1f}%\n"
                    f"  Bin: {b_live:.1f}%  HL: {h_live_v:.1f}%"
                )
                fired_keys.append(key)

        # ── Rate inversion checks (HL + Binance) ──────────────────────────────
        # Fires when BOTH conditions hold simultaneously on either exchange:
        #   1. Rate just flipped sign in the most recent 2 settlements
        #   2. Live net APY has dropped to <50% of entry (flip is actively hurting)
        # 2 flips in 40h = normal noise; flip in last 16h + degraded position = real signal.
        position_degraded = (entry > 0 and live_net < entry * 0.5)

        hl_rates_hist = hl_hist.get(sym, [])
        if len(hl_rates_hist) >= 2 and position_degraded:
            r_prev, r_last = hl_rates_hist[-2], hl_rates_hist[-1]
            if r_prev != 0 and r_last != 0 and (r_prev > 0) != (r_last > 0):
                key = f"{sym}:hl_inversion"
                if should_fire(key, alert_state):
                    recent = [f"{r:.0f}%" for r in hl_rates_hist[-3:]]
                    alerts.append(
                        f"⚠️ <b>HL INVERSION — {sym}</b>\n"
                        f"  HL rate just flipped: {' → '.join(recent)}\n"
                        f"  Entry: {entry:.1f}%  Live: {live_net:.1f}% (-{(entry-live_net)/entry*100:.0f}%)\n"
                        f"  Bin: {b_live:.1f}%  HL: {h_live_v:.1f}%  Age: {age_h:.1f}h"
                    )
                    fired_keys.append(key)

        bin_rates_hist = bin_hist.get(sym, [])
        if len(bin_rates_hist) >= 2 and position_degraded:
            r_prev, r_last = bin_rates_hist[-2], bin_rates_hist[-1]
            if r_prev != 0 and r_last != 0 and (r_prev > 0) != (r_last > 0):
                key = f"{sym}:bin_inversion"
                if should_fire(key, alert_state):
                    recent = [f"{r:.0f}%" for r in bin_rates_hist[-3:]]
                    alerts.append(
                        f"⚠️ <b>Bin INVERSION — {sym}</b>\n"
                        f"  Binance rate just flipped (last ~16h): {' → '.join(recent)}\n"
                        f"  Entry: {entry:.1f}%  Live: {live_net:.1f}% (-{(entry-live_net)/entry*100:.0f}%)\n"
                        f"  Bin: {b_live:.1f}%  HL: {h_live_v:.1f}%  Age: {age_h:.1f}h"
                    )
                    fired_keys.append(key)

    # Send if anything fired
    if alerts:
        header = f"⚙️ <b>Arb Alarm</b> — {now}\n"
        message = header + "\n\n".join(alerts)
        ok = send_telegram(message)
        if ok:
            for key in fired_keys:
                alert_state[key] = time.time()

        # ── Engineering trigger heuristics ────────────────────────────────────
        # Fire engineer on-demand when alarms indicate a code-level issue:
        #   1. HL inversion detected → scanner needs a rate-stability filter
        #   2. CRITICAL (negative APY) alarm → exit logic may be too slow
        #   3. 3+ distinct alarms in one cycle → systemic, not a one-off
        engineer_reason = None
        inversion_fired  = any("inversion" in k for k in fired_keys)
        critical_fired   = any(":negative"     in k for k in fired_keys)
        many_alarms      = len(fired_keys) >= ENGINEER_ALARM_THRESHOLD

        if inversion_fired:
            engineer_reason = f"HL inversion on {[k.split(':')[0] for k in fired_keys if 'inversion' in k]}"
        elif critical_fired:
            engineer_reason = f"negative APY on {[k.split(':')[0] for k in fired_keys if 'negative' in k]}"
        elif many_alarms:
            engineer_reason = f"{len(fired_keys)} alarms in one cycle"

        if engineer_reason:
            triggered = trigger_engineer(engineer_reason, alert_state)
            if triggered:
                send_telegram(
                    f"⚙️ <b>Engineering review triggered</b>\n"
                    f"  Reason: {engineer_reason}\n"
                    f"  Engineer will analyse logs, identify the issue, and fix it."
                )

        save_alert_state(alert_state)
    # else: silent — nothing to report


if __name__ == "__main__":
    asyncio.run(main())
