#!/usr/bin/env python3
"""
watchdog.py — Mechanical watchdog for funding-farm and cross-arb bots.

Checks process liveness, reads state files, tails logs for errors,
and POSTs a structured status report directly to Telegram.
No AI in the loop — pure deterministic code.

Usage:
    python3 watchdog.py              # run once and report
    cron: */2 * * * * python3 /path/to/watchdog.py
"""

from __future__ import annotations

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

BOT_TOKEN = "8236078404:AAGyY0RwES8fWJ1w2pF5Z3Bh7PV5P3HkBH8"
CHAT_ID   = "25836422"

FARM_DIR  = Path(__file__).parent
VENV_PYTHON = FARM_DIR.parent / "hl-mmbot" / "venv" / "bin" / "python3"

BOTS = {
    "cross_arb": {
        "label":      "Cross-Arb",
        "pgrep":      "cross_arb.py --run",
        "start_cmd":  f"cd {FARM_DIR} && source ../hl-mmbot/venv/bin/activate && nohup python3 cross_arb.py --run > /tmp/cross-arb.log 2>&1 &",
        "log":        Path("/tmp/cross-arb.log"),
        "state":      Path("/tmp/cross-arb-state.json"),
    },
}

# Daemons that should stay alive (checked/restarted via launchctl)
LAUNCHD_DAEMONS = {
    "publisher": {
        "label":  "com.crossarb.publisher",
        "pgrep":  "publisher.py",
        "log":    Path("/tmp/publisher.log"),
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def find_pid(pattern: str) -> int | None:
    """Return PID of first matching process, or None."""
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", pattern], text=True
        ).strip()
        pids = [int(x) for x in out.splitlines() if x.strip().isdigit()]
        return pids[0] if pids else None
    except subprocess.CalledProcessError:
        return None


def restart_bot(bot: dict) -> int | None:
    """Restart a bot and return new PID (or None on failure)."""
    subprocess.run(bot["start_cmd"], shell=True, executable="/bin/zsh")
    time.sleep(2)
    return find_pid(bot["pgrep"])


def tail_errors(log_path: Path, lines: int = 40) -> list[str]:
    """Return recent error/warning lines from log."""
    if not log_path.exists():
        return []
    try:
        out = subprocess.check_output(
            ["tail", "-n", str(lines), str(log_path)], text=True
        )
        flagged = []
        for line in out.splitlines():
            low = line.lower()
            if any(k in low for k in ("error", "exception", "traceback", "critical", "failed", "warning")):
                flagged.append(line.strip())
        return flagged[-5:]  # last 5 notable lines
    except Exception:
        return []


def read_state(state_path: Path) -> list:
    """Read JSON state file — returns a list of position dicts."""
    try:
        data = json.loads(state_path.read_text())
        # State files are plain lists; guard against dict wrapper just in case
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("positions", [])
        return []
    except Exception:
        return []


def fmt_positions_cross_arb(positions: list) -> str:
    if not positions:
        return "  No open positions"
    lines = []
    total_daily = 0.0
    for p in positions:
        sym    = p.get("symbol", "?")
        apy    = p.get("entry_net_apy", 0.0)
        notl   = p.get("notional_usdt", p.get("notional", 0.0))
        b_side = p.get("bin_side", "?")
        h_side = p.get("hl_side", "?")
        daily  = notl * apy / 100 / 365
        total_daily += daily
        lines.append(f"  {sym}: {apy:.1f}% APY | ${notl:.0f} | Bin={b_side} HL={h_side} | ~${daily:.3f}/day")
    lines.append(f"  Total: ~${total_daily:.3f}/day")
    return "\n".join(lines)


def fmt_positions_farm(positions: list) -> str:
    if not positions:
        return "  No open positions"
    lines = []
    total_realized = 0.0
    for p in positions:
        sym       = p.get("symbol", "?")
        direction = p.get("direction", "?")
        notl      = p.get("notional", 0.0)
        net_apy   = p.get("live_net_apy", p.get("net_apy", 0.0))
        realized  = p.get("funding_realized", 0.0)
        total_realized += realized
        lines.append(f"  {sym}: {net_apy:.1f}% net APY | ${notl:.0f} | dir={direction} | realized=${realized:.4f}")
    lines.append(f"  Total realized: ${total_realized:.4f}")
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    """POST message to Telegram Bot API."""
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
        print(f"[watchdog] Telegram send failed: {e}", file=sys.stderr)
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def check_launchd_daemons() -> list[str]:
    """Check launchd daemons and restart via launchctl if dead. Returns alert lines."""
    alerts = []
    for name, d in LAUNCHD_DAEMONS.items():
        pid = find_pid(d["pgrep"])
        if pid is None:
            # Not running — kick via launchctl
            label = d["label"]
            try:
                subprocess.run(
                    ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{label}"],
                    capture_output=True, timeout=10,
                )
                time.sleep(3)
                new_pid = find_pid(d["pgrep"])
                if new_pid:
                    alerts.append(f"⚠️ {name} was dead — restarted (PID {new_pid})")
                else:
                    alerts.append(f"❌ {name} dead, kickstart failed")
            except Exception as e:
                alerts.append(f"❌ {name} dead, kickstart error: {e}")
    return alerts


def main() -> None:
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    parts   = [f"🤖 <b>Bot Status</b> — {now_str}"]

    # Check launchd daemons (publisher etc.)
    daemon_alerts = check_launchd_daemons()
    if daemon_alerts:
        parts.append("")
        parts.extend(daemon_alerts)

    for key, bot in BOTS.items():
        label = bot["label"]
        pid   = find_pid(bot["pgrep"])
        restarted = False

        if pid is None:
            # Dead — attempt restart
            new_pid = restart_bot(bot)
            restarted = True
            pid = new_pid

        state = read_state(bot["state"])
        errors = tail_errors(bot["log"])

        # Status line
        if restarted:
            status = f"⚠️ <b>{label}</b> — was DEAD, restarted (PID {pid or 'unknown'})"
        else:
            status = f"✅ <b>{label}</b> — alive (PID {pid})"

        parts.append("")
        parts.append(status)

        # Positions
        if key == "cross_arb":
            parts.append(fmt_positions_cross_arb(state))
        else:
            parts.append(fmt_positions_farm(state))

        # Errors
        if errors:
            parts.append("  ⚠️ Recent issues:")
            for e in errors:
                parts.append(f"  • {e[:120]}")

    message = "\n".join(parts)
    sent = send_telegram(message)
    if not sent:
        print("[watchdog] Failed to deliver report", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
