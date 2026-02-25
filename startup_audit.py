#!/usr/bin/env python3
"""
startup_audit.py — Post-compaction startup audit.

Watches the main OpenClaw session for context compaction events by reading
the raw session JSONL transcript directly.  When a NEW compaction is detected
(one not yet handled), injects the required startup files into the session as
a one-shot systemEvent cron job so the AI re-reads them automatically.

Fully mechanical — no AI in the loop.  Pure Python, file reads, HTTP.

Usage:
    python3 startup_audit.py        # run once (called from launchd)

Detection:
    Reads `transcriptPath` from sessions_list, then scans the JSONL for
    {"type": "compaction"} events.  Picks the most recent timestamp.

Injection:
    Schedules a one-shot cron systemEvent via POST /tools/invoke → cron.add
    Fires ~15s after detection.

State:
    /tmp/startup-audit-state.json  →  last_handled_ts (float, epoch seconds)
"""

from __future__ import annotations

import json
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

GATEWAY_URL   = "http://localhost:18789"
GATEWAY_TOKEN = "cb60840e55cbc3483e013e587b6c11c0c4e6cbfe69cbe4d2"
SESSION_KEY   = "agent:main:main"
STATE_FILE    = Path("/tmp/startup-audit-state.json")

WORKSPACE     = Path("/Users/nicholas/workspace")
SESSIONS_DIR  = Path.home() / ".openclaw" / "agents" / "main" / "sessions"

BOT_TOKEN     = "8236078404:AAGyY0RwES8fWJ1w2pF5Z3Bh7PV5P3HkBH8"
CHAT_ID       = "25836422"

# Files to inject on compaction (relative to WORKSPACE)
STARTUP_FILES = [
    "SOUL.md",
    "USER.md",
    "IDENTITY.md",
    "MEMORY.md",
]

# ── State ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {"last_handled_ts": 0.0}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Gateway API ───────────────────────────────────────────────────────────────

def gateway_invoke(tool: str, args: dict) -> dict | None:
    payload = json.dumps({"tool": tool, "args": args}).encode()
    req = urllib.request.Request(
        f"{GATEWAY_URL}/tools/invoke",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {GATEWAY_TOKEN}",
            "Content-Type":  "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            if body.get("ok"):
                return body.get("result", {})
            print(f"[audit] gateway error: {body}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"[audit] gateway request failed: {e}", file=sys.stderr)
        return None


def get_transcript_path() -> Path | None:
    """
    Locate the active session transcript.

    Strategy:
    1. Ask sessions_list for transcriptPath → extract the UUID → look it up
       under SESSIONS_DIR (the gateway may return a stale/wrong absolute path).
    2. Fall back to the most recently modified JSONL in SESSIONS_DIR.
    """
    uuid: str | None = None

    result = gateway_invoke("sessions_list", {})
    if result:
        details = result.get("details", {})
        sessions: list = []
        if isinstance(details, dict):
            sessions = details.get("sessions", [])
        if not sessions:
            for item in result.get("content", []):
                try:
                    parsed = json.loads(item.get("text", ""))
                    sessions = parsed.get("sessions", [])
                    break
                except Exception:
                    pass
        for sess in sessions:
            if sess.get("key") == SESSION_KEY:
                tp = sess.get("transcriptPath", "")
                if tp:
                    # Try the path as-is first
                    p = Path(tp)
                    if p.exists():
                        return p
                    # Extract UUID from filename and look in SESSIONS_DIR
                    stem = Path(tp).stem          # e.g. "076b72ea-ce86-..."
                    candidate = SESSIONS_DIR / f"{stem}.jsonl"
                    if candidate.exists():
                        return candidate
                    uuid = stem
                break

    # Fallback: most recently modified JSONL in SESSIONS_DIR
    if SESSIONS_DIR.exists():
        files = sorted(SESSIONS_DIR.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
        if files:
            return files[-1]
    return None


# ── Compaction detection ──────────────────────────────────────────────────────

def find_latest_compaction(transcript: Path) -> float | None:
    """
    Scan the JSONL transcript for {"type": "compaction"} events.
    Returns the epoch-second timestamp of the MOST RECENT compaction, or None.
    """
    latest_ts: float | None = None
    try:
        with open(transcript) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("type") != "compaction":
                    continue
                raw_ts = ev.get("timestamp", "")
                try:
                    # ISO-8601 string like "2026-02-24T02:36:52.352Z"
                    dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                    ts = dt.timestamp()
                except Exception:
                    continue
                if latest_ts is None or ts > latest_ts:
                    latest_ts = ts
    except Exception as e:
        print(f"[audit] transcript read error: {e}", file=sys.stderr)
    return latest_ts


# ── Payload builder ───────────────────────────────────────────────────────────

def read_file_safe(path: Path) -> str:
    try:
        return path.read_text()
    except Exception:
        return "[not found]"


def build_daily_memory_paths() -> list[Path]:
    """Today's and yesterday's daily memory files (SGT = UTC+8)."""
    sgt = timezone(timedelta(hours=8))
    today     = datetime.now(sgt)
    yesterday = today - timedelta(days=1)
    paths = []
    for d in [today, yesterday]:
        p = WORKSPACE / "memory" / d.strftime("%Y-%m-%d.md")
        if p.exists():
            paths.append(p)
    return paths


def build_inject_text(compaction_ts: float) -> str:
    sgt_time = datetime.fromtimestamp(
        compaction_ts, tz=timezone(timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M SGT")

    sections = [
        f"[Startup Audit] Context was compacted at {sgt_time}.",
        "The following required startup files are being injected. Read and absorb them.",
        "=" * 60,
    ]

    for rel in STARTUP_FILES:
        content = read_file_safe(WORKSPACE / rel)
        sections.append(f"\n=== {rel} ===\n{content.strip()}")

    for path in build_daily_memory_paths():
        rel     = path.relative_to(WORKSPACE)
        content = read_file_safe(path)
        sections.append(f"\n=== {rel} ===\n{content.strip()}")

    sections += [
        "\n" + "=" * 60,
        "[Startup Audit Complete] All required context loaded. Proceed normally.",
    ]
    return "\n".join(sections)


# ── Injection ─────────────────────────────────────────────────────────────────

def schedule_inject(text: str) -> bool:
    fire_at = (
        datetime.now(timezone.utc) + timedelta(seconds=15)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    result = gateway_invoke("cron", {
        "action": "add",
        "job": {
            "name":     "startup-audit-inject",
            "schedule": {"kind": "at", "at": fire_at},
            "payload":  {"kind": "systemEvent", "text": text},
            "sessionTarget": "main",
            "enabled":  True,
        },
    })
    return result is not None


# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram(text: str) -> None:
    data = urllib.parse.urlencode({
        "chat_id":    CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
    }).encode()
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=data, method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[audit] Telegram failed: {e}", file=sys.stderr)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    state = load_state()
    last_handled = float(state.get("last_handled_ts", 0))

    transcript = get_transcript_path()
    if not transcript or not transcript.exists():
        print(f"[audit] transcript not found: {transcript}", file=sys.stderr)
        sys.exit(0)

    latest_compaction = find_latest_compaction(transcript)
    if latest_compaction is None:
        # No compaction events in transcript
        sys.exit(0)

    if latest_compaction <= last_handled:
        # Already handled
        sys.exit(0)

    sgt = datetime.fromtimestamp(
        latest_compaction, tz=timezone(timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M SGT")
    print(f"[audit] new compaction detected: {sgt} — injecting startup files")

    inject_text = build_inject_text(latest_compaction)
    ok          = schedule_inject(inject_text)

    if ok:
        state["last_handled_ts"]  = latest_compaction
        state["last_inject_utc"]  = datetime.now(timezone.utc).isoformat()
        state["last_compaction"]  = sgt
        save_state(state)

        send_telegram(
            "⚙️ <b>Startup Audit</b>\n"
            f"Compaction detected ({sgt}) — startup files injected into session.\n"
            "SOUL.md · USER.md · IDENTITY.md · MEMORY.md · daily notes"
        )
        print("[audit] injection scheduled OK.")
    else:
        print("[audit] injection scheduling FAILED.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
