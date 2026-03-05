# Fix B: Smart Entry — Patch File for cross_arb.py

## Summary of Changes

This patch implements four key improvements to the cross-exchange funding arbitrage bot to prevent entering on rate spikes that immediately revert:

1. **Rate Mean-Reversion Filter** — Requires net_apy to be above threshold in at least 2 of last 3 observations (filters one-off spikes)
2. **Rate Stability Score** — Adds coefficient-of-variation based stability metric (optionally gates entry at >0.3)
3. **Funding Timing Bonus/Penalty** — Adjusts APY ranking based on time until next funding settlement (+5% 1-4h before, -5% 0-1h after)
4. **Cooldown After Reversion** — Prevents re-entering symbols for 16h (2 funding intervals) after APY drops below exit threshold

---

## Change 1: Rate History Cache

### New Config Variables

Add after `BLACKLIST` definition (around line 60):

```python
# ── Smart Entry Config ───────────────────────────────────────────────────────
RATE_HISTORY_FILE   = "/tmp/cross-arb-rate-history.json"
COOLDOWN_FILE       = "/tmp/cross-arb-cooldowns.json"
MIN_ARB_APY_PERSISTENT = int(os.getenv("CARB_MIN_ARB_PERSISTENT", "2"))  # observations out of 3 that must exceed threshold
MIN_STABILITY_SCORE = float(os.getenv("CARB_MIN_STABILITY_SCORE", "0.3"))  # stability gate (0.0 = disabled)
```

### New Helper Functions

Add after the `load_state()` function (around line 115):

```python
# ── Rate History (Mean-Reversion Filter) ─────────────────────────────────────

def load_rate_history() -> dict[str, list[dict]]:
    """Load rate history from file."""
    if not os.path.exists(RATE_HISTORY_FILE):
        return {}
    try:
        with open(RATE_HISTORY_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_rate_history(history: dict[str, list[dict]]) -> None:
    """Save rate history to file."""
    with open(RATE_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def record_rates_for_symbols(
    rates: dict[str, tuple[float, float, float]],  # {sym: (bin_apy, hl_apy, net_apy)}
) -> None:
    """
    Record current rates for all symbols in history.
    Keeps last 6 observations per symbol.
    """
    history = load_rate_history()
    now = time.time()
    
    for sym, (bin_apy, hl_apy, net_apy) in rates.items():
        if sym not in history:
            history[sym] = []
        
        # Add new observation
        history[sym].append({
            "timestamp": now,
            "bin_apy": bin_apy,
            "hl_apy": hl_apy,
            "net_apy": net_apy,
        })
        
        # Keep only last 6 observations
        if len(history[sym]) > 6:
            history[sym] = history[sym][-6:]
    
    save_rate_history(history)


def check_rate_persistent(history: dict[str, list[dict]], symbol: str, min_apy: float) -> bool:
    """
    Check if symbol has net_apy >= min_apy in at least 2 of last 3 observations.
    Returns True if persistent opportunity, False if likely spike.
    """
    if symbol not in history or len(history[symbol]) < 3:
        # Not enough history — require 1 of last 2 at minimum
        if len(history.get(symbol, [])) >= 1:
            return history[symbol][-1].get("net_apy", 0) >= min_apy
        return False
    
    # Check last 3 observations
    recent = history[symbol][-3:]
    qualifying = sum(1 for obs in recent if obs.get("net_apy", 0) >= min_apy)
    
    return qualifying >= MIN_ARB_APY_PERSISTENT


def get_rate_history_for_symbol(history: dict[str, list[dict]], symbol: str) -> list[float]:
    """Get list of net_apy values for stability calculation."""
    if symbol not in history:
        return []
    return [obs.get("net_apy", 0) for obs in history[symbol]]


def rate_stability_score(history: list[float]) -> float:
    """
    Calculate stability score based on coefficient of variation.
    0.0 = wildly volatile, 1.0 = perfectly stable.
    """
    if len(history) < 2:
        return 0.0
    mean = sum(history) / len(history)
    if mean == 0:
        return 0.0
    variance = sum((x - mean) ** 2 for x in history) / len(history)
    std_dev = variance ** 0.5
    cv = std_dev / abs(mean)
    return max(0, 1 - cv)
```

### Modify `scan_opportunities` Function

**OLD CODE** (around line 260):

```python
async def scan_opportunities(
    hl_rates: dict[str, tuple[float, float]],  # {sym: (hr_rate, apy)}
    bin_rates: dict[str, float],               # {sym: apy}
    bin_volumes: dict[str, float],             # {sym: 24h_volume_usd}
    hl_volumes: dict[str, float],              # {sym: 24h_volume_usd}
    min_net_apy: float,
) -> list[ArbOpp]:
    """
    Find all cross-exchange arb opportunities above the APY threshold.
    """
    opps = []
    for sym in set(bin_rates.keys()) & set(hl_rates.keys()):
        if sym in BLACKLIST:
            continue
```

**NEW CODE**:

```python
async def scan_opportunities(
    hl_rates: dict[str, tuple[float, float]],  # {sym: (hr_rate, apy)}
    bin_rates: dict[str, float],               # {sym: apy}
    bin_volumes: dict[str, float],             # {sym: 24h_volume_usd}
    hl_volumes: dict[str, float],              # {sym: 24h_volume_usd}
    min_net_apy: float,
    apply_filters: bool = True,
) -> list[ArbOpp]:
    """
    Find all cross-exchange arb opportunities above the APY threshold.
    Applies mean-reversion filter and stability scoring when apply_filters=True.
    """
    # Load rate history for filtering
    rate_history = load_rate_history() if apply_filters else {}
    
    # Load cooldowns
    cooldowns = load_cooldowns() if apply_filters else {}
    
    opps = []
    for sym in set(bin_rates.keys()) & set(hl_rates.keys()):
        if sym in BLACKLIST:
            continue
        
        # Check cooldown (don't re-enter within 16h of close)
        if apply_filters and is_on_cooldown(cooldowns, sym):
            logger.debug("symbol_on_cooldown", symbol=sym)
            continue
        
        b_apy = bin_rates[sym]
        h_apy = hl_rates[sym][1]
```

And add after the net_apy calculation (before appending to opps):

```python
        if net >= min_net_apy:
            # Apply mean-reversion filter
            if apply_filters and not check_rate_persistent(rate_history, sym, min_net_apy):
                logger.debug("rate_not_persistent", symbol=sym, net_apy=net)
                continue
            
            # Apply stability score check (optional)
            if apply_filters and MIN_STABILITY_SCORE > 0:
                sym_history = get_rate_history_for_symbol(rate_history, sym)
                stability = rate_stability_score(sym_history)
                if stability < MIN_STABILITY_SCORE:
                    logger.debug("rate_not_stable", symbol=sym, stability=stability)
                    continue
            
            # Apply funding timing bonus/penalty
            adjusted_net = apply_funding_timing_bonus(net)
            
            opps.append(ArbOpp(
                symbol=sym,
                bin_apy=b_apy,
                hl_apy=h_apy,
                net_apy=adjusted_net,  # Use adjusted net for ranking
                raw_net_apy=net,       # Keep original for reference
                bin_side=bin_side,
                hl_side=hl_side,
                bin_vol=b_vol,
                hl_vol=h_vol,
            ))
```

### Add Funding Timing Functions

Add after `rate_stability_score()`:

```python
# ── Funding Timing Bonus/Penalty ───────────────────────────────────────────

def get_hours_until_funding() -> int:
    """
    Calculate hours until next funding settlement.
    Funding settles at 00:00, 08:00, 16:00 UTC.
    Returns hours until next settlement (0-7).
    Formula: (8 - (current_hour % 8)) % 8
    """
    import datetime
    utc_now = datetime.datetime.utcnow()
    current_hour = utc_now.hour
    
    # Next funding is at next 8-hour boundary
    hours_until = (8 - (current_hour % 8)) % 8
    return hours_until


def apply_funding_timing_bonus(net_apy: float) -> float:
    """
    Apply bonus or penalty to net_apy based on funding timing.
    +5% bonus if 1-4h before funding (maximize first payment capture)
    -5% penalty if 7-8h until next funding (0-1h after funding, wait too long)
    
    Hours until funding mapping:
    - 0h: at funding time (no bonus)
    - 1-4h: before funding (+5% bonus)
    - 5-6h: mid-cycle (no adjustment)
    - 7h: right after funding (-5% penalty, wait 7h for next)
    """
    hours_until = get_hours_until_funding()
    
    # 1-4 hours before funding: bonus
    if 1 <= hours_until <= 4:
        return net_apy * 1.05
    # 7 hours after funding: penalty (next funding is 7+ hours away)
    elif hours_until >= 7:
        return net_apy * 0.95
    else:
        return net_apy
```

---

## Change 2: Cooldown System

### Add Cooldown Functions

Add after the rate history functions:

```python
# ── Cooldown System ──────────────────────────────────────────────────────────

def load_cooldowns() -> dict[str, float]:
    """Load cooldown map from file. {symbol: expiry_timestamp}"""
    if not os.path.exists(COOLDOWN_FILE):
        return {}
    try:
        with open(COOLDOWN_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_cooldowns(cooldowns: dict[str, float]) -> None:
    """Save cooldown map to file."""
    with open(COOLDOWN_FILE, "w") as f:
        json.dump(cooldowns, f, indent=2)


def is_on_cooldown(cooldowns: dict[str, float], symbol: str) -> bool:
    """Check if symbol is currently on cooldown."""
    if symbol not in cooldowns:
        return False
    expiry = cooldowns[symbol]
    if time.time() >= expiry:
        # Expired — clean up
        del cooldowns[symbol]
        save_cooldowns(cooldowns)
        return False
    return True


def add_to_cooldown(symbol: str, duration_hours: float = 16.0) -> None:
    """
    Add symbol to cooldown after position closes due to reversion.
    Default: 2 funding intervals = 16 hours.
    """
    cooldowns = load_cooldowns()
    cooldowns[symbol] = time.time() + (duration_hours * 3600)
    save_cooldowns(cooldowns)
    logger.info("symbol_added_to_cooldown", symbol=symbol, duration_hours=duration_hours)


def cleanup_expired_cooldowns() -> None:
    """Remove expired cooldowns from file."""
    cooldowns = load_cooldowns()
    now = time.time()
    expired = [s for s, exp in cooldowns.items() if now >= exp]
    for s in expired:
        del cooldowns[s]
    if expired:
        save_cooldowns(cooldowns)
```

### Modify Exit Logic

**OLD CODE** (in `run_cross_arb`, around the exit check section):

```python
            if should_exit:
                if not pos.needs_close:
                    logger.warning("exit_signal", ...)
                    pos.needs_close = True
                    console.print(...)
                ok = await close_arb_position(pos)
                if ok:
                    # existing close logic...
                    positions.remove(pos)
                    save_state(positions)
```

**NEW CODE** — Add cooldown when closing due to APY reversion:

```python
            if should_exit:
                if not pos.needs_close:
                    logger.warning("exit_signal", ...)
                    pos.needs_close = True
                    console.print(...)
                ok = await close_arb_position(pos)
                if ok:
                    # Calculate exit APY and record close
                    # ...existing code...
                    
                    # If closed due to APY drop (not manual), add to cooldown
                    if live_net < EXIT_ARB_APY and not getattr(pos, '_manual_close', False):
                        add_to_cooldown(pos.symbol)
                        console.print(f"[dim]⏳ {pos.symbol} added to cooldown (16h)[/dim]")
                    
                    positions.remove(pos)
                    save_state(positions)
```

---

## Change 3: Record All Rates (Not Just Opportunities)

In the main scan loop (`run_cross_arb`), after fetching rates but before scanning for opportunities:

```python
                # ... existing rate fetching code ...

                # Record ALL rates for history tracking (not just opportunities)
                all_rates = {}
                for sym in set(bin_rates.keys()) & set(hl_rates.keys()):
                    if sym in BLACKLIST:
                        continue
                    b_apy = bin_rates[sym]
                    h_apy = hl_rates[sym][1]
                    b_vol = bin_vols.get(sym, 0.0)
                    h_vol = hl_vols.get(sym, 0.0)
                    if b_vol < MIN_BIN_VOL_USD or h_vol < MIN_HL_VOL_USD:
                        continue
                    
                    # Calculate net_apy for this symbol
                    if b_apy < 0 and h_apy < 0:
                        net = abs(b_apy) - abs(h_apy) if abs(b_apy) >= abs(h_apy) else abs(h_apy) - abs(b_apy)
                    elif b_apy > 0 and h_apy > 0:
                        net = b_apy - h_apy if b_apy >= h_apy else h_apy - b_apy
                    else:
                        net = abs(b_apy) + abs(h_apy)
                    
                    all_rates[sym] = (b_apy, h_apy, net)
                
                record_rates_for_symbols(all_rates)
                
                # Now scan for opportunities
                opps = await scan_opportunities(
                    hl_rates, bin_rates, bin_vols, hl_vols, MIN_ARB_APY
                )
```

---

## New Config Variables Summary

| Variable | Default | Description |
|----------|---------|-------------|
| `CARB_MIN_ARB_PERSISTENT` | `2` | Observations (out of 3) that must exceed MIN_ARB_APY |
| `CARB_MIN_STABILITY_SCORE` | `0.3` | Minimum stability score (0.0 = disabled) |
| `RATE_HISTORY_FILE` | `/tmp/cross-arb-rate-history.json` | Rate history cache |
| `COOLDOWN_FILE` | `/tmp/cross-arb-cooldowns.json` | Cooldown tracking file |

---

## New Files/Data Structures

### `/tmp/cross-arb-rate-history.json`
```json
{
  "BTC": [
    {"timestamp": 1699900000.0, "bin_apy": 12.5, "hl_apy": 8.2, "net_apy": 20.7},
    {"timestamp": 1699900300.0, "bin_apy": 11.8, "hl_apy": 7.9, "net_apy": 19.7},
    ...
  ],
  "ETH": [...]
}
```

### `/tmp/cross-arb-cooldowns.json`
```json
{
  "BTC": 1699960000.0,
  "SOL": 1699970000.0
}
```

---

## Dataclass Changes

### Modify ArbOpp to include raw_net_apy

**OLD CODE** (around line 75):

```python
@dataclass
class ArbOpp:
    symbol:       str
    bin_apy:      float
    hl_apy:       float
    net_apy:      float
    bin_side:     str
    hl_side:      str
    bin_vol:      float
    hl_vol:       float
```

**NEW CODE**:

```python
@dataclass
class ArbOpp:
    symbol:       str
    bin_apy:      float
    hl_apy:       float
    net_apy:      float   # Adjusted for timing bonus/penalty
    raw_net_apy:  float = 0.0  # Original net APY before adjustment
    bin_side:     str
    hl_side:      str
    bin_vol:      float
    hl_vol:       float
```

---

## Test Cases

See `/Users/nicholas/workspace/funding-farm/tests/test_smart_entry.py` for comprehensive tests.
