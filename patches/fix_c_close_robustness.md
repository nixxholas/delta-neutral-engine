# Fix C: Close Robustness — Patch Specification

This document specifies the exact code changes needed to implement robust close logic for the cross-exchange funding arbitrage bot.

## Summary of Changes

1. **Retry with Exponential Backoff** — Add retry logic (3 attempts, 2s/4s/8s backoff) to close operations with error type differentiation
2. **Half-Closed State Tracking** — Add tracking fields to `ArbPosition` to detect and handle half-closed states
3. **Critical Alert on Persistent Failure** — Alert when positions remain half-closed >10 minutes
4. **Startup Reconciliation** — On startup, verify actual exchange positions vs state file
5. **Circuit Breaker** — Global circuit breaker to pause new entries if too many close failures occur

---

## Change 1: Add New Fields to ArbPosition

### Old Code (lines ~85-110)
```python
@dataclass
class ArbPosition:
    symbol:        str
    bin_side:      str    # "buy" | "sell"
    hl_side:       str    # "buy" | "sell"
    bin_size:      float  # contracts on Binance
    hl_size:       float  # contracts on HL
    notional_usdt: float
    entry_bin_apy: float
    entry_hl_apy:  float
    entry_net_apy: float
    entry_ts:      float = field(default_factory=time.time)
    bin_order_id:  str   = ""
    hl_order_id:   str   = ""
    bin_entry_px:  float = 0.0
    hl_entry_px:   float = 0.0
    bin_closed:    bool  = False
    hl_closed:     bool  = False
    # Tracking
    last_bin_apy:  float = 0.0
    last_hl_apy:   float = 0.0
    last_rate_ts:  float = 0.0
    funding_realized_bin: float = 0.0
    funding_realized_hl:  float = 0.0
    needs_close:   bool  = False
```

### New Code
```python
@dataclass
class ArbPosition:
    symbol:        str
    bin_side:      str    # "buy" | "sell"
    hl_side:       str    # "buy" | "sell"
    bin_size:      float  # contracts on Binance
    hl_size:       float  # contracts on HL
    notional_usdt: float
    entry_bin_apy: float
    entry_hl_apy:  float
    entry_net_apy: float
    entry_ts:      float = field(default_factory=time.time)
    bin_order_id:  str   = ""
    hl_order_id:   str   = ""
    bin_entry_px:  float = 0.0
    hl_entry_px:   float = 0.0
    bin_closed:    bool  = False
    hl_closed:     bool  = False
    # Tracking
    last_bin_apy:  float = 0.0
    last_hl_apy:   float = 0.0
    last_rate_ts:  float = 0.0
    funding_realized_bin: float = 0.0
    funding_realized_hl:  float = 0.0
    needs_close:   bool  = False
    # --- NEW: Close robustness tracking ---
    bin_close_ts:  Optional[float] = None  # timestamp when Binance leg closed
    hl_close_ts:   Optional[float] = None   # timestamp when HL leg closed
    close_failure_count: int = 0           # how many times close has been attempted
```

---

## Change 2: Add Global State Variables

Add these near the top of the file after the config section:

### Old Code (after PERP_LEVERAGE config)
```python
PERP_LEVERAGE        = int(os.getenv(  "CARB_LEVERAGE",               "6"))  # 6x leverage (moderate)
SLIPPAGE             = float(os.getenv("CARB_SLIPPAGE",            "0.02"))  # 2% HL market order slippage
RT_FEE_PCT           = 0.0017   # Binance 0.05% + HL 0.035% = 0.085% per way × 2 (open+close) = 0.17%

STATE_FILE = "/tmp/cross-arb-state.json"
```

### New Code
```python
PERP_LEVERAGE        = int(os.getenv(  "CARB_LEVERAGE",               "6"))  # 6x leverage (moderate)
SLIPPAGE             = float(os.getenv("CARB_SLIPPAGE",            "0.02"))  # 2% HL market order slippage
SLIPPAGE_FORCE_CLOSE = float(os.getenv("CARB_SLIPPAGE_FORCE",       "0.04"))  # 4% slippage for forced closes
RT_FEE_PCT           = 0.0017   # Binance 0.05% + HL 0.035% = 0.085% per way × 2 (open+close) = 0.17%

STATE_FILE = "/tmp/cross-arb-state.json"
ALERTS_FILE = "/tmp/cross-arb-alerts.json"

# Circuit breaker state
_close_failure_timestamps: list[float] = []  # timestamps of recent close failures
CIRCUIT_BREAKER_WINDOW_S = 1800  # 30 minutes
CIRCUIT_BREAKER_THRESHOLD = 3   # 3 failures triggers pause
```

---

## Change 3: Add Helper Functions

Add these functions after the time-series functions and before the Scanner section:

### New Code (add this entire block)
```python
# ── Close Robustness Helpers ─────────────────────────────────────────────────

def is_circuit_breaker_active() -> bool:
    """Check if circuit breaker is active (too many recent close failures)."""
    global _close_failure_timestamps
    now = time.time()
    # Remove old timestamps outside the window
    _close_failure_timestamps = [
        ts for ts in _close_failure_timestamps
        if now - ts < CIRCUIT_BREAKER_WINDOW_S
    ]
    return len(_close_failure_timestamps) >= CIRCUIT_BREAKER_THRESHOLD


def record_close_failure() -> None:
    """Record a close failure for circuit breaker tracking."""
    global _close_failure_timestamps
    _close_failure_timestamps.append(time.time())


def load_alerts() -> list[dict]:
    """Load existing alerts from file."""
    if not os.path.exists(ALERTS_FILE):
        return []
    try:
        with open(ALERTS_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def save_alerts(alerts: list[dict]) -> None:
    """Save alerts to file."""
    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2)


def add_critical_alert(pos: ArbPosition, reason: str) -> None:
    """Add a critical alert for a persistent half-closed position."""
    alerts = load_alerts()
    alert = {
        "ts": time.time(),
        "symbol": pos.symbol,
        "reason": reason,
        "bin_closed": pos.bin_closed,
        "hl_closed": pos.hl_closed,
        "bin_close_ts": pos.bin_close_ts,
        "hl_close_ts": pos.hl_close_ts,
        "close_failure_count": pos.close_failure_count,
        "notional_usdt": pos.notional_usdt,
    }
    alerts.append(alert)
    # Keep only last 100 alerts
    alerts = alerts[-100:]
    save_alerts(alerts)
    
    # Console output with red color
    console.print(f"[red]🚨 CRITICAL ALERT: {reason}[/red]")
    console.print(f"   Symbol: {pos.symbol}, Notional: ${pos.notional_usdt:.0f}")
    console.print(f"   Binance closed: {pos.bin_closed}, HL closed: {pos.hl_closed}")
    console.print(f"   Failures: {pos.close_failure_count}")


async def close_leg_with_retry(
    close_fn,
    symbol: str,
    leg_name: str,
    max_retries: int = 3,
) -> any:
    """
    Execute a close function with exponential backoff retry.
    
    Returns:
        Result of close_fn on success
        
    Raises:
        Exception if all retries exhausted or non-retryable error
    """
    for attempt in range(max_retries):
        try:
            result = await close_fn()
            return result
        except Exception as e:
            err = str(e).lower()
            
            # Retryable errors: rate limit, timeout, 429
            retryable = (
                'rate limit' in err or 
                'timeout' in err or 
                '429' in err or
                'temporarily unavailable' in err or
                'service unavailable' in err
            )
            
            if retryable:
                wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                logger.warning(
                    f"{leg_name}_retry",
                    symbol=symbol,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    wait_seconds=wait,
                    error=str(e)[:100]
                )
                await asyncio.sleep(wait)
            else:
                # Non-retryable errors
                logger.error(
                    f"{leg_name}_unretryable",
                    symbol=symbol,
                    error=str(e)[:200]
                )
                raise
    
    raise Exception(f"{leg_name} failed after {max_retries} retries")


def check_position_health(pos: ArbPosition) -> tuple[bool, str]:
    """
    Check if a position is in a problematic state.
    
    Returns:
        (is_healthy, message)
    """
    now = time.time()
    
    # Check for half-closed state
    if pos.bin_closed != pos.hl_closed:
        # Determine which leg is still open
        half_closed_ts = pos.bin_close_ts if pos.bin_closed else pos.hl_close_ts
        if half_closed_ts is None:
            half_closed_ts = now  # assume it started when we noticed
        
        minutes_open = (now - half_closed_ts) / 60
        
        if minutes_open > 10:
            return False, f"Position half-closed for {minutes_open:.1f} minutes (>10min threshold)"
        else:
            return False, f"Position half-closed ({minutes_open:.1f}m)"
    
    # Check for excessive close failures
    if pos.close_failure_count >= 5:
        return False, f"Close failed {pos.close_failure_count} times (>=5 threshold)"
    
    return True, ""


# ── Startup Reconciliation ───────────────────────────────────────────────────

async def reconcile_positions(positions: list[ArbPosition]) -> list[ArbPosition]:
    """
    On startup, verify actual exchange positions vs state file.
    
    Returns:
        Updated list of positions with reconciled state
    """
    import ccxt.async_support as ccxt
    from hl_client import make_hl_client
    
    logger.info("reconciliation_start", positions=len(positions))
    
    if not positions:
        return []
    
    # Setup exchanges
    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    bin_ex = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })
    
    reconciled = []
    
    try:
        await bin_ex.load_markets()
        hl = make_hl_client()
        
        for pos in positions:
            discrepancies = []
            
            # Check Binance position
            bin_sym = pos.symbol + "/USDT:USDT"
            bin_still_open = False
            try:
                positions_bin = await bin_ex.fetch_positions([bin_sym])
                for p in positions_bin:
                    if p.get("symbol") == bin_sym:
                        actual_size = abs(float(p.get("contracts") or 0))
                        if actual_size > 1e-9:
                            bin_still_open = True
                        # Check if state is wrong
                        if pos.bin_closed and actual_size > 1e-9:
                            discrepancies.append(f"Binance: state=closed but exchange has {actual_size} contracts")
                        elif not pos.bin_closed and actual_size < 1e-9:
                            discrepancies.append(f"Binance: state=open but exchange is flat")
            except Exception as e:
                logger.warning("reconcile_bin_check_failed", symbol=pos.symbol, error=str(e)[:100])
            
            # Check HL position
            hl_pos = hl.get_position(pos.symbol)
            hl_still_open = hl_pos and abs(hl_pos.size) > 1e-9
            
            # Check if state is wrong
            if pos.hl_closed and hl_still_open:
                discrepancies.append(f"HL: state=closed but exchange has {hl_pos.size} contracts")
            elif not pos.hl_closed and not hl_still_open:
                discrepancies.append(f"HL: state=open but exchange is flat")
            
            if discrepancies:
                logger.warning(
                    "position_discrepancy",
                    symbol=pos.symbol,
                    discrepancies=discrepancies,
                    state_bin_closed=pos.bin_closed,
                    state_hl_closed=pos.hl_closed,
                    actual_bin_open=bin_still_open,
                    actual_hl_open=hl_still_open
                )
                
                # Fix state based on reality
                if not bin_still_open:
                    pos.bin_closed = True
                    if pos.bin_close_ts is None:
                        pos.bin_close_ts = time.time() - 3600  # Assume closed an hour ago
                if not hl_still_open:
                    pos.hl_closed = True
                    if pos.hl_close_ts is None:
                        pos.hl_close_ts = time.time() - 3600
                
                # If now fully closed, remove from active
                if pos.bin_closed and pos.hl_closed:
                    logger.info("reconciled_fully_closed", symbol=pos.symbol)
                    continue
                
                # If half-closed, mark for immediate attention
                if pos.bin_closed != pos.hl_closed:
                    pos.needs_close = True
                    pos.close_failure_count = max(pos.close_failure_count, 1)
                    logger.warning("reconciled_half_closed", symbol=pos.symbol)
            else:
                logger.info("reconciled_ok", symbol=pos.symbol)
            
            reconciled.append(pos)
        
        # Save reconciled state
        save_state(reconciled)
        logger.info("reconciliation_complete", original=len(positions), final=len(reconciled))
        
    finally:
        await bin_ex.close()
    
    return reconciled
```

---

## Change 4: Modify close_arb_position() Function

### Old Code (~lines 380-440)
```python
async def close_arb_position(pos: ArbPosition) -> bool:
    """
    Close both legs. Returns True if fully closed.
    Closes Binance leg first (easier to verify), then HL.
    """
    import ccxt.async_support as ccxt
    from hl_client import make_hl_client

    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret   = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    bin_ex   = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })

    try:
        await bin_ex.load_markets()
        bin_sym = pos.symbol + "/USDT:USDT"

        # ── Close Binance leg ──────────────────────────────────────────────
        if not pos.bin_closed:
            close_side = "sell" if pos.bin_side == "buy" else "buy"
            try:
                # Fetch actual position size first
                positions = await bin_ex.fetch_positions([bin_sym])
                actual_size = 0.0
                for p in positions:
                    if p.get("symbol") == bin_sym:
                        actual_size = abs(float(p.get("contracts") or 0))
                if actual_size < 1e-9:
                    logger.info("arb_bin_already_flat", symbol=pos.symbol)
                    pos.bin_closed = True
                else:
                    order = await bin_ex.create_order(
                        bin_sym, "market", close_side, actual_size,
                        params={"reduceOnly": True}
                    )
                    logger.info("arb_bin_closed", symbol=pos.symbol,
                                fill=order.get("average") or 0)
                    pos.bin_closed = True
            except Exception as e:
                logger.error("arb_bin_close_failed", symbol=pos.symbol, error=str(e)[:200])
                return False

        # ── Close HL leg ───────────────────────────────────────────────────
        if not pos.hl_closed:
            hl = make_hl_client()
            hl_pos = hl.get_position(pos.symbol)
            if not hl_pos or abs(hl_pos.size) < 1e-9:
                logger.info("arb_hl_already_flat", symbol=pos.symbol)
                pos.hl_closed = True
            else:
                ok, fill = hl.market_close(pos.symbol, slippage=SLIPPAGE)
                if ok:
                    pos.hl_closed = True
                    logger.info("arb_hl_closed", symbol=pos.symbol, fill=fill)
                else:
                    logger.error("arb_hl_close_FAILED_will_retry", symbol=pos.symbol)
                    return False

        both_done = pos.bin_closed and pos.hl_closed
        logger.info("arb_position_closed", symbol=pos.symbol, ok=both_done)
        return both_done

    finally:
        await bin_ex.close()
```

### New Code
```python
async def close_arb_position(pos: ArbPosition, force_slippage: bool = False) -> bool:
    """
    Close both legs. Returns True if fully closed.
    Closes Binance leg first (easier to verify), then HL.
    
    Args:
        pos: The position to close
        force_slippage: If True, use higher slippage tolerance (2x) for forced closes
    """
    import ccxt.async_support as ccxt
    from hl_client import make_hl_client

    key_path = os.getenv("BINANCE_PRIVATE_KEY_PATH", "")
    secret   = open(key_path).read() if key_path and os.path.exists(key_path) else os.getenv("BINANCE_API_SECRET", "")
    bin_ex   = ccxt.binanceusdm({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })

    slippage = SLIPPAGE_FORCE_CLOSE if force_slippage else SLIPPAGE
    
    try:
        await bin_ex.load_markets()
        bin_sym = pos.symbol + "/USDT:USDT"

        # ── Close Binance leg ──────────────────────────────────────────────
        if not pos.bin_closed:
            close_side = "sell" if pos.bin_side == "buy" else "buy"
            
            async def close_bin_leg():
                # Fetch actual position size first
                positions = await bin_ex.fetch_positions([bin_sym])
                actual_size = 0.0
                for p in positions:
                    if p.get("symbol") == bin_sym:
                        actual_size = abs(float(p.get("contracts") or 0))
                if actual_size < 1e-9:
                    logger.info("arb_bin_already_flat", symbol=pos.symbol)
                    return {"already_closed": True}
                order = await bin_ex.create_order(
                    bin_sym, "market", close_side, actual_size,
                    params={"reduceOnly": True}
                )
                return {"order": order}
            
            try:
                result = await close_leg_with_retry(
                    close_bin_leg, pos.symbol, "bin", max_retries=3
                )
                
                if result.get("already_closed"):
                    pos.bin_closed = True
                else:
                    order = result.get("order", {})
                    logger.info("arb_bin_closed", symbol=pos.symbol,
                                fill=order.get("average") or 0)
                    pos.bin_closed = True
                
                pos.bin_close_ts = time.time()
                
            except Exception as e:
                err = str(e).lower()
                # Handle special cases
                if "position not found" in err or "position size is zero" in err:
                    logger.info("arb_bin_already_flat", symbol=pos.symbol)
                    pos.bin_closed = True
                    pos.bin_close_ts = time.time()
                else:
                    logger.error("arb_bin_close_failed", symbol=pos.symbol, error=str(e)[:200])
                    record_close_failure()
                    pos.close_failure_count += 1
                    return False

        # ── Close HL leg ───────────────────────────────────────────────────
        if not pos.hl_closed:
            hl = make_hl_client()
            hl_pos = hl.get_position(pos.symbol)
            
            if not hl_pos or abs(hl_pos.size) < 1e-9:
                logger.info("arb_hl_already_flat", symbol=pos.symbol)
                pos.hl_closed = True
                pos.hl_close_ts = time.time()
            else:
                def close_hl_leg():
                    return hl.market_close(pos.symbol, slippage=slippage)
                
                try:
                    # For HL, we need to run sync code in async context
                    loop = asyncio.get_event_loop()
                    ok, fill = await loop.run_in_executor(None, close_hl_leg)
                    
                    if ok:
                        pos.hl_closed = True
                        pos.hl_close_ts = time.time()
                        logger.info("arb_hl_closed", symbol=pos.symbol, fill=fill)
                    else:
                        raise Exception("HL market_close returned False")
                        
                except Exception as e:
                    err = str(e).lower()
                    # Handle special cases
                    if "position already closed" in err or "position not found" in err:
                        logger.info("arb_hl_already_flat", symbol=pos.symbol)
                        pos.hl_closed = True
                        pos.hl_close_ts = time.time()
                    else:
                        logger.error("arb_hl_close_FAILED_will_retry", symbol=pos.symbol, error=str(e)[:200])
                        record_close_failure()
                        pos.close_failure_count += 1
                        return False

        both_done = pos.bin_closed and pos.hl_closed
        logger.info("arb_position_closed", symbol=pos.symbol, ok=both_done)
        
        if both_done:
            pos.needs_close = False
        
        return both_done

    finally:
        await bin_ex.close()
```

---

## Change 5: Modify run_cross_arb() to Add Reconciliation and Health Checks

### Find the start of run_cross_arb() and add reconciliation after load_state()

### Old Code (in run_cross_arb(), after positions loading)
```python
async def run_cross_arb() -> None:
    from hl_client import make_hl_client
    import ccxt.async_support as ccxt

    logger.info("cross_arb_starting",
                min_apy=MIN_ARB_APY, exit_apy=EXIT_ARB_APY,
                size=POSITION_SIZE_USDT, max_pos=MAX_POSITIONS)

    positions: list[ArbPosition] = load_state()
    last_scan_ts = 0.0
    last_checkpoint_ts = 0.0
    hl_equity = 0.0
    bin_equity = 0.0

    while True:
```

### New Code
```python
async def run_cross_arb() -> None:
    from hl_client import make_hl_client
    import ccxt.async_support as ccxt

    logger.info("cross_arb_starting",
                min_apy=MIN_ARB_APY, exit_apy=EXIT_ARB_APY,
                size=POSITION_SIZE_USDT, max_pos=MAX_POSITIONS)

    positions: list[ArbPosition] = load_state()
    
    # --- STARTUP RECONCILIATION ---
    positions = await reconcile_positions(positions)
    # -------------------------------
    
    last_scan_ts = 0.0
    last_checkpoint_ts = 0.0
    hl_equity = 0.0
    bin_equity = 0.0

    while True:
```

### Add circuit breaker check before scanning for new opportunities

Find the section where new opportunities are scanned and add a circuit breaker check:

### Old Code (in the "3. Scan for new opportunities" section)
```python
# ── 3. Scan for new opportunities ────────────────────────────────────
if now - last_scan_ts >= SCAN_INTERVAL_S:
```

### New Code
```python
# ── 3. Scan for new opportunities ────────────────────────────────────
# Check circuit breaker before scanning
if is_circuit_breaker_active():
    if not hasattr(run_cross_arb, '_circuit_breaker_logged'):
        logger.warning("circuit_breaker_active", 
                       failures=len(_close_failure_timestamps),
                       window_s=CIRCUIT_BREAKER_WINDOW_S)
        console.print(
            f"[red]⚠️  CIRCUIT BREAKER ACTIVE — Too many close failures "
            f"({len(_close_failure_timestamps)} in last {CIRCUIT_BREAKER_WINDOW_S//60}min)[/red]"
        )
        run_cross_arb._circuit_breaker_logged = True
    # Skip new entries but continue monitoring existing positions
else:
    run_cross_arb._circuit_breaker_logged = False

if now - last_scan_ts >= SCAN_INTERVAL_S and not is_circuit_breaker_active():
```

### Add health check for half-closed positions in the exit loop

In the "2. Exit check" section, after checking should_exit, add health checks:

### Old Code (in exit check loop, after should_exit logic)
```python
if should_exit:
    if not pos.needs_close:
        logger.warning("exit_signal",
                       symbol=pos.symbol,
                       live_net=round(live_net, 1),
                       threshold=EXIT_ARB_APY,
                       hold_hours=round(hold_hours, 1),
                       funding_earned=round(funding_earned, 4),
                       fee_cost=round(fee_cost, 4),
                       fee_covered=fee_covered)
        pos.needs_close = True
        console.print(...)
```

### New Code (add health check before normal exit logic)
```python
# Check position health (half-closed detection)
is_healthy, health_msg = check_position_health(pos)
if not is_healthy:
    if not pos.needs_close:
        pos.needs_close = True
        logger.warning("position_unhealthy", symbol=pos.symbol, reason=health_msg)
        
        # Check for persistent half-closed (>10 minutes)
        now_ts = time.time()
        half_closed = pos.bin_closed != pos.hl_closed
        if half_closed:
            half_ts = pos.bin_close_ts if pos.bin_closed else pos.hl_close_ts
            if half_ts and (now_ts - half_ts) > 600:  # 10 minutes
                # Critical: persistent half-closed
                add_critical_alert(pos, f"Persistent half-closed: {health_msg}")
                # Force close with higher slippage tolerance
                console.print(
                    f"[yellow]⚡ Force-closing {pos.symbol} with elevated slippage[/yellow]"
                )
                ok = await close_arb_position(pos, force_slippage=True)
                if ok:
                    positions.remove(pos)
                    save_state(positions)
                    console.print(f"[green]✓ {pos.symbol} force-closed[/green]")
                    continue
        elif pos.close_failure_count >= 5:
            # Too many failures - critical alert
            add_critical_alert(pos, f"Excessive close failures: {pos.close_failure_count}")

if should_exit or not is_healthy:
    if not pos.needs_close and is_healthy:
        # Only log exit signal if not already marked for close
        logger.warning("exit_signal",
                       symbol=pos.symbol,
                       live_net=round(live_net, 1),
                       threshold=EXIT_ARB_APY,
                       hold_hours=round(hold_hours, 1),
                       funding_earned=round(funding_earned, 4),
                       fee_cost=round(fee_cost, 4),
                       fee_covered=fee_covered)
        pos.needs_close = True
        console.print(...)
```

---

## Summary of New Functions and Variables

### New Dataclass Fields
- `bin_close_ts: Optional[float] = None` — timestamp when Binance leg closed
- `hl_close_ts: Optional[float] = None` — timestamp when HL leg closed
- `close_failure_count: int = 0` — number of close attempts

### New Global Variables
- `SLIPPAGE_FORCE_CLOSE` — slippage for forced closes (default 0.04 = 4%)
- `ALERTS_FILE` — path to alerts file
- `_close_failure_timestamps` — list of recent close failure timestamps
- `CIRCUIT_BREAKER_WINDOW_S` — window for circuit breaker (30 min)
- `CIRCUIT_BREAKER_THRESHOLD` — failures needed to trigger breaker (3)

### New Helper Functions
- `is_circuit_breaker_active() -> bool` — check if circuit breaker is active
- `record_close_failure() -> None` — record a failure for circuit breaker
- `load_alerts() -> list[dict]` — load alerts from file
- `save_alerts(alerts: list[dict]) -> None` — save alerts to file
- `add_critical_alert(pos, reason) -> None` — add critical alert for half-closed position
- `close_leg_with_retry(close_fn, symbol, leg_name, max_retries) -> any` — retry with backoff
- `check_position_health(pos) -> tuple[bool, str]` — check for problematic states
- `reconcile_positions(positions) -> list[ArbPosition]` — startup position reconciliation
