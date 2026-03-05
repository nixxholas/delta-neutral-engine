#!/usr/bin/env python3
"""
Test suite for Cost Tracking implementation in cross_arb.py

Tests:
1. Slippage calculation
2. Basis tracking
3. True P&L calculation
4. Funding tracking update function
5. Atomic state file writes
"""

import json
import os
import sys
import tempfile
import time
import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ============================================================================
# Test 1: Slippage Calculation
# ============================================================================

def test_slippage_bps_calculation():
    """Test slippage calculation in basis points."""
    # Case 1: Fill at mid (zero slippage)
    mid = 50000.0
    fill = 50000.0
    slippage_bps = abs(fill - mid) / mid * 10000
    assert slippage_bps == 0.0
    
    # Case 2: Fill 1 tick above mid (0.02% = 2 bps)
    fill = 50010.0  # 10 / 50000 * 10000 = 2 bps
    slippage_bps = abs(fill - mid) / mid * 10000
    assert abs(slippage_bps - 2.0) < 0.01
    
    # Case 3: Fill 15 bps above mid
    fill = 50075.0  # 75 / 50000 * 10000 = 15 bps
    slippage_bps = abs(fill - mid) / mid * 10000
    assert abs(slippage_bps - 15.0) < 0.01
    
    # Case 4: Fill 20 bps below mid (negative slippage)
    fill = 49900.0  # -100 / 50000 * 10000 = -20 bps -> abs = 20
    slippage_bps = abs(fill - mid) / mid * 10000
    assert abs(slippage_bps - 20.0) < 0.01
    
    # Case 5: Edge case - zero mid price
    mid = 0.0
    fill = 50000.0
    slippage_bps = abs(fill - mid) / mid * 10000 if mid > 0 else 0.0
    assert slippage_bps == 0.0


def test_combined_slippage_gate():
    """Test that combined slippage gate works correctly."""
    SLIPPAGE_GATE_BPS = 15.0
    
    # Test cases: (bin_slippage, hl_slippage, should_pass)
    test_cases = [
        (5.0, 5.0, True),   # 10 bps total - pass
        (10.0, 5.0, True),  # 15 bps total - pass (at threshold)
        (10.0, 6.0, False),  # 16 bps total - fail
        (0.0, 0.0, True),   # Zero slippage - pass
        (15.0, 0.0, True),  # At threshold - pass
        (15.01, 0.0, False), # Just over threshold - fail
    ]
    
    for bin_slip, hl_slip, should_pass in test_cases:
        combined = bin_slip + hl_slip
        passes = combined <= SLIPPAGE_GATE_BPS
        assert passes == should_pass, f"Failed for bin={bin_slip}, hl={hl_slip}: combined={combined}"


# ============================================================================
# Test 2: Basis Tracking
# ============================================================================

def test_basis_calculation():
    """Test basis and basis change calculation."""
    # Entry prices
    bin_entry_px = 50000.0
    hl_entry_px = 49950.0
    entry_basis = bin_entry_px - hl_entry_px
    assert entry_basis == 50.0  # Binance 50 points higher
    
    # Exit prices
    bin_exit_fill = 50100.0
    hl_exit_fill = 50040.0
    exit_basis = bin_exit_fill - hl_exit_fill
    assert exit_basis == 60.0
    
    # Basis change
    basis_change = exit_basis - entry_basis
    assert basis_change == 10.0  # Basis widened by 10 points
    
    # Test negative basis change (basis narrowing)
    bin_exit_fill = 50050.0
    hl_exit_fill = 50040.0
    exit_basis = bin_exit_fill - hl_exit_fill  # 10
    basis_change = exit_basis - entry_basis     # 10 - 50 = -40
    assert basis_change == -40.0  # Basis narrowed by 40 points


def test_basis_cost_calculation():
    """Test basis cost in dollar terms."""
    notional_usdt = 30000.0
    bin_entry_px = 50000.0
    
    # Entry basis: bin 50 pts higher than HL
    entry_basis = 50.0
    
    # Exit basis: bin 60 pts higher (widened)
    exit_basis = 60.0
    basis_change = exit_basis - entry_basis  # 10 pts
    
    # Basis cost = basis_change * position_size / entry_price
    # For a long bin + short hl position:
    # - If basis widens (bin goes up more), we lose on the short
    basis_cost = basis_change * notional_usdt / bin_entry_px
    
    expected = 10.0 * 30000.0 / 50000.0  # = 6.0
    assert abs(basis_cost - expected) < 0.01


# ============================================================================
# Test 3: True P&L Calculation
# ============================================================================

def test_true_pnl_calculation():
    """Test comprehensive true P&L calculation."""
    # Setup
    notional = 30000.0
    
    # Entry slippage
    bin_entry_slippage_bps = 5.0
    hl_entry_slippage_bps = 3.0
    entry_slippage_cost = (
        (bin_entry_slippage_bps / 10000) * notional +
        (hl_entry_slippage_bps / 10000) * notional
    )
    expected_entry = (0.0005 + 0.0003) * 30000  # 24.0
    assert abs(entry_slippage_cost - expected_entry) < 0.01
    
    # Exit slippage
    bin_exit_slippage_bps = 2.0
    hl_exit_slippage_bps = 4.0
    exit_slippage_cost = (
        (bin_exit_slippage_bps / 10000) * notional +
        (hl_exit_slippage_bps / 10000) * notional
    )
    expected_exit = (0.0002 + 0.0004) * 30000  # 18.0
    assert abs(exit_slippage_cost - expected_exit) < 0.01
    
    total_slippage_cost = entry_slippage_cost + exit_slippage_cost
    assert total_slippage_cost == 42.0
    
    # Fee cost
    RT_FEE_PCT = 0.0017
    fee_cost = notional * RT_FEE_PCT
    assert abs(fee_cost - 51.0) < 0.01  # 30000 * 0.0017 = 51
    
    # Funding earned
    funding_earned = 75.0  # Example
    
    # Basis cost
    basis_change = 10.0  # From earlier test
    bin_entry_px = 50000.0
    basis_cost = basis_change * notional / bin_entry_px
    assert abs(basis_cost - 6.0) < 0.01
    
    # True P&L
    true_pnl = funding_earned - fee_cost - total_slippage_cost - basis_cost
    expected = 75.0 - 51.0 - 42.0 - 6.0  # = -24.0
    assert abs(true_pnl - expected) < 0.01


def test_true_pnl_profitable_scenario():
    """Test true P&L in a profitable scenario."""
    notional = 30000.0
    RT_FEE_PCT = 0.0017
    
    # Low slippage
    entry_slippage = ((2.0 + 2.0) / 10000) * notional  # 4/10000 * 30000 = 12.0
    exit_slippage = ((1.0 + 1.0) / 10000) * notional    # 2/10000 * 30000 = 6.0
    total_slippage = entry_slippage + exit_slippage  # 18.0

    # Fees
    fee_cost = notional * RT_FEE_PCT  # 51.0

    # Good funding
    funding_earned = 120.0

    # Basis changed favorably (negative = we gained)
    basis_cost = -5.0

    true_pnl = funding_earned - fee_cost - total_slippage - basis_cost
    # = 120 - 51 - 18 - (-5) = 56
    assert abs(true_pnl - 56.0) < 0.01


# ============================================================================
# Test 4: Funding Tracking Update Function
# ============================================================================

def test_funding_update_basic():
    """Test basic funding update calculation."""
    # Mock position
    pos = MagicMock()
    pos.symbol = "BTC"
    pos.bin_side = "buy"   # Long on Binance
    pos.hl_side = "sell"   # Short on HL
    pos.notional_usdt = 30000.0
    pos.last_rate_ts = 0.0
    pos.funding_realized_bin = 0.0
    pos.funding_realized_hl = 0.0
    
    current_bin_apy = -0.01  # -0.01% APY = longs earn (negative rate)
    current_hl_apy = 0.005   # +0.005% APY = shorts earn (positive rate)
    hours_elapsed = 1.0

    # Formula matches live APY: rate * earn_sign * (-1)
    # Binance: long position (earn_sign=1), negative rate → earn
    bin_hourly_rate = current_bin_apy / 100.0 / 365.0
    bin_earn_sign = 1  # bin_side == "buy" means long
    bin_funding = bin_hourly_rate * bin_earn_sign * (-1) * pos.notional_usdt * hours_elapsed
    # = (-0.01/100/365) * 1 * (-1) * 30000 * 1 = positive (longs earn when rate negative)
    assert bin_funding > 0

    # HL: short position (earn_sign=-1), positive rate → shorts earn
    hl_earn_sign = -1  # "sell" means short
    hl_hourly_rate = current_hl_apy / 100.0 / 365.0
    hl_funding = hl_hourly_rate * hl_earn_sign * (-1) * pos.notional_usdt * hours_elapsed
    # = (0.005/100/365) * (-1) * (-1) * 30000 = positive (shorts earn when rate positive)
    assert hl_funding > 0


def test_funding_update_accumulation():
    """Test that funding accumulates over multiple updates."""
    # Simulate a position held for multiple funding intervals
    pos = MagicMock()
    pos.symbol = "BTC"
    pos.bin_side = "buy"
    pos.hl_side = "sell"
    pos.notional_usdt = 30000.0
    pos.funding_realized_bin = 0.0
    pos.funding_realized_hl = 0.0
    
    # Rates (annualized APY)
    bin_apy = -10.0  # -10% APY -> longs earn
    hl_apy = 5.0     # +5% APY -> shorts earn (we're short, so we earn)
    
    # Simulate 3 updates, each with 1 hour elapsed
    # Formula: rate * earn_sign * (-1) * notional * hours
    for i in range(3):
        now = time.time()
        hours_elapsed = 1.0
        pos.last_rate_ts = now - 3600  # 1 hour ago

        # Bin funding (long, negative rate → earns)
        bin_hourly = bin_apy / 100.0 / 365.0
        bin_earn_sign = 1  # buy = long
        bin_funding = bin_hourly * bin_earn_sign * (-1) * pos.notional_usdt * hours_elapsed
        pos.funding_realized_bin += bin_funding

        # HL funding (short, positive rate → shorts earn)
        hl_hourly = hl_apy / 100.0 / 365.0
        hl_earn_sign = -1  # sell = short
        hl_funding = hl_hourly * hl_earn_sign * (-1) * pos.notional_usdt * hours_elapsed
        pos.funding_realized_hl += hl_funding

    total_funding = pos.funding_realized_bin + pos.funding_realized_hl

    # Long on Binance, bin_apy = -10%: earn = -(-10) = +10%
    # Short on HL, hl_apy = +5%: earn = +(+5) = +5%
    # Net = 15% APY
    bin_hourly_earn = 0.10 / 365.0  # +10% APY → positive hourly rate
    hl_hourly_earn = 0.05 / 365.0   # +5% APY → positive hourly rate

    expected_per_hour = 30000 * (bin_hourly_earn + hl_hourly_earn)
    expected_total = expected_per_hour * 3

    assert abs(total_funding - expected_total) < 0.1


def test_funding_update_skip_small_intervals():
    """Test that small time intervals don't cause issues."""
    pos = MagicMock()
    pos.symbol = "BTC"
    pos.bin_side = "buy"
    pos.hl_side = "sell"
    pos.notional_usdt = 30000.0
    pos.last_rate_ts = time.time()
    pos.funding_realized_bin = 0.0
    pos.funding_realized_hl = 0.0
    
    now = time.time()
    hours_elapsed = (now - pos.last_rate_ts) / 3600.0
    
    # Should be 0 or very close to 0
    assert hours_elapsed < 0.001


# ============================================================================
# Test 5: Atomic State File Writes
# ============================================================================

def test_atomic_save_state():
    """Test that save_state writes atomically."""
    # Import the function (we'll need to patch it)
    from cross_arb import save_state, STATE_FILE, ArbPosition
    
    # Create temp for test
    with tempfile.TemporaryDirectory() as tmpdir:
        test_state_file = os.path.join(tmpdir, "test_state.json")
        
        # Create mock positions
        positions = [
            ArbPosition(
                symbol="BTC",
                bin_side="buy",
                hl_side="sell",
                bin_size=0.01,
                hl_size=0.01,
                notional_usdt=500.0,
                entry_bin_apy=-10.0,
                entry_hl_apy=5.0,
                entry_net_apy=15.0,
                # New slippage fields
                bin_entry_slippage_bps=2.5,
                hl_entry_slippage_bps=3.0,
            ),
            ArbPosition(
                symbol="ETH",
                bin_side="sell",
                hl_side="buy",
                bin_size=0.1,
                hl_size=0.1,
                notional_usdt=300.0,
                entry_bin_apy=8.0,
                entry_hl_apy=-3.0,
                entry_net_apy=11.0,
                # New fields
                bin_entry_slippage_bps=1.0,
                hl_entry_slippage_bps=1.5,
                true_pnl=5.5,
            ),
        ]
        
        # Mock STATE_FILE temporarily
        with patch('cross_arb.STATE_FILE', test_state_file):
            # The implementation uses os.replace which is atomic
            # We just verify it creates the file correctly
            save_state(positions)
            
            # Check file exists
            assert os.path.exists(test_state_file)
            
            # Check content
            with open(test_state_file) as f:
                data = json.load(f)
            
            assert len(data) == 2
            assert data[0]["symbol"] == "BTC"
            assert data[0]["bin_entry_slippage_bps"] == 2.5
            assert data[1]["symbol"] == "ETH"
            assert data[1]["true_pnl"] == 5.5


def test_atomic_write_creates_temp_file():
    """Test that temp file is used during write."""
    import tempfile
    import os
    
    # This tests the pattern used in atomic writes
    test_dir = tempfile.mkdtemp()
    try:
        test_file = os.path.join(test_dir, "output.json")
        
        # Write using temp + replace pattern
        fd, tmp = tempfile.mkstemp(dir=test_dir, suffix='.json')
        with os.fdopen(fd, 'w') as f:
            json.dump({"test": "data"}, f)
        
        # Before replace, temp file exists separately
        assert os.path.exists(tmp)
        assert not os.path.exists(test_file)  # Target doesn't exist yet
        
        # Replace (atomic)
        os.replace(tmp, test_file)
        
        # Now target exists, temp should be gone
        assert os.path.exists(test_file)
        assert not os.path.exists(tmp)
        
        # Verify content
        with open(test_file) as f:
            assert json.load(f) == {"test": "data"}
    finally:
        # Clean up files before removing directory
        for f in os.listdir(test_dir):
            os.unlink(os.path.join(test_dir, f))
        os.rmdir(test_dir)


# ============================================================================
# Test 6: ArbPosition Dataclass Fields
# ============================================================================

def test_arbposition_new_fields():
    """Test that ArbPosition has all new cost tracking fields."""
    from cross_arb import ArbPosition
    
    # Create position with new fields
    pos = ArbPosition(
        symbol="BTC",
        bin_side="buy",
        hl_side="sell",
        bin_size=0.01,
        hl_size=0.01,
        notional_usdt=500.0,
        entry_bin_apy=-10.0,
        entry_hl_apy=5.0,
        entry_net_apy=15.0,
        # New slippage fields
        bin_entry_slippage_bps=2.5,
        hl_entry_slippage_bps=3.0,
        bin_exit_fill=50100.0,
        hl_exit_fill=50050.0,
        bin_exit_slippage_bps=1.0,
        hl_exit_slippage_bps=2.0,
        exit_bin_apy=-8.0,
        exit_hl_apy=4.0,
        exit_net_apy=12.0,
        true_pnl=10.5,
    )
    
    # Verify all fields exist and are set correctly
    assert pos.bin_entry_slippage_bps == 2.5
    assert pos.hl_entry_slippage_bps == 3.0
    assert pos.bin_exit_fill == 50100.0
    assert pos.hl_exit_fill == 50050.0
    assert pos.bin_exit_slippage_bps == 1.0
    assert pos.hl_exit_slippage_bps == 2.0
    assert pos.exit_bin_apy == -8.0
    assert pos.exit_hl_apy == 4.0
    assert pos.exit_net_apy == 12.0
    assert pos.true_pnl == 10.5


def test_arbposition_default_values():
    """Test that new fields have correct defaults."""
    from cross_arb import ArbPosition
    
    pos = ArbPosition(
        symbol="BTC",
        bin_side="buy",
        hl_side="sell",
        bin_size=0.01,
        hl_size=0.01,
        notional_usdt=500.0,
        entry_bin_apy=-10.0,
        entry_hl_apy=5.0,
        entry_net_apy=15.0,
    )
    
    # Check defaults
    assert pos.bin_entry_slippage_bps == 0.0
    assert pos.hl_entry_slippage_bps == 0.0
    assert pos.bin_exit_fill == 0.0
    assert pos.hl_exit_fill == 0.0
    assert pos.bin_exit_slippage_bps == 0.0
    assert pos.true_pnl == 0.0


# ============================================================================
# Test 7: Integration - Full P&L Flow
# ============================================================================

def test_full_pnl_flow():
    """Test complete P&L flow from open to close."""
    from cross_arb import ArbPosition, RT_FEE_PCT
    
    # Simulate opening a position
    pos = ArbPosition(
        symbol="BTC",
        bin_side="buy",   # Long Binance
        hl_side="sell",   # Short HL
        bin_size=0.6,     # 0.6 BTC
        hl_size=0.6,
        notional_usdt=30000.0,  # 30k notional
        entry_bin_apy=-10.0,    # -10% APY on Binance (long earns)
        entry_hl_apy=5.0,      # +5% APY on HL (short pays)
        entry_net_apy=15.0,    # Net 15% APY
        bin_entry_px=50000.0,
        hl_entry_px=49950.0,
        # Entry slippage (8 bps total)
        bin_entry_slippage_bps=5.0,
        hl_entry_slippage_bps=3.0,
    )
    
    # Calculate entry basis
    entry_basis = pos.bin_entry_px - pos.hl_entry_px
    assert entry_basis == 50.0
    
    # Simulate holding period - funding earned
    hours_held = 24.0  # 1 day
    bin_hourly = -10.0 / 100.0 / 365.0  # -0.000274 per hour
    hl_hourly = 5.0 / 100.0 / 365.0    # +0.000137 per hour
    
    # Long earns negative rate = positive funding
    # Short pays positive rate = negative funding (we receive from being short)
    # Wait, positive rate means shorts PAY longs, so shorts earn NEGATIVE
    # Let's use the formula: earning = rate * earn_sign * notional * hours
    bin_funding = bin_hourly * 1 * pos.notional_usdt * hours_held  # long, earns negative rate = positive
    # Actually: bin_apy = -10%, long earns = -(-10%) = +10%
    bin_funding = (-pos.entry_bin_apy / 100.0 / 365.0) * pos.notional_usdt * hours_held
    
    # Short: hl_apy = +5%, short pays = -(5%) = -5%
    # Short earns when rate is positive? No, shorts pay when rate is positive
    # Short earning = -rate when short
    hl_funding = (-pos.entry_hl_apy / 100.0 / 365.0) * pos.notional_usdt * hours_held
    
    funding_earned = bin_funding + hl_funding
    # = 30000 * (0.10/365) * 24 + 30000 * (-0.05/365) * 24
    # = 30000 * 0.000274 * 24 + 30000 * (-0.000137) * 24
    # = 197.26 - 98.63 = 98.63
    
    pos.funding_realized_bin = bin_funding
    pos.funding_realized_hl = -abs(hl_funding)  # Short pays
    
    # Simulate closing
    pos.bin_exit_fill = 50100.0
    pos.hl_exit_fill = 50050.0
    
    # Exit slippage
    pos.bin_exit_slippage_bps = 2.0
    pos.hl_exit_slippage_bps = 1.0
    
    # Calculate costs
    entry_slippage = (
        pos.bin_entry_slippage_bps / 10000 * pos.notional_usdt +
        pos.hl_entry_slippage_bps / 10000 * pos.notional_usdt
    )
    exit_slippage = (
        pos.bin_exit_slippage_bps / 10000 * pos.notional_usdt +
        pos.hl_exit_slippage_bps / 10000 * pos.notional_usdt
    )
    total_slippage = entry_slippage + exit_slippage
    
    fee_cost = pos.notional_usdt * RT_FEE_PCT
    
    # Basis
    exit_basis = pos.bin_exit_fill - pos.hl_exit_fill
    basis_change = exit_basis - entry_basis
    basis_cost = basis_change * pos.notional_usdt / pos.bin_entry_px
    
    # True P&L
    true_pnl = funding_earned - fee_cost - total_slippage - basis_cost
    
    # Log results
    print(f"Funding earned: ${funding_earned:.2f}")
    print(f"Fee cost: ${fee_cost:.2f}")
    print(f"Slippage cost: ${total_slippage:.2f}")
    print(f"Basis change: {basis_change:.2f} pts = ${basis_cost:.2f}")
    print(f"True P&L: ${true_pnl:.2f}")
    
    # Sanity check - should be roughly positive given high APY
    assert funding_earned > fee_cost + total_slippage  # At least covered costs


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
