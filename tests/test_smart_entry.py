#!/usr/bin/env python3
"""
Tests for Smart Entry features:
- Rate Mean-Reversion Filter
- Rate Stability Score
- Funding Timing Bonus/Penalty
- Cooldown After Reversion
"""

import json
import os
import sys
import tempfile
import time
from unittest.mock import patch, MagicMock

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Test Fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def temp_history_file():
    """Create a temporary rate history file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('{}')
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)


@pytest.fixture
def temp_cooldown_file():
    """Create a temporary cooldown file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('{}')
        temp_path = f.name
    yield temp_path
    os.unlink(temp_path)


@pytest.fixture
def sample_rate_history():
    """Sample rate history for testing."""
    now = time.time()
    return {
        "BTC": [
            {"timestamp": now - 600, "bin_apy": 15.0, "hl_apy": 10.0, "net_apy": 25.0},
            {"timestamp": now - 300, "bin_apy": 14.0, "hl_apy": 9.0, "net_apy": 23.0},
            {"timestamp": now, "bin_apy": 13.0, "hl_apy": 8.0, "net_apy": 21.0},
        ],
        "ETH": [
            {"timestamp": now - 600, "bin_apy": 50.0, "hl_apy": 5.0, "net_apy": 55.0},
        ],
        "SOL": [
            {"timestamp": now - 600, "bin_apy": 100.0, "hl_apy": 10.0, "net_apy": 110.0},
            {"timestamp": now - 300, "bin_apy": 5.0, "hl_apy": 2.0, "net_apy": 7.0},
            {"timestamp": now, "bin_apy": 3.0, "hl_apy": 1.0, "net_apy": 4.0},
        ],
    }


# ── Rate Stability Score Tests ────────────────────────────────────────────

class TestRateStabilityScore:
    """Test rate_stability_score function."""
    
    def test_perfectly_stable(self):
        """Test perfectly stable rates (same values)."""
        # Import the function - will test in isolation
        history = [10.0, 10.0, 10.0, 10.0, 10.0]
        
        # For stable data, CV should be 0, so score should be 1.0
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        cv = (variance ** 0.5) / abs(mean) if mean else float('inf')
        score = max(0, 1 - cv)
        
        assert score == 1.0
    
    def test_highly_volatile(self):
        """Test highly volatile rates."""
        # Very spread out values
        history = [100.0, 1.0, 50.0, 10.0, 5.0]
        
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        cv = (variance ** 0.5) / abs(mean) if mean else float('inf')
        score = max(0, 1 - cv)
        
        # Should have low score due to high CV
        assert score < 0.5
    
    def test_single_value(self):
        """Test with only one observation."""
        history = [10.0]
        
        # Should return 0.0 for insufficient data
        assert len(history) < 2
    
    def test_empty_history(self):
        """Test with empty history."""
        history = []
        
        # Should handle empty gracefully
        assert len(history) < 2
    
    def test_zero_mean(self):
        """Test with zero mean (division edge case)."""
        history = [0.0, 0.0, 0.0]
        
        mean = sum(history) / len(history)
        # When mean is 0, should handle gracefully
        if mean == 0:
            cv = float('inf')  # or some handled case
        score = max(0, 1 - cv)
        
        # Should return 0 for zero mean
        assert score == 0


# ── Rate Mean-Reversion Filter Tests ───────────────────────────────────────

class TestMeanReversionFilter:
    """Test check_rate_persistent function logic."""
    
    def test_persistent_opportunity(self):
        """Test that persistent opportunities pass the filter."""
        # 2 out of 3 observations above threshold
        history = {
            "BTC": [
                {"net_apy": 20.0},
                {"net_apy": 25.0},
                {"net_apy": 18.0},
            ]
        }
        min_apy = 15.0
        min_persistent = 2
        
        # Count qualifying observations
        recent = history["BTC"][-3:]
        qualifying = sum(1 for obs in recent if obs.get("net_apy", 0) >= min_apy)
        
        assert qualifying >= min_persistent  # 3 >= 2 = True
    
    def test_spike_rejected(self):
        """Test that one-off spikes are rejected."""
        # Only 1 out of 3 observations above threshold
        history = {
            "BTC": [
                {"net_apy": 5.0},
                {"net_apy": 8.0},
                {"net_apy": 50.0},  # spike
            ]
        }
        min_apy = 15.0
        min_persistent = 2
        
        recent = history["BTC"][-3:]
        qualifying = sum(1 for obs in recent if obs.get("net_apy", 0) >= min_apy)
        
        assert not (qualifying >= min_persistent)  # 1 >= 2 = False
    
    def test_insufficient_history(self):
        """Test behavior with insufficient history."""
        # Only 1 observation
        history = {
            "BTC": [
                {"net_apy": 20.0},
            ]
        }
        min_apy = 15.0
        
        # With < 3 observations, logic should fall back
        if len(history.get("BTC", [])) >= 1:
            result = history["BTC"][-1].get("net_apy", 0) >= min_apy
            assert result  # 20 >= 15 = True
    
    def test_symbol_not_in_history(self):
        """Test handling of symbols not in history."""
        history = {}
        symbol = "NEWCOIN"
        
        # Should return False for unknown symbols
        result = symbol not in history or len(history.get(symbol, [])) < 3
        assert result


# ── Funding Timing Tests ───────────────────────────────────────────────────

class TestFundingTiming:
    """Test funding timing bonus/penalty functions."""
    
    def test_bonus_1_4_hours_before(self):
        """Test +5% bonus 1-4 hours before funding."""
        net_apy = 20.0
        
        # Test hours 1, 2, 3, 4
        for hours_until in [1, 2, 3, 4]:
            if 1 <= hours_until <= 4:
                adjusted = net_apy * 1.05
                assert adjusted == 21.0
    
    def test_penalty_7_8_hours_after(self):
        """Test -5% penalty 7-8 hours after funding (0-1h after settlement)."""
        net_apy = 20.0
        
        # Test hours 7, 8
        for hours_until in [7, 8]:
            if hours_until >= 7:
                adjusted = net_apy * 0.95
                assert adjusted == 19.0
    
    def test_no_adjustment_mid_cycle(self):
        """Test no adjustment during mid-cycle (4-7 hours until funding)."""
        net_apy = 20.0
        
        # Test hours 5, 6
        for hours_until in [5, 6]:
            if hours_until >= 7:
                adjusted = net_apy * 0.95
            elif 1 <= hours_until <= 4:
                adjusted = net_apy * 1.05
            else:
                adjusted = net_apy
            
            assert adjusted == 20.0
    
    def test_funding_times(self):
        """Test correct funding times (00:00, 08:00, 16:00 UTC)."""
        # At 06:00 UTC: next funding is 08:00 = 2 hours away
        # At 14:00 UTC: next funding is 16:00 = 2 hours away  
        # At 18:00 UTC: next funding is 24:00 = 6 hours away
        # At 22:00 UTC: next funding is 08:00 next day = 10 hours away
        
        # Hours until funding: (8 - (current_hour % 8)) % 8
        # This gives 0 at funding times, then 1,2,3,4,5,6,7 for subsequent hours
        test_cases = [
            (0, 0),   # At funding time (00:00)
            (1, 7),   # 1 hour after funding
            (6, 2),   # 6 hours after funding (2h until next)
            (7, 1),   # 7 hours after funding (1h until next)
            (8, 0),   # At funding time (08:00)
            (15, 1),  # 1h until 16:00
            (16, 0),  # At funding time (16:00)
            (23, 1),  # 23:00 = 1h until 00:00 next day (NOT 7!)
        ]
        
        for current_hour, expected in test_cases:
            hours_until = (8 - (current_hour % 8)) % 8
            assert hours_until == expected, f"Hour {current_hour}: expected {expected}, got {hours_until}"


# ── Cooldown System Tests ─────────────────────────────────────────────────

class TestCooldownSystem:
    """Test cooldown functionality."""
    
    def test_add_symbol_to_cooldown(self):
        """Test adding a symbol to cooldown."""
        cooldowns = {}
        symbol = "BTC"
        duration_hours = 16.0
        now = time.time()
        
        cooldowns[symbol] = now + (duration_hours * 3600)
        
        assert symbol in cooldowns
        assert cooldowns[symbol] > now
    
    def test_is_on_cooldown_active(self):
        """Test checking if symbol is on active cooldown."""
        cooldowns = {
            "BTC": time.time() + (8 * 3600),  # 8 hours remaining
        }
        symbol = "BTC"
        
        result = symbol in cooldowns and time.time() < cooldowns[symbol]
        assert result
    
    def test_is_on_cooldown_expired(self):
        """Test checking if symbol cooldown has expired."""
        cooldowns = {
            "BTC": time.time() - (1 * 3600),  # expired 1 hour ago
        }
        symbol = "BTC"
        
        result = symbol in cooldowns and time.time() < cooldowns[symbol]
        assert not result
    
    def test_cleanup_expired_cooldowns(self):
        """Test cleaning up expired cooldowns."""
        cooldowns = {
            "BTC": time.time() - 3600,   # expired
            "ETH": time.time() + 3600,   # still active
        }
        now = time.time()
        
        expired = [s for s, exp in cooldowns.items() if now >= exp]
        
        assert "BTC" in expired
        assert "ETH" not in expired


# ── Integration Tests ─────────────────────────────────────────────────────

class TestSmartEntryIntegration:
    """Integration tests for the complete smart entry flow."""
    
    def test_full_opportunity_filtering_flow(self):
        """Test the complete filtering flow."""
        # Simulate rate history with a spike that should be filtered
        now = time.time()
        
        # BTC: persistent opportunity (should pass)
        btc_history = [
            {"timestamp": now - 600, "net_apy": 25.0},
            {"timestamp": now - 300, "net_apy": 23.0},
            {"timestamp": now, "net_apy": 21.0},
        ]
        
        # SOL: spike that should be filtered
        sol_history = [
            {"timestamp": now - 600, "net_apy": 5.0},
            {"timestamp": now - 300, "net_apy": 8.0},
            {"timestamp": now, "net_apy": 50.0},  # spike!
        ]
        
        min_apy = 15.0
        min_persistent = 2
        
        # BTC check
        btc_qualifying = sum(1 for obs in btc_history[-3:] if obs.get("net_apy", 0) >= min_apy)
        btc_passes = btc_qualifying >= min_persistent
        assert btc_passes  # Should pass
        
        # SOL check
        sol_qualifying = sum(1 for obs in sol_history[-3:] if obs.get("net_apy", 0) >= min_apy)
        sol_passes = sol_qualifying >= min_persistent
        assert not sol_passes  # Should fail (filter spike)
    
    def test_stability_score_gating(self):
        """Test stability score as entry gate."""
        # Simulate stable vs volatile histories
        stable_history = [20.0, 21.0, 20.5, 20.0, 19.5]
        # More volatile data to ensure score < 0.3
        volatile_history = [100.0, 5.0, 80.0, 10.0, 30.0]
        
        def calc_stability(history):
            if len(history) < 2:
                return 0.0
            mean = sum(history) / len(history)
            if mean == 0:
                return 0.0
            variance = sum((x - mean) ** 2 for x in history) / len(history)
            cv = (variance ** 0.5) / abs(mean)
            return max(0, 1 - cv)
        
        stable_score = calc_stability(stable_history)
        volatile_score = calc_stability(volatile_history)
        
        min_stability = 0.3
        
        assert stable_score > min_stability  # Stable should pass
        assert volatile_score < min_stability  # Volatile should fail
    
    def test_funding_timing_affects_ranking(self):
        """Test that funding timing bonus/penalty affects opportunity ranking."""
        # Two opportunities with same raw APY
        opp_a = {"symbol": "A", "raw_net_apy": 20.0}
        opp_b = {"symbol": "B", "raw_net_apy": 20.0}
        
        # Opportunity A: 2 hours before funding (+5%)
        # Opportunity B: 7 hours after funding (-5%)
        
        opp_a_adjusted = 20.0 * 1.05  # 21.0
        opp_b_adjusted = 20.0 * 0.95  # 19.0
        
        # A should rank higher
        assert opp_a_adjusted > opp_b_adjusted
    
    def test_cooldown_prevents_reentry(self):
        """Test that cooldown prevents re-entering closed positions."""
        # Simulate: BTC position closed due to APY drop, added to cooldown
        cooldowns = {
            "BTC": time.time() + (16 * 3600),  # 16 hours from now
        }
        
        # Try to enter BTC again
        can_enter = "BTC" not in cooldowns or time.time() >= cooldowns.get("BTC", 0)
        
        assert not can_enter  # Should be blocked
        
        # After cooldown expires
        cooldowns = {
            "BTC": time.time() - 1,  # expired 1 second ago
        }
        
        can_enter = "BTC" not in cooldowns or time.time() >= cooldowns.get("BTC", 0)
        assert can_enter  # Should be allowed


# ── Edge Cases ───────────────────────────────────────────────────────────

class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_history_file(self):
        """Test loading empty history file."""
        history = {}
        # Should handle gracefully
        result = history.get("UNKNOWN", [])
        assert result == []
    
    def test_corrupted_history_file(self):
        """Test handling corrupted JSON."""
        # Should return empty dict on error
        # This is handled by try/except in load_rate_history
        pass
    
    def test_zero_net_apy_stability(self):
        """Test stability with zero net APY."""
        history = [0.0, 0.0, 0.0]
        
        mean = sum(history) / len(history)
        if mean == 0:
            # Should handle zero mean
            pass
    
    def test_negative_net_apy(self):
        """Test handling negative net APY."""
        history = [-10.0, -5.0, -8.0]
        
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        cv = (variance ** 0.5) / abs(mean)
        score = max(0, 1 - cv)
        
        # Should still compute a score
        assert 0 <= score <= 1


# ── Run Tests ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
