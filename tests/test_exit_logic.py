"""
Simulation tests for cross-arb exit logic.

Tests the fee break-even gate under various funding rate scenarios
to ensure we don't churn positions at a loss.

Run: python -m pytest tests/test_exit_logic.py -v
"""
import pytest
import time
from dataclasses import dataclass, field
from typing import List, Tuple

# ── Constants (mirror cross_arb.py) ──────────────────────────────────────────

RT_FEE_PCT = 0.0017          # 0.17% round-trip
BIN_TAKER = 0.0005            # Binance taker fee per side
HL_TAKER = 0.00035            # HL taker fee per side
FUNDING_INTERVAL_H = 8        # Funding paid every 8 hours


@dataclass
class SimPosition:
    """Simulated arb position."""
    symbol: str
    notional: float
    bin_side: str  # "buy" or "sell"
    hl_side: str
    entry_ts: float = 0.0
    funding_realized_bin: float = 0.0
    funding_realized_hl: float = 0.0
    needs_close: bool = False
    _fee_gate_logged: bool = False

    @property
    def fee_cost(self) -> float:
        return self.notional * RT_FEE_PCT

    @property
    def total_funding(self) -> float:
        return self.funding_realized_bin + self.funding_realized_hl

    @property
    def net_pnl(self) -> float:
        return self.total_funding - self.fee_cost


def apy_to_hourly_rate(apy_pct: float, notional: float) -> float:
    """Convert APY% to $/hour for a given notional."""
    return notional * (apy_pct / 100) / 365 / 24


def accrue_funding(pos: SimPosition, bin_apy: float, hl_apy: float, hours: float) -> float:
    """
    Accrue funding for `hours` at given APY rates.
    Returns the funding earned this period.
    
    Convention: rate < 0 → longs pay shorts → LONG earns
                rate > 0 → shorts pay longs → SHORT earns
    """
    bin_earn_sign = 1 if pos.bin_side == "buy" else -1
    hl_earn_sign = 1 if pos.hl_side == "buy" else -1

    # Funding earned per hour
    bin_hourly = apy_to_hourly_rate(abs(bin_apy), pos.notional)
    hl_hourly = apy_to_hourly_rate(abs(hl_apy), pos.notional)

    # Direction: if rate is negative and we're long, we earn
    bin_direction = -1 * bin_earn_sign * (1 if bin_apy >= 0 else -1)
    hl_direction = -1 * hl_earn_sign * (1 if hl_apy >= 0 else -1)

    bin_earned = bin_hourly * bin_direction * hours
    hl_earned = hl_hourly * hl_direction * hours

    pos.funding_realized_bin += bin_earned
    pos.funding_realized_hl += hl_earned
    return bin_earned + hl_earned


def compute_live_net_apy(pos: SimPosition, bin_apy: float, hl_apy: float) -> float:
    """Compute live net APY (mirrors cross_arb.py logic)."""
    bin_earn_sign = 1 if pos.bin_side == "buy" else -1
    hl_earn_sign = 1 if pos.hl_side == "buy" else -1
    live_bin = bin_apy * bin_earn_sign * (-1)
    live_hl = hl_apy * hl_earn_sign * (-1)
    return live_bin + live_hl


def should_exit(
    pos: SimPosition,
    live_net_apy: float,
    hold_hours: float,
    exit_apy: float = 5.0,
    min_hold_hours: float = 8.0,
) -> Tuple[bool, str]:
    """
    Mirror the exit logic from cross_arb.py.
    Returns (should_exit, reason).
    """
    funding_earned = pos.total_funding
    fee_cost = pos.fee_cost
    fee_covered = funding_earned >= fee_cost
    emergency_negative = live_net_apy < -10

    if pos.needs_close:
        return True, "needs_close_flag"
    if emergency_negative:
        return True, f"emergency_negative (APY={live_net_apy:.1f}%)"
    if live_net_apy < exit_apy:
        if fee_covered:
            return True, f"apy_below_threshold_fees_covered (funding=${funding_earned:.4f} >= fees=${fee_cost:.4f})"
        if hold_hours >= min_hold_hours:
            return True, f"apy_below_threshold_min_hold_reached ({hold_hours:.1f}h >= {min_hold_hours}h)"
        return False, f"apy_below_threshold_but_holding (funding=${funding_earned:.4f} < fees=${fee_cost:.4f}, {hold_hours:.1f}h < {min_hold_hours}h)"
    return False, "apy_above_threshold"


class FundingSchedule:
    """
    Describes a funding rate schedule over time.
    List of (duration_hours, bin_apy, hl_apy) segments.
    """
    def __init__(self, segments: List[Tuple[float, float, float]]):
        self.segments = segments  # [(hours, bin_apy, hl_apy), ...]

    @property
    def total_hours(self) -> float:
        return sum(s[0] for s in self.segments)


def simulate(
    notional: float,
    schedule: FundingSchedule,
    bin_side: str = "buy",
    hl_side: str = "sell",
    exit_apy: float = 5.0,
    min_hold_hours: float = 8.0,
    check_interval_hours: float = 1.0,
) -> dict:
    """
    Simulate a position through a funding schedule.
    Returns detailed results.
    """
    pos = SimPosition(
        symbol="TEST",
        notional=notional,
        bin_side=bin_side,
        hl_side=hl_side,
        entry_ts=0.0,
    )

    current_hour = 0.0
    exit_hour = None
    exit_reason = None
    history = []  # (hour, funding_total, net_pnl, live_apy, exited)

    for seg_hours, bin_apy, hl_apy in schedule.segments:
        seg_end = current_hour + seg_hours
        while current_hour < seg_end:
            step = min(check_interval_hours, seg_end - current_hour)
            accrue_funding(pos, bin_apy, hl_apy, step)
            current_hour += step

            live_net = compute_live_net_apy(pos, bin_apy, hl_apy)
            do_exit, reason = should_exit(pos, live_net, current_hour, exit_apy, min_hold_hours)

            history.append({
                "hour": round(current_hour, 2),
                "funding_total": round(pos.total_funding, 6),
                "fee_cost": round(pos.fee_cost, 6),
                "net_pnl": round(pos.net_pnl, 6),
                "live_apy": round(live_net, 2),
                "would_exit": do_exit,
                "reason": reason,
            })

            if do_exit and exit_hour is None:
                exit_hour = current_hour
                exit_reason = reason
                # Don't break — continue tracking what happens if we held

    return {
        "notional": notional,
        "fee_cost": pos.fee_cost,
        "total_hours": schedule.total_hours,
        "exit_hour": exit_hour,
        "exit_reason": exit_reason,
        "final_funding": pos.total_funding,
        "final_net_pnl": pos.net_pnl,
        "history": history,
    }


# ══════════════════════════════════════════════════════════════════════════════
# TEST SCENARIOS
# ══════════════════════════════════════════════════════════════════════════════


class TestBasicFeeGate:
    """Test that the fee break-even gate prevents premature exits."""

    def test_high_apy_never_triggers_exit(self):
        """Position with consistently high APY should never exit."""
        schedule = FundingSchedule([(48, -30.0, 10.0)])  # 30+10=40% net
        result = simulate(500, schedule)
        assert result["exit_hour"] is None
        assert result["final_net_pnl"] > 0

    def test_immediate_apy_drop_blocks_exit_until_min_hold(self):
        """APY drops immediately but fee gate holds position."""
        schedule = FundingSchedule([
            (1, -30.0, 10.0),   # 1h at 40% (entry)
            (24, -2.0, 1.0),    # drops to 3% net — below exit threshold
        ])
        result = simulate(500, schedule, min_hold_hours=8.0)
        # Should NOT exit at hour 2 (below threshold but no fee coverage)
        assert result["exit_hour"] >= 8.0  # held until min_hold_hours

    def test_fee_covered_allows_exit(self):
        """Once funding covers fees, position can exit on low APY."""
        # Need enough time at high APY to cover $0.85 fees
        # At 50% APY on $500: $0.0285/hr → 30h to cover
        schedule = FundingSchedule([
            (35, -40.0, 10.0),  # 35h at 50% net → earns ~$1.00
            (5, -2.0, 1.0),     # drops to 3% → below threshold
        ])
        result = simulate(500, schedule, min_hold_hours=8.0)
        # Should exit once APY drops AND fees are covered (around hour 35-36)
        assert result["exit_hour"] is not None
        assert result["exit_hour"] >= 35
        assert "fees_covered" in result["exit_reason"]

    def test_emergency_exit_on_deeply_negative(self):
        """Deeply negative APY triggers immediate exit regardless of fees."""
        # bin_side=buy: live_bin = apy * 1 * -1 = -apy
        # hl_side=sell: live_hl = apy * -1 * -1 = apy
        # For net < -10: need -bin_apy + hl_apy < -10
        # So bin_apy=50 (live=-50), hl_apy=-50 (live=-50) → net=-100
        schedule = FundingSchedule([
            (1, -30.0, 10.0),    # 1h normal
            (1, 50.0, -50.0),    # both legs paying → -100% net
        ])
        result = simulate(500, schedule)
        assert result["exit_hour"] is not None
        assert "emergency" in result["exit_reason"]


class TestFundingRateScenarios:
    """Real-world funding rate scenarios."""

    def test_scenario_good_then_shit_then_worse(self):
        """
        Nic's exact scenario: good first, then shit, then worse.
        Without fee gate: would exit at hour 4 at a loss.
        With fee gate: holds to min_hold, limiting damage.
        """
        schedule = FundingSchedule([
            (4, -30.0, 10.0),    # 4h at 40% APY — looks great
            (4, -5.0, 2.0),      # 4h at 7% — shit, below 10% threshold
            (4, 5.0, -2.0),      # 4h at -7% — actively losing funding
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # Without fee gate, old bot would exit at ~hour 4 (first APY drop)
        # With fee gate, should hold until min_hold then exit
        assert result["exit_hour"] >= 8.0

        # After 4h at 40% APY: earned ~$0.023 * 4 = $0.091
        # Fees: $0.85. Deep underwater.
        # Key: at least we didn't pay ANOTHER $0.85 to re-enter something else
        four_hour_funding = result["history"][3]["funding_total"]
        assert four_hour_funding < result["fee_cost"]  # Not yet fee-covered

    def test_scenario_gradual_decay(self):
        """APY gradually decays from great to mediocre."""
        schedule = FundingSchedule([
            (8, -40.0, 10.0),    # 8h at 50% 
            (8, -20.0, 5.0),     # 8h at 25%
            (8, -8.0, 2.0),      # 8h at 10% — at exit threshold
            (8, -3.0, 1.0),      # 8h at 4% — below threshold
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # Should hold through the decay since it's earning the whole time
        # Only exits once it drops below threshold AND either fees covered or min hold
        assert result["exit_hour"] is not None
        assert result["exit_hour"] >= 24  # Held through first 3 segments
        assert result["final_funding"] > 0

    def test_scenario_spike_and_crash(self):
        """High APY spike followed by immediate crash — classic trap."""
        schedule = FundingSchedule([
            (1, -200.0, 50.0),   # 1h at 250% APY — spike!
            (23, 10.0, -5.0),    # 23h deeply negative (-15%)
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # Emergency exit should trigger on deeply negative
        assert result["exit_hour"] is not None
        assert result["exit_hour"] <= 2  # Exits early due to emergency
        assert "emergency" in result["exit_reason"]

    def test_scenario_oscillating_rates(self):
        """Rates oscillate — good, bad, good, bad. Tests churn resistance."""
        schedule = FundingSchedule([
            (2, -30.0, 10.0),    # 2h good (40%)
            (2, -3.0, 1.0),      # 2h bad (4%)
            (2, -25.0, 8.0),     # 2h good again (33%)
            (2, -2.0, 0.0),      # 2h bad again (2%)
            (2, -30.0, 10.0),    # 2h good again (40%)
            (2, -3.0, 1.0),      # 2h bad (4%)
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # OLD logic: would have exited at hour 2 and re-entered at hour 4,
        # paying $0.85 × 3 round trips = $2.55 in fees
        # NEW logic: should hold through oscillations
        # At hour 8 (min hold), it has accumulated funding from the good periods
        assert result["exit_hour"] is None or result["exit_hour"] >= 8.0

    def test_scenario_slow_bleed(self):
        """Rate slowly bleeds from OK to slightly below threshold."""
        schedule = FundingSchedule([
            (12, -15.0, 5.0),    # 12h at 20% — ok but not great
            (12, -8.0, 3.0),     # 12h at 11% — just above threshold
            (12, -6.0, 2.0),     # 12h at 8% — just below threshold
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # Should hold well into the third segment — earning the whole time
        # At 20% for 12h + 11% for 12h → good funding accumulation
        assert result["exit_hour"] >= 24
        # Exit reason: either fees covered or min hold reached (both are acceptable)
        assert "apy_below_threshold" in result["exit_reason"]

    def test_scenario_one_leg_flips(self):
        """One exchange's rate flips sign — arb disappears."""
        schedule = FundingSchedule([
            (4, -30.0, 10.0),    # 4h normal: bin negative (long earns), hl positive (short earns) = 40%
            (20, 10.0, 10.0),    # bin flips positive → long PAYS. net = -10 + (-10) = -20%? No...
        ])
        # When bin_side=buy and rate is positive: earn_sign=1, live = rate * 1 * -1 = -rate
        # When hl_side=sell and rate is positive: earn_sign=-1, live = rate * -1 * -1 = rate  
        # So net = -10 + 10 = 0% (legs cancel)
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)
        # Net APY is ~0% after flip — below threshold
        # Should hold until min_hold since not emergency negative
        assert result["exit_hour"] >= 8.0


class TestBreakEvenMath:
    """Verify break-even calculations match expectations."""

    def test_fee_cost_at_500_notional(self):
        """$500 × 0.17% = $0.85."""
        pos = SimPosition(symbol="TEST", notional=500, bin_side="buy", hl_side="sell")
        assert abs(pos.fee_cost - 0.85) < 0.01

    def test_fee_cost_at_1000_notional(self):
        """$1000 × 0.17% = $1.70."""
        pos = SimPosition(symbol="TEST", notional=1000, bin_side="buy", hl_side="sell")
        assert abs(pos.fee_cost - 1.70) < 0.01

    def test_breakeven_hours_30pct(self):
        """At 30% APY on $500, break-even ~50 hours."""
        hourly = apy_to_hourly_rate(30, 500)
        fee = 500 * RT_FEE_PCT
        be_hours = fee / hourly
        assert 45 < be_hours < 55  # ~50h

    def test_breakeven_hours_100pct(self):
        """At 100% APY on $500, break-even ~15 hours."""
        hourly = apy_to_hourly_rate(100, 500)
        fee = 500 * RT_FEE_PCT
        be_hours = fee / hourly
        assert 12 < be_hours < 18  # ~15h

    def test_never_profitable_at_5pct(self):
        """At 5% APY, even after 100h, barely profitable."""
        schedule = FundingSchedule([(100, -5.0, 0.0)])
        result = simulate(500, schedule)
        # Earns 5% APY on $500 for 100h = $0.0057/hr × 100 = $0.57
        # Fees = $0.85
        # Still net negative after 100h!
        assert result["final_net_pnl"] < 0


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_zero_apy_holds_to_min_hold(self):
        """Zero APY from start — holds to min hold then exits."""
        schedule = FundingSchedule([(12, 0.0, 0.0)])
        result = simulate(500, schedule, exit_apy=5.0, min_hold_hours=8.0)
        assert result["exit_hour"] >= 8.0
        assert result["exit_hour"] <= 9.0  # exits soon after min hold

    def test_negative_apy_emergency_exits(self):
        """Negative APY beyond -10% triggers emergency exit."""
        schedule = FundingSchedule([(1, 20.0, 20.0)])  # -20-20=-40% for long/sell
        # Actually need to think about signs...
        # bin_side=buy, bin_apy=20: live = 20 * 1 * -1 = -20
        # hl_side=sell, hl_apy=20: live = 20 * -1 * -1 = 20  
        # net = -20 + 20 = 0. Not emergency.
        # Let me make it clearly negative:
        schedule = FundingSchedule([(1, 50.0, -50.0)])
        # bin_side=buy, bin_apy=50: live = 50 * 1 * -1 = -50 (paying)
        # hl_side=sell, hl_apy=-50: live = -50 * -1 * -1 = -50 (paying)
        # net = -100%. Emergency!
        result = simulate(500, schedule, exit_apy=5.0, min_hold_hours=8.0)
        assert result["exit_hour"] is not None
        assert result["exit_hour"] <= 1.0
        assert "emergency" in result["exit_reason"]

    def test_exactly_at_exit_threshold(self):
        """APY exactly at exit threshold — should NOT exit."""
        schedule = FundingSchedule([(24, -8.0, 2.0)])  # 10% = exactly at threshold
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)
        # 10% is NOT < 10%, so should not trigger
        assert result["exit_hour"] is None

    def test_just_below_exit_threshold(self):
        """APY just below exit threshold — fee gate applies."""
        schedule = FundingSchedule([(24, -7.0, 2.0)])  # 9% — just below 10%
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)
        assert result["exit_hour"] >= 8.0  # held to min hold

    def test_large_position_higher_fees(self):
        """Larger positions have proportionally higher fees."""
        schedule = FundingSchedule([
            (4, -30.0, 10.0),
            (8, -2.0, 1.0),
        ])
        result_500 = simulate(500, schedule, min_hold_hours=8.0)
        result_1500 = simulate(1500, schedule, min_hold_hours=8.0)

        # Both should hold to min_hold since fees aren't covered
        assert result_500["exit_hour"] >= 8.0
        assert result_1500["exit_hour"] >= 8.0
        # But the $1500 position has 3× the fees to cover
        assert result_1500["fee_cost"] == pytest.approx(result_500["fee_cost"] * 3, rel=0.01)


class TestHistoricalReplay:
    """Replay scenarios based on actual observed behavior."""

    def test_replay_anime_churn(self):
        """
        ANIME was opened and closed 4 times in the history.
        Each time: ~$400-590 notional, held 0.2-2.6h, net loss every time.
        Total damage: ~$5.65 in fees for ~$0.74 in funding.
        """
        # Simulate: enter at 30% APY, drops quickly
        schedule = FundingSchedule([
            (0.5, -25.0, 5.0),   # 30min at 30%
            (2, -3.0, 1.0),      # drops to 4%
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # Old bot: would exit at 0.5h, pay $0.85 fees, earn ~$0.008
        # New bot: holds to 8h minimum
        assert result["exit_hour"] is None or result["exit_hour"] >= 8.0

    def test_replay_layer_negative_pnl(self):
        """
        LAYER: $469 notional, held 2.5h, realized_pnl=-$1.19, fees=$1.31
        Total loss: -$2.50
        """
        schedule = FundingSchedule([
            (1, -100.0, 20.0),   # enter on high spike
            (1.5, 10.0, -5.0),   # rate completely flips
        ])
        result = simulate(469, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # With emergency exit on deeply negative, should still catch this
        # But the net APY here is -10-(-5) = ... let me compute
        # bin_side=buy, bin_apy=10: live = 10 * 1 * -1 = -10
        # hl_side=sell, hl_apy=-5: live = -5 * -1 * -1 = -5
        # net = -15%. Emergency exit!
        assert result["exit_hour"] is not None
        assert "emergency" in result["exit_reason"]

    def test_replay_profitable_skr(self):
        """
        SKR: only profitable close. $425, held 14h, +$0.12 net.
        This is what a good position looks like.
        """
        schedule = FundingSchedule([
            (14, -20.0, 5.0),    # 14h at 25% APY
            (2, -3.0, 1.0),      # drops below threshold
        ])
        result = simulate(425, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # Should exit after APY drops below threshold
        assert result["exit_hour"] is not None
        assert result["exit_hour"] >= 14
        # At 25% APY for 14h on $425: earns ~$0.17. Fees=$0.72. 
        # Not fee-covered yet, but min_hold passed, so exits via min_hold gate.
        # The key insight: 25% APY on $425 needs ~42h to break even. 
        # SKR was only profitable because it held 14h AND had higher actual rates.
        assert "apy_below_threshold" in result["exit_reason"]


class TestMinHoldSensitivity:
    """Test different min_hold_hours values."""

    @pytest.mark.parametrize("min_hold", [4, 8, 12, 24])
    def test_min_hold_prevents_early_exit(self, min_hold):
        """Position with low APY respects min_hold."""
        schedule = FundingSchedule([(min_hold + 4, -3.0, 1.0)])  # 4% the whole time
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=min_hold)
        assert result["exit_hour"] >= min_hold
        assert result["exit_hour"] <= min_hold + 1  # exits at next check after min

    def test_8h_min_hold_vs_old_behavior(self):
        """
        Compare: old bot (no fee gate) vs new bot (8h min hold).
        Oscillating rates that would cause 4 round trips in old bot.
        """
        # Old bot: exit at 2h, re-enter at 4h, exit at 6h, re-enter at 8h
        # Total fees with old: 4 × $0.85 = $3.40
        # New bot: hold through to 8h minimum
        # Total fees with new: 1 × $0.85 = $0.85 (just the original entry)
        schedule = FundingSchedule([
            (2, -30.0, 10.0),    # good
            (2, -3.0, 1.0),      # bad
            (2, -30.0, 10.0),    # good
            (2, -3.0, 1.0),      # bad
        ])
        result = simulate(500, schedule, exit_apy=10.0, min_hold_hours=8.0)

        # New bot holds through — no premature exit
        assert result["exit_hour"] is None or result["exit_hour"] >= 8.0

        # Calculate what old bot would have lost
        old_bot_fees = 4 * 500 * RT_FEE_PCT  # 4 round trips
        new_bot_fees = 1 * 500 * RT_FEE_PCT  # 1 round trip
        savings = old_bot_fees - new_bot_fees
        assert savings > 2.0  # saved at least $2.55


class TestProfitabilityThresholds:
    """Test what APY/duration combos are actually profitable."""

    @pytest.mark.parametrize("apy,expected_profitable_after_hours", [
        (30, 50),    # 30% APY → profitable after ~50h
        (50, 30),    # 50% APY → profitable after ~30h
        (100, 15),   # 100% APY → profitable after ~15h
        (200, 8),    # 200% APY → profitable after ~8h
        (500, 3),    # 500% APY → profitable after ~3h
    ])
    def test_profitability_timeline(self, apy, expected_profitable_after_hours):
        """Verify at what point positions become profitable."""
        schedule = FundingSchedule([(expected_profitable_after_hours * 1.2, -(apy * 0.7), apy * 0.3)])
        result = simulate(500, schedule)

        # Find first profitable hour
        profitable_hour = None
        for h in result["history"]:
            if h["net_pnl"] > 0:
                profitable_hour = h["hour"]
                break

        assert profitable_hour is not None, f"Never profitable at {apy}% APY"
        assert profitable_hour <= expected_profitable_after_hours * 1.3  # within 30% of expected


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
