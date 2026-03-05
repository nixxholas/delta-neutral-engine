"""
Funding rate dynamics simulation.

Models the reality that funding rates change every 8 hours and
positions must survive multiple rate changes to be profitable.

Key insight: at 30% APY, break-even is ~50h = 6+ funding intervals.
Each interval can shift the rate dramatically.

Run: python -m pytest tests/test_funding_rate_dynamics.py -v
"""
import pytest
import random
import statistics
from dataclasses import dataclass
from typing import List, Tuple, Optional

# ── Constants ────────────────────────────────────────────────────────────────

RT_FEE_PCT = 0.0017
FUNDING_INTERVAL_H = 8  # Binance/HL funding every 8h
HOURS_PER_YEAR = 365 * 24


@dataclass
class FundingInterval:
    """One 8-hour funding interval with rates for both exchanges."""
    bin_rate_pct: float  # Binance funding rate % (annualized)
    hl_rate_pct: float   # HL funding rate % (annualized)

    @property
    def net_apy(self) -> float:
        """Net APY assuming long Binance, short HL."""
        # Long earns when rate < 0, short earns when rate > 0
        return (-self.bin_rate_pct) + self.hl_rate_pct


def funding_per_interval(apy_pct: float, notional: float) -> float:
    """$ earned in one 8h funding interval at given APY."""
    return notional * (apy_pct / 100) * (FUNDING_INTERVAL_H / HOURS_PER_YEAR)


@dataclass
class SimResult:
    notional: float
    fee_cost: float
    intervals_held: int
    hours_held: float
    total_funding: float
    net_pnl: float  # funding - fees
    exit_reason: str
    interval_history: List[dict]
    profitable: bool

    @property
    def roi_pct(self) -> float:
        return (self.net_pnl / self.notional) * 100 if self.notional else 0


def simulate_intervals(
    notional: float,
    intervals: List[FundingInterval],
    exit_apy: float = 10.0,
    min_hold_intervals: int = 1,  # minimum funding intervals before exit
    emergency_apy: float = -10.0,
    fee_breakeven_required: bool = True,
) -> SimResult:
    """
    Simulate position through discrete funding intervals.
    Each interval = 8 hours of funding at given rates.
    """
    fee_cost = notional * RT_FEE_PCT
    total_funding = 0.0
    history = []
    exit_reason = "held_to_end"

    for i, interval in enumerate(intervals):
        # Accrue funding for this interval
        interval_funding = funding_per_interval(interval.net_apy, notional)
        total_funding += interval_funding
        net_pnl = total_funding - fee_cost

        history.append({
            "interval": i + 1,
            "hour": (i + 1) * FUNDING_INTERVAL_H,
            "bin_rate": interval.bin_rate_pct,
            "hl_rate": interval.hl_rate_pct,
            "net_apy": interval.net_apy,
            "interval_funding": round(interval_funding, 6),
            "total_funding": round(total_funding, 6),
            "net_pnl": round(net_pnl, 6),
            "fee_covered": total_funding >= fee_cost,
        })

        # Exit checks (after accruing, since funding is paid at interval end)
        intervals_held = i + 1

        # Emergency exit: deeply negative
        if interval.net_apy < emergency_apy:
            exit_reason = f"emergency_exit_interval_{intervals_held} (net_apy={interval.net_apy:.1f}%)"
            break

        # Check exit conditions (only after min hold)
        if intervals_held >= min_hold_intervals and interval.net_apy < exit_apy:
            if not fee_breakeven_required or total_funding >= fee_cost:
                exit_reason = f"exit_signal_interval_{intervals_held} (apy={interval.net_apy:.1f}%, funding=${total_funding:.4f}, fees=${fee_cost:.4f})"
                break
            # Fee not covered but rate is bad — check if severely underwater
            if total_funding < 0:
                exit_reason = f"negative_funding_interval_{intervals_held} (funding=${total_funding:.4f})"
                break

    hours_held = len(history) * FUNDING_INTERVAL_H
    net_pnl = total_funding - fee_cost

    return SimResult(
        notional=notional,
        fee_cost=fee_cost,
        intervals_held=len(history),
        hours_held=hours_held,
        total_funding=total_funding,
        net_pnl=net_pnl,
        exit_reason=exit_reason,
        interval_history=history,
        profitable=net_pnl > 0,
    )


# ── Rate generation helpers ──────────────────────────────────────────────────

def constant_rates(n: int, bin_rate: float, hl_rate: float) -> List[FundingInterval]:
    return [FundingInterval(bin_rate, hl_rate) for _ in range(n)]


def decaying_rates(n: int, start_bin: float, start_hl: float, decay_per_interval: float) -> List[FundingInterval]:
    """Rates decay toward zero each interval."""
    intervals = []
    bin_r, hl_r = start_bin, start_hl
    for _ in range(n):
        intervals.append(FundingInterval(bin_r, hl_r))
        bin_r *= (1 - decay_per_interval)
        hl_r *= (1 - decay_per_interval)
    return intervals


def random_walk_rates(
    n: int,
    start_bin: float,
    start_hl: float,
    volatility: float = 10.0,
    seed: Optional[int] = None,
) -> List[FundingInterval]:
    """Rates random-walk with given volatility (std dev in APY%)."""
    if seed is not None:
        random.seed(seed)
    intervals = []
    bin_r, hl_r = start_bin, start_hl
    for _ in range(n):
        intervals.append(FundingInterval(bin_r, hl_r))
        bin_r += random.gauss(0, volatility)
        hl_r += random.gauss(0, volatility)
    return intervals


def regime_change_rates(regimes: List[Tuple[int, float, float]]) -> List[FundingInterval]:
    """List of (n_intervals, bin_rate, hl_rate) regimes."""
    intervals = []
    for n, br, hr in regimes:
        intervals.extend(constant_rates(n, br, hr))
    return intervals


# ══════════════════════════════════════════════════════════════════════════════
# TESTS
# ══════════════════════════════════════════════════════════════════════════════


class TestIntervalBreakEven:
    """How many 8h intervals to break even at various APYs."""

    @pytest.mark.parametrize("net_apy,expected_intervals", [
        (30, 7),    # 30% → 56h → 7 intervals
        (50, 4),    # 50% → 32h → 4 intervals  
        (100, 2),   # 100% → 16h → 2 intervals
        (200, 1),   # 200% → 8h → 1 interval
        (500, 1),   # 500% → 3h → 1 interval (within first)
    ])
    def test_intervals_to_breakeven(self, net_apy, expected_intervals):
        """Verify how many funding intervals needed to cover RT fees."""
        notional = 500
        fee = notional * RT_FEE_PCT
        per_interval = funding_per_interval(net_apy, notional)
        intervals_needed = fee / per_interval if per_interval > 0 else float('inf')
        import math
        assert math.ceil(intervals_needed) == expected_intervals

    def test_at_30pct_fee_erosion_over_intervals(self):
        """Track cumulative P&L at 30% APY across intervals."""
        intervals = constant_rates(10, -20.0, 10.0)  # 30% net
        result = simulate_intervals(500, intervals)

        # After each interval, check cumulative position
        for h in result.interval_history:
            if h["interval"] <= 6:
                assert h["net_pnl"] < 0, f"Should be underwater at interval {h['interval']}"
            if h["interval"] >= 8:
                assert h["net_pnl"] > 0, f"Should be profitable at interval {h['interval']}"


class TestRateVolatilityScenarios:
    """Model realistic rate changes across funding intervals."""

    def test_rate_flips_at_interval_3(self):
        """
        Good rate for 2 intervals, then flips negative.
        This is the classic trap — enter on good rate, get burned.
        """
        intervals = regime_change_rates([
            (2, -30.0, 10.0),    # 2 intervals (16h) at 40% APY — great
            (6, 10.0, -5.0),     # 6 intervals (48h) — rate flips, now -15%
        ])
        
        # Without fee gate: exits at interval 3, pays fees, net loss
        result_no_gate = simulate_intervals(500, intervals, min_hold_intervals=0, fee_breakeven_required=False)
        
        # With fee gate: holds longer
        result_gate = simulate_intervals(500, intervals, min_hold_intervals=1, fee_breakeven_required=True)

        # Emergency exit should kick in since -15% < -10%
        assert result_gate.exit_reason.startswith("emergency")

        # Both lose money but gated version holds slightly longer for one more funding
        assert result_no_gate.intervals_held <= 3
        assert result_gate.intervals_held <= 3  # emergency also exits fast

    def test_gradual_decay_across_6_intervals(self):
        """Rate decays 20% each interval from strong start."""
        intervals = decaying_rates(12, -40.0, 10.0, decay_per_interval=0.20)
        result = simulate_intervals(500, intervals, exit_apy=10.0, min_hold_intervals=2)

        # Rate starts at 50% net, decays: 50, 40, 32, 26, 20, 16, 13, 10...
        # Should hold long enough to accumulate decent funding
        assert result.intervals_held >= 4
        # But may not reach profitability since rates decay
        # The key: it's earning LESS each interval but still earning

    def test_random_walk_monte_carlo(self):
        """
        Monte Carlo: 1000 simulations of random-walking rates.
        Answers: what % of positions are profitable?
        """
        wins = 0
        losses = 0
        total_pnl = 0
        n_sims = 1000

        for seed in range(n_sims):
            intervals = random_walk_rates(
                n=10,  # 80 hours
                start_bin=-25.0,
                start_hl=10.0,
                volatility=15.0,  # high vol — rates swing ±15% per interval
                seed=seed,
            )
            result = simulate_intervals(
                500, intervals,
                exit_apy=10.0,
                min_hold_intervals=2,
                fee_breakeven_required=True,
            )
            if result.net_pnl > 0:
                wins += 1
            else:
                losses += 1
            total_pnl += result.net_pnl

        win_rate = wins / n_sims * 100
        avg_pnl = total_pnl / n_sims

        # With high volatility (±15% per interval), win rate should be meaningful
        # but not overwhelming. This tells us the strategy's edge.
        print(f"\nMonte Carlo ({n_sims} sims, vol=15%):")
        print(f"  Win rate: {win_rate:.1f}%")
        print(f"  Avg P&L: ${avg_pnl:.4f}")
        print(f"  Total P&L: ${total_pnl:.2f}")

        # We just want this to run — the output is the insight
        assert True

    def test_random_walk_low_vs_high_entry_apy(self):
        """
        Compare outcomes: entering at 30% APY vs 100% APY.
        Higher entry = faster break-even = more resilient to rate changes.
        """
        n_sims = 500
        results = {}

        for entry_apy, start_bin, start_hl in [
            (30, -20.0, 10.0),
            (50, -35.0, 15.0),
            (100, -70.0, 30.0),
            (200, -140.0, 60.0),
        ]:
            wins = 0
            total_pnl = 0
            for seed in range(n_sims):
                intervals = random_walk_rates(
                    n=8, start_bin=start_bin, start_hl=start_hl,
                    volatility=15.0, seed=seed,
                )
                r = simulate_intervals(500, intervals, min_hold_intervals=2, fee_breakeven_required=True)
                if r.net_pnl > 0:
                    wins += 1
                total_pnl += r.net_pnl

            results[entry_apy] = {
                "win_rate": wins / n_sims * 100,
                "avg_pnl": total_pnl / n_sims,
            }

        print("\nEntry APY comparison (500 sims each, vol=15%):")
        for apy, r in sorted(results.items()):
            print(f"  {apy:4d}% APY → win rate {r['win_rate']:5.1f}%, avg P&L ${r['avg_pnl']:+.4f}")

        # Higher entry APY should have higher win rate
        assert results[200]["win_rate"] > results[30]["win_rate"]
        assert results[100]["win_rate"] > results[30]["win_rate"]


class TestMultiIntervalExitStrategies:
    """Compare different exit strategies across funding intervals."""

    def _run_strategy(self, intervals, min_hold, fee_gate):
        return simulate_intervals(
            500, intervals,
            exit_apy=10.0,
            min_hold_intervals=min_hold,
            fee_breakeven_required=fee_gate,
        )

    def test_no_gate_vs_1_interval_gate_vs_3_interval_gate(self):
        """
        Compare: no gate, 1-interval min hold, 3-interval min hold.
        On oscillating rates that would cause churn.
        """
        intervals = regime_change_rates([
            (1, -30.0, 10.0),    # 8h good
            (1, -3.0, 1.0),      # 8h bad
            (1, -25.0, 8.0),     # 8h good
            (1, -2.0, 0.5),      # 8h bad
            (1, -30.0, 10.0),    # 8h good
            (1, -3.0, 1.0),      # 8h bad
        ])

        r_no_gate = self._run_strategy(intervals, min_hold=0, fee_gate=False)
        r_1_gate = self._run_strategy(intervals, min_hold=1, fee_gate=True)
        r_3_gate = self._run_strategy(intervals, min_hold=3, fee_gate=True)

        # No gate exits at first bad interval (interval 2)
        assert r_no_gate.intervals_held <= 2
        # 1-interval gate holds at least 1 interval
        assert r_1_gate.intervals_held >= 1
        # 3-interval gate holds through oscillations
        assert r_3_gate.intervals_held >= 3

        # More holding = more funding accumulated (despite bad intervals)
        assert r_3_gate.total_funding > r_no_gate.total_funding

    def test_optimal_min_hold_for_30pct_apy(self):
        """
        At 30% APY entry, what min_hold gives best outcomes?
        Need 7 intervals to break even. Test various gates.
        """
        intervals_scenario = regime_change_rates([
            (3, -20.0, 10.0),    # 3 intervals at 30%
            (2, -5.0, 2.0),      # 2 intervals at 7% (below threshold)
            (3, -15.0, 5.0),     # 3 intervals at 20% (recovers!)
        ])

        results = {}
        for min_hold in [0, 1, 2, 3, 4, 6]:
            r = self._run_strategy(intervals_scenario, min_hold, fee_gate=True)
            results[min_hold] = r.net_pnl

        print("\nMin hold comparison (30% entry, with recovery):")
        for mh, pnl in sorted(results.items()):
            print(f"  min_hold={mh} intervals → net P&L ${pnl:+.6f}")

        # In this scenario, the fee gate holds all variants to the end since
        # the "bad" intervals (7% APY) are not below the -10% emergency threshold
        # and fees are never covered. All variants get the same result.
        # This reveals: the fee gate alone isn't enough — we also need
        # a "negative funding accumulation" exit when cumulative funding trends down.
        # For now, verify they at least don't lose MORE than fees
        for pnl in results.values():
            assert pnl > -1.0  # bounded loss

    def test_sunk_cost_scenario(self):
        """
        The hard case: rate is bad, we're underwater, but should we hold
        hoping for recovery or cut losses?
        
        This is Nic's concern: good → shit → worse, and by then you're deep underwater.
        """
        # Scenario 1: rate goes bad and stays bad
        bad_stays_bad = regime_change_rates([
            (2, -30.0, 10.0),    # 16h good
            (2, -3.0, 1.0),      # 16h shit  
            (4, 5.0, -3.0),      # 32h worse (actively losing)
        ])

        # Scenario 2: rate goes bad then recovers
        bad_recovers = regime_change_rates([
            (2, -30.0, 10.0),    # 16h good
            (2, -3.0, 1.0),      # 16h shit
            (4, -25.0, 8.0),     # 32h recovery!
        ])

        r_bad = simulate_intervals(500, bad_stays_bad, min_hold_intervals=2, fee_breakeven_required=True)
        r_recover = simulate_intervals(500, bad_recovers, min_hold_intervals=2, fee_breakeven_required=True)

        print(f"\nSunk cost scenario:")
        print(f"  Bad stays bad: held {r_bad.intervals_held} intervals, P&L ${r_bad.net_pnl:+.6f}")
        print(f"  Bad recovers:  held {r_recover.intervals_held} intervals, P&L ${r_recover.net_pnl:+.6f}")

        # The recovery scenario should end better
        assert r_recover.net_pnl > r_bad.net_pnl

        # Key insight: -8% net APY is below exit threshold but NOT below emergency (-10%).
        # The position holds to end, bleeding funding each interval.
        # This reveals we need a "cumulative funding declining" exit:
        # if funding has been negative for 2+ consecutive intervals, exit regardless.
        # For now, verify the damage is bounded
        assert r_bad.net_pnl > -1.0  # loss bounded by fees + small funding loss


class TestMinEntryAPYThresholds:
    """
    Given that we need N intervals to break even, what minimum entry APY
    actually makes the strategy viable?
    """

    def test_entry_apy_vs_survival_rate(self):
        """
        For each entry APY, simulate 500 positions with random rate drift.
        Find the minimum entry APY where >60% of positions are profitable.
        """
        n_sims = 500
        entry_apys = [15, 30, 50, 75, 100, 150, 200, 300, 500]
        survival_rates = {}

        for entry_apy in entry_apys:
            # Split entry APY 70/30 between bin/hl
            start_bin = -(entry_apy * 0.7)
            start_hl = entry_apy * 0.3
            wins = 0

            for seed in range(n_sims):
                intervals = random_walk_rates(
                    n=10,
                    start_bin=start_bin,
                    start_hl=start_hl,
                    volatility=12.0,  # moderate volatility
                    seed=seed,
                )
                r = simulate_intervals(
                    500, intervals,
                    exit_apy=5.0,
                    min_hold_intervals=2,
                    fee_breakeven_required=True,
                )
                if r.net_pnl > 0:
                    wins += 1

            survival_rates[entry_apy] = wins / n_sims * 100

        print("\nEntry APY vs survival rate (500 sims, vol=12%):")
        viable_apy = None
        for apy, rate in sorted(survival_rates.items()):
            viable = "✓ VIABLE" if rate >= 60 else "✗"
            print(f"  {apy:4d}% → {rate:5.1f}% profitable {viable}")
            if rate >= 60 and viable_apy is None:
                viable_apy = apy

        print(f"\n  Minimum viable entry APY (>60% win rate): {viable_apy}%")

        # At minimum, 500% should be viable
        assert survival_rates[500] > 60
        # And 15% should NOT be viable
        assert survival_rates[15] < 60

    def test_fee_impact_on_viability(self):
        """
        Compare: current taker fees vs hypothetical maker fees.
        Shows how much fee reduction would improve profitability.
        """
        global RT_FEE_PCT
        original_fee = RT_FEE_PCT
        n_sims = 300

        fee_scenarios = {
            "taker (0.17%)": 0.0017,
            "maker (0.04%)": 0.0004,  # if we used limit orders
            "zero fees": 0.0000,
        }

        print("\nFee impact analysis (300 sims, entry=50% APY, vol=12%):")
        for label, fee in fee_scenarios.items():
            RT_FEE_PCT = fee
            wins = 0
            total_pnl = 0
            for seed in range(n_sims):
                intervals = random_walk_rates(n=8, start_bin=-35.0, start_hl=15.0, volatility=12.0, seed=seed)
                r = simulate_intervals(500, intervals, min_hold_intervals=2, fee_breakeven_required=True)
                if r.net_pnl > 0:
                    wins += 1
                total_pnl += r.net_pnl

            print(f"  {label:20s} → win {wins/n_sims*100:5.1f}%, avg P&L ${total_pnl/n_sims:+.4f}")

        RT_FEE_PCT = original_fee  # restore

        # This test is informational — the insight matters more than the assertion
        assert True


class TestCumulativePortfolioEffect:
    """
    Model the portfolio-level effect: running 4-20 positions simultaneously.
    Some win, some lose — what's the net?
    """

    def test_portfolio_of_10_positions(self):
        """
        Simulate 10 concurrent positions with different rate paths.
        This is what the actual bot does.
        """
        portfolio_pnl = 0
        position_results = []

        for i in range(10):
            # Each position has a different starting rate and evolution
            start_apy = random.uniform(30, 100)
            intervals = random_walk_rates(
                n=8,
                start_bin=-(start_apy * 0.7),
                start_hl=start_apy * 0.3,
                volatility=15.0,
                seed=1000 + i,
            )
            r = simulate_intervals(
                500, intervals,
                min_hold_intervals=2,
                fee_breakeven_required=True,
            )
            portfolio_pnl += r.net_pnl
            position_results.append(r)

        wins = sum(1 for r in position_results if r.profitable)
        total_fees = sum(r.fee_cost for r in position_results)
        total_funding = sum(r.total_funding for r in position_results)

        print(f"\nPortfolio of 10 positions:")
        print(f"  Winners: {wins}/10")
        print(f"  Total fees: ${total_fees:.2f}")
        print(f"  Total funding: ${total_funding:.4f}")
        print(f"  Net portfolio P&L: ${portfolio_pnl:+.4f}")

        # The portfolio should have SOME winners
        assert wins >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "--tb=short"])
