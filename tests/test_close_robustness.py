#!/usr/bin/env python3
"""
Tests for Close Robustness (Fix C) — cross_arb.py

Tests the following features:
1. Retry with exponential backoff
2. Half-closed state tracking
3. Critical alerts
4. Startup reconciliation
5. Circuit breaker
"""

import asyncio
import json
import os
import sys
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock imports before importing the module under test
os.environ["BINANCE_API_KEY"] = "test"
os.environ["BINANCE_API_SECRET"] = "test"
os.environ["HYPERLIQUID_WALLET"] = "0x724abDC8263228095343e9190D3c4d35486F452c"

# Create mock hl_client
class MockHLClient:
    def __init__(self):
        self.positions = {}
    
    def get_position(self, symbol):
        return self.positions.get(symbol)
    
    def set_position(self, symbol, size):
        self.positions[symbol] = MagicMock(size=size)
    
    def get_mid(self, symbol):
        return 50000.0
    
    def round_size(self, symbol, size):
        return size
    
    def market_open(self, symbol, is_buy, size, slippage=0.02):
        return True, "order_id", 50000.0
    
    def market_close(self, symbol, slippage=0.02):
        return True, 50000.0
    
    def get_equity(self):
        return 100000.0
    
    def get_withdrawable(self):
        return 50000.0
    
    class _info:
        @property
        def meta_and_asset_ctxs(self):
            return ({"universe": []}, [])


@pytest.fixture
def mock_hl_client():
    return MockHLClient()


@pytest.fixture
def arb_position():
    """Create a test ArbPosition."""
    from dataclasses import dataclass, field
    from cross_arb import ArbPosition
    return ArbPosition(
        symbol="BTC",
        bin_side="buy",
        hl_side="sell",
        bin_size=0.01,
        hl_size=0.01,
        notional_usdt=500.0,
        entry_bin_apy=-10.0,
        entry_hl_apy=5.0,
        entry_net_apy=15.0,
        entry_ts=time.time(),
        bin_order_id="bin123",
        hl_order_id="hl123",
        bin_entry_px=50000.0,
        hl_entry_px=50000.0,
    )


class TestRetryWithBackoff:
    """Test the retry with exponential backoff logic."""
    
    @pytest.mark.asyncio
    async def test_retry_success_first_attempt(self):
        """Test successful close on first attempt - no retry needed."""
        from cross_arb import close_leg_with_retry
        
        call_count = 0
        
        async def successful_close():
            nonlocal call_count
            call_count += 1
            return {"success": True}
        
        result = await close_leg_with_retry(
            successful_close, "BTC", "bin", max_retries=3
        )
        
        assert result == {"success": True}
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_success_after_rate_limit(self):
        """Test successful close after rate limit error."""
        from cross_arb import close_leg_with_retry
        
        call_count = 0
        attempt_times = []
        
        async def flaky_close():
            nonlocal call_count
            call_count += 1
            attempt_times.append(time.time())
            
            if call_count < 3:
                # Rate limit error on first 2 attempts
                raise Exception("Rate limit exceeded (429)")
            return {"success": True}
        
        with patch('cross_arb.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await close_leg_with_retry(
                flaky_close, "BTC", "bin", max_retries=3
            )
        
        assert result == {"success": True}
        assert call_count == 3
        # Check exponential backoff: 2s, 4s
        assert mock_sleep.call_count == 2
    
    @pytest.mark.asyncio
    async def test_retry_timeout_error(self):
        """Test retry on timeout error."""
        from cross_arb import close_leg_with_retry
        
        call_count = 0
        
        async def timeout_close():
            nonlocal call_count
            call_count += 1
            
            if call_count < 2:
                raise Exception("Request timeout")
            return {"success": True}
        
        with patch('cross_arb.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await close_leg_with_retry(
                timeout_close, "BTC", "bin", max_retries=3
            )
        
        assert result == {"success": True}
        assert call_count == 2
    
    @pytest.mark.asyncio
    async def test_no_retry_insufficient_balance(self):
        """Test no retry on non-retryable error (insufficient balance)."""
        from cross_arb import close_leg_with_retry
        
        call_count = 0
        
        async def insufficient_balance_close():
            nonlocal call_count
            call_count += 1
            raise Exception("Insufficient balance")
        
        with pytest.raises(Exception) as exc_info:
            await close_leg_with_retry(
                insufficient_balance_close, "BTC", "bin", max_retries=3
            )
        
        assert "Insufficient balance" in str(exc_info.value)
        assert call_count == 1  # No retries for non-retryable errors
    
    @pytest.mark.asyncio
    async def test_no_retry_position_not_found(self):
        """Test no retry on position not found error."""
        from cross_arb import close_leg_with_retry
        
        call_count = 0
        
        async def not_found_close():
            nonlocal call_count
            call_count += 1
            raise Exception("Position not found")
        
        with pytest.raises(Exception) as exc_info:
            await close_leg_with_retry(
                not_found_close, "BTC", "bin", max_retries=3
            )
        
        assert "Position not found" in str(exc_info.value)
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self):
        """Test exception when all retries are exhausted."""
        from cross_arb import close_leg_with_retry
        
        call_count = 0
        
        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise Exception("Rate limit exceeded (429)")
        
        with patch('cross_arb.asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(Exception) as exc_info:
                await close_leg_with_retry(
                    always_fails, "BTC", "bin", max_retries=3
                )
        
        assert "bin failed after 3 retries" in str(exc_info.value)
        assert call_count == 3


class TestHalfClosedStateTracking:
    """Test half-closed state tracking in ArbPosition."""
    
    def test_default_tracking_fields(self, arb_position):
        """Test that new tracking fields default to None/0."""
        assert arb_position.bin_close_ts is None
        assert arb_position.hl_close_ts is None
        assert arb_position.close_failure_count == 0
    
    def test_set_bin_close_timestamp(self, arb_position):
        """Test setting Binance close timestamp."""
        arb_position.bin_closed = True
        arb_position.bin_close_ts = time.time()
        
        assert arb_position.bin_close_ts is not None
        assert arb_position.hl_close_ts is None
    
    def test_set_hl_close_timestamp(self, arb_position):
        """Test setting HL close timestamp."""
        arb_position.hl_closed = True
        arb_position.hl_close_ts = time.time()
        
        assert arb_position.hl_close_ts is not None
        assert arb_position.bin_close_ts is None
    
    def test_increment_failure_count(self, arb_position):
        """Test incrementing close failure count."""
        assert arb_position.close_failure_count == 0
        
        arb_position.close_failure_count += 1
        assert arb_position.close_failure_count == 1
        
        arb_position.close_failure_count += 1
        assert arb_position.close_failure_count == 2


class TestCriticalAlerts:
    """Test critical alert generation and persistence."""
    
    @pytest.fixture
    def alerts_file(self, tmp_path):
        """Create temp alerts file path."""
        alerts_file = tmp_path / "test_alerts.json"
        with patch('cross_arb.ALERTS_FILE', str(alerts_file)):
            yield str(alerts_file)
    
    def test_save_and_load_alerts(self, alerts_file):
        """Test saving and loading alerts."""
        from cross_arb import save_alerts, load_alerts
        
        test_alerts = [
            {"ts": time.time(), "symbol": "BTC", "reason": "test1"},
            {"ts": time.time(), "symbol": "ETH", "reason": "test2"},
        ]
        
        save_alerts(test_alerts)
        loaded = load_alerts()
        
        assert len(loaded) == 2
        assert loaded[0]["symbol"] == "BTC"
        assert loaded[1]["symbol"] == "ETH"
    
    def test_add_critical_alert(self, arb_position, alerts_file):
        """Test adding a critical alert."""
        from cross_arb import add_critical_alert, load_alerts as _load_alerts

        arb_position.bin_closed = True
        arb_position.hl_closed = False
        arb_position.close_failure_count = 5
        arb_position.notional_usdt = 500.0

        with patch('cross_arb.console') as mock_console:
            add_critical_alert(arb_position, "Persistent half-closed")

        alerts = _load_alerts()

        assert len(alerts) == 1
        assert alerts[0]["symbol"] == "BTC"
        assert alerts[0]["reason"] == "Persistent half-closed"
        assert alerts[0]["close_failure_count"] == 5

    def test_alerts_max_100(self, arb_position, alerts_file):
        """Test that alerts are capped at 100."""
        from cross_arb import add_critical_alert, load_alerts as _load_alerts

        # Add 105 alerts
        for i in range(105):
            arb_position.symbol = f"SYM{i}"
            with patch('cross_arb.console'):
                add_critical_alert(arb_position, f"test {i}")

        alerts = _load_alerts()

        # Should only keep last 100
        assert len(alerts) == 100


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_inactive_at_start(self):
        """Test circuit breaker starts inactive."""
        # Reset global state
        import cross_arb
        cross_arb._close_failure_timestamps = []
        
        from cross_arb import is_circuit_breaker_active
        
        assert is_circuit_breaker_active() == False
    
    def test_circuit_breaker_activates_after_threshold(self):
        """Test circuit breaker activates after threshold failures."""
        import cross_arb
        cross_arb._close_failure_timestamps = []
        
        from cross_arb import record_close_failure, is_circuit_breaker_active, CIRCUIT_BREAKER_THRESHOLD
        
        # Record failures up to threshold
        for _ in range(CIRCUIT_BREAKER_THRESHOLD):
            record_close_failure()
        
        assert is_circuit_breaker_active() == True
    
    def test_circuit_breaker_resets_old_failures(self):
        """Test circuit breaker ignores failures outside time window."""
        import cross_arb
        cross_arb._close_failure_timestamps = []
        
        from cross_arb import (
            record_close_failure, 
            is_circuit_breaker_active, 
            CIRCUIT_BREAKER_WINDOW_S
        )
        
        # Record 3 failures
        for _ in range(3):
            record_close_failure()
        
        assert is_circuit_breaker_active() == True
        
        # Simulate time passing - old failures should expire
        # (In real test, we'd mock time.time, but here we just verify the logic)
        cross_arb._close_failure_timestamps = [
            time.time() - CIRCUIT_BREAKER_WINDOW_S - 100  # Outside window
        ]
        
        assert is_circuit_breaker_active() == False


class TestPositionHealthCheck:
    """Test position health checking."""
    
    def test_healthy_position(self, arb_position):
        """Test that a fully open position is healthy."""
        from cross_arb import check_position_health
        
        is_healthy, msg = check_position_health(arb_position)
        
        assert is_healthy == True
        assert msg == ""
    
    def test_half_closed_recently(self, arb_position):
        """Test half-closed position detected."""
        from cross_arb import check_position_health
        
        arb_position.bin_closed = True
        arb_position.hl_closed = False
        arb_position.bin_close_ts = time.time()  # Just closed
        
        is_healthy, msg = check_position_health(arb_position)
        
        assert is_healthy == False
        assert "half-closed" in msg.lower()
    
    def test_half_closed_over_10_minutes(self, arb_position):
        """Test half-closed >10 minutes is critical."""
        from cross_arb import check_position_health
        
        arb_position.bin_closed = True
        arb_position.hl_closed = False
        arb_position.bin_close_ts = time.time() - 700  # 11+ minutes ago
        
        is_healthy, msg = check_position_health(arb_position)
        
        assert is_healthy == False
        assert "10min" in msg.lower() or "minutes" in msg.lower()
    
    def test_excessive_failures(self, arb_position):
        """Test detection of excessive close failures."""
        from cross_arb import check_position_health
        
        arb_position.close_failure_count = 5
        
        is_healthy, msg = check_position_health(arb_position)
        
        assert is_healthy == False
        assert "5" in msg and "failed" in msg.lower()
    
    def test_fully_closed_is_healthy(self, arb_position):
        """Test that fully closed position passes health check."""
        from cross_arb import check_position_health
        
        arb_position.bin_closed = True
        arb_position.hl_closed = True
        
        is_healthy, msg = check_position_health(arb_position)
        
        assert is_healthy == True


class TestStartupReconciliation:
    """Test startup position reconciliation."""

    @staticmethod
    def _make_mock_bin_ex(fetch_positions_return=None):
        """Create mock Binance exchange with AsyncMock methods."""
        mock_bin_ex = AsyncMock()
        mock_bin_ex.fetch_positions = AsyncMock(return_value=fetch_positions_return or [])
        mock_bin_ex.load_markets = AsyncMock()
        mock_bin_ex.close = AsyncMock()
        return mock_bin_ex

    @staticmethod
    def _make_ccxt_mock(mock_bin_ex):
        """Create a proper ccxt mock that works with 'import ccxt.async_support as ccxt'."""
        mock_async_support = MagicMock()
        mock_async_support.binanceusdm = MagicMock(return_value=mock_bin_ex)
        mock_ccxt_pkg = MagicMock()
        mock_ccxt_pkg.async_support = mock_async_support
        return mock_ccxt_pkg, mock_async_support

    @pytest.mark.asyncio
    async def test_reconcile_all_match(self, arb_position):
        """Test reconciliation when state matches actual."""
        import cross_arb

        positions = [arb_position]

        mock_bin_ex = self._make_mock_bin_ex([
            {"symbol": "BTC/USDT:USDT", "contracts": 0.01}
        ])
        mock_ccxt_pkg, mock_async = self._make_ccxt_mock(mock_bin_ex)

        mock_hl = MagicMock()
        mock_hl_pos = MagicMock()
        mock_hl_pos.size = 0.01
        mock_hl.get_position = MagicMock(return_value=mock_hl_pos)
        mock_hl_mod = MagicMock()
        mock_hl_mod.make_hl_client = MagicMock(return_value=mock_hl)

        with patch.dict('sys.modules', {'ccxt': mock_ccxt_pkg, 'ccxt.async_support': mock_async, 'hl_client': mock_hl_mod}):
            with patch.object(cross_arb, 'save_state'):
                reconciled = await cross_arb.reconcile_positions(positions)

        assert len(reconciled) == 1
        assert reconciled[0].bin_closed == False
        assert reconciled[0].hl_closed == False

    @pytest.mark.asyncio
    async def test_reconcile_state_wrong_bin_closed(self, arb_position):
        """Test reconciliation when state says open but exchange is flat (Binance)."""
        import cross_arb

        positions = [arb_position]

        mock_bin_ex = self._make_mock_bin_ex([
            {"symbol": "BTC/USDT:USDT", "contracts": 0.0}
        ])
        mock_ccxt_pkg, mock_async = self._make_ccxt_mock(mock_bin_ex)

        mock_hl = MagicMock()
        mock_hl_pos = MagicMock()
        mock_hl_pos.size = 0.01
        mock_hl.get_position = MagicMock(return_value=mock_hl_pos)
        mock_hl_mod = MagicMock()
        mock_hl_mod.make_hl_client = MagicMock(return_value=mock_hl)

        with patch.dict('sys.modules', {'ccxt': mock_ccxt_pkg, 'ccxt.async_support': mock_async, 'hl_client': mock_hl_mod}):
            with patch.object(cross_arb, 'save_state'):
                reconciled = await cross_arb.reconcile_positions(positions)

        assert len(reconciled) == 1
        assert reconciled[0].bin_closed == True
        assert reconciled[0].hl_closed == False
        assert reconciled[0].needs_close == True

    @pytest.mark.asyncio
    async def test_reconcile_removes_fully_closed(self, arb_position):
        """Test that fully closed positions are removed from active list."""
        import cross_arb

        positions = [arb_position]

        mock_bin_ex = self._make_mock_bin_ex([])
        mock_ccxt_pkg, mock_async = self._make_ccxt_mock(mock_bin_ex)

        mock_hl = MagicMock()
        mock_hl.get_position = MagicMock(return_value=None)
        mock_hl_mod = MagicMock()
        mock_hl_mod.make_hl_client = MagicMock(return_value=mock_hl)

        with patch.dict('sys.modules', {'ccxt': mock_ccxt_pkg, 'ccxt.async_support': mock_async, 'hl_client': mock_hl_mod}):
            with patch.object(cross_arb, 'save_state'):
                reconciled = await cross_arb.reconcile_positions(positions)

        assert len(reconciled) == 0


class TestCloseArbPositionIntegration:
    """Integration tests for close_arb_position with retry logic."""

    @staticmethod
    def _make_ccxt_mock(mock_bin_ex):
        """Create a proper ccxt mock that works with 'import ccxt.async_support as ccxt'."""
        mock_async_support = MagicMock()
        mock_async_support.binanceusdm = MagicMock(return_value=mock_bin_ex)
        mock_ccxt_pkg = MagicMock()
        mock_ccxt_pkg.async_support = mock_async_support
        return mock_ccxt_pkg, mock_async_support

    @pytest.mark.asyncio
    async def test_close_bin_leg_with_retry(self, arb_position):
        """Test Binance close with retry on rate limit."""
        import cross_arb

        arb_position.bin_closed = False
        arb_position.hl_closed = True

        mock_bin_ex = AsyncMock()
        mock_bin_ex.load_markets = AsyncMock()
        mock_bin_ex.close = AsyncMock()
        mock_bin_ex.fetch_ticker = AsyncMock(return_value={"last": 50000.0})

        call_count = 0
        async def mock_fetch_positions(symbols):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Rate limit exceeded (429)")
            return [{"symbol": "BTC/USDT:USDT", "contracts": 0.01}]

        mock_bin_ex.fetch_positions = mock_fetch_positions

        async def mock_create_order(sym, order_type, side, size, params=None):
            return {"id": "order123", "average": 50000.0}

        mock_bin_ex.create_order = mock_create_order

        mock_hl = MagicMock()
        mock_hl.get_mid = MagicMock(return_value=50000.0)

        mock_ccxt_pkg, mock_async = self._make_ccxt_mock(mock_bin_ex)
        mock_hl_mod = MagicMock()
        mock_hl_mod.make_hl_client = MagicMock(return_value=mock_hl)

        with patch.dict('sys.modules', {'ccxt': mock_ccxt_pkg, 'ccxt.async_support': mock_async, 'hl_client': mock_hl_mod}):
            result = await cross_arb.close_arb_position(arb_position)

        assert result == True
        assert arb_position.bin_closed == True

    @pytest.mark.asyncio
    async def test_close_handles_position_already_closed(self, arb_position):
        """Test handling of 'position already closed' from exchange."""
        import cross_arb

        arb_position.bin_closed = False
        arb_position.hl_closed = True

        mock_bin_ex = AsyncMock()
        mock_bin_ex.load_markets = AsyncMock()
        mock_bin_ex.close = AsyncMock()
        mock_bin_ex.fetch_ticker = AsyncMock(return_value={"last": 50000.0})

        async def mock_fetch_positions(symbols):
            return [{"symbol": "BTC/USDT:USDT", "contracts": 0.0}]

        mock_bin_ex.fetch_positions = mock_fetch_positions

        mock_hl = MagicMock()
        mock_hl.get_mid = MagicMock(return_value=50000.0)

        mock_ccxt_pkg, mock_async = self._make_ccxt_mock(mock_bin_ex)
        mock_hl_mod = MagicMock()
        mock_hl_mod.make_hl_client = MagicMock(return_value=mock_hl)

        with patch.dict('sys.modules', {'ccxt': mock_ccxt_pkg, 'ccxt.async_support': mock_async, 'hl_client': mock_hl_mod}):
            result = await cross_arb.close_arb_position(arb_position)

        assert result == True
        assert arb_position.bin_closed == True


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
