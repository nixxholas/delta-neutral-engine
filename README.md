# funding-farm

Delta-neutral funding rate farm for Binance USDT-M perpetuals.

## Strategy

**Spot long + Short perp** = delta neutral, earns when longs pay shorts (positive funding)  
**Spot short + Long perp** = delta neutral, earns when shorts pay longs (negative funding)

Funding is paid every 8 hours. APY = rate_per_8h × 3 × 365.

## Usage

```bash
source ../hl-mmbot/venv/bin/activate

# Scan opportunities
python scanner.py --min 10

# Start the farm (opens positions, monitors, auto-exits when rate drops)
python farm.py --run

# Check status
python farm.py --status

# Close everything
python farm.py --close
```

## Config (.env)

```
FARM_SIZE_USDT=500        # notional per position leg
FARM_MIN_ENTRY_APY=15     # minimum APY to enter
FARM_EXIT_APY=5           # exit when APY drops below this
FARM_MAX_POSITIONS=3      # max concurrent positions
```

## Notes

- On demo: only perp leg is executed (no spot API). Spot leg is notional.
- On mainnet: wire in spot exchange (same Binance account, spot API) for the full hedge.
- Testnet funding rates are often synthetic/extreme. Real market rates will be lower.
