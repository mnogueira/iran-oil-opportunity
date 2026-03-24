# Operations

## MT5 Probe

```powershell
python scripts/probe_mt5.py
```

This checks:

- whether the `MetaTrader5` Python package imports
- whether the terminal can be initialized
- whether the attached account looks like a demo environment
- which oil-related symbols the broker exposes

## Collect MT5 History

```powershell
python scripts/collect_mt5_history.py collect-history --timeframe H4 --bars 2000 --output data/mt5/primary_h4.csv
```

Useful flags:

- `--symbol XBRUSD`
- `--timeframe H1`
- `--bars 5000`
- `--output data/mt5/primary.csv`

## Backtest

```powershell
python scripts/backtest_strategy.py --input data/reference/fred_brent_ovx_q1_2026.csv
```

## Paper Service

Start:

```powershell
python scripts/paper_trade_service.py start --mode demo
```

Status:

```powershell
python scripts/paper_trade_service.py status --mode demo
```

Tail:

```powershell
python scripts/paper_trade_service.py tail --mode demo --source events
```

Stop:

```powershell
python scripts/paper_trade_service.py stop --mode demo
```
