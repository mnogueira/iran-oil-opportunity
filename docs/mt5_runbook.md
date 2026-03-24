# MT5 Runbook

## Preconditions

- Use a demo account only.
- Make sure the MetaTrader 5 desktop terminal is installed and logged in once manually.
- Confirm the Python package `MetaTrader5` is installed.

## Local Check

```powershell
python scripts/probe_mt5.py
```

Expected:

- Terminal info
- Account info
- Demo-account validation
- Discovered oil-like symbols such as `BRENT`, `UKOIL`, `WTI`, `USOIL`, `XBRUSD`, `XTIUSD`

## History Download

```powershell
python scripts/collect_mt5_history.py --timeframe H1 --bars 5000
```

This script:

- Connects to MT5.
- Discovers broker symbols automatically if none are provided.
- Saves CSV bars into `data/mt5/`.

## Paper Service

Start:

```powershell
python scripts/paper_trade_service.py start --mode demo
```

Status:

```powershell
python scripts/paper_trade_service.py status --mode demo
```

Tail recent events:

```powershell
python scripts/paper_trade_service.py tail --mode demo --source events --lines 25
```

Stop:

```powershell
python scripts/paper_trade_service.py stop --mode demo
```

## Notes From This Workspace

On `2026-03-24`, `MetaTrader5.initialize()` was able to locate the installed terminal path but currently returned `IPC timeout` in this environment. That usually means one of:

- the desktop terminal is not fully open;
- the first-run/login flow still needs to be completed in the GUI;
- the terminal and Python process cannot complete their local IPC handshake from the current session.
