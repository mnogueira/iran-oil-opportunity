# iran-oil-opportunity

Research and demo-only MT5 paper-trading code for the extreme oil volatility created by the March 2026 Iran war and Strait of Hormuz disruption.

## Thesis

This repo is built around one core idea: the oil market is no longer reacting to a vague geopolitical threat. As of March 24, 2026, it is repricing a live supply shock with violent swings between sustained-disruption panic and ceasefire-driven reversal. That creates three tradable regimes:

1. Event momentum: buy upside breakouts when disruption risk is accelerating and volatility is expanding.
2. Panic fade: short overstretched spikes only after de-escalation signals appear and volatility starts rolling over.
3. Information lead: monitor Farsi and Arabic headlines before English-language wires fully absorb them, then convert local-language escalation and de-escalation into a tradable event score.

The implementation is MT5-first and demo-only. It discovers broker symbols dynamically, prefers Brent and WTI oil CFDs when available, and mirrors signals through the `MetaTrader5` Python package only if the connected account is a demo account.

## What Is In This Repo

- `src/iran_oil_opportunity/`
  Quant logic, MT5 integration, paper-trading state, monitoring, and local-language news monitoring.
- `scripts/`
  Research, backtest, MT5 history collection, local-news polling, and paper-trading service wrappers.
- `tests/`
  Unit tests for strategy logic, symbol discovery, backtest behavior, and service health helpers.
- `docs/`
  Current market research, sources, and an operations runbook.

## Strategy Shape

### Core market regime engine

- Bias long when breakout structure, ATR expansion, wide Bollinger bands, and disruption-sensitive event signals all point in the same direction.
- Bias short only after discrete spikes, a minimum delay from the local peak, and evidence that the information regime is cooling rather than worsening.
- Use small size by default and ATR-based stop placement because daily swings in March 2026 have repeatedly exceeded 7-13%.

### Information edge module

- Poll Farsi and Arabic news sources such as IRNA, Tasnim, Fars, ISNA, Mehr, Khabar Online, Al Mayadeen, Al Alam, and Shafaq.
- Score headlines with a multilingual escalation/de-escalation lexicon before they are translated into English.
- Feed the resulting `local_news_score` into the same strategy engine that also consumes optional prediction-market, tanker-flow, and shipping stress inputs.

### Execution model

- `shadow` mode: journal signals and PnL locally without sending demo orders.
- `demo` mode: route mirrored orders only after an MT5 demo-account check passes.
- Service wrapper: heartbeat, status file, kill switch, stdout and stderr logs, and a detached runner pattern adapted from `C:/Dev/tradebot`.

## Quick Start

### 1. Verify MT5

```powershell
python scripts/probe_mt5.py
```

### 2. Discover broker oil symbols

```powershell
python scripts/collect_mt5_history.py --discover-only
```

### 3. Backtest from a CSV or the bundled reference dataset

```powershell
python scripts/backtest_strategy.py --csv data/reference/brent_ovx_q1_2026.csv
```

### 4. Poll local-language news

```powershell
python scripts/local_news_monitor.py --once
```

### 5. Start the paper-trading loop

```powershell
python scripts/paper_trade_service.py start --mode shadow
```

If MT5 is healthy and the account is a demo account, you can switch to `--mode demo --submit-orders`.

## Current Status In This Workspace

- `git init` completed locally.
- The installed MT5 terminal and demo profile were found on disk.
- Direct `MetaTrader5.initialize()` calls currently fail with `IPC timeout` in this sandbox, so live MT5 collection and demo-order routing are implemented but could not be executed end-to-end from this environment.
- Shell-level HTTP is blocked here, so live news, translation, and prediction-market fetchers are implemented as optional modules but were not run in this workspace.

## Guardrails

- Demo accounts only.
- No real-money operations.
- Small-capital sizing defaults.
- Kill-switch file support for the runner.
- Risk halts for drawdown, daily loss, and stale runtime health.

## Read Next

- [docs/research.md](docs/research.md)
- [docs/operations.md](docs/operations.md)
- [docs/sources.md](docs/sources.md)

