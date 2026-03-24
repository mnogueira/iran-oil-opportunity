# Research Summary

## Current View

The market regime on March 24, 2026 is best described as a sustained-disruption shock with violent de-escalation squeezes, not a one-direction trend and not a normal post-headline fade.

Working conclusions:

- The supply shock is real enough that threat-only analogs are insufficient by themselves.
- Volatility is high enough that stop placement and position size dominate raw signal quality.
- Breakout longs still have edge during renewed disruption headlines.
- Mean reversion shorts need confirmation from de-escalation signals, not just stretched price action.
- Local-language Farsi and Arabic news can provide an information lead before English-language wires reprice the same event.

## Chosen Strategy

The repo implements a regime-switching oil strategy with three modules:

1. `event_momentum`
   Long breakouts when ATR expansion, wide Bollinger bands, strong upside structure, and positive disruption signals line up.
2. `panic_fade`
   Short overstretched spikes only after a minimum delay from the peak and evidence that stress is cooling.
3. `local_news_alpha`
   Poll regional Farsi and Arabic headlines, score escalation or de-escalation, and feed the resulting event score into the main engine.

## Validation Approach

- MT5 data collector for broker-native oil bars when the terminal bridge is healthy.
- CSV backtester for MT5 exports or other aligned bar files.
- Bundled reference dataset using official public Brent and OVX observations from early January through mid-March 2026.
- Research script that computes spike, volatility, and regime diagnostics on top of those bars.

## Seed Backtest

Running `python scripts/backtest_strategy.py` on the bundled Brent-plus-OVX dataset for `2026-01-02` through `2026-03-16` produced:

- total return: `1.39%`
- annualized return: `7.18%`
- annualized volatility: `2.85%`
- Sharpe: `2.52`
- max drawdown: `0.30%`
- trades: `4`
- win rate: `75%`

This is an event-window sanity check, not a final production proof. The right next step is still a longer MT5 broker-history backtest once the local MT5 bridge is reachable.

## Important Caveat

The environment available for this build has two hard runtime constraints:

- direct shell HTTP access is blocked
- MT5 IPC attachment is timing out

That means the live-data collectors are implemented and documented, but they could not be executed from this workspace on March 24, 2026.
