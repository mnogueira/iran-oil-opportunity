import unittest
from pathlib import Path

from iran_oil_opportunity.backtest import run_backtest
from iran_oil_opportunity.market_data import load_price_frame


class BacktestTests(unittest.TestCase):
    def test_seed_dataset_backtests(self) -> None:
        frame = load_price_frame(Path("data/reference/fred_brent_ovx_q1_2026.csv"))
        result = run_backtest(frame)
        self.assertGreaterEqual(result.trades, 1)
        self.assertFalse(result.equity_curve.empty)


if __name__ == "__main__":
    unittest.main()
