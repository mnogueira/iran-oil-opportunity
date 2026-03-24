import unittest
from pathlib import Path

from iran_oil_opportunity.backtest import run_backtest
from iran_oil_opportunity.market_data import load_price_frame


class BacktestTests(unittest.TestCase):
    def test_seed_dataset_backtests(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        frame = load_price_frame(repo_root / "data/reference/fred_brent_ovx_q1_2026.csv")
        result = run_backtest(frame)
        self.assertGreaterEqual(result.trades, 1)
        self.assertFalse(result.equity_curve.empty)

    def test_ib_h1_defaults_trade_more_actively(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        frame = load_price_frame(repo_root / "data/ib/cl_h1.csv")
        result = run_backtest(frame, bars_per_year=8760)
        self.assertGreaterEqual(result.trades, 25)
        self.assertFalse(result.equity_curve.empty)


if __name__ == "__main__":
    unittest.main()
