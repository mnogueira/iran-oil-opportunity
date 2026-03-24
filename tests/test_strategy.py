import unittest

import pandas as pd

from iran_oil_opportunity.features import build_feature_frame
from iran_oil_opportunity.strategy import IranOilShockStrategy


class StrategyTests(unittest.TestCase):
    def test_breakout_signal_triggers(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [70.0, 70.4, 70.8, 71.1, 71.5, 72.0, 72.4, 72.9, 73.3, 73.8, 74.2, 74.6, 75.0, 75.4, 80.2],
                "high": [70.2, 70.6, 71.0, 71.3, 71.7, 72.2, 72.6, 73.1, 73.5, 74.0, 74.4, 74.8, 75.2, 75.6, 80.5],
                "low": [69.8, 70.2, 70.6, 70.9, 71.3, 71.8, 72.2, 72.7, 73.1, 73.6, 74.0, 74.4, 74.8, 75.2, 79.8],
                "tick_volume": [10, 11, 12, 12, 13, 14, 14, 15, 15, 16, 18, 20, 22, 24, 45],
                "stress_level": [40.0, 41.0, 42.0, 43.0, 45.0, 46.0, 48.0, 50.0, 52.0, 55.0, 57.0, 59.0, 61.0, 64.0, 82.0],
            },
            index=pd.date_range("2026-03-01", periods=15, freq="D", tz="UTC"),
        )
        features = build_feature_frame(frame)
        decision = IranOilShockStrategy().decide(features)
        self.assertEqual(decision.signal, 1)
        self.assertEqual(decision.regime, "shock_breakout")

    def test_reversal_signal_triggers(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [80.0, 81.0, 82.0, 83.0, 84.5, 86.0, 88.0, 91.0, 95.0, 99.0, 104.0, 110.0, 116.0, 122.0, 118.0],
                "high": [80.5, 81.5, 82.5, 83.5, 85.0, 86.5, 88.5, 91.5, 95.5, 99.5, 104.5, 110.5, 116.5, 122.5, 118.5],
                "low": [79.5, 80.5, 81.5, 82.5, 84.0, 85.5, 87.5, 90.5, 94.5, 98.5, 103.5, 109.5, 115.5, 121.5, 117.5],
                "tick_volume": [12, 12, 13, 14, 15, 16, 18, 20, 22, 24, 27, 31, 35, 38, 44],
                "stress_level": [50.0, 52.0, 54.0, 56.0, 58.0, 62.0, 66.0, 72.0, 78.0, 86.0, 94.0, 102.0, 110.0, 118.0, 121.0],
            },
            index=pd.date_range("2026-03-01", periods=15, freq="D", tz="UTC"),
        )
        features = build_feature_frame(frame)
        decision = IranOilShockStrategy().decide(features)
        self.assertEqual(decision.signal, -1)
        self.assertEqual(decision.regime, "panic_reversal")

    def test_strong_recent_news_lowers_long_thresholds(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [98.0, 98.4, 98.9, 99.2, 99.7],
                "rolling_high": [99.0, 99.2, 99.4, 99.8, 100.0],
                "rolling_low": [97.0, 97.2, 97.5, 97.7, 98.0],
                "stress_level": [24.0, 25.0, 26.0, 27.0, 28.0],
                "stress_change_3": [0.0, 0.0, 0.0, -10.0, -10.0],
                "return_1": [0.001, 0.002, 0.003, 0.004, 0.005],
                "return_3": [0.010, 0.012, 0.014, 0.016, 0.018],
                "price_zscore": [0.2, 0.3, 0.4, 0.5, 0.55],
                "trend_gap": [0.005, 0.006, 0.007, 0.008, 0.01],
                "atr_pct": [0.012, 0.012, 0.012, 0.012, 0.012],
                "event_score": [0.20, 0.22, 0.24, 0.28, 0.32],
                "local_news_score": [0.15, 0.20, 0.25, 0.65, 0.70],
                "headline_count": [1, 1, 1, 3, 4],
                "prediction_market_score": [0.0, 0.0, 0.0, 0.0, 0.0],
            },
            index=pd.date_range("2026-03-24 14:00:00", periods=5, freq="h", tz="UTC"),
        )
        decision = IranOilShockStrategy().decide(frame)
        self.assertEqual(decision.signal, 1)
        self.assertIn(decision.regime, {"shock_breakout", "news_confirmed_breakout"})

    def test_low_recent_news_lowers_short_thresholds(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [103.0, 104.0, 105.0, 106.0, 104.8],
                "rolling_high": [104.0, 105.0, 106.0, 107.0, 107.5],
                "rolling_low": [101.0, 101.5, 102.0, 102.5, 103.0],
                "stress_level": [35.0, 36.0, 38.0, 39.0, 40.0],
                "stress_change_3": [0.0, 0.0, 0.0, 2.0, 2.0],
                "return_1": [0.003, 0.004, 0.005, 0.006, -0.011],
                "return_3": [0.010, 0.012, 0.014, 0.016, -0.018],
                "price_zscore": [0.20, 0.25, 0.30, 0.40, 0.60],
                "trend_gap": [0.02, 0.03, 0.04, 0.05, 0.07],
                "atr_pct": [0.014, 0.014, 0.014, 0.014, 0.014],
                "event_score": [0.0, 0.0, 0.0, 0.0, 0.0],
                "local_news_score": [0.40, 0.30, 0.20, 0.05, 0.00],
                "headline_count": [2, 2, 2, 3, 4],
                "prediction_market_score": [0.0, 0.0, 0.0, 0.0, 0.0],
            },
            index=pd.date_range("2026-03-24 14:00:00", periods=5, freq="h", tz="UTC"),
        )
        decision = IranOilShockStrategy().decide(frame)
        self.assertEqual(decision.signal, -1)
        self.assertEqual(decision.regime, "panic_reversal")


if __name__ == "__main__":
    unittest.main()
