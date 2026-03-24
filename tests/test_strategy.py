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


if __name__ == "__main__":
    unittest.main()
