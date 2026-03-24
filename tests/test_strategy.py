import unittest

import pandas as pd

from iran_oil_opportunity.features import build_feature_frame
from iran_oil_opportunity.strategy import IranOilShockStrategy


class StrategyTests(unittest.TestCase):
    def test_breakout_signal_triggers(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [70.0, 71.0, 72.5, 73.0, 74.0, 79.0],
                "high": [70.2, 71.2, 72.7, 73.2, 74.2, 79.2],
                "low": [69.8, 70.8, 72.1, 72.8, 73.8, 78.8],
                "tick_volume": [10, 12, 14, 18, 20, 40],
                "stress_level": [40.0, 42.0, 45.0, 50.0, 62.0, 75.0],
            },
            index=pd.date_range("2026-03-01", periods=6, freq="D", tz="UTC"),
        )
        features = build_feature_frame(frame)
        decision = IranOilShockStrategy().decide(features)
        self.assertEqual(decision.signal, 1)
        self.assertEqual(decision.regime, "shock_breakout")

    def test_reversal_signal_triggers(self) -> None:
        frame = pd.DataFrame(
            {
                "close": [80.0, 83.0, 87.0, 95.0, 102.0, 99.0],
                "high": [80.5, 83.5, 87.5, 95.5, 102.5, 99.5],
                "low": [79.5, 82.5, 86.5, 94.5, 101.5, 98.5],
                "tick_volume": [12, 14, 18, 22, 30, 40],
                "stress_level": [50.0, 55.0, 70.0, 90.0, 110.0, 120.0],
            },
            index=pd.date_range("2026-03-01", periods=6, freq="D", tz="UTC"),
        )
        features = build_feature_frame(frame)
        decision = IranOilShockStrategy().decide(features)
        self.assertEqual(decision.signal, -1)
        self.assertEqual(decision.regime, "panic_reversal")


if __name__ == "__main__":
    unittest.main()
