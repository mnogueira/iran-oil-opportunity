import unittest

import pandas as pd

from iran_oil_opportunity.cross_asset import analyze_cross_asset_opportunities


class CrossAssetTests(unittest.TestCase):
    def test_scanner_finds_positive_oil_proxy(self) -> None:
        index = pd.date_range("2026-03-01", periods=16, freq="D", tz="UTC")
        frames = {
            "UKOIL": pd.DataFrame({"close": [70, 71, 72, 73, 75, 77, 79, 81, 83, 84, 86, 88, 91, 95, 99, 104]}, index=index),
            "PETR4": pd.DataFrame({"close": [30, 30.2, 30.4, 30.6, 31, 31.2, 31.3, 31.4, 31.6, 31.8, 32.0, 32.2, 32.5, 33.0, 33.2, 33.4]}, index=index),
            "AZUL4": pd.DataFrame({"close": [15, 15.1, 15.0, 14.9, 14.8, 14.7, 14.6, 14.5, 14.3, 14.1, 13.9, 13.7, 13.5, 13.3, 13.2, 13.1]}, index=index),
        }
        opportunities = analyze_cross_asset_opportunities(
            frames,
            base_symbol="UKOIL",
            categories={"PETR4": "oil_stock", "AZUL4": "airline"},
        )
        self.assertGreaterEqual(len(opportunities), 2)
        self.assertEqual(opportunities[0].symbol, "PETR4")
        self.assertEqual(opportunities[0].signal, 1)


if __name__ == "__main__":
    unittest.main()
