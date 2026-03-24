import unittest

import pandas as pd

from iran_oil_opportunity.alternative_data import merge_alt_data


class AlternativeDataTests(unittest.TestCase):
    def test_merge_alt_data_backfills_latest_event(self) -> None:
        price = pd.DataFrame(
            {"close": [100.0, 101.0, 102.0]},
            index=pd.to_datetime(
                ["2026-03-24T10:00:00Z", "2026-03-24T11:00:00Z", "2026-03-24T12:00:00Z"],
                utc=True,
            ),
        )
        alt = pd.DataFrame(
            {"local_news_score": [0.7, -0.4]},
            index=pd.to_datetime(["2026-03-24T09:30:00Z", "2026-03-24T11:30:00Z"], utc=True),
        )
        merged = merge_alt_data(price, alt)
        self.assertAlmostEqual(float(merged.iloc[0]["local_news_score"]), 0.7)
        self.assertAlmostEqual(float(merged.iloc[1]["local_news_score"]), 0.7)
        self.assertAlmostEqual(float(merged.iloc[2]["local_news_score"]), -0.4)


if __name__ == "__main__":
    unittest.main()
