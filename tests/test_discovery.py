import unittest

from iran_oil_opportunity.discovery import (
    choose_brent_wti_pair,
    choose_primary_oil_symbol,
    discover_candidates,
)


class DiscoveryTests(unittest.TestCase):
    def test_discovers_broker_aliases(self) -> None:
        symbols = ["EURUSD", "UKOIL.raw", "USOIL-pro", "XOM"]
        candidates = discover_candidates(symbols)
        self.assertEqual(candidates[0].symbol, "UKOIL.raw")
        self.assertEqual(candidates[1].symbol, "USOIL-pro")

    def test_choose_primary_prefers_brent(self) -> None:
        symbols = ["USOIL", "UKOIL"]
        self.assertEqual(choose_primary_oil_symbol(symbols, preferred="brent"), "UKOIL")

    def test_choose_pair(self) -> None:
        brent, wti = choose_brent_wti_pair(["XTIUSD", "XBRUSD"])
        self.assertEqual(brent, "XBRUSD")
        self.assertEqual(wti, "XTIUSD")

    def test_discovers_ib_brent_alias(self) -> None:
        candidates = discover_candidates(["CL", "BZM6", "PETR4"])
        self.assertEqual(candidates[0].symbol, "BZM6")
        self.assertEqual(candidates[0].category, "brent")


if __name__ == "__main__":
    unittest.main()
