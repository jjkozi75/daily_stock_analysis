# -*- coding: utf-8 -*-
"""Offline unit tests for TwSectorFetcher (台股上市類股漲跌排行 / 成交比重).

Fixtures follow the real TWSE OpenAPI MI_INDEX and BFIAMU layouts (key names,
民國 / 西元 dates, signed 漲跌百分比, space-padded sector names) observed on
2026-11-13 -- no network is touched.
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.tw_sector_fetcher import TwSectorFetcher, sector_name_of  # noqa: E402


def _mi(name, close, direction, points, pct, date="1151113"):
    return {
        "日期": date, "指數": name, "收盤指數": close, "漲跌": direction,
        "漲跌點數": points, "漲跌百分比": pct, "特殊處理註記": "",
    }


MI_INDEX_FIXTURE = [
    _mi("寶島股價指數", "48117.24", "+", "661.19", "1.39"),
    _mi("發行量加權股價指數", "43675.12", "+", "583.25", "1.35"),
    _mi("臺指反向一倍指數", "1290.34", "-", "17.70", "-1.35"),
    _mi("電子工業類指數", "2500.00", "+", "30.00", "1.21"),          # composite parent
    _mi("水泥類指數", "129.97", "-", "0.18", "-0.14"),
    _mi("食品類指數", "2100.00", "+", "11.69", "0.56"),
    _mi("半導體類指數", "1480.33", "+", "17.92", "1.23"),
    _mi("金融保險類指數", "3744.99", "+", "79.01", "2.16"),
    _mi("航運類指數", "200.00", "-", "3.00", "-1.48"),
    _mi("半導體類報酬指數", "3000.00", "+", "36.00", "1.23"),        # total-return twin
    _mi("電子類兩倍槓桿指數", "900.00", "+", "20.00", "2.27"),       # leveraged
    _mi("觀光餐旅類指數", "100.00", "", "0.00", "--"),               # unparseable pct
]

BFIAMU_FIXTURE = {
    "stat": "OK",
    "date": "20261113",
    "title": "115年11月13日各類指數日成交量值",
    "fields": ["分類指數名稱", "成交股數", "成交金額", "成交筆數", "漲跌指數"],
    "data": [
        ["水泥類指數          ", "35,723,619", "1,000,000,000", "11,319", "-0.18"],
        ["食品類指數          ", "23,080,645", "1,000,000,000", "14,493", "11.69"],
        ["電子工業類指數      ", "900,000,000", "70,000,000,000", "900,000", "30.00"],
        ["半導體類指數        ", "300,000,000", "6,000,000,000", "300,000", "17.92"],
        ["金融保險類指數      ", "200,000,000", "1,500,000,000", "90,000", "79.01"],
        ["航運類指數          ", "100,000,000", "500,000,000", "50,000", "-3.00"],
    ],
}


def _fetcher_with(mi=MI_INDEX_FIXTURE, bfiamu=BFIAMU_FIXTURE):
    fetcher = TwSectorFetcher()

    def fake_get_json(url, params=None):
        payload = mi if "MI_INDEX" in url else bfiamu
        if isinstance(payload, Exception):
            raise payload
        return payload

    return fetcher, patch.object(fetcher, "_get_json", side_effect=fake_get_json)


class TestSectorNames(unittest.TestCase):
    def test_leaf_sector_names(self):
        self.assertEqual(sector_name_of("半導體類指數"), "半導體")
        self.assertEqual(sector_name_of("水泥類指數          "), "水泥")

    def test_non_sectors_are_rejected(self):
        for name in (
            "發行量加權股價指數", "寶島股價指數", "半導體類報酬指數", "電子類兩倍槓桿指數",
            "電子類反向指數", "電子工業類指數", "化學生技醫療類指數", "", None,
        ):
            self.assertIsNone(sector_name_of(name), name)


class TestTwSectorFetcher(unittest.TestCase):
    def test_rankings_sorted_by_signed_change(self):
        fetcher, patcher = _fetcher_with()
        with patcher:
            top, bottom = fetcher.get_sector_rankings(2)
        self.assertEqual([item["name"] for item in top], ["金融保險", "半導體"])
        self.assertEqual([item["name"] for item in bottom], ["航運", "水泥"])
        self.assertEqual(top[0]["change_pct"], 2.16)
        self.assertEqual(bottom[0]["change_pct"], -1.48)
        self.assertEqual(top[0]["date"], "20261113")  # 民國 1151113 -> 西元

    def test_composites_variants_and_unparseable_rows_are_excluded(self):
        fetcher, patcher = _fetcher_with()
        with patcher:
            names = {item["name"] for item in fetcher.get_sector_table()}
        self.assertEqual(names, {"水泥", "食品", "半導體", "金融保險", "航運"})

    def test_direction_column_wins_over_an_unsigned_percentage(self):
        mi = [_mi("水泥類指數", "129.97", "-", "0.18", "0.14")]  # sign dropped upstream
        fetcher, patcher = _fetcher_with(mi=mi)
        with patcher:
            table = fetcher.get_sector_table()
        self.assertEqual(table[0]["change_pct"], -0.14)

    def test_turnover_share_uses_leaf_sectors_only(self):
        fetcher, patcher = _fetcher_with()
        with patcher:
            table = {item["name"]: item for item in fetcher.get_sector_table()}
            turnover = fetcher.get_turnover_rankings(2)
        # Leaf total = 1.0 + 1.0 + 6.0 + 1.5 + 0.5 = 10.0 bn; the 70 bn composite is ignored.
        self.assertEqual(table["半導體"]["amount_share_pct"], 60.0)
        self.assertEqual(table["金融保險"]["amount_share_pct"], 15.0)
        self.assertEqual(table["航運"]["amount"], 500_000_000.0)
        self.assertEqual([item["name"] for item in turnover], ["半導體", "金融保險"])

    def test_turnover_skipped_when_trading_days_differ(self):
        fetcher, patcher = _fetcher_with(bfiamu=dict(BFIAMU_FIXTURE, date="20261112"))
        with patcher:
            table = fetcher.get_sector_table()
            turnover = fetcher.get_turnover_rankings()
        self.assertTrue(table)
        self.assertTrue(all("amount_share_pct" not in item for item in table))
        self.assertEqual(turnover, [])

    def test_turnover_failure_keeps_change_ranking(self):
        fetcher, patcher = _fetcher_with(bfiamu=RuntimeError("WAF page"))
        with patcher:
            rankings = fetcher.get_sector_rankings(1)
        self.assertEqual(rankings[0][0]["name"], "金融保險")
        self.assertNotIn("amount", rankings[0][0])

    def test_change_feed_failure_fails_open(self):
        fetcher, patcher = _fetcher_with(mi=RuntimeError("boom"))
        with patcher:
            self.assertEqual(fetcher.get_sector_table(), [])
            self.assertIsNone(fetcher.get_sector_rankings())

    def test_unexpected_shapes_fail_open(self):
        for payload in ({}, None, "html", [1, 2], [{"指數": "半導體類指數"}]):
            self.assertEqual(TwSectorFetcher.parse_mi_index(payload), [])
        for payload in (None, [], {"stat": "很抱歉，沒有符合條件的資料!"},
                        {"stat": "OK", "fields": ["改名"], "data": [["x", "1"]]}):
            self.assertEqual(TwSectorFetcher.parse_bfiamu(payload), ({}, None))

    def test_table_is_cached_and_callers_get_copies(self):
        fetcher, patcher = _fetcher_with()
        with patcher as get_json:
            first = fetcher.get_sector_table()
            first[0]["name"] = "mutated"
            second = fetcher.get_sector_table()
        self.assertEqual(get_json.call_count, 2)  # MI_INDEX + BFIAMU, once
        self.assertNotIn("mutated", {item["name"] for item in second})

    def test_circuit_opens_after_repeated_failures(self):
        fetcher, patcher = _fetcher_with(mi=RuntimeError("down"))
        with patcher as get_json:
            for _ in range(6):
                fetcher.get_sector_table()
        self.assertEqual(get_json.call_count, 3)  # then skipped until cooldown


if __name__ == "__main__":
    unittest.main()
