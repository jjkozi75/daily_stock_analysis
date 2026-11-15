# -*- coding: utf-8 -*-
"""Offline tests for the multi-day 三大法人 flow summary (台股資金流向, 1/5/10 日).

The dated T86 endpoint is replaced by an in-memory calendar so the walker's
weekend / holiday / failure handling is pinned without touching the network.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.tw_institutional_fetcher import TwInstitutionalFetcher  # noqa: E402

_FIELDS = [
    "證券代號", "證券名稱",
    "外陸資買賣超股數(不含外資自營商)", "投信買賣超股數", "自營商買賣超股數", "三大法人買賣超股數",
]
_NO_DATA = {"stat": "很抱歉，沒有符合條件的資料!"}
ANCHOR = "20261113"  # Friday
_ANCHOR_RECORD = {
    "stock_code": "2330", "date": ANCHOR, "market": "上市", "source": "TWSE-T86", "unit": "shares",
    "foreign_net": 700, "trust_net": 200, "dealer_net": 100, "total_net": 1000,
}


def _payload(date, rows):
    return {
        "stat": "OK", "date": date, "fields": _FIELDS,
        "data": [[code, "名稱", f"{net:,}", "0", "0", f"{net:,}"] for code, net in rows.items()],
    }


def _weekdays_before(anchor, count):
    day, out = datetime.strptime(anchor, "%Y%m%d"), []
    while len(out) < count:
        day -= timedelta(days=1)
        if day.weekday() < 5:
            out.append(day.strftime("%Y%m%d"))
    return out


class _Calendar:
    """Fake dated T86: ``responses[date]`` is a payload, an Exception, or absent (no data)."""

    def __init__(self, responses):
        self.responses = responses
        self.requested = []

    def __call__(self, url, params=None):
        date = (params or {}).get("date")
        self.requested.append(date)
        response = self.responses.get(date, _NO_DATA)
        if isinstance(response, Exception):
            raise response
        return response


def _flow(responses, code="2330.TW", record=None, **fetcher_kwargs):
    fetcher = TwInstitutionalFetcher(min_request_interval=0, **fetcher_kwargs)
    calendar = _Calendar(responses)
    with patch.object(fetcher, "_get_json", side_effect=calendar):
        flow = fetcher.get_institutional_flow(code, dict(record or _ANCHOR_RECORD), wait_seconds=5)
    return flow, calendar, fetcher


class TestTwInstitutionalFlow(unittest.TestCase):
    def test_full_windows_sum_latest_day_plus_earlier_trading_days(self):
        days = _weekdays_before(ANCHOR, 9)
        responses = {day: _payload(day, {"2330": (i + 1) * 10, "2317": 5}) for i, day in enumerate(days)}
        flow, calendar, _ = _flow(responses)
        self.assertEqual(flow["net_1d"], 1000)
        self.assertEqual(flow["net_5d"], 1000 + 10 + 20 + 30 + 40)
        self.assertEqual(flow["net_10d"], 1000 + sum((i + 1) * 10 for i in range(9)))
        self.assertEqual(flow["days_available"], 10)
        self.assertEqual(flow["unit"], "shares")
        self.assertEqual(flow["date"], ANCHOR)
        # Weekends are never requested; the walk stops once 9 earlier days are loaded.
        self.assertEqual(calendar.requested, days)

    def test_holiday_is_skipped_not_counted(self):
        days = _weekdays_before(ANCHOR, 10)
        holiday = days[1]
        responses = {day: _payload(day, {"2330": 1}) for day in days if day != holiday}
        flow, calendar, _ = _flow(responses)
        self.assertIn(holiday, calendar.requested)
        self.assertEqual(flow["days_available"], 10)
        self.assertEqual(flow["net_5d"], 1004)
        self.assertEqual(flow["net_10d"], 1009)

    def test_stock_absent_from_a_trading_day_counts_as_zero(self):
        days = _weekdays_before(ANCHOR, 9)
        responses = {day: _payload(day, {"2317": 99}) for day in days}  # 2330 not listed
        flow, _, _ = _flow(responses)
        self.assertEqual(flow["net_5d"], 1000)
        self.assertEqual(flow["net_10d"], 1000)

    def test_negative_flows_keep_their_sign(self):
        days = _weekdays_before(ANCHOR, 9)
        responses = {day: _payload(day, {"2330": -500}) for day in days}
        flow, _, _ = _flow(responses, record=dict(_ANCHOR_RECORD, total_net=-1000))
        self.assertEqual(flow["net_5d"], -3000)
        self.assertEqual(flow["net_10d"], -5500)

    def test_failure_mid_walk_reports_only_complete_windows(self):
        days = _weekdays_before(ANCHOR, 9)
        responses = {day: _payload(day, {"2330": 1}) for day in days}
        responses[days[6]] = RuntimeError("WAF page")  # 7th earlier day unknown
        flow, calendar, _ = _flow(responses)
        self.assertEqual(flow["net_5d"], 1004)        # 5-day window is complete
        self.assertIsNone(flow["net_10d"])            # never a partial 10-day sum
        self.assertEqual(flow["days_available"], 7)
        self.assertEqual(calendar.requested[-1], days[6])  # walk stopped at the gap

    def test_unexpected_body_or_wrong_served_date_stops_the_walk(self):
        days = _weekdays_before(ANCHOR, 9)
        for bad in ({"stat": "系統忙碌"}, _payload("20200101", {"2330": 1}), {"stat": "OK", "date": days[0]}, "html"):
            responses = {day: _payload(day, {"2330": 1}) for day in days}
            responses[days[0]] = bad
            flow, _, _ = _flow(responses)
            self.assertEqual(flow["days_available"], 1, bad)
            self.assertIsNone(flow["net_5d"])
            self.assertIsNone(flow["net_10d"])
            self.assertEqual(flow["net_1d"], 1000)

    def test_tpex_returns_latest_day_only_without_any_request(self):
        record = dict(_ANCHOR_RECORD, stock_code="6488", market="上櫃", source="TPEx-OpenAPI")
        flow, calendar, _ = _flow({}, code="6488.TWO", record=record)
        self.assertEqual(flow["net_1d"], 1000)
        self.assertIsNone(flow["net_5d"])
        self.assertEqual(calendar.requested, [])

    def test_non_tw_code_and_missing_record_fail_open(self):
        fetcher = TwInstitutionalFetcher(min_request_interval=0)
        self.assertIsNone(fetcher.get_institutional_flow("AAPL", dict(_ANCHOR_RECORD)))
        self.assertIsNone(fetcher.get_institutional_flow("2330.TW", {"date": ANCHOR}))
        with patch.object(fetcher, "get_institutional_net", return_value=None):
            self.assertIsNone(fetcher.get_institutional_flow("2330.TW"))

    def test_history_is_walked_once_and_shared_across_stocks(self):
        days = _weekdays_before(ANCHOR, 9)
        responses = {day: _payload(day, {"2330": 1, "2317": 2}) for day in days}
        fetcher = TwInstitutionalFetcher(min_request_interval=0)
        calendar = _Calendar(responses)
        with patch.object(fetcher, "_get_json", side_effect=calendar):
            first = fetcher.get_institutional_flow("2330.TW", dict(_ANCHOR_RECORD), wait_seconds=5)
            second = fetcher.get_institutional_flow(
                "2317.TW", dict(_ANCHOR_RECORD, stock_code="2317", total_net=-10), wait_seconds=5
            )
        self.assertEqual(len(calendar.requested), 9)
        self.assertEqual(first["net_10d"], 1009)
        self.assertEqual(second["net_10d"], -10 + 18)

    def test_zero_wait_returns_latest_day_without_blocking(self):
        fetcher = TwInstitutionalFetcher(min_request_interval=0)
        with patch.object(fetcher, "_walk_history"):  # walker never finishes
            flow = fetcher.get_institutional_flow("2330.TW", dict(_ANCHOR_RECORD), wait_seconds=0)
        self.assertEqual(flow["net_1d"], 1000)
        self.assertIsNone(flow["net_5d"])


if __name__ == "__main__":
    unittest.main()
