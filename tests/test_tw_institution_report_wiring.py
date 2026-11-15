# -*- coding: utf-8 -*-
"""v2 report-wiring tests: tw 三大法人 (institutional flows) into the offshore institution block.

Pins the maintainer-confirmed contract for issue #1777 v2:
  - tw with data        -> institution coverage 'ok', raw net figures surfaced.
  - tw fetch-failed/None -> institution stays 'not_supported' (fail-open, main flow alive).
  - us/hk/jp/kr          -> institution stays 'not_supported' AND the tw fetcher is never
                            called (strictly-additive: other markets byte-identical).
  - tw institution data carries RAW figures only — no capital_flow_signal / score / schema.

Mirrors tests/test_fundamental_context.py's offshore pattern; fully offline (no network),
so it runs under the blocking backend gate (`pytest -m "not network"`).
"""

import os
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.base import DataFetcherManager

_TW_FETCHER_METHOD = (
    "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher.get_institutional_net"
)

# Shape mirrors TwInstitutionalFetcher._build_record (real 2330 @ 20260629).
_FAKE_REC = {
    "stock_code": "2330",
    "date": "20260629",
    "market": "上市",
    "source": "TWSE-T86",
    "unit": "shares",
    "foreign_net": -1912490,
    "trust_net": 919216,
    "dealer_net": 996850,
    "total_net": 3576,
}

_OFFSHORE_CFG = SimpleNamespace(
    enable_fundamental_pipeline=True,
    fundamental_cache_ttl_seconds=0,
    fundamental_stage_timeout_seconds=1.5,
    fundamental_fetch_timeout_seconds=0.8,
    fundamental_retry_max=1,
)

_EMPTY_BUNDLE = {
    "status": "not_supported",
    "growth": {},
    "earnings": {},
    "belong_boards": [],
    "source_chain": [],
    "errors": [],
}


class TestTwInstitutionReportWiring(unittest.TestCase):
    def setUp(self):
        # Keep every test in this file offline: the tw path now also reads the 類股
        # table and may start the multi-day 三大法人 history walker.
        self._sector_patch = patch(
            "data_provider.tw_sector_fetcher.TwSectorFetcher.get_sector_table", return_value=[]
        )
        self._history_patch = patch(
            "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher._walk_history",
            side_effect=lambda anchor, state: state["done"].set(),
        )
        self.sector_mock = self._sector_patch.start()
        self._history_patch.start()
        self.addCleanup(self._sector_patch.stop)
        self.addCleanup(self._history_patch.stop)

    def _context(self, code, institutional_return=None, institutional_side_effect=None):
        """Run get_fundamental_context(code) offline; returns (ctx, tw_fetcher_mock)."""
        manager = DataFetcherManager(fetchers=[])
        kwargs = {}
        if institutional_side_effect is not None:
            kwargs["side_effect"] = institutional_side_effect
        else:
            kwargs["return_value"] = institutional_return
        with patch("src.config.get_config", return_value=_OFFSHORE_CFG), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch(
                    "data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                    return_value=_EMPTY_BUNDLE,
                ), \
                patch(_TW_FETCHER_METHOD, **kwargs) as tw_mock:
            ctx = manager.get_fundamental_context(code)
        return ctx, tw_mock

    # ---- tw with data: institution surfaces the raw net figures --------------------
    def test_tw_institution_populated_when_fetcher_has_data(self):
        ctx, tw_mock = self._context("2330.TW", institutional_return=dict(_FAKE_REC))
        self.assertEqual(ctx["market"], "tw")
        self.assertEqual(ctx["coverage"].get("institution"), "ok")
        data = ctx["institution"]["data"]
        self.assertEqual(data["foreign_net"], -1912490)
        self.assertEqual(data["trust_net"], 919216)
        self.assertEqual(data["dealer_net"], 996850)
        self.assertEqual(data["total_net"], 3576)
        self.assertEqual(data["unit"], "shares")
        self.assertEqual(data["source"], "TWSE-T86")
        # dragon_tiger has no Taiwan equivalent; boards stays not_supported when the
        # 類股 table is unavailable (stubbed empty in setUp).
        for block in ("dragon_tiger", "boards"):
            self.assertEqual(ctx["coverage"].get(block), "not_supported")
        # capital_flow is derived from the same 三大法人 record: latest day only here,
        # because no earlier trading days were loaded -> 5d / 10d stay None (never a
        # partial sum).
        self.assertEqual(ctx["coverage"].get("capital_flow"), "ok")
        stock_flow = ctx["capital_flow"]["data"]["stock_flow"]
        self.assertEqual(stock_flow["main_net_inflow"], 3576)
        self.assertIsNone(stock_flow["inflow_5d"])
        self.assertIsNone(stock_flow["inflow_10d"])
        self.assertEqual(stock_flow["unit"], "shares")
        self.assertEqual(stock_flow["basis"], "tw_institutional_net")
        self.assertEqual(ctx["capital_flow"]["data"]["sector_rankings"], {"top": [], "bottom": []})
        # institution data must surface: the top-level status (which consumers key off)
        # is not 'not_supported' even though valuation/growth/earnings are unavailable.
        self.assertNotEqual(ctx["status"], "not_supported")
        tw_mock.assert_called_with("2330.TW")

    # ---- tw genuine-zero day is kept (record present, nets 0) ----------------------
    def test_tw_institution_keeps_genuine_zero(self):
        zero_rec = dict(_FAKE_REC, foreign_net=0, trust_net=0, dealer_net=0, total_net=0)
        ctx, _ = self._context("6488.TWO", institutional_return=zero_rec)
        self.assertEqual(ctx["coverage"].get("institution"), "ok")
        self.assertEqual(ctx["institution"]["data"]["foreign_net"], 0)
        self.assertEqual(ctx["institution"]["data"]["total_net"], 0)

    # ---- tw fail-open: None -> not_supported, no raise -----------------------------
    def test_tw_institution_fail_open_when_fetcher_returns_none(self):
        ctx, _ = self._context("2330.TW", institutional_return=None)
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
        self.assertEqual(ctx["institution"].get("data"), {})

    # ---- tw fail-open: fetcher raises -> not_supported, main flow uninterrupted -----
    def test_tw_institution_fail_open_when_fetcher_raises(self):
        ctx, _ = self._context("2330.TW", institutional_side_effect=RuntimeError("boom"))
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
        self.assertEqual(ctx["institution"].get("data"), {})
        # the rest of the context still built (no exception bubbled out)
        self.assertEqual(ctx["market"], "tw")

    # ---- strictly-additive: us is byte-identical AND the tw fetcher is never called -
    def test_us_institution_unchanged_and_tw_fetcher_not_called(self):
        ctx, tw_mock = self._context("AAPL", institutional_return=dict(_FAKE_REC))
        self.assertEqual(ctx["market"], "us")
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
        self.assertEqual(ctx["institution"].get("data"), {})
        self.assertEqual(tw_mock.call_count, 0)

    # ---- B2: every other offshore market (hk/jp/kr) untouched + fetcher unused ------
    def test_other_offshore_markets_institution_unchanged(self):
        for code, market in (("0700.HK", "hk"), ("7203.T", "jp"), ("005930.KS", "kr")):
            ctx, tw_mock = self._context(code, institutional_return=dict(_FAKE_REC))
            self.assertEqual(ctx["market"], market, f"{code} routed to {ctx['market']}")
            self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
            self.assertEqual(tw_mock.call_count, 0)

    # ---- fail-open on a fetcher WIRING/init failure (not just a fetch failure) ------
    def test_tw_institution_fail_open_when_fetcher_init_raises(self):
        manager = DataFetcherManager(fetchers=[])
        with patch("src.config.get_config", return_value=_OFFSHORE_CFG), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch(
                    "data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                    return_value=_EMPTY_BUNDLE,
                ), \
                patch(
                    "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher",
                    side_effect=RuntimeError("init boom"),
                ):
            ctx = manager.get_fundamental_context("2330.TW")  # must NOT raise
        self.assertEqual(ctx["market"], "tw")
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
        self.assertEqual(ctx["institution"].get("data"), {})

    # ---- a record missing a core net field is NOT shown as a clean 'ok' -------------
    def test_tw_institution_not_ok_when_core_net_missing(self):
        broken = dict(_FAKE_REC, foreign_net=None)  # a core component missing
        ctx, _ = self._context("2330.TW", institutional_return=broken)
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
        self.assertEqual(ctx["institution"].get("data"), {})

    # ---- a slow fetch must NOT push the analysis past the fundamental stage budget ---
    def test_tw_institution_fetch_respects_stage_timeout(self):
        slow_cfg = SimpleNamespace(
            enable_fundamental_pipeline=True,
            fundamental_cache_ttl_seconds=0,
            fundamental_stage_timeout_seconds=0.3,
            fundamental_fetch_timeout_seconds=0.3,
            fundamental_retry_max=1,
        )
        manager = DataFetcherManager(fetchers=[])

        def _slow(_code):
            time.sleep(2.0)  # simulate a slow / rate-limited TWSE-TPEx call
            return dict(_FAKE_REC)

        start = time.time()
        with patch("src.config.get_config", return_value=slow_cfg), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch(
                    "data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                    return_value=_EMPTY_BUNDLE,
                ), \
                patch(_TW_FETCHER_METHOD, side_effect=_slow):
            ctx = manager.get_fundamental_context("2330.TW")
        elapsed = time.time() - start
        # the 2s fetch must be abandoned at the ~0.3s stage budget, not block the analysis
        self.assertLess(elapsed, 1.5, f"institution fetch ignored the stage timeout ({elapsed:.2f}s)")
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")

    # ---- negative: institution data carries RAW figures only, no derived signal ----
    def test_tw_institution_data_has_no_derived_signal_or_score(self):
        ctx, _ = self._context("2330.TW", institutional_return=dict(_FAKE_REC))
        data = ctx["institution"]["data"]
        self.assertEqual(
            set(data.keys()),
            {"foreign_net", "trust_net", "dealer_net", "total_net", "unit", "date", "source"},
        )
        forbidden = ("capital_flow_signal", "signal", "score", "weight", "normalized", "rating")
        for key in forbidden:
            self.assertNotIn(key, data, f"derived key '{key}' leaked into institution data")


    # ---- institution must use the STAGE budget, not the small per-symbol fetch cap -----
    def test_tw_institution_not_starved_by_small_fetch_timeout(self):
        # The 三大法人 block is a whole-market download (~4-5s). It must run under the
        # remaining STAGE budget, not the (small) per-symbol fetch_timeout — otherwise the
        # first/only stock of a run coin-flips to not_supported. A fetch slower than
        # fetch_timeout but within the stage budget must still land as 'ok'.
        cfg = SimpleNamespace(
            enable_fundamental_pipeline=True,
            fundamental_cache_ttl_seconds=0,
            fundamental_stage_timeout_seconds=8.0,   # generous stage budget
            fundamental_fetch_timeout_seconds=0.3,   # tiny per-symbol cap (would starve institution)
            fundamental_retry_max=1,
        )
        manager = DataFetcherManager(fetchers=[])

        def _slowish(_code):
            time.sleep(1.0)  # > fetch_timeout(0.3) but << stage budget(8) -> must complete
            return dict(_FAKE_REC)

        with patch("src.config.get_config", return_value=cfg), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch(
                    "data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                    return_value=_EMPTY_BUNDLE,
                ), \
                patch(_TW_FETCHER_METHOD, side_effect=_slowish):
            ctx = manager.get_fundamental_context("2330.TW")
        self.assertEqual(ctx["coverage"].get("institution"), "ok")
        self.assertEqual(ctx["institution"]["data"]["total_net"], _FAKE_REC["total_net"])

    # ---- FUNDAMENTAL_FETCH_TIMEOUT_SECONDS=0 disables per-fetch fetches — incl institution
    def test_tw_institution_disabled_when_fetch_timeout_zero(self):
        # fetch_timeout=0 is the existing "disable per-fetch fundamental fetches" config
        # (valuation/bundle skip). Institution must honour it too, not bypass it via the
        # stage budget.
        cfg = SimpleNamespace(
            enable_fundamental_pipeline=True,
            fundamental_cache_ttl_seconds=0,
            fundamental_stage_timeout_seconds=8.0,
            fundamental_fetch_timeout_seconds=0.0,   # disabled
            fundamental_retry_max=1,
        )
        manager = DataFetcherManager(fetchers=[])
        with patch("src.config.get_config", return_value=cfg), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch(
                    "data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                    return_value=_EMPTY_BUNDLE,
                ), \
                patch(_TW_FETCHER_METHOD, return_value=dict(_FAKE_REC)) as tw_mock:
            ctx = manager.get_fundamental_context("2330.TW")
        self.assertEqual(ctx["coverage"].get("institution"), "not_supported")
        self.assertEqual(tw_mock.call_count, 0)  # institution fetch skipped when disabled

    # ---- tw 類股排行 -> boards; 三大法人多日累計 -> capital_flow ----------------------
    def test_tw_boards_and_multi_day_flow_populated(self):
        table = [
            {"name": "金融保險", "change_pct": 2.16, "amount_share_pct": 15.0},
            {"name": "半導體", "change_pct": 1.23, "amount_share_pct": 60.0},
            {"name": "航運", "change_pct": -1.48},
        ]
        self.sector_mock.return_value = table
        flow = {"date": "20260629", "unit": "shares", "source": "TWSE-T86",
                "net_1d": 3576, "net_5d": -120000, "net_10d": 450000, "days_available": 10}
        with patch(
            "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher.get_institutional_flow",
            return_value=flow,
        ):
            ctx, _ = self._context("2330.TW", institutional_return=dict(_FAKE_REC))
        self.assertEqual(ctx["coverage"].get("boards"), "ok")
        boards = ctx["boards"]["data"]
        self.assertEqual([item["name"] for item in boards["top"]], ["金融保險", "半導體", "航運"])
        self.assertEqual(boards["bottom"][0]["name"], "航運")
        self.assertEqual([item["name"] for item in boards["turnover_top"]], ["半導體", "金融保險"])
        self.assertEqual(ctx["coverage"].get("capital_flow"), "ok")
        stock_flow = ctx["capital_flow"]["data"]["stock_flow"]
        self.assertEqual(
            (stock_flow["main_net_inflow"], stock_flow["inflow_5d"], stock_flow["inflow_10d"]),
            (3576, -120000, 450000),
        )
        self.assertEqual(ctx["coverage"].get("dragon_tiger"), "not_supported")
        self.assertEqual(ctx["status"], "ok")

    def test_tw_capital_flow_stays_not_supported_without_institution_record(self):
        ctx, _ = self._context("2330.TW", institutional_return=None)
        self.assertEqual(ctx["coverage"].get("capital_flow"), "not_supported")
        self.assertEqual(ctx["capital_flow"]["data"], {})

    def test_tw_flow_summary_failure_falls_back_to_latest_day(self):
        with patch(
            "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher.get_institutional_flow",
            side_effect=RuntimeError("boom"),
        ):
            ctx, _ = self._context("2330.TW", institutional_return=dict(_FAKE_REC))
        stock_flow = ctx["capital_flow"]["data"]["stock_flow"]
        self.assertEqual(stock_flow["main_net_inflow"], 3576)
        self.assertIsNone(stock_flow["inflow_5d"])

    def test_non_tw_markets_never_touch_the_tw_sector_or_flow_sources(self):
        with patch(
            "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher.get_institutional_flow"
        ) as flow_mock:
            for code in ("AAPL", "hk00700", "7203.T", "005930.KS"):
                ctx, _ = self._context(code, institutional_return=dict(_FAKE_REC))
                self.assertEqual(ctx["coverage"].get("capital_flow"), "not_supported", code)
                self.assertEqual(ctx["coverage"].get("boards"), "not_supported", code)
        self.assertEqual(flow_mock.call_count, 0)
        self.assertEqual(self.sector_mock.call_count, 0)

    def test_agent_capital_flow_tool_path_supports_tw(self):
        manager = DataFetcherManager(fetchers=[])
        flow = {"net_1d": 3576, "net_5d": 10, "net_10d": -20, "days_available": 10}
        with patch("src.config.get_config", return_value=_OFFSHORE_CFG), \
                patch(_TW_FETCHER_METHOD, return_value=dict(_FAKE_REC)), \
                patch(
                    "data_provider.tw_institutional_fetcher.TwInstitutionalFetcher.get_institutional_flow",
                    return_value=flow,
                ):
            block = manager.get_capital_flow_context("2330.TW")
            us_block = manager.get_capital_flow_context("AAPL")
        self.assertEqual(block["status"], "ok")
        self.assertEqual(block["data"]["stock_flow"]["inflow_10d"], -20)
        self.assertEqual(block["data"]["stock_flow"]["basis"], "tw_institutional_net")
        self.assertEqual(us_block["status"], "not_supported")


if __name__ == "__main__":
    unittest.main()
