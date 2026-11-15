# -*- coding: utf-8 -*-
"""TwSectorFetcher — Taiwan (TWSE 上市) 類股漲跌排行 + 類股成交比重.

Data-layer only, ``tw``-only, strictly additive and fail-open. It mirrors the
conventions of ``tw_institutional_fetcher.py``: official open data, no API key,
whole-table cache, circuit breaker, and "never raise into the caller".

Sources (政府開放資料, OGDL v1):
  - 類股指數漲跌: TWSE OpenAPI ``/v1/exchangeReport/MI_INDEX``
    list of ``{"日期": "1151113", "指數": "半導體類指數", "收盤指數": "1480.33",
    "漲跌": "+", "漲跌點數": "17.92", "漲跌百分比": "1.23", ...}``
    (date is 民國; ``漲跌百分比`` carries its own minus sign).
  - 類股成交金額: TWSE ``/rwd/zh/afterTrading/BFIAMU?response=json``
    ``fields = ["分類指數名稱", "成交股數", "成交金額", "成交筆數", "漲跌指數"]``
    (date is 西元 ``YYYYMMDD``; names are space-padded; values comma-formatted).

Scope notes:
  - Covers the TWSE (上市) industry classification only. TPEx (上櫃) 類股 are not
    included.
  - ``amount_share_pct`` is each sector's share of the summed leaf-sector
    turnover (成交比重). It is a *where is the money trading* measure, NOT a net
    inflow: Taiwan publishes no per-sector net capital flow.
  - Composite parents (電子工業 / 化學生技醫療 ...) are excluded from the ranking
    so a parent never shadows its own children.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from data_provider.realtime_types import CircuitBreaker
from data_provider.tw_institutional_fetcher import minguo_to_ad

logger = logging.getLogger(__name__)

_MI_INDEX_URL = "https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX"
_BFIAMU_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BFIAMU"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

_SECTOR_SUFFIX = "類指數"
# Leveraged / inverse / total-return variants share the 類 naming but are not sectors.
_NON_SECTOR_MARKERS = ("報酬", "槓桿", "反向", "兩倍", "一倍")
# Composite parents whose members are ranked individually.
_COMPOSITE_SECTORS = frozenset(
    {"水泥窯製", "塑膠化工", "機電", "化學生技醫療", "電子工業"}
)

_BFIAMU_NAME = "分類指數名稱"
_BFIAMU_AMOUNT = "成交金額"


def _to_float(value: Any) -> Optional[float]:
    """Parse a TWSE numeric cell; empty / ``--`` / non-numeric -> ``None``."""
    try:
        text = str(value).replace(",", "").replace(" ", "").strip()
    except (TypeError, ValueError):
        return None
    if text in ("", "-", "--", "—"):
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if number == number and abs(number) != float("inf") else None


def sector_name_of(index_name: Any) -> Optional[str]:
    """``"半導體類指數"`` -> ``"半導體"``; anything that is not a leaf sector -> ``None``."""
    text = str(index_name or "").replace("　", " ").strip()
    if not text.endswith(_SECTOR_SUFFIX):
        return None
    if any(marker in text for marker in _NON_SECTOR_MARKERS):
        return None
    name = text[: -len(_SECTOR_SUFFIX)].strip()
    if not name or name in _COMPOSITE_SECTORS:
        return None
    return name


class TwSectorFetcher:
    """Fetch TWSE 類股 daily change (%) and turnover share."""

    name = "TwSectorFetcher"

    def __init__(self, *, cache_ttl_seconds: int = 900, timeout: int = 10) -> None:
        self._cache_ttl = cache_ttl_seconds
        self._timeout = timeout
        self._lock = threading.Lock()
        self._fetch_lock = threading.Lock()
        self._table: List[Dict[str, Any]] = []
        self._table_at = 0.0
        self._breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=300.0)

    # ------------------------------------------------------------------ public
    def get_sector_table(self) -> List[Dict[str, Any]]:
        """Return every leaf sector as a dict, or ``[]`` on any failure (fail-open).

        Each item: ``{"name", "change_pct", "close", "date", "source"}`` plus
        ``"amount"`` (TWD) / ``"amount_share_pct"`` when BFIAMU is available for
        the same trading day.
        """
        cached = self._read_cache()
        if cached is not None:
            return cached
        with self._fetch_lock:  # coalesce concurrent callers into one upstream fetch
            cached = self._read_cache()
            if cached is not None:
                return cached
            if not self._breaker.is_available("mi_index"):
                logger.info("[tw-sector] MI_INDEX circuit OPEN -> skip fetch, fail-open")
                return []
            try:
                table = self._fetch_changes()
            except Exception as exc:  # noqa: BLE001 - fail-open by contract
                self._breaker.record_failure("mi_index", str(exc))
                logger.info("[tw-sector] MI_INDEX fetch failed: %s", exc)
                return []
            self._breaker.record_success("mi_index")
            if not table:
                return []
            self._merge_turnover(table)
            with self._lock:
                self._table = table
                self._table_at = time.time()
            return [dict(item) for item in table]

    def get_sector_rankings(self, n: int = 5) -> Optional[Tuple[List[Dict], List[Dict]]]:
        """(領漲 top-n, 領跌 top-n) by ``change_pct``; ``None`` when no data.

        Same return contract as the A-share ``get_sector_rankings`` fetchers, so
        existing consumers (``item["name"]`` / ``item["change_pct"]``) work as is.
        """
        table = self.get_sector_table()
        if not table:
            return None
        ordered = sorted(table, key=lambda item: item["change_pct"], reverse=True)
        count = max(0, int(n))
        top = ordered[:count]
        bottom = list(reversed(ordered))[:count]
        return top, bottom

    def get_turnover_rankings(self, n: int = 5) -> List[Dict[str, Any]]:
        """Sectors with the largest share of turnover (成交比重), or ``[]``."""
        table = [item for item in self.get_sector_table() if item.get("amount_share_pct") is not None]
        table.sort(key=lambda item: item["amount_share_pct"], reverse=True)
        return table[: max(0, int(n))]

    # ----------------------------------------------------------------- internal
    def _read_cache(self) -> Optional[List[Dict[str, Any]]]:
        with self._lock:
            if self._table and (time.time() - self._table_at) < self._cache_ttl:
                return [dict(item) for item in self._table]
        return None

    def _get_json(self, url: str, params: Optional[dict] = None) -> Any:
        resp = requests.get(
            url,
            params=params,
            headers={"User-Agent": _UA, "Accept": "application/json"},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _fetch_changes(self) -> List[Dict[str, Any]]:
        payload = self._get_json(_MI_INDEX_URL)
        return self.parse_mi_index(payload)

    @staticmethod
    def parse_mi_index(payload: Any) -> List[Dict[str, Any]]:
        """Parse the MI_INDEX OpenAPI list into leaf-sector rows (pure function)."""
        if not isinstance(payload, list):
            return []
        rows: List[Dict[str, Any]] = []
        seen = set()
        for raw in payload:
            if not isinstance(raw, dict):
                continue
            name = sector_name_of(raw.get("指數"))
            if name is None or name in seen:
                continue
            change_pct = _to_float(raw.get("漲跌百分比"))
            if change_pct is None:  # missing / unparseable -> drop, never a fabricated 0
                continue
            # 漲跌百分比 is signed in the feed; still reconcile with the explicit
            # direction column so a format change cannot flip a sector's sign.
            direction = str(raw.get("漲跌") or "").strip()
            if direction == "-" and change_pct > 0:
                change_pct = -change_pct
            elif direction == "+" and change_pct < 0:
                change_pct = -change_pct
            seen.add(name)
            rows.append(
                {
                    "name": name,
                    "change_pct": change_pct,
                    "close": _to_float(raw.get("收盤指數")),
                    "date": minguo_to_ad(raw.get("日期", "")),
                    "source": "TWSE-MI_INDEX",
                }
            )
        return rows

    def _merge_turnover(self, table: List[Dict[str, Any]]) -> None:
        """Best-effort: attach 成交金額 / 成交比重 from BFIAMU (same trading day only)."""
        if not self._breaker.is_available("bfiamu"):
            return
        try:
            payload = self._get_json(_BFIAMU_URL, {"response": "json"})
        except Exception as exc:  # noqa: BLE001 - turnover is optional
            self._breaker.record_failure("bfiamu", str(exc))
            logger.info("[tw-sector] BFIAMU fetch failed (change ranking kept): %s", exc)
            return
        self._breaker.record_success("bfiamu")
        amounts, amount_date = self.parse_bfiamu(payload)
        if not amounts:
            return
        table_dates = {item.get("date") for item in table}
        if amount_date is None or table_dates != {amount_date}:
            # Different trading days (one feed updated before the other) -> do not mix.
            logger.info("[tw-sector] BFIAMU date %s != MI_INDEX %s -> skip turnover", amount_date, table_dates)
            return
        total = sum(amounts.get(item["name"], 0.0) for item in table)
        if total <= 0:
            return
        for item in table:
            amount = amounts.get(item["name"])
            if amount is None:
                continue
            item["amount"] = amount
            item["amount_share_pct"] = round(amount / total * 100, 2)

    @staticmethod
    def parse_bfiamu(payload: Any) -> Tuple[Dict[str, float], Optional[str]]:
        """Parse BFIAMU into ``({sector_name: 成交金額}, 西元 date)`` (pure function)."""
        if not isinstance(payload, dict) or payload.get("stat") != "OK":
            return {}, None
        fields = payload.get("fields")
        rows = payload.get("data")
        if not isinstance(fields, list) or not isinstance(rows, list):
            return {}, None
        try:
            name_idx = fields.index(_BFIAMU_NAME)
            amount_idx = fields.index(_BFIAMU_AMOUNT)
        except ValueError:  # column renamed -> fail-open
            return {}, None
        amounts: Dict[str, float] = {}
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= max(name_idx, amount_idx):
                continue
            name = sector_name_of(row[name_idx])
            amount = _to_float(row[amount_idx])
            if name is None or amount is None:
                continue
            amounts[name] = amount
        date_text = str(payload.get("date") or "").strip()
        ad_date = date_text if (date_text.isdigit() and len(date_text) == 8) else None
        return amounts, ad_date


_shared_fetcher: Optional[TwSectorFetcher] = None
_shared_lock = threading.Lock()


def get_tw_sector_fetcher() -> TwSectorFetcher:
    """Process-wide fetcher so stock analysis and market review share one cache."""
    global _shared_fetcher
    with _shared_lock:
        if _shared_fetcher is None:
            _shared_fetcher = TwSectorFetcher()
        return _shared_fetcher
