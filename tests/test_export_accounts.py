"""Pure-function tests for scripts/export_accounts.py.

Covers the tab-name → dashboard-month-key parser and the collision
detection in discover_drilldown_tabs. Does not exercise gspread."""

import sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from export_accounts import (  # noqa: E402
    discover_drilldown_tabs,
    parse_tab_to_month_key,
)


# ─── parse_tab_to_month_key ──────────────────────────────────────────────────

def test_canonical_3letter_data_uppercase():
    # Mar'26 + 6 months back → 2025-09
    assert parse_tab_to_month_key("Mar'26 Invoice Data") == "2025-09"


def test_canonical_3letter_oct_nov_2026():
    # The two missing-on-live-dashboard cases the issue is about.
    assert parse_tab_to_month_key("Oct'26 Invoice Data") == "2026-04"
    assert parse_tab_to_month_key("Nov'26 Invoice Data") == "2026-05"


def test_legacy_lowercase_data():
    # Feb'26 + 6 months back → 2025-08
    assert parse_tab_to_month_key("Feb'26 Invoice data") == "2025-08"
    assert parse_tab_to_month_key("May'26 Invoice data") == "2025-11"


def test_legacy_full_month_name():
    # April'26 + 6 months back → 2025-10
    assert parse_tab_to_month_key("April'26 Invoice Data") == "2025-10"


def test_year_boundary():
    # Jan'27 + 6 months back → 2026-07
    assert parse_tab_to_month_key("Jan'27 Invoice Data") == "2026-07"
    # Mar'27 + 6 months back → 2026-09
    assert parse_tab_to_month_key("Mar'27 Invoice Data") == "2026-09"


def test_unrelated_tab_silently_skipped(caplog):
    # No warning for unrelated tab names — they just don't match.
    caplog.clear()
    assert parse_tab_to_month_key("DASHBOARD") is None
    assert parse_tab_to_month_key("MASTER_BOOK") is None
    assert parse_tab_to_month_key("PRICING") is None
    assert parse_tab_to_month_key("SALES") is None
    assert caplog.records == []


def test_typo_in_month_logs_warning_and_returns_none(caplog):
    # Looks Invoice-Data-shaped but month is unrecognised → warn + skip,
    # don't crash the run.
    import logging
    caplog.set_level(logging.WARNING)
    result = parse_tab_to_month_key("Octobr'26 Invoice Data")
    assert result is None
    assert any("unrecognised" in r.message.lower() for r in caplog.records)


def test_uppercase_data_rejected():
    # Spec accepts Data and data, not DATA.
    assert parse_tab_to_month_key("Apr'26 Invoice DATA") is None


def test_extra_whitespace_rejected():
    # Tabs in the master sheet use exactly one space — keep parser strict
    # so subtle whitespace drift surfaces as a warning instead of silent acceptance.
    assert parse_tab_to_month_key("Apr'26  Invoice Data") is None
    assert parse_tab_to_month_key(" Apr'26 Invoice Data") is None


# ─── discover_drilldown_tabs ─────────────────────────────────────────────────

def _fake_spreadsheet(tab_titles):
    """Minimal stub: spreadsheet.worksheets() returns a list of objects with .title."""
    sheets = [SimpleNamespace(title=t) for t in tab_titles]
    return SimpleNamespace(worksheets=lambda: sheets)


def test_discovery_returns_sorted_dict():
    ss = _fake_spreadsheet([
        "DASHBOARD",
        "Nov'26 Invoice Data",
        "Feb'26 Invoice data",
        "Apr'26 Invoice Data",
        "MASTER_BOOK",
    ])
    discovered, collisions = discover_drilldown_tabs(ss)
    assert collisions == []
    assert list(discovered.keys()) == ["2025-08", "2025-10", "2026-05"]
    assert discovered["2025-08"] == "Feb'26 Invoice data"
    assert discovered["2025-10"] == "Apr'26 Invoice Data"
    assert discovered["2026-05"] == "Nov'26 Invoice Data"


def test_discovery_handles_full_set_through_nov26():
    # Live state per the brief: Feb'26 through Nov'26 exist, with three
    # legacy variants in the historical positions.
    ss = _fake_spreadsheet([
        "Feb'26 Invoice data",        # legacy
        "Mar'26 Invoice Data",
        "April'26 Invoice Data",      # legacy
        "May'26 Invoice data",        # legacy
        "Jun'26 Invoice Data",
        "Jul'26 Invoice Data",
        "Aug'26 Invoice Data",
        "Sep'26 Invoice Data",
        "Oct'26 Invoice Data",        # newly visible
        "Nov'26 Invoice Data",        # newly visible
        "DASHBOARD",
        "PRICING",
    ])
    discovered, collisions = discover_drilldown_tabs(ss)
    assert collisions == []
    assert list(discovered.keys()) == [
        "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
        "2026-01", "2026-02", "2026-03", "2026-04", "2026-05",
    ]


def test_collision_returned_not_resolved():
    # If both the legacy and canonical variant of the same month exist,
    # collision is reported. discover_drilldown_tabs does not pick a winner.
    ss = _fake_spreadsheet([
        "April'26 Invoice Data",      # legacy → 2025-10
        "Apr'26 Invoice Data",        # canonical → also 2025-10
        "Mar'26 Invoice Data",        # → 2025-09 (no collision)
    ])
    discovered, collisions = discover_drilldown_tabs(ss)
    assert len(collisions) == 1
    key, first, dup = collisions[0]
    assert key == "2025-10"
    assert {first, dup} == {"April'26 Invoice Data", "Apr'26 Invoice Data"}
    # The non-colliding tab is still in `discovered`.
    assert discovered["2025-09"] == "Mar'26 Invoice Data"


def test_empty_spreadsheet():
    ss = _fake_spreadsheet([])
    discovered, collisions = discover_drilldown_tabs(ss)
    assert discovered == {}
    assert collisions == []
