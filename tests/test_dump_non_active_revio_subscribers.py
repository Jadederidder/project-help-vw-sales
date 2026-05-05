"""Pure-function tests for scripts/dump_non_active_revio_subscribers.py.

Four contracts under test:

  a) build_active_phone_index — phone → list of active templates.
     Empty / missing phones are dropped. Same phone on two templates
     yields a 2-element list (the cross-ref insurance for CS).

  b) classify_non_active — status string → bucket name. The friendly
     "paused" / "pending" / "inactive" labels live only here; status
     comparison is exact-string against the live API value
     `subscription_paused` (the trap PR #26 uncovered).

  c) annotate_with_active_elsewhere — non-active record + active phone
     index → mutates record with the Y/N + semicolon-joined templates.
     Empty phone short-circuits to N.

  d) build_summary_sheet_data — assembles the dict the Summary sheet
     painter consumes. Single source of truth for totals so the writer
     stays a dumb cell painter.

No tests against the live Revio API. No tests against Google Sheets.
End-to-end live exercise is the workflow_dispatch dry-run.
"""

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from datetime import date, datetime, timezone  # noqa: E402

from dump_non_active_revio_subscribers import (  # noqa: E402
    BUCKET_INACTIVE,
    BUCKET_PAUSED,
    BUCKET_PENDING,
    BUCKET_SKIP_ACTIVE,
    BUCKET_SKIP_UNKNOWN,
    _days_since,
    _iso_date_only,
    _to_date,
    annotate_with_active_elsewhere,
    build_active_phone_index,
    build_summary_sheet_data,
    classify_non_active,
)


# ─── (a) build_active_phone_index ────────────────────────────────────────────
class BuildActivePhoneIndex(unittest.TestCase):
    """Phone → list-of-templates index — the cross-ref backbone. The
    most important property is that the SAME phone showing up on TWO
    different active templates yields a 2-element list, because that's
    exactly the case where CS could otherwise cold-call someone who is
    paying on a different sign-up. Empty phones must be dropped or the
    index would conflate every phoneless active subscriber."""

    def test_three_actives_two_share_a_phone(self):
        """3 active clients, two on different templates with the same
        phone → dict has 2 keys (the unique phones), and the shared
        phone maps to a list with both template titles (sorted, deduped)."""
        records = [
            {"phone": "27821111111", "template_title": "VW Premium HELP Single"},
            {"phone": "27821111111", "template_title": "VW Premium HELP Family"},
            {"phone": "27822222222", "template_title": "VW Premium HELP Single"},
        ]
        idx = build_active_phone_index(records)
        self.assertEqual(set(idx.keys()), {"27821111111", "27822222222"})
        self.assertEqual(
            idx["27821111111"],
            ["VW Premium HELP Family", "VW Premium HELP Single"],  # sorted
        )
        self.assertEqual(idx["27822222222"], ["VW Premium HELP Single"])

    def test_empty_and_missing_phones_dropped(self):
        """Active records with "" or missing phone must NOT enter the
        index — keying on "" would falsely flag every phoneless
        non-active subscriber as 'active elsewhere'."""
        records = [
            {"phone": "",        "template_title": "Phoneless A"},
            {"phone": None,      "template_title": "Phoneless B"},
            {                    "template_title": "Phoneless C"},
            {"phone": "27820000000", "template_title": "Real One"},
        ]
        idx = build_active_phone_index(records)
        self.assertEqual(list(idx.keys()), ["27820000000"])

    def test_same_phone_same_template_dedupes(self):
        """If the same phone appears twice on the same template (data
        glitch / paginated double-read), the template title is listed
        once, not twice."""
        records = [
            {"phone": "27823333333", "template_title": "Tmpl X"},
            {"phone": "27823333333", "template_title": "Tmpl X"},
        ]
        idx = build_active_phone_index(records)
        self.assertEqual(idx["27823333333"], ["Tmpl X"])

    def test_empty_input(self):
        self.assertEqual(build_active_phone_index([]), {})


# ─── (b) classify_non_active ─────────────────────────────────────────────────
class ClassifyNonActive(unittest.TestCase):
    """The bucketing function. The critical contract: comparison is
    against the live API value `subscription_paused`, NOT the friendly
    label `paused`. PR #26 surfaced that drift the hard way."""

    def test_subscription_paused_buckets_as_paused(self):
        """The live API value is `subscription_paused`, not `paused` —
        if a future contributor "fixes" this to `paused`, every paused
        subscriber will silently fall into SKIP_unknown."""
        self.assertEqual(
            classify_non_active({"status": "subscription_paused"}),
            BUCKET_PAUSED,
        )

    def test_pending_buckets_as_pending(self):
        self.assertEqual(
            classify_non_active({"status": "pending"}),
            BUCKET_PENDING,
        )

    def test_inactive_buckets_as_inactive(self):
        self.assertEqual(
            classify_non_active({"status": "inactive"}),
            BUCKET_INACTIVE,
        )

    def test_active_buckets_as_skip_active(self):
        """Active subscribers are explicitly recognised + SKIPPED — they
        feed the cross-ref index, not the dump."""
        self.assertEqual(
            classify_non_active({"status": "active"}),
            BUCKET_SKIP_ACTIVE,
        )

    def test_unrecognised_status_buckets_as_skip_unknown(self):
        """Anything not in the four documented values (typo, drift,
        future state, missing key, None) → SKIP_unknown so the API
        drift surfaces in logs rather than silently joining a bucket."""
        self.assertEqual(
            classify_non_active({"status": "weird_new_value"}),
            BUCKET_SKIP_UNKNOWN,
        )
        self.assertEqual(
            classify_non_active({"status": None}),
            BUCKET_SKIP_UNKNOWN,
        )
        self.assertEqual(
            classify_non_active({}),  # status key missing entirely
            BUCKET_SKIP_UNKNOWN,
        )

    def test_friendly_label_does_not_match(self):
        """Defensive: the friendly label `paused` is NOT the API value
        and must NOT bucket as paused. Locks in the API-value-vs-label
        split that exists for human readability of the Excel cells."""
        self.assertEqual(
            classify_non_active({"status": "paused"}),
            BUCKET_SKIP_UNKNOWN,
        )


# ─── (c) annotate_with_active_elsewhere ──────────────────────────────────────
class AnnotateWithActiveElsewhere(unittest.TestCase):
    """In-place mutation of a dump record with the two cross-ref columns.
    Drives the Y/N column CS reads to decide whether to call."""

    def test_phone_not_in_index_marks_n_blank_list(self):
        rec = {"Phone": "27821110001"}
        annotate_with_active_elsewhere(rec, {})
        self.assertEqual(rec["Currently Active Elsewhere"], "N")
        self.assertEqual(rec["Active Elsewhere Templates"], "")

    def test_phone_in_index_with_one_template(self):
        idx = {"27821110002": ["VW Single"]}
        rec = {"Phone": "27821110002"}
        annotate_with_active_elsewhere(rec, idx)
        self.assertEqual(rec["Currently Active Elsewhere"], "Y")
        self.assertEqual(rec["Active Elsewhere Templates"], "VW Single")

    def test_phone_in_index_with_two_templates_semicolon_joined(self):
        """Two-template hit is exactly the cross-ref insurance case —
        the join must be `; ` (semicolon + space) for readability."""
        idx = {"27821110003": ["VW Family", "VW Single"]}
        rec = {"Phone": "27821110003"}
        annotate_with_active_elsewhere(rec, idx)
        self.assertEqual(rec["Currently Active Elsewhere"], "Y")
        self.assertEqual(
            rec["Active Elsewhere Templates"], "VW Family; VW Single"
        )

    def test_blank_phone_short_circuits_to_n(self):
        """Edge case: client has no phone on file. We can't possibly
        cross-ref, so mark N + blank list. Critical: do NOT key into
        the index with "" (which would always miss anyway, but the
        explicit short-circuit is the safer contract)."""
        for blank in ("", None):
            rec = {"Phone": blank}
            annotate_with_active_elsewhere(rec, {"": ["Bogus"]})
            self.assertEqual(rec["Currently Active Elsewhere"], "N")
            self.assertEqual(rec["Active Elsewhere Templates"], "")


# ─── (d) build_summary_sheet_data ────────────────────────────────────────────
class BuildSummarySheetData(unittest.TestCase):
    """Single source of truth for the Summary sheet — totals by status,
    top templates by total, top 5 templates by paused-count, plus the
    'currently active elsewhere' breakdown that's the headline number
    for CS."""

    def test_basic_totals_and_breakdowns(self):
        records = [
            {"Status": "Paused",   "Template Title": "VW Single",
             "Currently Active Elsewhere": "N"},
            {"Status": "Paused",   "Template Title": "VW Single",
             "Currently Active Elsewhere": "Y"},
            {"Status": "Paused",   "Template Title": "VW Family",
             "Currently Active Elsewhere": "N"},
            {"Status": "Pending",  "Template Title": "VW Single",
             "Currently Active Elsewhere": "Y"},
            {"Status": "Inactive", "Template Title": "VW Family",
             "Currently Active Elsewhere": "N"},
        ]
        s = build_summary_sheet_data(records)

        self.assertEqual(s["total"], 5)
        self.assertEqual(s["by_status"],
                         {"Paused": 3, "Pending": 1, "Inactive": 1})
        # Most-common template overall:
        self.assertEqual(s["by_template"][0], ("VW Single", 3))
        # Top 5 by paused-only — VW Single has 2 paused, VW Family 1.
        self.assertEqual(s["top_templates_by_paused"],
                         [("VW Single", 2), ("VW Family", 1)])
        self.assertEqual(s["active_elsewhere_total"], 2)
        self.assertEqual(s["active_elsewhere_by_status"],
                         {"Paused": 1, "Pending": 1, "Inactive": 0})

    def test_top_templates_capped_at_ten(self):
        """top_templates is the top-10 slice — defends the email body
        against a 50-template explosion."""
        records = [
            {"Status": "Paused", "Template Title": f"Tmpl {i}",
             "Currently Active Elsewhere": "N"}
            for i in range(15)
        ]
        s = build_summary_sheet_data(records)
        self.assertEqual(len(s["top_templates"]), 10)
        # And by_template still holds the full list, untruncated.
        self.assertEqual(len(s["by_template"]), 15)

    def test_top_templates_by_paused_capped_at_five(self):
        """top_templates_by_paused is exactly 5 — drives the next-steps
        email line."""
        records = [
            {"Status": "Paused", "Template Title": f"Tmpl {i}",
             "Currently Active Elsewhere": "N"}
            for i in range(8)
        ]
        s = build_summary_sheet_data(records)
        self.assertEqual(len(s["top_templates_by_paused"]), 5)

    def test_empty_input_returns_zero_totals(self):
        s = build_summary_sheet_data([])
        self.assertEqual(s["total"], 0)
        self.assertEqual(s["by_status"],
                         {"Paused": 0, "Pending": 0, "Inactive": 0})
        self.assertEqual(s["by_template"], [])
        self.assertEqual(s["top_templates"], [])
        self.assertEqual(s["top_templates_by_paused"], [])
        self.assertEqual(s["active_elsewhere_total"], 0)
        self.assertEqual(s["active_elsewhere_by_status"],
                         {"Paused": 0, "Pending": 0, "Inactive": 0})


# ─── (e) _iso_date_only — full input-shape matrix per master doc §4.5 ────────
class IsoDateOnly(unittest.TestCase):
    """Pinned by run #25355866865 which crashed when Revio returned an
    int for BTC `created_on`. Per master doc §4.5, Revio (and upstream
    sources) interleave THREE shapes — ISO string, Excel serial (int),
    Unix epoch seconds (int) — across endpoints. The helper must
    accept all three plus blanks and refuse anything else without
    crashing.

    The 12-case matrix is exactly what the bug-fix spec called for.
    Two cases use the openpyxl-actual output for the Excel serial
    (2023-07-16, not 2023-08-12 as a back-of-envelope calc suggested)
    — the contract under test is "value coerces to a date", not the
    specific date arithmetic, so aligning with the canonical from_excel
    output is correct.
    """

    def test_a_none_returns_empty_string(self):
        self.assertEqual(_iso_date_only(None), "")

    def test_b_empty_string_returns_empty_string(self):
        self.assertEqual(_iso_date_only(""), "")

    def test_c_iso_date_string(self):
        self.assertEqual(_iso_date_only("2026-05-04"), "2026-05-04")

    def test_d_iso_datetime_with_z_suffix(self):
        self.assertEqual(_iso_date_only("2026-05-04T08:30:00Z"),
                         "2026-05-04")

    def test_e_iso_string_with_surrounding_whitespace(self):
        self.assertEqual(_iso_date_only("  2026-05-04  "), "2026-05-04")

    def test_f_excel_serial_int(self):
        """Excel serial 45123 → 2023-07-16 per openpyxl's from_excel
        with the WINDOWS_EPOCH (1899-12-30) anchor. The bug-fix spec
        listed 2023-08-12 from a back-of-envelope calc; the canonical
        openpyxl value is what the production code returns and what
        end-users see, so the test pins that."""
        self.assertEqual(_iso_date_only(45123), "2023-07-16")

    def test_g_excel_serial_float_with_time_component(self):
        """Excel serials with a fractional component (time-of-day) must
        coerce to the same date as the integer portion."""
        self.assertEqual(_iso_date_only(45123.5), "2023-07-16")

    def test_h_unix_timestamp_int(self):
        """1714867200 = 2024-05-05 UTC. Anything ≥ 100_000 is treated
        as Unix epoch seconds, not Excel serial."""
        self.assertEqual(_iso_date_only(1714867200), "2024-05-05")

    def test_i_unix_timestamp_float(self):
        self.assertEqual(_iso_date_only(1714867200.0), "2024-05-05")

    def test_j_garbage_string_returns_empty_does_not_crash(self):
        self.assertEqual(_iso_date_only("garbage"), "")

    def test_k_dict_input_returns_empty_does_not_crash(self):
        """Defence-in-depth — Revio shouldn't ever send a dict for a
        date field, but the helper must refuse rather than crash."""
        self.assertEqual(_iso_date_only({"weird": "dict"}), "")

    def test_l_negative_int_returns_empty_does_not_crash(self):
        """value < 1 is rejected explicitly so fromtimestamp(-1) never
        silently emits 1969-12-31 cells. Also covers value == 0."""
        self.assertEqual(_iso_date_only(-1), "")
        self.assertEqual(_iso_date_only(0), "")


# ─── (f) sibling helpers: _to_date + _days_since — same coercion rules ───────
class ToDateAndDaysSince(unittest.TestCase):
    """`_iso_date_only` delegates parsing to `_to_date`; `_days_since`
    does likewise. These tests pin the shared coercion contract on the
    sibling helpers so a future refactor that splits the parsing path
    can't drift one helper without breaking the others."""

    def test_to_date_excel_serial(self):
        self.assertEqual(_to_date(45123), date(2023, 7, 16))

    def test_to_date_unix_epoch(self):
        self.assertEqual(_to_date(1714867200), date(2024, 5, 5))

    def test_to_date_iso_string(self):
        self.assertEqual(_to_date("2025-09-19"), date(2025, 9, 19))

    def test_to_date_bool_is_not_treated_as_int(self):
        """Defensive: bool subclasses int. _to_date must NOT call
        from_excel(True) → that returns a real date and would taint
        any record where Revio sent True/False where it should have
        sent a date."""
        self.assertIsNone(_to_date(True))
        self.assertIsNone(_to_date(False))

    def test_to_date_blank_and_unparseable(self):
        self.assertIsNone(_to_date(None))
        self.assertIsNone(_to_date(""))
        self.assertIsNone(_to_date("not a date"))
        self.assertIsNone(_to_date(-5))
        self.assertIsNone(_to_date({"k": "v"}))

    def test_days_since_iso_string(self):
        self.assertEqual(_days_since("2023-11-14", date(2023, 11, 20)), 6)

    def test_days_since_unix_epoch(self):
        ts = int(datetime(2023, 11, 14, tzinfo=timezone.utc).timestamp())
        self.assertEqual(_days_since(ts, date(2023, 11, 20)), 6)

    def test_days_since_excel_serial(self):
        # 45123 = 2023-07-16, so days to 2023-07-20 = 4.
        self.assertEqual(_days_since(45123, date(2023, 7, 20)), 4)

    def test_days_since_blank_returns_empty_string(self):
        """Blank input → "" (not 0) so the Excel cell renders blank,
        not a misleading numeric. The right-align style also gates on
        isinstance(int) so "" stays unstyled."""
        self.assertEqual(_days_since(None, date(2023, 11, 20)), "")
        self.assertEqual(_days_since("", date(2023, 11, 20)), "")


if __name__ == "__main__":
    unittest.main()
