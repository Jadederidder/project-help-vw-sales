# ============================================================
# tests/test_convert_account_expiry.py
# Run with: pytest tests/test_convert_account_expiry.py
# ============================================================

import os
import sys
import unittest
from datetime import date
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import convert_account_expiry as cae
from scripts import revio_subscription as rs


# ─── Pure account-number normalisation ───────────────────────────────────────
class NormAccount(unittest.TestCase):
    def test_passthrough_string(self):
        self.assertEqual(cae._norm_account("87015446128"), "87015446128")

    def test_int(self):
        self.assertEqual(cae._norm_account(87015446128), "87015446128")

    def test_float_dot_zero(self):
        self.assertEqual(cae._norm_account(87015446128.0), "87015446128")

    def test_string_dot_zero(self):
        self.assertEqual(cae._norm_account("87015446128.0"), "87015446128")

    def test_strips_non_digits(self):
        self.assertEqual(cae._norm_account("acc:87015446128"), "87015446128")

    def test_blank(self):
        self.assertEqual(cae._norm_account(""), "")
        self.assertEqual(cae._norm_account(None), "")


# ─── ACCOUNT EXPIRY substring matching ───────────────────────────────────────
class IsAccountExpiry(unittest.TestCase):
    def test_canonical_with_account_suffix(self):
        self.assertTrue(cae.is_account_expiry(
            "ACCOUNT EXPIRY DATE WITHIN MONTHS RANGE OF 12 acc:87015446128"))

    def test_lowercase(self):
        self.assertTrue(cae.is_account_expiry(
            "account expiry date within months range of 12 acc:87015446128"))

    def test_leading_whitespace(self):
        self.assertTrue(cae.is_account_expiry(
            "   ACCOUNT EXPIRY DATE WITHIN MONTHS RANGE OF 12 acc:87015446128"))

    def test_other_rejection_does_not_match(self):
        for msg in [
            "ACCOUNT NOT OPEN acc:87029622736",
            "INVALID DEALER CODE",
            "A VAP OF THIS CATEGORY ALREADY EXISTS",
            "",
            None,
        ]:
            self.assertFalse(cae.is_account_expiry(msg), msg=msg)

    def test_close_but_different_does_not_match(self):
        # Different number range — must not match
        self.assertFalse(cae.is_account_expiry(
            "ACCOUNT EXPIRY DATE WITHIN MONTHS RANGE OF 24 acc:87015446128"))


# ─── compute_next_debit_date — the core business rule ───────────────────────
class ComputeNextDebitDate(unittest.TestCase):
    def test_today_before_debit_day_uses_this_month(self):
        # 2026-05-10, debit day 21 → 2026-05-21
        self.assertEqual(
            rs.compute_next_debit_date("2026/05/21", date(2026, 5, 10)),
            "2026-05-21",
        )

    def test_today_equal_to_debit_day_uses_next_month(self):
        # rule: today.day < debit_day uses this month, else next month.
        # Equal → next month.
        self.assertEqual(
            rs.compute_next_debit_date("2026/05/21", date(2026, 5, 21)),
            "2026-06-21",
        )

    def test_today_after_debit_day_uses_next_month(self):
        # 2026-05-22, debit day 21 → 2026-06-21
        self.assertEqual(
            rs.compute_next_debit_date("2026/05/21", date(2026, 5, 22)),
            "2026-06-21",
        )

    def test_december_rolls_to_january(self):
        # 2026-12-25, debit day 5 → 2027-01-05
        self.assertEqual(
            rs.compute_next_debit_date("2026/01/05", date(2026, 12, 25)),
            "2027-01-05",
        )

    def test_blank_falls_back_to_day_28(self):
        # 2026-05-10, blank → 2026-05-28 (28 > 10, this month)
        self.assertEqual(
            rs.compute_next_debit_date("", date(2026, 5, 10)),
            "2026-05-28",
        )

    def test_invalid_date_falls_back_to_day_28(self):
        self.assertEqual(
            rs.compute_next_debit_date("not-a-date", date(2026, 5, 10)),
            "2026-05-28",
        )

    def test_short_month_clamps_day(self):
        # Debit day 31, today April 15 → April has 30 days → 2026-04-30
        self.assertEqual(
            rs.compute_next_debit_date("2026/05/31", date(2026, 4, 15)),
            "2026-04-30",
        )

    def test_february_clamps_to_28(self):
        # Debit day 30, today Jan 31 → today.day (31) >= 30 → next month (Feb
        # 2026, non-leap) → day 30 clamped to 28 → 2026-02-28
        self.assertEqual(
            rs.compute_next_debit_date("2026/01/30", date(2026, 1, 31)),
            "2026-02-28",
        )

    def test_february_leap_year_clamps_to_29(self):
        # Same setup in 2028 (leap year) → Feb has 29 days
        self.assertEqual(
            rs.compute_next_debit_date("2028/01/30", date(2028, 1, 31)),
            "2028-02-29",
        )

    def test_bare_integer_day_of_month(self):
        # Defensive: spec wording suggested col X might be an integer
        self.assertEqual(
            rs.compute_next_debit_date("21", date(2026, 5, 10)),
            "2026-05-21",
        )

    def test_iso_dash_format(self):
        self.assertEqual(
            rs.compute_next_debit_date("2026-05-21", date(2026, 5, 10)),
            "2026-05-21",
        )


# ─── Phone & address helpers ─────────────────────────────────────────────────
class NormalisePhone(unittest.TestCase):
    def test_9_digit_no_prefix(self):
        # SALES sample is 9 digits with no leading 0 or 27
        self.assertEqual(rs.normalise_phone("714357375"), "27714357375")

    def test_10_digit_with_leading_0(self):
        self.assertEqual(rs.normalise_phone("0714357375"), "27714357375")

    def test_11_digit_with_27(self):
        self.assertEqual(rs.normalise_phone("27714357375"), "27714357375")

    def test_int_passes(self):
        self.assertEqual(rs.normalise_phone(714357375), "27714357375")

    def test_float_dot_zero(self):
        self.assertEqual(rs.normalise_phone(714357375.0), "27714357375")

    def test_blank(self):
        self.assertEqual(rs.normalise_phone(None), "")
        self.assertEqual(rs.normalise_phone(""), "")


class BuildName(unittest.TestCase):
    def test_concat(self):
        self.assertEqual(rs.build_full_name("CATHERINE", "TINKLER"),
                         "CATHERINE TINKLER")

    def test_blanks(self):
        self.assertEqual(rs.build_full_name("CATHERINE", ""), "CATHERINE")
        self.assertEqual(rs.build_full_name("", "TINKLER"), "TINKLER")
        self.assertEqual(rs.build_full_name(None, None), "")


class BuildStreet(unittest.TestCase):
    def test_concat_two_lines(self):
        self.assertEqual(
            rs.build_street_address("13 MONTROSE TERRACE", "FLAT 8"),
            "13 MONTROSE TERRACE, FLAT 8",
        )

    def test_drops_blank_line2(self):
        self.assertEqual(
            rs.build_street_address("13 MONTROSE TERRACE", ""),
            "13 MONTROSE TERRACE",
        )


# ─── Revio payload construction ──────────────────────────────────────────────
class BuildClientPayload(unittest.TestCase):
    def setUp(self):
        # Mirrors a real SALES row (live header keys)
        self.sales_row = {
            "Policy Number":                          "SN135075",
            "VW/Audi Product":                        "ALLRHLP",
            "FirstName":                              "CATHERINE",
            "Surname":                                "TINKLER",
            "Email Address (VW/Audi Campaign 1)":     "cathtinkler@gmail.com",
            "Mobile Number (VW/Audi Campaign 1)":     "714357375",
            "Physical Line1":                         "13 MONTROSE TERRACE",
            "Physical Line2":                         "",
            "Physical Suburb":                        "BISHOPSCOURT",
            "Physical City":                          "",
            "Physical Post Code":                     "7708",
            "Bank Account Number (VW/Audi)":          "62881227336",
            "Bank":                                   "First National Bank (FNB)",
            "Branch Code (VW/Audi Campaign 1)":       "250655",
            "Debit_Order_Date":                       "2026/05/21",
            "WesBank Account Number":                 "87016341747",
        }

    def test_full_payload(self):
        p = rs.build_client_payload(self.sales_row, "VW-SN135075")
        self.assertEqual(p["email"], "cathtinkler@gmail.com")
        self.assertEqual(p["full_name"], "CATHERINE TINKLER")
        self.assertEqual(p["personal_code"], "VW-SN135075")
        self.assertEqual(p["bank_account"], "62881227336")
        self.assertEqual(p["bank_code"], "250655")
        self.assertEqual(p["phone"], "27714357375")
        self.assertEqual(p["country"], "ZA")
        self.assertEqual(p["zip_code"], "7708")
        self.assertEqual(p["street_address"], "13 MONTROSE TERRACE")

    @patch.dict(os.environ, {"REVIO_BRAND_ID": "e73e30ef-197c-4e2c-a4f9-e13483284abb"})
    def test_brand_id_injected_from_env(self):
        # v1.6: brand_id is required by Revio docs on POST /clients/.
        p = rs.build_client_payload(self.sales_row, "VW-SN135075")
        self.assertEqual(p["brand_id"], "e73e30ef-197c-4e2c-a4f9-e13483284abb")

    def test_brand_id_blank_when_env_unset(self):
        # Defensive: still emit the key even if env var is missing —
        # Revio will then 400 with a clear validation error rather than
        # silently dropping the field. main()'s pre-flight catches this
        # before any POST is made in non-DRY_RUN.
        os.environ.pop("REVIO_BRAND_ID", None)
        p = rs.build_client_payload(self.sales_row, "VW-SN135075")
        self.assertIn("brand_id", p)
        self.assertEqual(p["brand_id"], "")

    def test_blank_city_falls_back_to_suburb(self):
        p = rs.build_client_payload(self.sales_row, "VW-SN135075")
        self.assertEqual(p["city"], "BISHOPSCOURT")

    def test_explicit_city_wins_over_suburb(self):
        self.sales_row["Physical City"] = "CAPE TOWN"
        p = rs.build_client_payload(self.sales_row, "VW-SN135075")
        self.assertEqual(p["city"], "CAPE TOWN")

    def test_personal_code_truncates_at_32(self):
        long = "VW-" + "X" * 100
        p = rs.build_client_payload(self.sales_row, long)
        self.assertEqual(len(p["personal_code"]), 32)


class ResolveTemplate(unittest.TestCase):
    """v1.6: template UUIDs come from env vars (REVIO_TEMPLATE_VW_SINGLE_ID,
    REVIO_TEMPLATE_VW_FAMILY_ID), not from a /billing_templates/ enumeration.
    Tests patch os.environ to assert the lookup wires through correctly."""

    @patch.dict(os.environ, {
        "REVIO_TEMPLATE_VW_SINGLE_ID": "7befa418-0508-4d81-91ab-e077a65cb872",
        "REVIO_TEMPLATE_VW_FAMILY_ID": "018dd06c-475c-4cf5-afc9-da2e1b5218b1",
    })
    def test_allrhlp_resolves_from_env(self):
        self.assertEqual(
            rs.resolve_template_id("ALLRHLP"),
            "7befa418-0508-4d81-91ab-e077a65cb872",
        )

    @patch.dict(os.environ, {
        "REVIO_TEMPLATE_VW_SINGLE_ID": "7befa418-0508-4d81-91ab-e077a65cb872",
        "REVIO_TEMPLATE_VW_FAMILY_ID": "018dd06c-475c-4cf5-afc9-da2e1b5218b1",
    })
    def test_allrhfm_resolves_from_env(self):
        self.assertEqual(
            rs.resolve_template_id("ALLRHFM"),
            "018dd06c-475c-4cf5-afc9-da2e1b5218b1",
        )

    def test_unknown_product_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            rs.resolve_template_id("ALLNOPE")
        self.assertIn("ALLNOPE", str(ctx.exception))

    @patch.dict(os.environ, {"REVIO_TEMPLATE_VW_SINGLE_ID": ""}, clear=False)
    def test_missing_env_var_raises_with_clear_diagnostic(self):
        # Wipe the var if anything in the test runner set it
        os.environ.pop("REVIO_TEMPLATE_VW_SINGLE_ID", None)
        with self.assertRaises(RuntimeError) as ctx:
            rs.resolve_template_id("ALLRHLP")
        self.assertIn("REVIO_TEMPLATE_VW_SINGLE_ID", str(ctx.exception))

    @patch.dict(os.environ, {
        "REVIO_TEMPLATE_VW_SINGLE_ID": "single-id",
        "REVIO_TEMPLATE_VW_FAMILY_ID": "family-id",
    })
    def test_no_billing_templates_api_call_made(self):
        # The whole point of v1.6: resolve_template_id must NOT hit the API.
        with patch.object(rs.requests, "get") as mock_get:
            rs.resolve_template_id("ALLRHLP")
            mock_get.assert_not_called()


# ─── REJECTIONS H/J state machine — pure transition rules ──────────────────
class ComputeRejectionStateTransition(unittest.TestCase):
    def test_converted_with_h_populated_j_blank_saves_and_blanks(self):
        # First conversion: H populated, J blank. Save H→J, blank H.
        h, j = cae.compute_rejection_state_transition("87015446128", "", "CONVERTED")
        self.assertEqual(h, "")
        self.assertEqual(j, "87015446128")

    def test_pending_with_h_populated_j_blank_saves_and_blanks(self):
        # Same flow as CONVERTED — PENDING also saves + blanks
        h, j = cae.compute_rejection_state_transition("87015446128", "", "PENDING")
        self.assertEqual(h, "")
        self.assertEqual(j, "87015446128")

    def test_converted_with_j_already_populated_only_blanks_h(self):
        # Re-running: J already saved on previous run. Just ensure H blank.
        h, j = cae.compute_rejection_state_transition(
            "87015446128", "87015446128", "CONVERTED"
        )
        self.assertEqual(h, "")
        self.assertIsNone(j)  # J unchanged

    def test_converted_already_clean_is_noop(self):
        # Idempotent: re-running on a row already in the converted state.
        h, j = cae.compute_rejection_state_transition("", "87015446128", "CONVERTED")
        self.assertIsNone(h)
        self.assertIsNone(j)

    def test_pending_already_clean_is_noop(self):
        h, j = cae.compute_rejection_state_transition("", "87015446128", "PENDING")
        self.assertIsNone(h)
        self.assertIsNone(j)

    def test_moved_to_rejections_restores_h_from_j(self):
        # Age-out: J populated, H blank → restore H ← J. J kept for audit.
        h, j = cae.compute_rejection_state_transition(
            "", "87015446128", "MOVED_TO_REJECTIONS"
        )
        self.assertEqual(h, "87015446128")
        self.assertIsNone(j)  # J unchanged (audit trail)

    def test_moved_to_rejections_with_no_j_is_noop(self):
        # Defensive: nothing to restore from
        h, j = cae.compute_rejection_state_transition("", "", "MOVED_TO_REJECTIONS")
        self.assertIsNone(h)
        self.assertIsNone(j)

    def test_error_blanks_h_and_saves_j(self):
        # v1.5: ERROR is treated as in-flight retry; H gets blanked and J
        # saved exactly like PENDING/CONVERTED, so the dashboard formula
        # excludes them (they self-heal silently if they later succeed).
        h, j = cae.compute_rejection_state_transition(
            "87015446128", "", "ERROR: timeout connecting to Revio"
        )
        self.assertEqual(h, "")
        self.assertEqual(j, "87015446128")

    def test_error_with_j_already_saved_only_blanks_h(self):
        h, j = cae.compute_rejection_state_transition(
            "87015446128", "87015446128", "ERROR: 500 server error"
        )
        self.assertEqual(h, "")
        self.assertIsNone(j)

    def test_error_idempotent_on_already_in_flight_row(self):
        h, j = cae.compute_rejection_state_transition(
            "", "87015446128", "ERROR: 503 unavailable"
        )
        self.assertIsNone(h)
        self.assertIsNone(j)

    def test_unknown_status_does_not_touch_h_or_j(self):
        # Anything that's not CONVERTED / PENDING / ERROR / MOVED — leave alone.
        h, j = cae.compute_rejection_state_transition(
            "87015446128", "", "WHATEVER"
        )
        self.assertIsNone(h)
        self.assertIsNone(j)

    def test_double_apply_is_idempotent(self):
        # Apply CONVERTED transition twice → second call is a no-op.
        # Step 1: H='123', J='' → H='', J='123'
        h1, j1 = cae.compute_rejection_state_transition("123", "", "CONVERTED")
        # Step 2: simulate the post-state and re-apply → no-op
        h2, j2 = cae.compute_rejection_state_transition(h1 or "", j1 or "", "CONVERTED")
        self.assertIsNone(h2)
        self.assertIsNone(j2)


# ─── ApplyRejectionState — integration via mocked write_cell ───────────────
class ApplyRejectionState(unittest.TestCase):
    """Verify which columns get written for each transition. Calls into
    cae.write_cell are captured so we can assert exact (col, value) pairs."""

    def _captured_writes(self, current_h, current_j, new_status, dry_run=True):
        """Return the list of (col_letter, value) writes that apply_rejection_state
        would emit for these inputs."""
        writes = []

        def fake_write_cell(svc, tab, row_1based, col_letter, value, dry_run=False):
            writes.append((col_letter, value))

        # h=col H (idx 7), i=col I (idx 8), j=col J (idx 9)
        with patch.object(cae, "write_cell", side_effect=fake_write_cell):
            cae.apply_rejection_state(
                svc=None, row_num=42,
                h_idx=7, i_idx=8, j_idx=9,
                current_h=current_h, current_j=current_j,
                new_status=new_status, dry_run=dry_run,
            )
        return writes

    def test_converted_writes_i_h_j(self):
        writes = self._captured_writes("87015446128", "", "CONVERTED")
        # Expect: I=CONVERTED, H='', J='87015446128'
        self.assertIn(("I", "CONVERTED"), writes)
        self.assertIn(("H", ""), writes)
        self.assertIn(("J", "87015446128"), writes)

    def test_pending_writes_i_h_j(self):
        writes = self._captured_writes("87015446128", "", "PENDING")
        self.assertIn(("I", "PENDING"), writes)
        self.assertIn(("H", ""), writes)
        self.assertIn(("J", "87015446128"), writes)

    def test_moved_to_rejections_writes_i_and_h_only(self):
        # Restore H from J; J unchanged.
        writes = self._captured_writes("", "87015446128", "MOVED_TO_REJECTIONS")
        self.assertIn(("I", "MOVED_TO_REJECTIONS"), writes)
        self.assertIn(("H", "87015446128"), writes)
        # J must NOT be re-written
        j_writes = [w for w in writes if w[0] == "J"]
        self.assertEqual(j_writes, [], msg="J must be untouched on age-out")

    def test_error_writes_i_h_j(self):
        # v1.5: ERROR transitions blank H and save J (same as PENDING/CONVERTED)
        writes = self._captured_writes(
            "87015446128", "", "ERROR: 400 invalid bank_account"
        )
        self.assertIn(("I", "ERROR: 400 invalid bank_account"), writes)
        self.assertIn(("H", ""), writes)
        self.assertIn(("J", "87015446128"), writes)

    def test_idempotent_re_run_on_converted_row(self):
        # Row already in CONVERTED state: H='', J='87015446128'. Re-run.
        # Should only write I (status); no H or J writes.
        writes = self._captured_writes("", "87015446128", "CONVERTED")
        cols_written = sorted({w[0] for w in writes})
        self.assertEqual(cols_written, ["I"],
                         msg=f"expected only I to be written, got {writes}")

    def test_re_run_with_dirty_h_self_heals(self):
        # Defensive: row had Conversion_Status set on a prior run by an
        # older script that didn't blank H. J was saved. New run sees
        # H still populated + J populated → must blank H now.
        writes = self._captured_writes(
            "87015446128", "87015446128", "CONVERTED"
        )
        self.assertIn(("I", "CONVERTED"), writes)
        self.assertIn(("H", ""), writes)
        # J already saved; do not re-write
        j_writes = [w for w in writes if w[0] == "J"]
        self.assertEqual(j_writes, [])


# ─── should_skip_conversion_status — daily skip filter ────────────────────
class ShouldSkipConversionStatus(unittest.TestCase):
    """v1.5: only CONVERTED + MOVED_TO_REJECTIONS terminate retries.
    PENDING and ERROR rows fall through and are retried on every daily run."""

    def test_blank_does_not_skip(self):
        self.assertFalse(cae.should_skip_conversion_status(""))
        self.assertFalse(cae.should_skip_conversion_status(None))

    def test_converted_skips(self):
        self.assertTrue(cae.should_skip_conversion_status("CONVERTED"))

    def test_moved_to_rejections_skips(self):
        self.assertTrue(cae.should_skip_conversion_status("MOVED_TO_REJECTIONS"))

    def test_pending_does_not_skip(self):
        # Retried by recheck_pending_conversions; new-rejection scan also
        # passes them through and the already_pending branch normalises.
        self.assertFalse(cae.should_skip_conversion_status("PENDING"))

    def test_error_does_not_skip(self):
        # The whole point of v1.5: ERROR rows get retried tomorrow.
        self.assertFalse(cae.should_skip_conversion_status(
            "ERROR: 500 internal server error"))

    def test_case_insensitive(self):
        self.assertTrue(cae.should_skip_conversion_status("converted"))
        self.assertTrue(cae.should_skip_conversion_status("Moved_To_Rejections"))


# ─── Revio retry logic ──────────────────────────────────────────────────────
class _FakeResponse:
    def __init__(self, status_code, body=""):
        self.status_code = status_code
        self.text = body

    def json(self):
        import json as _j
        return _j.loads(self.text or "{}")


class DoRequestWithRetry(unittest.TestCase):
    """v1.5: _do_request_with_retry wraps requests.<method> with backoff
    on 5xx / 408 / 429 / network errors. 400/401/403/404 don't retry."""

    def _run(self, side_effect, method="post"):
        """Helper: patch requests.<method> + time.sleep, run the retry
        helper, return (final_response, sleep_calls, call_count)."""
        sleep_calls = []
        call_count = {"n": 0}

        def fake_method(*args, **kwargs):
            call_count["n"] += 1
            v = side_effect[call_count["n"] - 1]
            if isinstance(v, Exception):
                raise v
            return v

        with patch.object(rs.requests, method, side_effect=fake_method), \
             patch.object(rs.time, "sleep",
                          side_effect=lambda s: sleep_calls.append(s)):
            try:
                r = rs._do_request_with_retry(method, "https://x/", timeout=1)
            except Exception as e:
                return ("raised", e), sleep_calls, call_count["n"]
        return r, sleep_calls, call_count["n"]

    def test_success_first_try_no_retry(self):
        r, sleeps, n = self._run([_FakeResponse(200, '{"id": "abc"}')])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(sleeps, [])
        self.assertEqual(n, 1)

    def test_5xx_then_success_on_attempt_2(self):
        r, sleeps, n = self._run([
            _FakeResponse(503, "service unavailable"),
            _FakeResponse(200, '{"id": "abc"}'),
        ])
        self.assertEqual(r.status_code, 200)
        # First-attempt failure should sleep RETRY_BACKOFFS_SECONDS[0] = 5s
        self.assertEqual(sleeps, [rs.RETRY_BACKOFFS_SECONDS[0]])
        self.assertEqual(n, 2)

    def test_5xx_three_times_returns_last_response(self):
        r, sleeps, n = self._run([
            _FakeResponse(500),
            _FakeResponse(500),
            _FakeResponse(500),
        ])
        # After exhausting attempts, the last 5xx response is returned —
        # the caller decides what to do (in our case: raise + mark ERROR).
        self.assertEqual(r.status_code, 500)
        # 2 sleeps: between attempts 1→2 and 2→3
        self.assertEqual(sleeps, [
            rs.RETRY_BACKOFFS_SECONDS[0],
            rs.RETRY_BACKOFFS_SECONDS[1],
        ])
        self.assertEqual(n, 3)

    def test_400_does_not_retry(self):
        # 400 is non-retryable per spec — fail fast
        r, sleeps, n = self._run([_FakeResponse(400, "validation failed")])
        self.assertEqual(r.status_code, 400)
        self.assertEqual(sleeps, [])
        self.assertEqual(n, 1)

    def test_401_does_not_retry(self):
        r, sleeps, n = self._run([_FakeResponse(401)])
        self.assertEqual(r.status_code, 401)
        self.assertEqual(sleeps, [])

    def test_403_does_not_retry(self):
        r, sleeps, n = self._run([_FakeResponse(403)])
        self.assertEqual(r.status_code, 403)
        self.assertEqual(sleeps, [])

    def test_404_does_not_retry(self):
        r, sleeps, n = self._run([_FakeResponse(404)])
        self.assertEqual(r.status_code, 404)
        self.assertEqual(sleeps, [])

    def test_408_retries(self):
        # Request timeout — retryable
        r, sleeps, n = self._run([_FakeResponse(408), _FakeResponse(200, "{}")])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(n, 2)

    def test_429_retries(self):
        # Rate limit — retryable
        r, sleeps, n = self._run([_FakeResponse(429), _FakeResponse(200, "{}")])
        self.assertEqual(r.status_code, 200)

    def test_connection_error_retries_then_raises(self):
        import requests as _requests
        e = _requests.exceptions.ConnectionError("network down")
        out, sleeps, n = self._run([e, e, e])
        self.assertEqual(out[0], "raised")
        self.assertIsInstance(out[1], _requests.exceptions.ConnectionError)
        self.assertEqual(n, 3)

    def test_connection_error_then_success(self):
        import requests as _requests
        e = _requests.exceptions.Timeout("timed out")
        r, sleeps, n = self._run([e, _FakeResponse(200, "{}")])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(n, 2)


# ─── Personal-code collision pre-check ──────────────────────────────────────
class FindClientByPersonalCode(unittest.TestCase):

    def _patched(self, fake_response):
        """Run find_client_by_personal_code with requests.get patched to
        return fake_response. time.sleep is also patched so retries
        don't actually sleep."""
        with patch.object(rs.requests, "get", return_value=fake_response), \
             patch.object(rs.time, "sleep"), \
             patch.object(rs, "REVIO_API_KEY", "fake-key"):
            return rs.find_client_by_personal_code("VW-SN135075")

    def test_existing_client_returns_id(self):
        body = '{"results": [{"id": "abc-123", "personal_code": "VW-SN135075"}]}'
        result = self._patched(_FakeResponse(200, body))
        self.assertEqual(result, "abc-123")

    def test_no_match_returns_none(self):
        body = '{"results": []}'
        result = self._patched(_FakeResponse(200, body))
        self.assertIsNone(result)

    def test_different_personal_code_in_response_returns_none(self):
        # Defensive: API might return a fuzzy match — only accept exact.
        body = '{"results": [{"id": "x", "personal_code": "VW-SN999999"}]}'
        result = self._patched(_FakeResponse(200, body))
        self.assertIsNone(result)

    def test_non_200_lookup_returns_none_does_not_raise(self):
        # If the lookup itself fails, fall through to POST — don't block
        # the conversion just because GET flaked.
        result = self._patched(_FakeResponse(503, "unavailable"))
        self.assertIsNone(result)

    def test_blank_personal_code_short_circuits(self):
        # Don't even hit the API
        with patch.object(rs.requests, "get") as mock_get:
            r = rs.find_client_by_personal_code("")
            self.assertIsNone(r)
            mock_get.assert_not_called()


# ─── Existing-subscriber pre-check ──────────────────────────────────────────
class IsAlreadySubscriber(unittest.TestCase):

    def _patched(self, fake_response, client_id="cid-target"):
        with patch.object(rs.requests, "get", return_value=fake_response), \
             patch.object(rs.time, "sleep"), \
             patch.object(rs, "REVIO_API_KEY", "fake-key"):
            return rs.is_already_subscriber("tmpl-id", client_id)

    def test_subscriber_present_returns_true(self):
        body = '{"results": [{"client_id": "cid-target"}], "next": null}'
        self.assertTrue(self._patched(_FakeResponse(200, body)))

    def test_subscriber_absent_returns_false(self):
        body = '{"results": [{"client_id": "cid-other"}], "next": null}'
        self.assertFalse(self._patched(_FakeResponse(200, body)))

    def test_uses_id_field_as_fallback(self):
        # Some Revio shapes use `id` instead of `client_id`
        body = '{"results": [{"id": "cid-target"}], "next": null}'
        self.assertTrue(self._patched(_FakeResponse(200, body)))

    def test_non_200_returns_false_proceed_to_add(self):
        # Don't block conversion if GET flakes; let the POST decide.
        self.assertFalse(self._patched(_FakeResponse(500, "oops")))

    def test_blank_client_id_short_circuits(self):
        with patch.object(rs.requests, "get") as mock_get:
            self.assertFalse(rs.is_already_subscriber("tmpl-id", ""))
            mock_get.assert_not_called()


# ─── End-to-end self-heal: ERROR row converts on subsequent run ────────────
class ErrorRowSelfHeals(unittest.TestCase):
    """Verify the v1.5 self-healing path:
    a row that errored out on day 0 (H blanked, I=ERROR, J=acc) gets retried
    via the new-rejection scan on day 1 because should_skip_conversion_status
    excludes it. The acc lookup falls back from H (blank) to J. With the
    idempotent pre-checks, the second attempt at create_client + add_subscriber
    is safe.
    """

    def test_error_status_passes_skip_filter_for_retry(self):
        # The whole self-heal hinges on this: ERROR rows aren't terminal.
        self.assertFalse(cae.should_skip_conversion_status("ERROR: 503"))
        self.assertFalse(cae.should_skip_conversion_status("ERROR: 500 timeout"))

    def test_error_state_machine_keeps_h_blank_on_retry(self):
        # Day 1: row state was ERROR (H='', I='ERROR: ...', J=acc).
        # We re-attempt, succeed → CONVERTED.
        # apply CONVERTED transition with H='', J='acc' → only I gets written
        # (H already blank, J already saved). Idempotent.
        h, j = cae.compute_rejection_state_transition(
            "", "87015446128", "CONVERTED"
        )
        self.assertIsNone(h)
        self.assertIsNone(j)


# ─── _parse_date_loose ───────────────────────────────────────────────────────
class ParseDateLoose(unittest.TestCase):
    def test_slash_format(self):
        self.assertEqual(cae._parse_date_loose("2026/04/29"), date(2026, 4, 29))

    def test_iso_format(self):
        self.assertEqual(cae._parse_date_loose("2026-04-29"), date(2026, 4, 29))

    def test_blank(self):
        self.assertIsNone(cae._parse_date_loose(""))
        self.assertIsNone(cae._parse_date_loose(None))

    def test_unparseable(self):
        self.assertIsNone(cae._parse_date_loose("garbage"))

    # ─── Excel/Sheets serial number — the bug fix ──────────────────────────
    # read_tab uses valueRenderOption=UNFORMATTED_VALUE so date-formatted
    # cells arrive here as int days-since-1899-12-30, not strings. The
    # 9-row backfill regression on the live sheet was caused by this
    # parser silently returning None for ints, which let every ACCOUNT
    # EXPIRY row through the backfill_from filter.

    def test_int_serial_is_parsed_as_date(self):
        # 45919 = the actual value Sheets returned for the 2025-09-19 row
        self.assertEqual(cae._parse_date_loose(45919), date(2025, 9, 19))

    def test_int_serial_for_recent_dates(self):
        # 45933 → 2025-10-03, 45966 → 2025-11-05 (also from the live sheet)
        self.assertEqual(cae._parse_date_loose(45933), date(2025, 10, 3))
        self.assertEqual(cae._parse_date_loose(45966), date(2025, 11, 5))

    def test_int_serial_for_2026_backfill_row(self):
        # 2026-04-29 → serial 46141
        self.assertEqual(cae._parse_date_loose(46141), date(2026, 4, 29))

    def test_float_serial(self):
        # Some integrations stringify-then-parse-back; cover the float path
        self.assertEqual(cae._parse_date_loose(45919.0), date(2025, 9, 19))

    def test_numeric_string_serial(self):
        # Defensive: if a tool stringified the int before reaching us
        self.assertEqual(cae._parse_date_loose("45919"), date(2025, 9, 19))
        self.assertEqual(cae._parse_date_loose("45919.0"), date(2025, 9, 19))

    def test_bool_is_not_treated_as_serial(self):
        # bool is a subclass of int in Python — guard against True → 1899-12-31
        self.assertIsNone(cae._parse_date_loose(True))
        self.assertIsNone(cae._parse_date_loose(False))

    def test_zero_returns_epoch_origin(self):
        # Edge case: serial 0 is the Excel/Sheets epoch origin
        self.assertEqual(cae._parse_date_loose(0), date(1899, 12, 30))

    def test_string_format_takes_precedence_over_serial(self):
        # If a string also looks like a number it should be tried as a
        # date string first (none match, then serial fallback applies).
        # '20251019' is not a valid date format we accept, so it falls
        # through to numeric → would be a future serial year → None.
        self.assertIsNone(cae._parse_date_loose("20251019"))

    def test_backfill_filter_full_scenario(self):
        # End-to-end: with the live UNFORMATTED_VALUE serials, the filter
        # correctly partitions the 9 ACCOUNT EXPIRY rows around the
        # 2026-04-29 backfill cutoff.
        backfill_from = date(2026, 4, 29)
        live_serials = {
            "2025-09-19 #1": 45919,
            "2025-09-19 #2": 45919,
            "2025-09-19 #3": 45919,
            "2025-10-03":    45933,
            "2025-10-27":    45957,
            "2025-11-05":    45966,
            "2026-04-29 #1": 46141,
            "2026-04-29 #2": 46141,
            "2026-04-30":    46142,
        }
        skipped = []
        processed = []
        for label, serial in live_serials.items():
            d = cae._parse_date_loose(serial)
            self.assertIsNotNone(d, msg=f"failed to parse {label}")
            (skipped if d < backfill_from else processed).append(label)
        # 6 historical rows skipped, 3 recent rows processed — matches
        # JD's expected output from the dry_run.
        self.assertEqual(len(skipped), 6)
        self.assertEqual(len(processed), 3)


if __name__ == "__main__":
    unittest.main()
