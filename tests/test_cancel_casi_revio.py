# ============================================================
# tests/test_cancel_casi_revio.py
# Run with: pytest tests/test_cancel_casi_revio.py
# ============================================================

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import openpyxl

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import cancel_casi_revio as ccr


FIXTURE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scripts", "fixtures", "cancellations_with_status.xlsx",
)


def _load_sheet(name):
    wb = openpyxl.load_workbook(FIXTURE, data_only=True)
    ws = wb[name]
    rows = [[c.value if c.value is not None else "" for c in row] for row in ws.iter_rows()]
    return rows[0], rows[1:]


# ─── Pure helpers ────────────────────────────────────────────────────────────
class PhoneNormalisation(unittest.TestCase):
    def test_local_10_digit(self):
        self.assertEqual(ccr.normalize_phone("0821234567"), "27821234567")

    def test_already_international(self):
        self.assertEqual(ccr.normalize_phone("27821234567"), "27821234567")

    def test_plus_prefix(self):
        self.assertEqual(ccr.normalize_phone("+27821234567"), "27821234567")

    def test_with_spaces_and_dashes(self):
        self.assertEqual(ccr.normalize_phone("(082) 123-4567"), "27821234567")

    def test_lost_leading_zero_int(self):
        self.assertEqual(ccr.normalize_phone(821234567), "27821234567")

    def test_lost_leading_zero_float_dot_zero_str(self):
        self.assertEqual(ccr.normalize_phone("821234567.0"), "27821234567")

    def test_lost_leading_zero_float(self):
        self.assertEqual(ccr.normalize_phone(821234567.0), "27821234567")

    def test_blank_returns_empty(self):
        for v in ["", None, "   "]:
            self.assertEqual(ccr.normalize_phone(v), "")

    def test_garbage_returns_empty(self):
        for v in ["not a phone", "123", "abc-def-ghij"]:
            self.assertEqual(ccr.normalize_phone(v), "")


class DecideAction(unittest.TestCase):
    def test_known_cancel_reasons(self):
        for r in ["LEGAL STATUS", "CUSTOMER REQUESTS CANCELLATION",
                  "ARREAR CANCELLATION", "VAP LOADED IN ERROR"]:
            self.assertEqual(ccr.decide_action(r), ("cancel", ""), msg=r)

    def test_case_insensitive(self):
        self.assertEqual(ccr.decide_action("legal status")[0], "cancel")
        self.assertEqual(ccr.decide_action("Legal Status")[0], "cancel")

    def test_blank_is_cancel(self):
        for v in ["", None, "   "]:
            self.assertEqual(ccr.decide_action(v)[0], "cancel")

    def test_unknown_is_skip(self):
        action, note = ccr.decide_action("FRAUDULENT APPLICATION")
        self.assertEqual(action, "skip")
        self.assertIn("FRAUDULENT APPLICATION", note)


class CoverRouting(unittest.TestCase):
    def test_audi_substring(self):
        self.assertEqual(ccr.cover_for_dea("AUDI CENTRE WESTRAND"), ccr.COVER_AUDI)
        self.assertEqual(ccr.cover_for_dea("audi menlyn"), ccr.COVER_AUDI)

    def test_vw_default(self):
        self.assertEqual(ccr.cover_for_dea("HATFIELD VW BRAAMFONTEIN"), ccr.COVER_VW)
        self.assertEqual(ccr.cover_for_dea(""), ccr.COVER_VW)


class HeaderBindings(unittest.TestCase):
    def test_fixture_binds_all_required_cancellations_fields(self):
        headers, _ = _load_sheet("CANCELLATIONS")
        b = ccr.bind_columns(headers)
        for key in ccr.REQUIRED_FIELDS:
            self.assertIsNotNone(b[key], f"{key} should be bound; headers={headers[:5]}…")

    def test_phone_no_longer_in_required(self):
        # Phone moved to SALES — REQUIRED_FIELDS for CANCELLATIONS must not include it
        self.assertNotIn("phone", ccr.REQUIRED_FIELDS)

    def test_id_number_falls_back_to_cus_identity(self):
        b = ccr.bind_columns(["CUS_IDENTITY_OR_REG_NUM"])
        self.assertEqual(b["id_number"], 0)


# ─── SALES phone map (pure) ──────────────────────────────────────────────────
class SalesPhoneMap(unittest.TestCase):
    def test_basic_map_from_fixture(self):
        headers, rows = _load_sheet("SALES")
        m = ccr.build_sales_phone_map(headers, rows)
        # Row 2: account_match
        self.assertEqual(m["by_account"]["87000000002"], "0822222222")
        self.assertEqual(m["by_id"]["8502020002345"], "0822222222")
        # Row 4: lost-leading-zero phone is preserved RAW (as a string after
        # cell coercion); normalisation happens later in normalize_phone.
        self.assertEqual(m["by_account"]["87000000004"], "821234567")
        # Row 7: malformed phone — still indexed; normaliser will reject it later
        self.assertEqual(m["by_account"]["87000000007"], "abc-def-ghij")
        # Blank-phone row is skipped entirely
        self.assertNotIn("87000000999", m["by_account"])
        self.assertNotIn("9999999999999", m["by_id"])

    def test_missing_required_sales_column_raises(self):
        with self.assertRaises(RuntimeError) as cx:
            ccr.build_sales_phone_map(["account number", "phone_number"], [])
        self.assertIn("id_number", str(cx.exception))

    def test_skips_blank_acct_or_id(self):
        m = ccr.build_sales_phone_map(
            ["account number", "phone_number", "IDENTITY_OR_REG_NUM"],
            [["", "0821234567", "8501010001234"],   # only id indexed
             ["87000000002", "0833333333", ""]],     # only acct indexed
        )
        self.assertEqual(m["by_id"]["8501010001234"], "0821234567")
        self.assertNotIn("", m["by_account"])
        self.assertEqual(m["by_account"]["87000000002"], "0833333333")
        self.assertNotIn("", m["by_id"])


class LookupPhone(unittest.TestCase):
    def setUp(self):
        self.maps = {
            "by_account": {"87000000002": "0822222222"},
            "by_id":      {"7503030003456": "0833333333"},
            "rows": 2,
        }

    def test_account_match(self):
        self.assertEqual(ccr.lookup_phone(self.maps, "87000000002", "anything"),
                         ("0822222222", "account_number"))

    def test_id_fallback(self):
        # Account doesn't match → falls back to id
        self.assertEqual(ccr.lookup_phone(self.maps, "11111", "7503030003456"),
                         ("0833333333", "id_number"))

    def test_account_takes_priority_over_id(self):
        # Both keys would match; account must win
        m = {"by_account": {"A": "111"}, "by_id": {"X": "222"}, "rows": 1}
        self.assertEqual(ccr.lookup_phone(m, "A", "X"), ("111", "account_number"))

    def test_no_match(self):
        self.assertEqual(ccr.lookup_phone(self.maps, "99", "99"), (None, None))

    def test_blank_keys(self):
        self.assertEqual(ccr.lookup_phone(self.maps, "", ""), (None, None))
        self.assertEqual(ccr.lookup_phone(self.maps, None, None), (None, None))


# ─── Full process_rows against fixture ───────────────────────────────────────
class ProcessRowsAgainstFixture(unittest.TestCase):
    """Drive the full row loop against the fixture with mocked Casi + Sheets.
    DRY_RUN is left off so we exercise the post-Casi branches."""

    def setUp(self):
        self.cancel_headers, self.cancel_rows = _load_sheet("CANCELLATIONS")
        self.bindings = ccr.bind_columns(self.cancel_headers)
        sales_headers, sales_rows = _load_sheet("SALES")
        self.phone_maps = ccr.build_sales_phone_map(sales_headers, sales_rows)
        self._dry_run_was = ccr.DRY_RUN
        ccr.DRY_RUN = False
        self._token_was = ccr._casi_token
        ccr._casi_token = "fake-token"

    def tearDown(self):
        ccr.DRY_RUN = self._dry_run_was
        ccr._casi_token = self._token_was

    @patch("scripts.cancel_casi_revio.requests.delete")
    def test_full_run(self, mock_delete):
        mock_delete.return_value = MagicMock(
            status_code=200, text='{"removed":{"results":1}}',
            json=lambda: {"removed": {"results": 1}, "failed": {"results": 0}},
        )
        svc = MagicMock()
        stats, detail = ccr.process_rows(svc, self.cancel_rows, self.bindings,
                                         self.phone_maps)

        # 7 rows: 1 already, 1 unknown reason, 1 not in Sales, 1 malformed phone,
        # 3 cancellable (rows 2, 3, 4)
        self.assertEqual(stats["examined"], 7)
        self.assertEqual(stats["already_processed"], 1)
        self.assertEqual(stats["cancelled"], 3)
        self.assertEqual(stats["no_phone"], 1)
        self.assertEqual(stats["phone_invalid"], 1)
        # "skipped" includes unknown-reason + no_phone + phone_invalid
        self.assertEqual(stats["skipped"], 3)
        self.assertEqual(stats["errors"], 0)

        # Casi was called exactly 3 times (the 3 cancellable rows)
        self.assertEqual(mock_delete.call_count, 3)

        # Phone source breakdown: row 2 + row 4 via account, row 3 via id
        self.assertEqual(stats["phone_via_account_number"], 2)
        self.assertEqual(stats["phone_via_id_number"], 1)

        # Cover routing: row 2 VW, row 3 Audi, row 4 VW
        urls = [c.args[0] for c in mock_delete.call_args_list]
        self.assertEqual(sum(1 for u in urls if u.endswith(f"/covers/{ccr.COVER_VW}/users")), 2)
        self.assertEqual(sum(1 for u in urls if u.endswith(f"/covers/{ccr.COVER_AUDI}/users")), 1)

        # Every Casi DELETE is phone-only
        for call in mock_delete.call_args_list:
            payload = call.kwargs["json"]
            self.assertEqual(len(payload), 1)
            self.assertIn("cellphone", payload[0])
            self.assertNotIn("reference", payload[0])

        # Lost-leading-zero phone (row 4, raw 821234567) was normalised to 27821234567
        sent_phones = [c.kwargs["json"][0]["cellphone"] for c in mock_delete.call_args_list]
        self.assertIn("27821234567", sent_phones)

    @patch("scripts.cancel_casi_revio.requests.delete")
    def test_casi_error_isolated(self, mock_delete):
        # The first cancellable row gets a 500; others still run.
        mock_delete.side_effect = [
            MagicMock(status_code=500, text="Internal Server Error", json=lambda: {}),
            MagicMock(status_code=200, text='{"removed":{"results":1}}',
                      json=lambda: {"removed": {"results": 1}}),
            MagicMock(status_code=200, text='{"removed":{"results":0}}',
                      json=lambda: {"removed": {"results": 0}}),
        ]
        svc = MagicMock()
        stats, _ = ccr.process_rows(svc, self.cancel_rows, self.bindings,
                                    self.phone_maps)
        self.assertEqual(stats["errors"], 1)
        self.assertEqual(stats["cancelled"], 2)  # 1 ok + 1 not_found-counted-as-processed
        self.assertEqual(mock_delete.call_count, 3)


if __name__ == "__main__":
    unittest.main()
