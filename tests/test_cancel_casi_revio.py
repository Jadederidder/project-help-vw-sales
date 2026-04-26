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


def _load_fixture():
    wb = openpyxl.load_workbook(FIXTURE, data_only=True)
    ws = wb["CANCELLATIONS"]
    rows = [[c.value if c.value is not None else "" for c in row] for row in ws.iter_rows()]
    return rows[0], rows[1:]


class PhoneNormalisation(unittest.TestCase):
    def test_local_10_digit(self):
        self.assertEqual(ccr.normalize_phone("0821234567"), "27821234567")

    def test_already_international(self):
        self.assertEqual(ccr.normalize_phone("27821234567"), "27821234567")

    def test_plus_prefix(self):
        self.assertEqual(ccr.normalize_phone("+27821234567"), "27821234567")

    def test_with_spaces_and_dashes(self):
        self.assertEqual(ccr.normalize_phone("(082) 123-4567"), "27821234567")

    def test_lost_leading_zero(self):
        # Sheets may store a phone as a number, dropping the leading 0
        self.assertEqual(ccr.normalize_phone(821234567), "27821234567")
        self.assertEqual(ccr.normalize_phone("821234567.0"), "27821234567")

    def test_blank_returns_empty(self):
        self.assertEqual(ccr.normalize_phone(""), "")
        self.assertEqual(ccr.normalize_phone(None), "")
        self.assertEqual(ccr.normalize_phone("   "), "")

    def test_garbage_returns_empty(self):
        self.assertEqual(ccr.normalize_phone("not a phone"), "")
        self.assertEqual(ccr.normalize_phone("123"), "")  # too short


class DecideAction(unittest.TestCase):
    def test_known_cancel_reasons(self):
        for r in ["LEGAL STATUS", "CUSTOMER REQUESTS CANCELLATION",
                  "ARREAR CANCELLATION", "VAP LOADED IN ERROR"]:
            self.assertEqual(ccr.decide_action(r), ("cancel", ""), msg=r)

    def test_case_insensitive(self):
        self.assertEqual(ccr.decide_action("legal status")[0], "cancel")
        self.assertEqual(ccr.decide_action("Legal Status")[0], "cancel")

    def test_blank_is_cancel(self):
        self.assertEqual(ccr.decide_action("")[0], "cancel")
        self.assertEqual(ccr.decide_action(None)[0], "cancel")
        self.assertEqual(ccr.decide_action("   ")[0], "cancel")

    def test_unknown_is_skip(self):
        action, note = ccr.decide_action("FRAUDULENT APPLICATION")
        self.assertEqual(action, "skip")
        self.assertIn("FRAUDULENT APPLICATION", note)
        self.assertIn("manual review", note)


class CoverRouting(unittest.TestCase):
    def test_audi_substring(self):
        self.assertEqual(ccr.cover_for_dea("AUDI CENTRE WESTRAND"), ccr.COVER_AUDI)
        self.assertEqual(ccr.cover_for_dea("audi menlyn"), ccr.COVER_AUDI)

    def test_vw_default(self):
        self.assertEqual(ccr.cover_for_dea("HATFIELD VW BRAAMFONTEIN"), ccr.COVER_VW)
        self.assertEqual(ccr.cover_for_dea("VW SANDTON"), ccr.COVER_VW)

    def test_blank_defaults_to_vw(self):
        self.assertEqual(ccr.cover_for_dea(""), ccr.COVER_VW)
        self.assertEqual(ccr.cover_for_dea(None), ccr.COVER_VW)


class HeaderBindings(unittest.TestCase):
    def test_fixture_binds_all_required_fields(self):
        headers, _ = _load_fixture()
        b = ccr.bind_columns(headers)
        for key in ccr.REQUIRED_FIELDS:
            self.assertIsNotNone(b[key], f"{key} should be bound; headers={headers}")

    def test_fuzzy_matches_underscore_and_space(self):
        b = ccr.bind_columns(["processed_date", "Casi Status", "Notes",
                              "Phone Number", "VAP_CANCEL_REASON_DESC",
                              "DEA NAME", "ACCOUNT_NUMBER"])
        self.assertEqual(b["processed_date"], 0)
        self.assertEqual(b["casi_status"], 1)
        self.assertEqual(b["notes"], 2)
        self.assertEqual(b["phone"], 3)
        self.assertEqual(b["reason"], 4)
        self.assertEqual(b["dea_name"], 5)
        self.assertEqual(b["account_number"], 6)

    def test_id_number_falls_back_to_cus_identity(self):
        # The upstream sync writes CUS_IDENTITY_OR_REG_NUM — variant must match
        b = ccr.bind_columns(["CUS_IDENTITY_OR_REG_NUM"])
        self.assertEqual(b["id_number"], 0)

    def test_missing_phone_column_returns_none(self):
        b = ccr.bind_columns(["Processed Date", "Casi Status", "Notes",
                              "VAP_CANCEL_REASON_DESC", "DEA_NAME", "ACCOUNT_NUMBER"])
        self.assertIsNone(b["phone"])


class ColumnLetter(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(ccr._col_letter(0), "A")
        self.assertEqual(ccr._col_letter(25), "Z")
        self.assertEqual(ccr._col_letter(26), "AA")
        self.assertEqual(ccr._col_letter(40), "AO")


class ProcessRowsAgainstFixture(unittest.TestCase):
    """Drive the full row loop against the fixture with mocked Casi + Sheets.
    DRY_RUN is left off so we exercise the post-Casi branches; the mocked
    Sheets svc.spreadsheets().values().batchUpdate() captures writes."""

    def setUp(self):
        self.headers, self.rows = _load_fixture()
        self.bindings = ccr.bind_columns(self.headers)
        self._dry_run_was = ccr.DRY_RUN
        ccr.DRY_RUN = False
        self._token_was = ccr._casi_token
        ccr._casi_token = "fake-token"  # bypass auth in tests

    def tearDown(self):
        ccr.DRY_RUN = self._dry_run_was
        ccr._casi_token = self._token_was

    @patch("scripts.cancel_casi_revio.requests.delete")
    def test_full_run(self, mock_delete):
        # Default: every Casi DELETE returns "removed 1"
        mock_delete.return_value = MagicMock(
            status_code=200, text='{"removed":{"results":1}}',
            json=lambda: {"removed": {"results": 1}, "failed": {"results": 0}},
        )
        svc = MagicMock()
        stats, detail = ccr.process_rows(svc, self.rows, self.bindings)

        self.assertEqual(stats["examined"], 7)
        self.assertEqual(stats["already_processed"], 1)  # row 1
        self.assertEqual(stats["skipped"], 2)            # rows 5, 6 (unknown + no-phone)
        self.assertEqual(stats["no_phone"], 1)           # row 6
        self.assertEqual(stats["cancelled"], 4)          # rows 2, 3, 4, 7
        self.assertEqual(stats["errors"], 0)

        # Casi was called 4 times (rows 2, 3, 4, 7) — others short-circuit before DELETE
        self.assertEqual(mock_delete.call_count, 4)

        # Verify cover routing on the actual calls
        call_urls = [c.args[0] for c in mock_delete.call_args_list]
        self.assertEqual(sum(1 for u in call_urls if u.endswith(f"/covers/{ccr.COVER_AUDI}/users")), 1)
        self.assertEqual(sum(1 for u in call_urls if u.endswith(f"/covers/{ccr.COVER_VW}/users")), 3)

        # Verify each Casi call sends phone-only payload
        for call in mock_delete.call_args_list:
            payload = call.kwargs["json"]
            self.assertEqual(len(payload), 1)
            self.assertIn("cellphone", payload[0])
            self.assertNotIn("reference", payload[0])

    @patch("scripts.cancel_casi_revio.requests.delete")
    def test_casi_error_is_isolated(self, mock_delete):
        # Make the FIRST Casi call fail with 500 — subsequent should still run
        responses = [
            MagicMock(status_code=500, text="Internal Server Error",
                      json=lambda: {}),
            MagicMock(status_code=200, text='{"removed":{"results":1}}',
                      json=lambda: {"removed": {"results": 1}}),
            MagicMock(status_code=200, text='{"removed":{"results":0}}',
                      json=lambda: {"removed": {"results": 0}, "failed": {"results": 1}}),
            MagicMock(status_code=200, text='{"removed":{"results":1}}',
                      json=lambda: {"removed": {"results": 1}}),
        ]
        mock_delete.side_effect = responses
        svc = MagicMock()
        stats, _ = ccr.process_rows(svc, self.rows, self.bindings)

        self.assertEqual(stats["errors"], 1)
        self.assertEqual(stats["cancelled"], 3)  # 1 ok + 1 not_found counted as processed + 1 ok
        self.assertEqual(mock_delete.call_count, 4)


if __name__ == "__main__":
    unittest.main()
