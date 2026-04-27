# ============================================================
# tests/test_sync_rejections.py
# Run with: pytest tests/test_sync_rejections.py
# ============================================================

import io
import os
import sys
import unittest
import zipfile

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import sync_rejections as sr


# Realistic CSV header copied from the spec — includes the slash in
# "ACCEPT/REJECT IND" so the slash-stripping in _norm gets exercised.
CSV_HEADER = ("VAP CATEGORY,VAP SUPPLIER,SUB CODE MARKETER,ACCEPT/REJECT IND,"
              "ACCOUNT,OLD SYSTEM ACCOUNT NO,RELOAD ACCOUNT,PRODUCT CODE,"
              "PRODUCT TERM,EFFECTIVE DATE,POLICY COST,REPAY PERIOD,"
              "ACCEPTED REJECTED,ERROR MESSAGE")

# Each row is a list aligned with CSV_HEADER above.
CSV_ROWS = [
    # 1. R + real error (kept)
    ["VAP", "FBI MOTOR", "MKT01", "R", "87026569628", "", "", "ALLRHLP",
     "12", "2026/04/24", "89", "1", "R", "ACCOUNT NUMBER NOT FOUND acc:87026569628"],
    # 2. R + duplicate-VAP (filtered)
    ["VAP", "FBI MOTOR", "MKT01", "R", "87026569629", "", "", "ALLRHLP",
     "12", "2026/04/24", "89", "1", "R",
     "A VAP OF THIS CATEGORY ALREADY EXISTS FOR THIS ACCOUNT"],
    # 3. A row (filtered)
    ["VAP", "FBI MOTOR", "MKT01", "A", "87026569630", "", "", "ALLRHLP",
     "12", "2026/04/24", "89", "1", "A", ""],
    # 4. R with leading-whitespace duplicate-VAP (filtered, case-insensitive)
    ["VAP", "FBI MOTOR", "MKT01", "R", "87026569631", "", "", "ALLRHLP",
     "12", "2026/04/24", "89", "1", "R",
     "  a vap of this category already exists for this acc"],
    # 5. R + Audi supplier + different real error (kept)
    ["VAP", "FBI MOTOR", "MKT02", "R", "87026569632", "", "", "ALLRHLP",
     "12", "2026/04/25", "159", "1", "R", "INVALID DEALER CODE acc:87026569632"],
]


def _build_test_zip():
    """Return (zip_bytes, csv_name) with the canned data above."""
    csv_name = "sp_rvap001vw20260424servicefbi_motorfbi_motor.csv"
    csv_text = CSV_HEADER + "\n" + "\n".join(",".join(r) for r in CSV_ROWS) + "\n"
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_text.encode(sr.CSV_ENCODING))
    return bio.getvalue(), csv_name


# ─── Pure helpers ────────────────────────────────────────────────────────────
class NormHelper(unittest.TestCase):
    def test_strips_whitespace_underscore_slash_and_lowercases(self):
        self.assertEqual(sr._norm("ACCEPT/REJECT IND"), "acceptrejectind")
        self.assertEqual(sr._norm("ACCEPT_REJECT_IND"), "acceptrejectind")
        self.assertEqual(sr._norm("Accept Reject Ind"), "acceptrejectind")
        self.assertEqual(sr._norm("ACCOUNT_NUMBER"),    "accountnumber")
        self.assertEqual(sr._norm("ACCOUNT"),           "account")

    def test_blank_inputs(self):
        self.assertEqual(sr._norm(""), "")
        self.assertEqual(sr._norm(None), "")


class DuplicateVapMatcher(unittest.TestCase):
    def test_canonical_prefix(self):
        self.assertTrue(sr._is_duplicate_vap(
            "A VAP OF THIS CATEGORY ALREADY EXISTS for account 123"))

    def test_lowercase(self):
        self.assertTrue(sr._is_duplicate_vap(
            "a vap of this category already exists"))

    def test_leading_whitespace(self):
        self.assertTrue(sr._is_duplicate_vap(
            "   A VAP OF THIS CATEGORY ALREADY EXISTS for ..."))

    def test_other_error_not_matched(self):
        for err in ["ACCOUNT NUMBER NOT FOUND",
                    "INVALID DEALER CODE",
                    "",
                    "VAP CATEGORY DOES NOT EXIST"]:
            self.assertFalse(sr._is_duplicate_vap(err), msg=err)


class NormaliseAccount(unittest.TestCase):
    def test_string_passthrough(self):
        self.assertEqual(sr._normalise_account("87026569628"), "87026569628")

    def test_int(self):
        self.assertEqual(sr._normalise_account(87026569628), "87026569628")

    def test_float_with_dot_zero(self):
        self.assertEqual(sr._normalise_account(87026569628.0), "87026569628")

    def test_string_with_dot_zero_suffix(self):
        self.assertEqual(sr._normalise_account("87026569628.0"), "87026569628")

    def test_blank(self):
        self.assertEqual(sr._normalise_account(""), "")
        self.assertEqual(sr._normalise_account(None), "")


# ─── Column binding ──────────────────────────────────────────────────────────
class CsvBindings(unittest.TestCase):
    def test_canonical_headers_bind(self):
        b = sr.bind_csv_columns(CSV_HEADER.split(","))
        self.assertEqual(b["accept_reject_ind"], "ACCEPT/REJECT IND")
        self.assertEqual(b["account_number"],    "ACCOUNT")
        self.assertEqual(b["error_message"],     "ERROR MESSAGE")
        for k in ("vap_supplier", "product_term", "effective_date",
                  "policy_cost", "accepted_rejected"):
            self.assertIsNotNone(b[k], k)

    def test_variant_headers_bind(self):
        b = sr.bind_csv_columns(["accept_reject_ind", "ACCOUNT", "Error Message"])
        self.assertEqual(b["accept_reject_ind"], "accept_reject_ind")
        self.assertEqual(b["account_number"],    "ACCOUNT")
        self.assertEqual(b["error_message"],     "Error Message")


class SheetBindings(unittest.TestCase):
    def test_8_col_sheet_binds_all_logical_fields(self):
        # The actual REJECTIONS tab — 8 columns in spec order
        sheet = ["ACCEPT/REJECT IND", "VAP SUPPLIER", "PRODUCT TERM",
                 "EFFECTIVE DATE", "POLICY COST", "ACCEPTED REJECTED",
                 "ERROR MESSAGE", "ACCOUNT_NUMBER"]
        b = sr.bind_sheet_columns(sheet)
        self.assertEqual(b["accept_reject_ind"], 0)
        self.assertEqual(b["vap_supplier"],      1)
        self.assertEqual(b["product_term"],      2)
        self.assertEqual(b["effective_date"],    3)
        self.assertEqual(b["policy_cost"],       4)
        self.assertEqual(b["accepted_rejected"], 5)
        self.assertEqual(b["error_message"],     6)
        self.assertEqual(b["account_number"],    7)

    def test_missing_column_returns_none(self):
        b = sr.bind_sheet_columns(["ACCEPT/REJECT IND", "VAP SUPPLIER"])
        self.assertEqual(b["accept_reject_ind"], 0)
        self.assertIsNone(b["account_number"])


# ─── Filter ──────────────────────────────────────────────────────────────────
class FilterAndTransform(unittest.TestCase):
    def setUp(self):
        # Build CSV-DictReader-shaped input from CSV_ROWS
        keys = CSV_HEADER.split(",")
        self.src = [dict(zip(keys, row)) for row in CSV_ROWS]
        self.bindings = sr.bind_csv_columns(keys)

    def test_keeps_only_real_R_rows(self):
        kept, stats = sr.filter_and_transform(self.src, self.bindings)
        # rows 1 and 5 above are kept
        self.assertEqual(len(kept), 2)
        self.assertEqual([r["account_number"] for r in kept],
                         ["87026569628", "87026569632"])

    def test_stats(self):
        _, stats = sr.filter_and_transform(self.src, self.bindings)
        self.assertEqual(stats["examined"], 5)
        self.assertEqual(stats["skipped_a"], 1)        # row 3
        self.assertEqual(stats["skipped_dup"], 2)      # rows 2 + 4
        self.assertEqual(stats["malformed"], 0)

    def test_empty_input(self):
        kept, stats = sr.filter_and_transform([], self.bindings)
        self.assertEqual(kept, [])
        self.assertEqual(stats, {"examined": 0, "skipped_a": 0,
                                 "skipped_dup": 0, "malformed": 0})

    def test_kept_rows_carry_full_field_set(self):
        kept, _ = sr.filter_and_transform(self.src, self.bindings)
        for r in kept:
            for logical, _, _ in sr.FIELDS:
                self.assertIn(logical, r, msg=logical)


# ─── Sheet alignment ─────────────────────────────────────────────────────────
class AlignToSheet(unittest.TestCase):
    def test_values_placed_at_bound_indexes(self):
        sheet = ["ACCEPT/REJECT IND", "VAP SUPPLIER", "PRODUCT TERM",
                 "EFFECTIVE DATE", "POLICY COST", "ACCEPTED REJECTED",
                 "ERROR MESSAGE", "ACCOUNT_NUMBER"]
        b = sr.bind_sheet_columns(sheet)
        rows = [{"accept_reject_ind": "R", "vap_supplier": "FBI MOTOR",
                 "product_term": "12", "effective_date": "2026/04/24",
                 "policy_cost": "89", "accepted_rejected": "R",
                 "error_message": "ACCOUNT NUMBER NOT FOUND",
                 "account_number": "87026569628"}]
        out = sr.align_to_sheet(rows, b, len(sheet))
        self.assertEqual(out, [["R", "FBI MOTOR", "12", "2026/04/24",
                                "89", "R", "ACCOUNT NUMBER NOT FOUND",
                                "87026569628"]])

    def test_account_number_force_string_for_int(self):
        # Even if a row dict somehow has an int account, align must emit a string
        sheet = ["ACCOUNT_NUMBER"]
        b = sr.bind_sheet_columns(sheet)
        out = sr.align_to_sheet([{"account_number": 87026569628}], b, 1)
        self.assertEqual(out, [["87026569628"]])
        self.assertIsInstance(out[0][0], str)

    def test_account_number_force_string_for_dot_zero_float(self):
        sheet = ["ACCOUNT_NUMBER"]
        b = sr.bind_sheet_columns(sheet)
        out = sr.align_to_sheet([{"account_number": 87026569628.0}], b, 1)
        self.assertEqual(out, [["87026569628"]])

    def test_unbound_sheet_columns_stay_blank(self):
        # Sheet has an extra column "RUN_DATE" that we don't write to
        sheet = ["ACCOUNT_NUMBER", "RUN_DATE"]
        b = sr.bind_sheet_columns(sheet)
        out = sr.align_to_sheet([{"account_number": "87026569628"}], b, 2)
        self.assertEqual(out, [["87026569628", ""]])


# ─── End-to-end zip → filter ─────────────────────────────────────────────────
class ZipParseAndFilter(unittest.TestCase):
    def test_parse_zip_extracts_csv(self):
        zip_bytes, expected_name = _build_test_zip()
        csv_name, src_rows = sr.parse_zip(zip_bytes, "test.zip")
        self.assertEqual(csv_name, expected_name)
        self.assertEqual(len(src_rows), 5)

    def test_zip_with_no_csv_raises(self):
        bio = io.BytesIO()
        with zipfile.ZipFile(bio, "w") as zf:
            zf.writestr("readme.txt", "hello")
        with self.assertRaises(RuntimeError):
            sr.parse_zip(bio.getvalue(), "noop.zip")

    def test_full_pipeline_zip_to_kept_rows(self):
        zip_bytes, _ = _build_test_zip()
        _, src_rows = sr.parse_zip(zip_bytes, "test.zip")
        bindings = sr.bind_csv_columns(src_rows[0].keys())
        kept, stats = sr.filter_and_transform(src_rows, bindings)
        self.assertEqual(len(kept), 2)
        self.assertEqual([_normalise(r) for r in kept], [
            ("R", "FBI MOTOR", "12", "2026/04/24", "89", "R",
             "ACCOUNT NUMBER NOT FOUND acc:87026569628", "87026569628"),
            ("R", "FBI MOTOR", "12", "2026/04/25", "159", "R",
             "INVALID DEALER CODE acc:87026569632", "87026569632"),
        ])


def _normalise(row):
    return tuple(row[k] for k, _, _ in sr.FIELDS)


if __name__ == "__main__":
    unittest.main()
