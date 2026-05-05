"""Pure-function tests for scripts/backfill_vw_pending_banking.py.

Five contracts under test:

  a) classify_template — bucket each template title to its
     remediation path. Pins the VW vs Auto Ped split that drives
     the whole script.

  b) probe_bank_account / probe_bank_code — same shape checks the
     diagnostic uses. Determines whether a record needs patching at
     all and whether SALES data is good enough to use.

  c) build_sales_phone_index — phone-keyed lookup with last-write-
     wins on duplicates. The matching contract for "look up customer
     in SALES by phone".

  d) compute_patch_payload — the load-bearing decision: for each
     (client, sales_match) pair, what (if anything) gets PATCHed.
     Conservative rules: only patch fields that fail their probe AND
     have a passing replacement in SALES.

  e) assert_patch_payload_safe — belt-and-braces guard locking the
     PATCH payload to {bank_account, bank_code}. Defends against any
     future code path injecting status / personal_code / etc.

No tests against live Revio. No tests against live Google Sheets.
"""

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from backfill_vw_pending_banking import (  # noqa: E402
    ALLOWED_PATCH_FIELDS,
    SALES_BANK_ACC_COL,
    SALES_BANK_CODE_COL,
    SALES_PHONE_COL,
    assert_patch_payload_safe,
    build_sales_phone_index,
    classify_template,
    compute_patch_payload,
    probe_bank_account,
    probe_bank_code,
)


# ─── (a) classify_template ──────────────────────────────────────────────────
class ClassifyTemplate(unittest.TestCase):
    def test_vw_titles_classify_as_vw(self):
        self.assertEqual(classify_template("VW Premium HELP Single R89"), "vw")
        self.assertEqual(classify_template("VW Premium HELP Family R159"), "vw")

    def test_auto_ped_titles_classify_as_auto_ped(self):
        for t in ("Auto Ped Embedded Family",
                  "Auto Pedigree Family R159",
                  "Auto Pedigree R89"):
            self.assertEqual(classify_template(t), "auto_ped",
                             msg=f"{t!r} should bucket as auto_ped")

    def test_other_titles_classify_as_other(self):
        """Defensive: any future template (R59, AUDI, OLD BILLING, etc.)
        falls into "other" and is skipped — never patched."""
        for t in ("R59 OLD BILLING", "AUDI Premium HELP Single R89",
                  "VW Single R89 V2 (typo'd)",  # whitespace + version drift
                  "", None):
            self.assertEqual(classify_template(t), "other",
                             msg=f"{t!r} should bucket as other")


# ─── (b) probes ─────────────────────────────────────────────────────────────
class ProbeBankAccount(unittest.TestCase):
    def test_blank_inputs(self):
        self.assertEqual(probe_bank_account(""), "blank")
        self.assertEqual(probe_bank_account(None), "blank")
        self.assertEqual(probe_bank_account("   "), "blank")

    def test_non_numeric(self):
        self.assertEqual(probe_bank_account("abc"), "non_numeric")
        self.assertEqual(probe_bank_account("???"), "non_numeric")

    def test_too_short(self):
        # 8 digits — below the 9-digit floor (e.g. some ZA banks fall here)
        self.assertEqual(probe_bank_account("12345678"), "too_short")
        self.assertEqual(probe_bank_account("1234"), "too_short")

    def test_too_long(self):
        # 12 digits — above the 11-digit ceiling
        self.assertEqual(probe_bank_account("123456789012"), "too_long")

    def test_ok(self):
        self.assertEqual(probe_bank_account("123456789"), "ok")    # 9d
        self.assertEqual(probe_bank_account("12345678901"), "ok")  # 11d
        # whitespace tolerated
        self.assertEqual(probe_bank_account(" 1234567890 "), "ok")


class ProbeBankCode(unittest.TestCase):
    def test_blank_inputs(self):
        self.assertEqual(probe_bank_code(""), "blank")
        self.assertEqual(probe_bank_code(None), "blank")

    def test_wrong_length(self):
        # 5d — the most common bad shape per diagnostic (28 records).
        # Likely missing leading zero.
        self.assertEqual(probe_bank_code("12345"), "wrong_length")
        self.assertEqual(probe_bank_code("1234567"), "wrong_length")

    def test_ok(self):
        self.assertEqual(probe_bank_code("632005"), "ok")   # ABSA universal
        self.assertEqual(probe_bank_code("250655"), "ok")   # FNB universal
        # Trailing whitespace + non-digit chars stripped before probe
        self.assertEqual(probe_bank_code(" 632005 "), "ok")


# ─── (c) build_sales_phone_index ────────────────────────────────────────────
class BuildSalesPhoneIndex(unittest.TestCase):
    def test_normalises_phones_into_keys(self):
        rows = [
            {SALES_PHONE_COL: "0821111111",
             SALES_BANK_ACC_COL: "111111111", SALES_BANK_CODE_COL: "632005"},
            {SALES_PHONE_COL: "27822222222",
             SALES_BANK_ACC_COL: "222222222", SALES_BANK_CODE_COL: "250655"},
        ]
        idx = build_sales_phone_index(rows)
        self.assertEqual(set(idx.keys()), {"27821111111", "27822222222"})

    def test_blank_or_missing_phones_dropped(self):
        """Empty/missing phone rows must NOT enter the index — keying
        on "" would falsely match every phoneless pending Client."""
        rows = [
            {SALES_PHONE_COL: "",
             SALES_BANK_ACC_COL: "X", SALES_BANK_CODE_COL: "Y"},
            {SALES_PHONE_COL: None,
             SALES_BANK_ACC_COL: "X", SALES_BANK_CODE_COL: "Y"},
            {                                  # phone column entirely absent
             SALES_BANK_ACC_COL: "X", SALES_BANK_CODE_COL: "Y"},
            {SALES_PHONE_COL: "0823333333",
             SALES_BANK_ACC_COL: "333333333", SALES_BANK_CODE_COL: "470010"},
        ]
        idx = build_sales_phone_index(rows)
        self.assertEqual(list(idx.keys()), ["27823333333"])

    def test_duplicate_phones_last_write_wins(self):
        """SALES is sorted by Created Time ascending (per
        sync_sales_to_sheets.py) so the LATER row is the more recent
        sign-up. We want the latest banking, not the original."""
        rows = [
            {SALES_PHONE_COL: "0824444444",
             SALES_BANK_ACC_COL: "OLD_ACC", SALES_BANK_CODE_COL: "OLD_CODE"},
            {SALES_PHONE_COL: "0824444444",
             SALES_BANK_ACC_COL: "NEW_ACC", SALES_BANK_CODE_COL: "NEW_CODE"},
        ]
        idx = build_sales_phone_index(rows)
        self.assertEqual(idx["27824444444"][SALES_BANK_ACC_COL], "NEW_ACC")
        self.assertEqual(idx["27824444444"][SALES_BANK_CODE_COL], "NEW_CODE")

    def test_empty_input(self):
        self.assertEqual(build_sales_phone_index([]), {})


# ─── (d) compute_patch_payload — the load-bearing decision ──────────────────
class ComputePatchPayload(unittest.TestCase):
    """The rules:
      - Patch a field iff client value FAILS probe AND sales value
        PASSES probe.
      - Never touch a field already OK on the client.
      - Never overwrite with bad SALES data ("no improvement").
    Each test pins one branch of that rule."""

    def test_both_blank_on_client_both_ok_in_sales_patches_both(self):
        p = compute_patch_payload("", "", "123456789", "632005")
        self.assertEqual(p, {"bank_account": "123456789",
                             "bank_code":    "632005"})

    def test_only_bank_account_blank_patches_only_bank_account(self):
        """A common shape: bank_account broken, bank_code already OK.
        Don't touch the bank_code even if SALES has a different OK
        value — we don't second-guess live data."""
        p = compute_patch_payload(
            client_bank_acc="",         client_bank_code="632005",
            sales_bank_acc="123456789", sales_bank_code="999999",  # different but OK
        )
        self.assertEqual(p, {"bank_account": "123456789"})
        self.assertNotIn("bank_code", p)

    def test_5digit_bank_code_in_client_patches_from_sales(self):
        """The 28-record case from the diagnostic — bank_code is 5
        digits, SALES has the proper 6-digit value, patch it."""
        p = compute_patch_payload(
            client_bank_acc="123456789", client_bank_code="63200",
            sales_bank_acc="123456789",  sales_bank_code="632005",
        )
        self.assertEqual(p, {"bank_code": "632005"})

    def test_both_already_ok_returns_empty(self):
        """No patch needed → empty dict → caller short-circuits and
        skips the network call."""
        p = compute_patch_payload(
            "123456789", "632005", "999999999", "470010",
        )
        self.assertEqual(p, {})

    def test_sales_value_also_bad_does_not_patch(self):
        """Conservative: don't overwrite blank with blank, or 5-digit
        with 5-digit. SALES has to actually IMPROVE the field."""
        p = compute_patch_payload(
            client_bank_acc="", client_bank_code="63200",
            sales_bank_acc="", sales_bank_code="63200",  # also broken
        )
        self.assertEqual(p, {})

    def test_partial_improvement_only_patches_improvable_fields(self):
        """SALES has good bank_account but blank bank_code. Patch
        only the bank_account; leave the broken bank_code alone for
        manual review (better visible-broken than silently patched
        with another blank)."""
        p = compute_patch_payload(
            client_bank_acc="", client_bank_code="63200",
            sales_bank_acc="123456789", sales_bank_code="",
        )
        self.assertEqual(p, {"bank_account": "123456789"})

    def test_strips_whitespace_on_patch_values(self):
        p = compute_patch_payload(
            "", "", "  123456789  ", "  632005  ",
        )
        self.assertEqual(p, {"bank_account": "123456789",
                             "bank_code":    "632005"})


# ─── (e) PATCH payload safety guard ─────────────────────────────────────────
class AssertPatchPayloadSafe(unittest.TestCase):
    def test_canonical_two_keys_pass(self):
        assert_patch_payload_safe({"bank_account": "123", "bank_code": "456"})
        assert_patch_payload_safe({"bank_account": "123"})  # subset OK
        assert_patch_payload_safe({})  # empty OK (caller would skip anyway)

    def test_status_key_raises(self):
        """The single most-feared accidental injection: someone
        adding status='cancelled' to the payload (which is read-only
        on pending BTCs anyway, but the safety net catches it before
        the network)."""
        with self.assertRaises(RuntimeError) as cm:
            assert_patch_payload_safe({
                "bank_account": "123", "bank_code": "456",
                "status": "cancelled",
            })
        self.assertIn("status", str(cm.exception))
        self.assertIn("unexpected keys", str(cm.exception).lower())

    def test_personal_code_key_raises(self):
        """personal_code remediation was DEFERRED. If a future code
        path slips it into the payload, the guard surfaces the
        regression before it hits live data."""
        with self.assertRaises(RuntimeError) as cm:
            assert_patch_payload_safe({
                "bank_account": "123", "personal_code": "VW-123456",
            })
        self.assertIn("personal_code", str(cm.exception))

    def test_arbitrary_extra_key_raises(self):
        with self.assertRaises(RuntimeError) as cm:
            assert_patch_payload_safe({
                "bank_account": "123", "foo": "bar",
            })
        self.assertIn("foo", str(cm.exception))


class AllowedPatchFieldsContract(unittest.TestCase):
    """Sanity check that the allowlist is exactly the two banking
    fields. If any field name drifts (typo, rename), tests + guard
    drift together rather than silently."""

    def test_allowed_fields_exact_set(self):
        self.assertEqual(set(ALLOWED_PATCH_FIELDS),
                         {"bank_account", "bank_code"})


if __name__ == "__main__":
    unittest.main()
