"""Pure-function tests for scripts/silence_existing_revio_subscribers.py.

Two contracts under test:

  1. identify_clients_to_patch buckets each BTC by status × flag-state.
     Active subscribers are the only ones eligible for PATCH; everything
     else (paused / inactive / pending / unknown) is bucketed into a
     skip pile and counted but never touched.

  2. The PATCH payload is locked to the three comm flags. Any extra key
     (especially `status`) raises before the network call — the safety
     net even if a future code path tries to inject one.

Live Revio I/O is exercised via the workflow_dispatch dry-run, not here.
"""

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from silence_existing_revio_subscribers import (  # noqa: E402
    ALLOWED_PATCH_FIELDS,
    COMM_FLAGS,
    _assert_patch_payload_safe,
    identify_clients_to_patch,
)


def _client(send_invoice_on_add_subscriber=False,
            send_invoice_on_charge_failure=False,
            send_receipt=False,
            status="active",
            **extra):
    """Build a BillingTemplateClient stub. Defaults to status=active +
    all-flags-false (the boring "already silent" case). Override per
    test."""
    return {
        "id":               extra.pop("id", "btc-uuid-1"),
        "client_id":        extra.pop("client_id", "client-uuid-1"),
        "personal_code":    extra.pop("personal_code", "PCODE_TEST"),
        "status":           status,
        "send_invoice_on_add_subscriber":  send_invoice_on_add_subscriber,
        "send_invoice_on_charge_failure":  send_invoice_on_charge_failure,
        "send_receipt":                    send_receipt,
        **extra,
    }


def _bucket_codes(buckets, key):
    """Helper: return list of personal_codes in a given bucket, for
    concise assertions."""
    return [item["client"]["personal_code"] for item in buckets[key]]


# ─── Status filter + flag-state bucketing ────────────────────────────────────
class IdentifyClientsToPatch(unittest.TestCase):
    """The pure-function contract: every input BTC lands in exactly one
    of the six buckets, deterministically by status × flag-state."""

    def test_active_with_one_flag_true_goes_to_to_patch(self):
        """(a) status=active + ≥1 flag not False → to_patch with the diff."""
        buckets = identify_clients_to_patch([
            _client(send_invoice_on_add_subscriber=True,
                    personal_code="ACTIVE_LOUD"),
        ])
        self.assertEqual(_bucket_codes(buckets, "to_patch"), ["ACTIVE_LOUD"])
        self.assertEqual(buckets["skipped_already_silent"], [])
        self.assertEqual(
            buckets["to_patch"][0]["non_silent_flags"],
            {"send_invoice_on_add_subscriber": True},
        )

    def test_active_with_all_flags_false_goes_to_already_silent(self):
        """(b) status=active + all 3 flags False → skipped_already_silent
        (idempotent skip; PATCHing would change nothing)."""
        buckets = identify_clients_to_patch([
            _client(personal_code="ACTIVE_SILENT"),
        ])
        self.assertEqual(buckets["to_patch"], [])
        self.assertEqual(_bucket_codes(buckets, "skipped_already_silent"),
                         ["ACTIVE_SILENT"])

    def test_paused_client_skipped_regardless_of_flag_state(self):
        """(c) status=subscription_paused → skipped_paused, never patched.
        Two clients exercise the "any flag state" requirement: one with
        flags already false, one with flags blaring."""
        buckets = identify_clients_to_patch([
            _client(status="subscription_paused", personal_code="PAUSED_QUIET"),
            _client(status="subscription_paused", personal_code="PAUSED_LOUD",
                    send_invoice_on_add_subscriber=True,
                    send_invoice_on_charge_failure=True,
                    send_receipt=True),
        ])
        self.assertEqual(buckets["to_patch"], [])
        self.assertEqual(set(_bucket_codes(buckets, "skipped_paused")),
                         {"PAUSED_QUIET", "PAUSED_LOUD"})
        # Crucially: paused-with-loud-flags is NOT in to_patch
        self.assertNotIn("PAUSED_LOUD", _bucket_codes(buckets, "to_patch"))

    def test_inactive_client_skipped_regardless_of_flag_state(self):
        """(d) status=inactive → skipped_inactive."""
        buckets = identify_clients_to_patch([
            _client(status="inactive", personal_code="INACTIVE_LOUD",
                    send_receipt=True),
        ])
        self.assertEqual(buckets["to_patch"], [])
        self.assertEqual(_bucket_codes(buckets, "skipped_inactive"),
                         ["INACTIVE_LOUD"])

    def test_pending_client_skipped_regardless_of_flag_state(self):
        """(e) status=pending → skipped_pending."""
        buckets = identify_clients_to_patch([
            _client(status="pending", personal_code="PENDING_LOUD",
                    send_invoice_on_charge_failure=True),
        ])
        self.assertEqual(buckets["to_patch"], [])
        self.assertEqual(_bucket_codes(buckets, "skipped_pending"),
                         ["PENDING_LOUD"])

    def test_unknown_status_skipped_into_unknown_bucket(self):
        """(f) status not in the 4 documented values (typo, drift, future
        new state) → skipped_unknown_status. Defensive against API drift."""
        buckets = identify_clients_to_patch([
            _client(status="weird_unknown_value", personal_code="WEIRD"),
            _client(status=None, personal_code="NULL_STATUS"),
            _client(personal_code="MISSING_STATUS_KEY",
                    status=None),  # explicit None covers the missing-key case
        ])
        self.assertEqual(buckets["to_patch"], [])
        self.assertEqual(
            set(_bucket_codes(buckets, "skipped_unknown_status")),
            {"WEIRD", "NULL_STATUS", "MISSING_STATUS_KEY"},
        )

    def test_mixed_population_lands_in_correct_buckets(self):
        """Realistic mixed input — assert each test client lands in
        exactly the right bucket. End-to-end sanity check."""
        clients = [
            _client(personal_code="A", send_receipt=True),                          # active loud
            _client(personal_code="B"),                                             # active silent
            _client(personal_code="C", status="subscription_paused"),               # paused
            _client(personal_code="D", status="inactive"),                          # inactive
            _client(personal_code="E", status="pending"),                           # pending
            _client(personal_code="F", status="???"),                               # unknown
            _client(personal_code="G", send_invoice_on_charge_failure=True),        # active loud
        ]
        buckets = identify_clients_to_patch(clients)
        self.assertEqual(set(_bucket_codes(buckets, "to_patch")),
                         {"A", "G"})
        self.assertEqual(_bucket_codes(buckets, "skipped_already_silent"),
                         ["B"])
        self.assertEqual(_bucket_codes(buckets, "skipped_paused"), ["C"])
        self.assertEqual(_bucket_codes(buckets, "skipped_inactive"), ["D"])
        self.assertEqual(_bucket_codes(buckets, "skipped_pending"), ["E"])
        self.assertEqual(_bucket_codes(buckets, "skipped_unknown_status"),
                         ["F"])

    def test_active_with_missing_flag_field_is_patched(self):
        """A field absent entirely is treated as non-silent (not False) →
        the client gets patched. Defensive against a Revio shape change
        that drops a default-true field."""
        client_with_missing = {
            "id": "btc-x", "client_id": "client-x",
            "personal_code": "MISSING_FIELD",
            "status": "active",
            # send_invoice_on_add_subscriber field absent entirely
            "send_invoice_on_charge_failure": False,
            "send_receipt": False,
        }
        buckets = identify_clients_to_patch([client_with_missing])
        self.assertEqual(_bucket_codes(buckets, "to_patch"),
                         ["MISSING_FIELD"])
        self.assertIn(
            "send_invoice_on_add_subscriber",
            buckets["to_patch"][0]["non_silent_flags"],
        )
        self.assertIsNone(
            buckets["to_patch"][0]["non_silent_flags"]
                  ["send_invoice_on_add_subscriber"]
        )

    def test_empty_input_returns_empty_buckets(self):
        buckets = identify_clients_to_patch([])
        self.assertEqual(buckets["to_patch"], [])
        self.assertEqual(buckets["skipped_already_silent"], [])
        self.assertEqual(buckets["skipped_paused"], [])
        self.assertEqual(buckets["skipped_inactive"], [])
        self.assertEqual(buckets["skipped_pending"], [])
        self.assertEqual(buckets["skipped_unknown_status"], [])


# ─── PATCH payload safety guard ──────────────────────────────────────────────
class PatchPayloadSafetyGuard(unittest.TestCase):
    """The PATCH payload is locked to exactly the three comm flags.
    Anything else raises BEFORE the network call so we can never
    accidentally mutate `status` or any other field on a subscriber."""

    def test_canonical_three_keys_pass(self):
        """(g) Payload with the exact 3 allowed keys → no error."""
        canonical = {
            "send_invoice_on_add_subscriber":  "false",
            "send_invoice_on_charge_failure":  "false",
            "send_receipt":                    "false",
        }
        # Should not raise
        _assert_patch_payload_safe(canonical)

    def test_extra_status_key_raises(self):
        """(h) An extra `status` key (the most-feared accidental injection)
        raises. The error message must surface the offending key."""
        with self.assertRaises(RuntimeError) as cm:
            _assert_patch_payload_safe({
                "send_invoice_on_add_subscriber":  "false",
                "send_invoice_on_charge_failure":  "false",
                "send_receipt":                    "false",
                "status":                          "active",
            })
        self.assertIn("status", str(cm.exception))
        self.assertIn("unexpected keys", str(cm.exception).lower())

    def test_extra_arbitrary_key_raises(self):
        """(i) Any non-allowed key raises, not just `status`."""
        with self.assertRaises(RuntimeError) as cm:
            _assert_patch_payload_safe({
                "send_invoice_on_add_subscriber":  "false",
                "send_invoice_on_charge_failure":  "false",
                "send_receipt":                    "false",
                "foo":                             "bar",
            })
        self.assertIn("foo", str(cm.exception))

    def test_subset_of_allowed_keys_passes(self):
        """A payload with fewer keys than allowed (e.g. only one flag
        being patched) is safe. The guard is about EXTRA keys, not
        missing ones."""
        _assert_patch_payload_safe({"send_receipt": "false"})


class CommFlagsContract(unittest.TestCase):
    """Sanity check that the three flag names match the master-doc §4.2
    contract exactly. If any of these names drift (typo, rename), the
    tests + the production code drift together rather than silently."""

    def test_comm_flags_names_and_count(self):
        self.assertEqual(
            tuple(COMM_FLAGS),
            ("send_invoice_on_add_subscriber",
             "send_invoice_on_charge_failure",
             "send_receipt"),
        )

    def test_allowed_patch_fields_matches_comm_flags(self):
        """The PATCH safety set is exactly the comm flags — the two
        constants must stay in lockstep."""
        self.assertEqual(set(ALLOWED_PATCH_FIELDS), set(COMM_FLAGS))


if __name__ == "__main__":
    unittest.main()
