"""Pure-function tests for scripts/silence_existing_revio_subscribers.py.

Covers identify_clients_to_patch — the only logic that's testable without
hitting the Revio API. All Revio I/O paths are exercised via the real
workflow_dispatch dry-run before any merge.
"""

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from silence_existing_revio_subscribers import (  # noqa: E402
    COMM_FLAGS,
    identify_clients_to_patch,
)


def _client(send_invoice_on_add_subscriber=False,
            send_invoice_on_charge_failure=False,
            send_receipt=False,
            **extra):
    """Build a BillingTemplateClient stub with the three comm flags set
    explicitly. Extra fields (id, personal_code, etc.) are merged in."""
    return {
        "id":               extra.pop("id", "btc-uuid-1"),
        "client_id":        extra.pop("client_id", "client-uuid-1"),
        "personal_code":    extra.pop("personal_code", "PCODE_TEST"),
        "send_invoice_on_add_subscriber":  send_invoice_on_add_subscriber,
        "send_invoice_on_charge_failure":  send_invoice_on_charge_failure,
        "send_receipt":                    send_receipt,
        **extra,
    }


class IdentifyClientsToPatch(unittest.TestCase):
    """The pure-function contract: any BTC where one or more of the three
    comm flags is not strictly False gets included; a BTC with all-three-
    False is skipped (no PATCH needed)."""

    def test_all_three_already_false_returns_empty(self):
        clients = [
            _client(),  # all defaults False
            _client(personal_code="PCODE_2"),
        ]
        self.assertEqual(identify_clients_to_patch(clients), [])

    def test_one_flag_true_is_included(self):
        clients = [
            _client(send_invoice_on_add_subscriber=True),
        ]
        out = identify_clients_to_patch(clients)
        self.assertEqual(len(out), 1)
        self.assertEqual(
            out[0]["non_silent_flags"],
            {"send_invoice_on_add_subscriber": True},
        )
        # The full client dict is preserved for downstream PATCH addressing
        self.assertEqual(out[0]["client"]["id"], "btc-uuid-1")

    def test_all_three_true_includes_all_three_in_diff(self):
        clients = [
            _client(send_invoice_on_add_subscriber=True,
                    send_invoice_on_charge_failure=True,
                    send_receipt=True),
        ]
        out = identify_clients_to_patch(clients)
        self.assertEqual(len(out), 1)
        self.assertEqual(
            set(out[0]["non_silent_flags"].keys()),
            set(COMM_FLAGS),
        )
        for flag in COMM_FLAGS:
            self.assertIs(out[0]["non_silent_flags"][flag], True)

    def test_mix_of_silent_and_non_silent_clients(self):
        """Realistic input: some subscribers already silent, some not.
        Only the non-silent ones are returned."""
        clients = [
            _client(personal_code="ALREADY_SILENT"),
            _client(personal_code="HALF_LOUD",
                    send_invoice_on_charge_failure=True),
            _client(personal_code="ALL_LOUD",
                    send_invoice_on_add_subscriber=True,
                    send_invoice_on_charge_failure=True,
                    send_receipt=True),
            _client(personal_code="ALSO_SILENT"),
        ]
        out = identify_clients_to_patch(clients)
        self.assertEqual(len(out), 2)
        codes = [item["client"]["personal_code"] for item in out]
        self.assertEqual(set(codes), {"HALF_LOUD", "ALL_LOUD"})

    def test_missing_flag_treated_as_not_silent(self):
        """A BTC missing one of the flag keys is treated as non-silent —
        we want the PATCH to make the absence explicit. Defensive against
        a future Revio API shape change that drops a default-true field."""
        client_with_missing_field = {
            "id":               "btc-uuid-x",
            "client_id":        "client-uuid-x",
            "personal_code":    "MISSING_FIELD",
            # send_invoice_on_add_subscriber field absent entirely
            "send_invoice_on_charge_failure":  False,
            "send_receipt":                    False,
        }
        out = identify_clients_to_patch([client_with_missing_field])
        self.assertEqual(len(out), 1)
        self.assertIn("send_invoice_on_add_subscriber",
                      out[0]["non_silent_flags"])
        # The missing-field value comes through as None
        self.assertIsNone(
            out[0]["non_silent_flags"]["send_invoice_on_add_subscriber"]
        )

    def test_empty_input_returns_empty(self):
        self.assertEqual(identify_clients_to_patch([]), [])


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


if __name__ == "__main__":
    unittest.main()
