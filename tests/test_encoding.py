"""Regression test for the cp1252 fixture.

The VW cancellation CSV encodes the en-dash as byte 0x96 (Windows-1252).
Decoded as cp1252 that byte maps to U+2013 (EN DASH).
Decoded as utf-8 or latin-1 you get mojibake or a decode error.

If this test ever starts failing, someone re-saved the fixture through a
UTF-8 editor and the sync will start writing mojibake into the sheet.
"""

from pathlib import Path

FIXTURE = (
    Path(__file__).resolve().parent.parent
    / "scripts" / "fixtures" / "sample_cancellations.csv"
)


def test_fixture_is_cp1252_with_endash():
    assert FIXTURE.exists(), (
        f"Missing fixture: {FIXTURE}. See scripts/fixtures/README.md."
    )
    raw = FIXTURE.read_bytes()

    assert b"\x96" in raw, (
        "Fixture must contain cp1252 byte 0x96 (en-dash). "
        "If it doesn't, the file has been re-saved in a UTF-8 editor."
    )

    text = raw.decode("cp1252")
    assert "\u2013" in text, (
        "cp1252 byte 0x96 must decode to en-dash U+2013. "
        "If this fails the fixture is mis-encoded."
    )
