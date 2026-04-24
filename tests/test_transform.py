"""Schema regression test for sync_cancellations.transform_row.

The VW cancellations sheet has 36 columns using the raw CSV header names
(with ACC_NUM renamed to ACCOUNT_NUMBER). A prior version used a made-up
16-field schema and appended rows with 30 blank cells — this guards against
that reverting.
"""

import csv
import io
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from sync_cancellations import (  # noqa: E402
    CSV_COLUMNS,
    CSV_ENCODING,
    DATE_FIELDS,
    NUMERIC_FIELDS,
    transform_row,
)

FIXTURE = HERE.parent / "scripts" / "fixtures" / "sample_cancellations.csv"


def _parse_fixture():
    raw = FIXTURE.read_bytes().decode(CSV_ENCODING)
    return list(csv.DictReader(io.StringIO(raw)))


def test_fixture_has_four_rows():
    assert len(_parse_fixture()) == 4


def test_csv_columns_has_36_unique_entries():
    assert len(CSV_COLUMNS) == 36
    assert len(set(CSV_COLUMNS)) == 36


def test_transform_row_produces_exactly_the_36_csv_columns():
    for src in _parse_fixture():
        row = transform_row(src)
        assert set(row.keys()) == set(CSV_COLUMNS), (
            f"Expected exactly the 36 CSV_COLUMNS keys; "
            f"extra={set(row) - set(CSV_COLUMNS)} "
            f"missing={set(CSV_COLUMNS) - set(row)}"
        )


def test_acc_num_is_renamed_to_account_number():
    for src in _parse_fixture():
        assert "ACC_NUM" in src, "fixture sanity: source uses ACC_NUM"
        row = transform_row(src)
        assert "ACC_NUM" not in row
        assert "ACCOUNT_NUMBER" in row
        assert row["ACCOUNT_NUMBER"] not in (None, "")


def test_expected_account_numbers_present():
    rows = [transform_row(r) for r in _parse_fixture()]
    got = {str(r["ACCOUNT_NUMBER"]) for r in rows}
    assert got == {"87028413213", "87029299498", "87030062313", "87030453843"}


def test_numeric_fields_are_typed():
    row0 = transform_row(_parse_fixture()[0])
    assert isinstance(row0["ACCOUNT_NUMBER"], int)
    assert row0["ACCOUNT_NUMBER"] == 87028413213
    assert row0["VAP_PREMIUM_AMT"] == 89
    assert isinstance(row0["VAP_PREMIUM_AMT"], int)
    assert row0["TOT_PREMIUM_COLLECTED"] == 534


def test_date_fields_are_iso():
    row0 = transform_row(_parse_fixture()[0])
    assert row0["VAP_EFFECTIVE_DATE"] == "2025-09-19"
    assert row0["VAP_STATUS_DATE"] == "2026-04-22"
    assert row0["ACC_EXPIRY_DATE"] == "2031-08-31"


def test_identifier_columns_stay_as_strings():
    row0 = transform_row(_parse_fixture()[0])
    # SA ID numbers have leading zeros — must not become int.
    assert row0["CUS_IDENTITY_OR_REG_NUM"] == "0005115081084"
    assert isinstance(row0["CUS_IDENTITY_OR_REG_NUM"], str)
    # OLD_SYSTEM_ACCNUM has letters.
    assert row0["OLD_SYSTEM_ACCNUM"] == "VRG49441X"


def test_blank_numeric_stays_blank_not_zero():
    # row 3 (index 2) has blank TOT_PREMIUM_COLLECTED.
    row2 = transform_row(_parse_fixture()[2])
    assert row2["TOT_PREMIUM_COLLECTED"] == ""


def test_numeric_and_date_fields_are_subsets_of_csv_columns():
    assert NUMERIC_FIELDS.issubset(CSV_COLUMNS)
    assert DATE_FIELDS.issubset(CSV_COLUMNS)
