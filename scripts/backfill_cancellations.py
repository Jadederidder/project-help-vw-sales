#!/usr/bin/env python3
"""
scripts/backfill_cancellations.py

One-off back-fill for CANCELLATIONS rows that were appended half-empty
before the 36-column schema fix. Re-parses the 22 April 2026 fixture and
fills only the blank cells on matched rows — never overwrites.

Usage:
  DRY_RUN=true python3 scripts/backfill_cancellations.py   # preview
  python3 scripts/backfill_cancellations.py                # live

Matches rows by ACCOUNT_NUMBER. Expected matches:
  87028413213, 87029299498, 87030062313, 87030453843
"""

import csv
import io
import logging
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from sync_cancellations import (  # noqa: E402
    CSV_COLUMNS,
    CSV_ENCODING,
    SHEET_ID,
    TAB_NAME,
    _col_letter,
    _norm,
    account_number_col_idx,
    get_sheets_service,
    transform_row,
)

logger = logging.getLogger(__name__)

FIXTURE = HERE / "fixtures" / "sample_cancellations.csv"
DRY_RUN = os.environ.get("DRY_RUN", "").lower() == "true"
EXPECTED_ACCOUNTS = {"87028413213", "87029299498", "87030062313", "87030453843"}


def _account_str(val):
    if val in (None, ""):
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _is_blank(cell):
    """Sheets UNFORMATTED_VALUE treats empty as '' or absent — both blank here."""
    return cell is None or cell == ""


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger.info("=" * 60)
    logger.info("BACKFILL CANCELLATIONS  (dry_run=%s)", DRY_RUN)
    logger.info("=" * 60)

    # ── 1. Parse fixture ──────────────────────────────────────────────────────
    text = FIXTURE.read_bytes().decode(CSV_ENCODING)
    src_rows = list(csv.DictReader(io.StringIO(text)))
    transformed = [transform_row(r) for r in src_rows]
    by_acc = {_account_str(r.get("ACCOUNT_NUMBER")): r for r in transformed}
    logger.info("Fixture: %d rows, accounts=%s", len(transformed), sorted(by_acc))

    missing_in_fixture = EXPECTED_ACCOUNTS - set(by_acc)
    if missing_in_fixture:
        logger.error("Expected accounts not in fixture: %s", sorted(missing_in_fixture))
        sys.exit(1)

    # ── 2. Read sheet header + all data rows ──────────────────────────────────
    service = get_sheets_service()
    header_res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!1:1",
    ).execute()
    header = (header_res.get("values") or [[]])[0]
    if not header:
        raise RuntimeError(f"{TAB_NAME} header is empty")
    end_letter = _col_letter(len(header) - 1)
    logger.info("Sheet header (%d cols): %s", len(header), header)

    acc_idx = account_number_col_idx(header)
    if acc_idx is None:
        raise RuntimeError("ACCOUNT_NUMBER column not found in sheet header")

    data_res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A2:{end_letter}",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    data = data_res.get("values", [])
    logger.info("Sheet data rows: %d", len(data))

    # Build { normalized_header → (col_idx, csv_column_name) } so we only
    # ever write to known CSV columns. Unknown sheet columns are left alone.
    csv_norm_to_name = { _norm(c): c for c in CSV_COLUMNS }
    header_slot = {}  # col_idx → csv_column_name
    for col_idx, col_name in enumerate(header):
        match = csv_norm_to_name.get(_norm(col_name))
        if match is not None:
            header_slot[col_idx] = match

    # ── 3. Walk sheet, plan per-cell updates ──────────────────────────────────
    # Per matched account: row number + list of fills + list of skips.
    plan = {}  # acc_str → {"row": int, "fills": [(col_name, val)], "skips": [(col_name, existing)]}
    updates = []  # (row_num_1based, col_idx_0based, value, col_name) — only fills

    for row_offset, row in enumerate(data):
        row_num = row_offset + 2
        sheet_acc = _account_str(row[acc_idx] if acc_idx < len(row) else None)
        if not sheet_acc or sheet_acc not in by_acc:
            continue
        new_row = by_acc[sheet_acc]
        entry = {"row": row_num, "fills": [], "skips": []}
        for col_idx, csv_name in header_slot.items():
            new_val = new_row.get(csv_name, "")
            if new_val in (None, ""):
                continue
            existing = row[col_idx] if col_idx < len(row) else ""
            if _is_blank(existing):
                entry["fills"].append((csv_name, new_val))
                updates.append((row_num, col_idx, new_val, csv_name))
            else:
                entry["skips"].append((csv_name, existing))
        plan[sheet_acc] = entry

    matched = set(plan)
    unmatched = EXPECTED_ACCOUNTS - matched
    if unmatched:
        logger.error("ABORT — expected account(s) not found in sheet: %s",
                     sorted(unmatched))
        logger.error("No writes will be performed.")
        sys.exit(1)

    # ── 4. Report plan (per expected account) ─────────────────────────────────
    logger.info("")
    logger.info("─" * 60)
    logger.info("PLAN (per matched account):")
    logger.info("─" * 60)
    for acc in sorted(EXPECTED_ACCOUNTS):
        entry = plan[acc]
        logger.info("")
        logger.info("  ACCOUNT_NUMBER = %s  →  sheet row %d", acc, entry["row"])
        logger.info("    would FILL (%d cells):", len(entry["fills"]))
        if entry["fills"]:
            for col_name, val in entry["fills"]:
                logger.info("      %-30s = %r", col_name, val)
        else:
            logger.info("      (none — row already complete)")
        logger.info("    would SKIP (%d cells — already populated, not overwriting):",
                    len(entry["skips"]))
        if entry["skips"]:
            for col_name, existing in entry["skips"]:
                logger.info("      %-30s existing=%r", col_name, existing)
        else:
            logger.info("      (none)")
    logger.info("")
    logger.info("─" * 60)

    total_rows = len(matched)
    if DRY_RUN:
        logger.info("%d cells would be updated across %d rows (DRY RUN — no writes performed)",
                    len(updates), total_rows)
        return
    if not updates:
        logger.info("0 cells updated across %d rows (nothing to fill)", total_rows)
        return

    # ── 5. Apply ──────────────────────────────────────────────────────────────
    data_body = [
        {
            "range": f"{TAB_NAME}!{_col_letter(ci)}{rn}",
            "values": [[v]],
        }
        for rn, ci, v, _ in updates
    ]
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": data_body},
    ).execute()
    logger.info("%d cells updated across %d rows", len(updates), total_rows)


if __name__ == "__main__":
    main()
