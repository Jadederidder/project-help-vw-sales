#!/usr/bin/env python3
# ============================================================
# scripts/backfill_sn_to_sales.py
#
# ONE-OFF — backfill the SN_NUMBER column (col AN) of the SALES tab from
# the latest Wesbank EOD file on SFTP. For every existing SALES row with
# a non-empty WesBank account number (col V) AND an empty SN_NUMBER cell,
# look up the Policy Number in the SFTP file by account number and write
# it to col AN of that row.
#
# Idempotent: rows with an existing SN_NUMBER value are skipped.
#
# Run with DRY_RUN=true first to print the plan without writing.
# After a clean live run, archive or delete this file — sync_sales_to_sheets.py
# handles new rows from the next Monday onwards.
#
# Env:
#   SFTP_PASSWORD              SFTP password
#   GOOGLE_SHEETS_CREDENTIALS  service-account JSON
#   EMAIL_SENDER               (optional) sender for summary email
#   EMAIL_PASSWORD             (optional) password for summary email
#   EMAIL_RECIPIENT            (optional) defaults to jd@projecthelp.io
#   DRY_RUN                    true → no writes, no email-to-all (only jd)
# ============================================================

import io
import logging
import os
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sync_sales_to_sheets import (
    SHEET_ID, SHEET_TAB,
    SN_NUMBER_VARIANTS, EXPECTED_SN_COL_IDX,
    _norm, _col_letter, find_sn_col_idx,
    get_sftp_client, find_latest_file, download_file,
    get_sheets_service,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DRY_RUN = os.environ.get("DRY_RUN", "").lower() == "true"
DRY_RUN_RECIPIENT = "jd@projecthelp.io"

# ACCOUNT_NUMBER lives in col V of the SALES tab.
ACCOUNT_NUMBER_COL_IDX = 21    # 0-based; col V


def _norm_account(v):
    """Coerce sheet/CSV cell to comparable account-number string. Handles
    floats with .0 suffix and trailing whitespace."""
    if v is None or v == "":
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def build_account_to_policy_map(buf):
    """Read the SFTP xlsx into a dict {account_number: policy_number}.
    Skips rows missing either. Last entry wins on duplicates."""
    df = pd.read_excel(buf)
    logger.info("SFTP file: %d rows, %d cols", len(df), len(df.columns))
    if "WesBank Account Number" not in df.columns:
        raise RuntimeError("Source file missing 'WesBank Account Number' column. "
                           f"Columns seen: {list(df.columns)[:10]}…")
    if "Policy Number" not in df.columns:
        raise RuntimeError("Source file missing 'Policy Number' column. "
                           f"Columns seen: {list(df.columns)[:10]}…")

    out = {}
    blank_acc = blank_policy = 0
    for _, row in df.iterrows():
        acc = _norm_account(row.get("WesBank Account Number"))
        if not acc:
            blank_acc += 1
            continue
        policy = row.get("Policy Number")
        try:
            if pd.isna(policy):
                blank_policy += 1
                continue
        except (TypeError, ValueError):
            pass
        if isinstance(policy, float) and policy.is_integer():
            policy = str(int(policy))
        else:
            policy = str(policy).strip()
            if policy.endswith(".0"):
                policy = policy[:-2]
        if not policy:
            blank_policy += 1
            continue
        out[acc] = policy
    logger.info("Account → Policy map: %d entries (skipped %d blank acc, %d blank policy)",
                len(out), blank_acc, blank_policy)
    return out


def read_sales_state(service):
    """Returns (sn_col_idx, [{row_num, account, existing_sn}, ...])."""
    # Header row to bind SN_NUMBER
    res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=SHEET_TAB + "!1:1",
    ).execute()
    header = (res.get("values") or [[]])[0]
    sn_idx = find_sn_col_idx(header)
    if sn_idx is None:
        if len(header) <= EXPECTED_SN_COL_IDX or not header[EXPECTED_SN_COL_IDX]:
            logger.info("SN_NUMBER header missing/blank — backfill will populate col AN (idx %d)",
                        EXPECTED_SN_COL_IDX)
            sn_idx = EXPECTED_SN_COL_IDX
        else:
            raise RuntimeError(
                f"col AN header is {header[EXPECTED_SN_COL_IDX]!r} — "
                "does not match any SN_NUMBER variant. Refusing to overwrite. "
                "Fix the header (e.g. set it to 'SN _NUMBER') and re-run."
            )
    else:
        logger.info("SN_NUMBER bound at col %s (idx %d)",
                    _col_letter(sn_idx), sn_idx)

    # Read both columns row-aligned: V (account) through AN (sn_number).
    # Range from V to the SN col covers everything we need.
    acc_letter = _col_letter(ACCOUNT_NUMBER_COL_IDX)
    sn_letter  = _col_letter(sn_idx)
    res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!{acc_letter}2:{sn_letter}",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    rows = res.get("values", [])
    sn_offset_in_range = sn_idx - ACCOUNT_NUMBER_COL_IDX

    sales = []
    for i, row in enumerate(rows):
        sheet_row = i + 2  # 1-based +1 for header
        acc = _norm_account(row[0]) if len(row) > 0 else ""
        existing_sn = ""
        if len(row) > sn_offset_in_range:
            v = row[sn_offset_in_range]
            existing_sn = _norm_account(v) if v not in (None, "") else ""
        sales.append({"row_num": sheet_row, "account": acc, "existing_sn": existing_sn})
    logger.info("SALES rows scanned: %d", len(sales))
    return sn_idx, sales


def write_updates(service, sn_col_idx, updates):
    """Apply updates in chunks via values.batchUpdate. Each update has
    {row_num, value}. Uses valueInputOption='RAW' so policy numbers stay
    as text (no scientific notation for long numerics)."""
    if not updates:
        return 0
    sn_letter = _col_letter(sn_col_idx)
    # Each update is a single-cell range
    data = [
        {"range": f"{SHEET_TAB}!{sn_letter}{u['row_num']}",
         "values": [[u["value"]]]}
        for u in updates
    ]
    CHUNK = 500
    written = 0
    for i in range(0, len(data), CHUNK):
        chunk = data[i:i + CHUNK]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        written += len(chunk)
        logger.info("  …wrote %d / %d cells", written, len(data))
    return written


def send_summary_email(stats, source_file, dry_run):
    sender  = os.environ.get("EMAIL_SENDER", "")
    pwd     = os.environ.get("EMAIL_PASSWORD", "")
    recip_s = os.environ.get("EMAIL_RECIPIENT", "")
    if not sender or not pwd:
        logger.info("EMAIL_SENDER/EMAIL_PASSWORD not set — skipping summary email")
        return
    if dry_run:
        recipients = [DRY_RUN_RECIPIENT]
    else:
        recipients = [r.strip() for r in recip_s.split(",") if r.strip()] or [DRY_RUN_RECIPIENT]

    subject_prefix = "[DRY RUN] " if dry_run else ""
    subject = (f"{subject_prefix}SALES SN_NUMBER backfill — "
               f"{stats['matched']} matched, {stats['unmatched']} unmatched, "
               f"{stats['already_populated']} already populated")
    body = "\n".join([
        f"SALES tab SN_NUMBER backfill — {datetime.utcnow().isoformat(timespec='seconds')}Z",
        "",
        f"  Source file (SFTP)   : {source_file}",
        f"  SALES rows scanned   : {stats['scanned']}",
        f"  Already populated    : {stats['already_populated']}",
        f"  Blank account number : {stats['blank_account']}",
        f"  Matched (would write): {stats['matched']}",
        f"  Unmatched in SFTP    : {stats['unmatched']}",
        "",
        "DRY RUN — no cells written." if dry_run else "Cells written: see log.",
    ]) + "\n"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content(body)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, pwd)
        smtp.send_message(msg)
    logger.info("Summary email sent → %s", msg["To"])


def main():
    logger.info("=" * 60)
    logger.info("SALES SN_NUMBER BACKFILL")
    logger.info("Dry run : %s", DRY_RUN)
    logger.info("=" * 60)

    # Step 1 — pull latest SFTP file
    ssh, sftp = get_sftp_client()
    try:
        folder, filename = find_latest_file(sftp)
        buf = download_file(sftp, folder, filename)
    finally:
        sftp.close(); ssh.close()
    logger.info("Source file: %s", filename)

    # Step 2 — build account → policy map
    acct_to_policy = build_account_to_policy_map(buf)

    # Step 3 — read SALES state
    service = get_sheets_service()
    sn_col_idx, sales = read_sales_state(service)

    # Step 4 — plan updates
    stats = {"scanned": len(sales), "already_populated": 0, "blank_account": 0,
             "matched": 0, "unmatched": 0}
    updates = []
    unmatched_samples = []
    for s in sales:
        if not s["account"]:
            stats["blank_account"] += 1
            continue
        if s["existing_sn"]:
            stats["already_populated"] += 1
            continue
        policy = acct_to_policy.get(s["account"])
        if policy is None:
            stats["unmatched"] += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append((s["row_num"], s["account"]))
            continue
        stats["matched"] += 1
        updates.append({"row_num": s["row_num"], "value": policy})

    logger.info("─" * 60)
    logger.info("Plan:")
    logger.info("  scanned            : %d", stats["scanned"])
    logger.info("  already populated  : %d", stats["already_populated"])
    logger.info("  blank account#     : %d", stats["blank_account"])
    logger.info("  matched (to write) : %d", stats["matched"])
    logger.info("  unmatched in SFTP  : %d", stats["unmatched"])
    if unmatched_samples:
        logger.info("  unmatched samples (first 5): %s", unmatched_samples)
    logger.info("─" * 60)

    if DRY_RUN:
        logger.info("DRY RUN — not writing. First 10 planned updates:")
        for u in updates[:10]:
            logger.info("  row %5d  →  AN: %s", u["row_num"], u["value"])
        if len(updates) > 10:
            logger.info("  … (%d more)", len(updates) - 10)
    elif updates:
        logger.info("Writing %d cell(s) to %s col AN …",
                    len(updates), SHEET_TAB)
        write_updates(service, sn_col_idx, updates)
        logger.info("Done.")
    else:
        logger.info("No updates to write.")

    send_summary_email(stats, filename, dry_run=DRY_RUN)
    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
