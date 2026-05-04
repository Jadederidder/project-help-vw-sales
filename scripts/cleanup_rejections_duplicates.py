#!/usr/bin/env python3
"""
scripts/cleanup_rejections_duplicates.py

One-off cleanup for the REJECTIONS tab.

WHY: Before the H ∪ J dedup fix in sync_rejections.py, daily Wesbank re-sends
of ACCOUNT EXPIRY rejections were appended as fresh rows whenever a row was
mid-conversion (col H blanked, account stashed in col J — see master doc
§4.5). This script removes the resulting duplicate rows.

WHAT IT DOES:
  - Reads the REJECTIONS tab.
  - For each row, computes the "canonical account" = col H if non-blank,
    otherwise col J. Rows with neither are skipped (logged).
  - Groups rows by canonical account. For any group with >1 row:
      * Keeps the EARLIEST row (lowest sheet row index — the original).
      * Marks the rest for deletion.
  - DRY_RUN=true (default): logs every row it would delete and exits without
    writing to the sheet.
  - DRY_RUN=false: deletes rows via batchUpdate deleteDimension calls,
    processed BOTTOM-UP (highest row index first) to avoid index drift.
  - Sends a run-summary email at the end (same pattern as sync_rejections).

ENV:
  GOOGLE_SHEETS_CREDENTIALS  service-account JSON
  EMAIL_SENDER               Gmail sender for the summary email
  EMAIL_PASSWORD             Gmail app password
  EMAIL_RECIPIENT            comma-separated recipient list (production only)
  DRY_RUN                    "true" (default) → no sheet writes, email jd only
"""

import json
import logging
import os
import smtplib
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from email_template import RunSummary, build_run_summary_email  # noqa: E402
from sync_rejections import (  # noqa: E402
    DRY_RUN_RECIPIENT,
    REJECTIONS_ORIGINAL_ACCOUNT_COL,
    SCOPES,
    SHEET_ID,
    TAB_NAME,
    _col_letter,
    _find_header_idx,
    _normalise_account,
    bind_sheet_columns,
)

logger = logging.getLogger(__name__)

# DRY_RUN defaults to TRUE here (opposite of sync_rejections) — this script
# deletes rows, so the safe default is to preview first.
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"


# ─── Sheets ──────────────────────────────────────────────────────────────────
def get_sheets_service():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS not set")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json), scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


def get_sheet_gid(svc, tab_name):
    meta = svc.spreadsheets().get(
        spreadsheetId=SHEET_ID,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"Tab {tab_name!r} not found")


def read_full_tab(svc):
    """Returns (header, list_of_data_rows). Each data row is padded to header
    length so column-index access is safe. UNFORMATTED_VALUE keeps long
    account numbers from arriving as scientific notation."""
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A1:ZZ",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    rows = res.get("values", [])
    if not rows:
        return [], []
    header = rows[0]
    n = len(header)
    data = [list(r) + [""] * (n - len(r)) for r in rows[1:]]
    return header, data


# ─── Pure: identify duplicates ───────────────────────────────────────────────
def identify_duplicates(data_rows, h_idx, j_idx):
    """Pure function. Returns:
      - keep:    [(row_1based, canonical_acct), ...] earliest of each group
      - delete:  [(row_1based, canonical_acct, row_values), ...] dups to drop
      - skipped: [(row_1based, reason), ...] rows excluded from grouping
                 (e.g. blank in both H and J)

    `data_rows` is 0-indexed; row 0 here is sheet row 2 (header is row 1).
    """
    by_acct = defaultdict(list)   # canonical_acct -> [(row_1based, row_values)]
    skipped = []

    for i, row in enumerate(data_rows):
        row_1based = i + 2  # +2: 0-indexed → 1-based, +1 for header row
        h_val = _normalise_account(row[h_idx]) if h_idx < len(row) else ""
        j_val = ""
        if j_idx is not None and j_idx < len(row):
            j_val = _normalise_account(row[j_idx])
        canonical = h_val or j_val
        if not canonical:
            skipped.append((row_1based,
                            "blank canonical account (H and J both empty)"))
            continue
        by_acct[canonical].append((row_1based, row))

    keep = []
    delete = []
    for acct, hits in by_acct.items():
        # Lowest row index = original (earliest appended)
        hits.sort(key=lambda t: t[0])
        keep.append((hits[0][0], acct))
        for row_1based, row_values in hits[1:]:
            delete.append((row_1based, acct, row_values))

    # Sort delete BOTTOM-UP — required so deleteDimension calls don't drift
    delete.sort(key=lambda t: t[0], reverse=True)
    return keep, delete, skipped


def delete_rows(svc, sheet_gid, delete_rows_1based):
    """Issue one batchUpdate with one deleteDimension request per row,
    processed bottom-up (caller's responsibility — `delete_rows_1based`
    must already be sorted descending)."""
    if not delete_rows_1based:
        return
    requests = []
    for row_1based in delete_rows_1based:
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_gid,
                    "dimension": "ROWS",
                    # API uses 0-based half-open ranges
                    "startIndex": row_1based - 1,
                    "endIndex": row_1based,
                },
            },
        })
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": requests},
    ).execute()


# ─── Email ───────────────────────────────────────────────────────────────────
def _row_field(row, idx):
    if idx is None or idx >= len(row):
        return ""
    v = row[idx]
    return str(v) if v is not None else ""


def _build_summary(run_date, kept_count, delete_planned, skipped, examined,
                   dry_run, error_summary, duration_seconds, header,
                   sheet_bindings):
    if error_summary:
        outcome = "failure"
        headline = "Error during cleanup"
        summary = (
            f"Cleanup failed before completion. {error_summary}. "
            f"No rows deleted; manual investigation needed."
        )
    elif not delete_planned:
        outcome = "noop"
        headline = "No duplicates found"
        summary = (
            f"Examined {examined} row(s) in {TAB_NAME}. "
            f"No duplicates detected — nothing to delete."
        )
    else:
        outcome = "success"
        verb = "would be deleted" if dry_run else "deleted"
        headline = f"{len(delete_planned)} duplicate row(s) {verb}"
        summary = (
            f"Examined {examined} row(s) in {TAB_NAME}. "
            f"Found {kept_count} canonical account(s) with duplicates; "
            f"{len(delete_planned)} duplicate row(s) {verb} "
            f"(earliest row of each group kept). "
            f"{len(skipped)} row(s) skipped (blank canonical account)."
        )

    numbers = {
        "Rows examined": examined,
        "Canonical accounts with duplicates": kept_count,
        ("Duplicates that would be deleted" if dry_run
         else "Duplicates deleted"): len(delete_planned),
        "Rows skipped (blank canonical)": len(skipped),
    }

    # Up to 25 rows of detail, so the email stays readable but a real cleanup
    # batch (we expect ~6 rows from the reported incident) shows fully.
    next_steps = []
    if delete_planned:
        ar_idx = sheet_bindings.get("accept_reject_ind")
        eff_idx = sheet_bindings.get("effective_date")
        err_idx = sheet_bindings.get("error_message")
        conv_idx = _find_header_idx(header, "Conversion_Status")

        sample_lines = []
        for row_1based, acct, row_values in delete_planned[:25]:
            err = _row_field(row_values, err_idx)
            if len(err) > 60:
                err = err[:57] + "…"
            sample_lines.append(
                f"row {row_1based}: acct={acct} "
                f"status={_row_field(row_values, conv_idx) or '∅'} "
                f"effective={_row_field(row_values, eff_idx)} "
                f"error={err!r}"
            )
        if len(delete_planned) > 25:
            sample_lines.append(
                f"… and {len(delete_planned) - 25} more "
                f"(see workflow logs for full list)"
            )
        next_steps.append("Rows targeted: " + " | ".join(sample_lines))

        if dry_run:
            next_steps.append(
                "Re-run with DRY_RUN=false to perform the deletions."
            )

    return RunSummary(
        workflow_name="VW Rejections Duplicate Cleanup",
        run_date=run_date,
        mode="dry_run" if dry_run else "production",
        outcome=outcome,
        headline=headline,
        summary_paragraph=summary,
        numbers=numbers,
        duration_seconds=duration_seconds,
        next_steps=next_steps,
        sheet_url=f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit",
    )


def send_summary_email(run_date, kept_count, delete_planned, skipped,
                       examined, dry_run, header, sheet_bindings,
                       error_summary="", duration_seconds=0.0):
    sender = os.environ.get("EMAIL_SENDER")
    pwd = os.environ.get("EMAIL_PASSWORD")
    recip_s = os.environ.get("EMAIL_RECIPIENT", "")
    if not sender or not pwd:
        logger.warning("EMAIL_SENDER / EMAIL_PASSWORD not set — skipping summary email")
        return
    if dry_run:
        recipients = [DRY_RUN_RECIPIENT]
    else:
        recipients = [r.strip() for r in recip_s.split(",") if r.strip()]
    if not recipients:
        logger.warning("No recipients — skipping summary email")
        return

    summary = _build_summary(
        run_date=run_date,
        kept_count=kept_count,
        delete_planned=delete_planned,
        skipped=skipped,
        examined=examined,
        dry_run=dry_run,
        error_summary=error_summary,
        duration_seconds=duration_seconds,
        header=header,
        sheet_bindings=sheet_bindings,
    )
    subject, html_body = build_run_summary_email(summary)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content("HTML email — see HTML part for the run summary.")
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, pwd)
        smtp.send_message(msg)
    logger.info("Summary email sent → %s%s",
                ", ".join(recipients),
                " (DRY RUN)" if dry_run else "")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    run_date = datetime.now(timezone.utc)
    started = time.monotonic()
    logger.info("=" * 60)
    logger.info("VW REJECTIONS DUPLICATE CLEANUP")
    logger.info("Run date : %s", run_date.isoformat(timespec="seconds"))
    logger.info("Dry run  : %s", DRY_RUN)
    logger.info("=" * 60)

    try:
        svc = get_sheets_service()
        header, data = read_full_tab(svc)
        if not header:
            raise RuntimeError(f"{TAB_NAME} tab is empty / missing header")
        logger.info("Read %d data row(s) from %s (header: %d cols)",
                    len(data), TAB_NAME, len(header))

        sheet_bindings = bind_sheet_columns(header)
        h_idx = sheet_bindings.get("account_number")
        if h_idx is None:
            raise RuntimeError(f"{TAB_NAME} has no ACCOUNT_NUMBER column")
        j_idx = _find_header_idx(header, REJECTIONS_ORIGINAL_ACCOUNT_COL)
        logger.info("Column indexes: H (ACCOUNT_NUMBER)=%s, J (%s)=%s",
                    _col_letter(h_idx),
                    REJECTIONS_ORIGINAL_ACCOUNT_COL,
                    _col_letter(j_idx) if j_idx is not None else "<missing>")

        keep, delete, skipped = identify_duplicates(data, h_idx, j_idx)
        logger.info("Duplicate analysis: %d canonical account(s) with dups, "
                    "%d row(s) to delete, %d row(s) skipped (blank canonical)",
                    len(keep), len(delete), len(skipped))

        for row_1based, reason in skipped:
            logger.info("  skipped row %d: %s", row_1based, reason)

        if not delete:
            logger.info("No duplicates — nothing to do.")
        else:
            for row_1based, acct, row_values in delete:
                err_idx = sheet_bindings.get("error_message")
                eff_idx = sheet_bindings.get("effective_date")
                conv_idx = _find_header_idx(header, "Conversion_Status")
                err = _row_field(row_values, err_idx)
                if len(err) > 80:
                    err = err[:77] + "…"
                logger.info(
                    "%s row %d: acct=%s status=%s effective=%s error=%r",
                    "[DRY RUN] would delete" if DRY_RUN else "deleting",
                    row_1based, acct,
                    _row_field(row_values, conv_idx) or "∅",
                    _row_field(row_values, eff_idx),
                    err,
                )

            if not DRY_RUN:
                sheet_gid = get_sheet_gid(svc, TAB_NAME)
                # delete is already sorted bottom-up by identify_duplicates()
                delete_rows(svc, sheet_gid,
                            [row_1based for row_1based, _, _ in delete])
                logger.info("Deleted %d row(s) from %s", len(delete), TAB_NAME)

        send_summary_email(
            run_date=run_date,
            kept_count=len(keep),
            delete_planned=delete,
            skipped=skipped,
            examined=len(data),
            dry_run=DRY_RUN,
            header=header,
            sheet_bindings=sheet_bindings,
            duration_seconds=time.monotonic() - started,
        )

    except Exception as e:
        logger.exception("Cleanup failed: %s", e)
        try:
            send_summary_email(
                run_date=run_date,
                kept_count=0,
                delete_planned=[],
                skipped=[],
                examined=0,
                dry_run=DRY_RUN,
                header=[],
                sheet_bindings={},
                error_summary=str(e),
                duration_seconds=time.monotonic() - started,
            )
        except Exception as e2:
            logger.error("Summary email also failed: %s", e2)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
