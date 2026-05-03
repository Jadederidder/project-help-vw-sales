#!/usr/bin/env python3
# ============================================================
# scripts/sync_sales_to_sheets.py
# Runs every Monday at 17:13 SAST.
# Raw-SFTP passthrough — no column transformation. The SALES tab in the
# VW Reporting Master Book mirrors the Wesbank EOD file's column order
# and header text exactly. This script only:
#   1. Pulls the latest VW_Audi EOD .xlsx from SFTP.
#   2. Reads the SALES header to bind WesBank Account Number (dedupe key),
#      Created Time (sort key), and a few preview-only columns.
#   3. Filters SFTP rows down to those whose account number is NOT in
#      the dedupe set built from SALES col Y.
#   4. (Live) formats SALES col Y as TEXT, appends new rows in the same
#      column order as the live header, sorts the data range by Created
#      Time ascending, emails a summary.
#   5. (Dry-run) logs the same plan plus a 5-row preview and emails a
#      [DRY RUN] heartbeat to jd only — no writes, no Casi-style sort.
# ============================================================

import io
import json
import logging
import os
import re
import smtplib
import sys
import time
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

import paramiko
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.path.insert(0, str(Path(__file__).resolve().parent))
from email_template import RunSummary, build_run_summary_email  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
SFTP_HOST     = "eu-west-1.sftpcloud.io"
SFTP_PORT     = 22
SFTP_USER     = "projecthelp@projecthelp.co.za"
SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD", "")
SFTP_FOLDER   = "ProjectHelp/VW & Audi Sales"

SHEET_ID = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SHEET_TAB = "SALES"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

EMAIL_SENDER    = os.environ.get("EMAIL_SENDER", "")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "")
DRY_RUN_RECIPIENT = "jd@projecthelp.co.za"

DRY_RUN = os.environ.get("DRY_RUN", "").lower() == "true"

# Defensive header variants for cols we *must* bind. _norm collapses
# whitespace + underscores + slashes and lowercases, so e.g.
# "WesBank Account Number" / "WesBank_Account_Number" / "wesbank account number"
# all collapse to the same string.
HEADER_VARIANTS = {
    "account":      ["WesBank Account Number", "Wesbank Account Number",
                     "Account Number", "account number"],
    "created_time": ["Created Time (VW/Audi Campaign 1)", "Created Time"],
    "manufacturer": ["Manufacturer"],
    "first_name":   ["FirstName", "First Name"],
    "last_name":    ["Surname", "Last Name"],
}


# ─── Pure helpers ────────────────────────────────────────────────────────────
def _norm(s):
    return re.sub(r"[\s_/]", "", str(s or "").lower())


def _col_letter(idx_0):
    n = idx_0 + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _norm_account(v):
    """Strip non-digits — defeats scientific notation, .0 suffixes,
    whitespace, and any other formatting quirks. JD's safety req."""
    if v is None or v == "":
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return re.sub(r"\D", "", s)


def find_col(header, variants):
    """Return the 0-based index of the first variant that matches a header
    cell, or None."""
    norm_to_idx = {_norm(h): i for i, h in enumerate(header) if h}
    for v in variants:
        idx = norm_to_idx.get(_norm(v))
        if idx is not None:
            return idx
    return None


def bind_columns(header):
    """Returns {logical: idx_or_None}. Critical fields fail loud later if None."""
    return {key: find_col(header, variants)
            for key, variants in HEADER_VARIANTS.items()}


def _format_cell(val):
    """Coerce a pandas cell value into something Sheets API will accept as
    text (with valueInputOption='RAW')."""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    s = str(val) if val is not None else ""
    return "" if s in ("nan", "NaT", "None") else s


def align_row_to_header(row, header):
    """Build a list aligned to the live SALES header order. Each cell is
    the row's value for that header (or empty if absent)."""
    out = []
    for h in header:
        if not h:
            out.append("")
            continue
        out.append(_format_cell(row.get(h, "")))
    return out


# ─── SFTP ────────────────────────────────────────────────────────────────────
def get_sftp_client():
    logger.info("Connecting to SFTP %s as %s", SFTP_HOST, SFTP_USER)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD)
    return ssh, ssh.open_sftp()


def find_latest_file(sftp):
    files = sftp.listdir(SFTP_FOLDER)
    xlsx = sorted(f for f in files if f.endswith(".xlsx") and "VW_Audi" in f)
    if not xlsx:
        raise RuntimeError(f"No VW_Audi *.xlsx in {SFTP_FOLDER}")
    latest = xlsx[-1]
    logger.info("Latest SFTP file: %s", latest)
    return latest


def download_file(sftp, filename):
    buf = io.BytesIO()
    sftp.getfo(f"{SFTP_FOLDER}/{filename}", buf)
    buf.seek(0)
    return buf


# ─── Sheets ──────────────────────────────────────────────────────────────────
def get_sheets_service():
    raw = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
    if not raw:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS not set")
    creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_sheet_id(svc, tab_name):
    meta = svc.spreadsheets().get(
        spreadsheetId=SHEET_ID,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"Tab {tab_name!r} not found")


def read_header(svc):
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{SHEET_TAB}!1:1",
    ).execute()
    return (res.get("values") or [[]])[0]


def read_existing_accounts(svc, acct_idx):
    letter = _col_letter(acct_idx)
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!{letter}2:{letter}",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    return {acc for acc in (_norm_account(r[0]) for r in res.get("values", []) if r) if acc}


def read_total_data_rows(svc):
    """Returns the count of populated data rows (excluding header)."""
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{SHEET_TAB}!A:A",
    ).execute()
    n = len(res.get("values", []))
    return max(n - 1, 0)


def format_account_col_as_text(svc, sheet_id, acct_idx):
    """One-shot repeatCell — sets format of every data cell in the account
    column to TEXT. Idempotent."""
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{
            "repeatCell": {
                "range": {
                    "sheetId":           sheet_id,
                    "startRowIndex":     1,                # skip header row
                    "startColumnIndex":  acct_idx,
                    "endColumnIndex":    acct_idx + 1,
                },
                "cell":   {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        }]},
    ).execute()


def append_rows(svc, rows):
    if not rows:
        return
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def sort_by_column(svc, sheet_id, sort_col_idx, total_data_rows, header_cols):
    """Sort SALES!2:end ascending by sort_col_idx. Header row preserved."""
    if total_data_rows <= 1:
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{
            "sortRange": {
                "range": {
                    "sheetId":          sheet_id,
                    "startRowIndex":    1,                       # row 2
                    "endRowIndex":      total_data_rows + 1,     # +1 for header
                    "startColumnIndex": 0,
                    "endColumnIndex":   header_cols,
                },
                "sortSpecs": [{"dimensionIndex": sort_col_idx,
                               "sortOrder": "ASCENDING"}],
            }
        }]},
    ).execute()


# ─── Email ───────────────────────────────────────────────────────────────────
def _format_dt(v):
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d %H:%M")
    return str(v) if v is not None else ""


def _build_run_summary(stats, duration_seconds):
    new_rows = stats["new_rows"]
    dry_run = stats["dry_run"]
    mfr_counts = stats.get("mfr_counts") or {}

    if new_rows:
        outcome = "success"
        verb = "would be appended" if dry_run else "added to SALES tab"
        headline = f"{new_rows} new sale{'s' if new_rows != 1 else ''} {verb}"
        mfr_part = ""
        if mfr_counts:
            mfr_part = " (" + ", ".join(
                f"{v} {k.title() or 'unknown'}"
                for k, v in sorted(mfr_counts.items(), key=lambda x: -x[1])
            ) + ")"
        summary = (
            f"Pulled latest {stats['source_file']} from SFTP "
            f"({stats['file_rows']:,} rows in file). "
            f"{new_rows:,} new sales {'would be ' if dry_run else ''}appended "
            f"after deduping against {stats['existing_in_sales']:,} existing "
            f"account numbers{mfr_part}. Created Time range: "
            f"{stats['min_created']} → {stats['max_created']}."
        )
    else:
        outcome = "noop"
        headline = "No new sales to append"
        summary = (
            f"Pulled latest {stats['source_file']} from SFTP "
            f"({stats['file_rows']:,} rows in file). "
            f"All {stats['existing_in_sales']:,} accounts already present in SALES — "
            f"nothing to append."
        )

    numbers = {
        "Source file": stats["source_file"],
        "Rows in source file": f"{stats['file_rows']:,}",
        "Already in SALES": f"{stats['existing_in_sales']:,}",
        ("Would-be appended" if dry_run else "Appended"): f"{new_rows:,}",
        "SALES rows after": f"{stats['total_after']:,}",
        "Created Time range": f"{stats['min_created']} → {stats['max_created']}",
    }
    if mfr_counts:
        numbers["By Manufacturer"] = ", ".join(
            f"{k or '(blank)'}: {v:,}"
            for k, v in sorted(mfr_counts.items(), key=lambda x: -x[1])
        )

    sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid=0"

    return RunSummary(
        workflow_name="VW Sales Sync",
        run_date=datetime.now(),
        mode="dry_run" if dry_run else "production",
        outcome=outcome,
        headline=headline,
        summary_paragraph=summary,
        numbers=numbers,
        duration_seconds=duration_seconds,
        sheet_url=sheet_url,
    )


def send_summary_email(stats, duration_seconds=0.0):
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        logger.warning("EMAIL_SENDER / EMAIL_PASSWORD not set — skipping email")
        return
    if stats["dry_run"]:
        recipients = [DRY_RUN_RECIPIENT]
    else:
        recipients = [r.strip() for r in EMAIL_RECIPIENT.split(",") if r.strip()]
        if not recipients:
            recipients = [DRY_RUN_RECIPIENT]

    summary = _build_run_summary(stats, duration_seconds)
    subject, html_body = build_run_summary_email(summary)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = ", ".join(recipients)
    msg.set_content("HTML email — see HTML part for the run summary.")
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(EMAIL_SENDER, EMAIL_PASSWORD)
        s.send_message(msg)
    logger.info("Summary email sent → %s", msg["To"])


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    run_dt = datetime.utcnow()
    started = time.monotonic()
    logger.info("=" * 60)
    logger.info("VW/AUDI SALES SYNC (raw passthrough)")
    logger.info("Run date : %s UTC", run_dt.isoformat(timespec="seconds"))
    logger.info("Dry run  : %s", DRY_RUN)
    logger.info("=" * 60)

    # Step 1 — SFTP fetch
    ssh, sftp = get_sftp_client()
    try:
        filename = find_latest_file(sftp)
        buf = download_file(sftp, filename)
    finally:
        sftp.close(); ssh.close()
    df = pd.read_excel(buf)
    logger.info("Source file : %s  (%d rows × %d cols)",
                filename, len(df), len(df.columns))

    # Step 2 — read SALES header + bind required cols
    svc = get_sheets_service()
    header = read_header(svc)
    if not header:
        raise RuntimeError(f"{SHEET_TAB} header row is empty")
    logger.info("SALES header: %d cols", len(header))
    bindings = bind_columns(header)

    missing = [k for k in ("account", "created_time")
               if bindings[k] is None]
    if missing:
        logger.error("Could not bind required SALES columns: %s. "
                     "Header was: %s", missing, header)
        sys.exit(2)

    h_acct = header[bindings["account"]]
    h_ct   = header[bindings["created_time"]]
    logger.info("  account-number col : %s (idx %d, header %r)",
                _col_letter(bindings["account"]), bindings["account"], h_acct)
    logger.info("  created-time   col : %s (idx %d, header %r)",
                _col_letter(bindings["created_time"]), bindings["created_time"], h_ct)
    for opt in ("manufacturer", "first_name", "last_name"):
        idx = bindings[opt]
        logger.info("  %-15s col : %s",
                    opt, "(not bound)" if idx is None
                    else f"{_col_letter(idx)} (idx {idx}, header {header[idx]!r})")

    # Step 3 — dedupe set from SALES col Y
    if h_acct not in df.columns:
        logger.error("SFTP file is missing the source column %r — cannot dedupe. "
                     "Source columns: %s", h_acct, list(df.columns))
        sys.exit(3)
    existing = read_existing_accounts(svc, bindings["account"])
    logger.info("Existing accounts in SALES: %d", len(existing))

    # Step 4 — filter SFTP rows to new ones (also dedupe within file)
    new_rows = []
    seen_in_run = set()
    blank_acct = 0
    in_sales = 0
    in_run = 0
    for _, row in df.iterrows():
        acc = _norm_account(row.get(h_acct))
        if not acc:
            blank_acct += 1
            continue
        if acc in existing:
            in_sales += 1
            continue
        if acc in seen_in_run:
            in_run += 1
            continue
        seen_in_run.add(acc)
        new_rows.append(row)

    logger.info("Filter result:")
    logger.info("  blank account#       : %d", blank_acct)
    logger.info("  already in SALES     : %d", in_sales)
    logger.info("  duplicate within file: %d", in_run)
    logger.info("  NEW rows             : %d", len(new_rows))

    # Step 5 — preview (always)
    if new_rows:
        for i, r in enumerate(new_rows[:5], start=1):
            logger.info("  preview %d: acct=%-14s mfr=%-12s ct=%s  name=%s %s",
                        i,
                        r.get(h_acct, ""),
                        (r.get(header[bindings["manufacturer"]], "")
                         if bindings["manufacturer"] is not None else ""),
                        _format_dt(r.get(h_ct, "")),
                        (r.get(header[bindings["first_name"]], "")
                         if bindings["first_name"] is not None else ""),
                        (r.get(header[bindings["last_name"]], "")
                         if bindings["last_name"] is not None else ""))

    # Step 6 — stats
    if new_rows:
        ct_series = pd.to_datetime(
            pd.Series([r.get(h_ct) for r in new_rows]), errors="coerce")
        min_ct = _format_dt(ct_series.min())
        max_ct = _format_dt(ct_series.max())
    else:
        min_ct = max_ct = "(no new rows)"

    if new_rows and bindings["manufacturer"] is not None:
        h_mfr = header[bindings["manufacturer"]]
        mfr_counts = pd.Series(
            [str(r.get(h_mfr, "")).strip().upper() for r in new_rows]
        ).value_counts().to_dict()
    else:
        mfr_counts = {}

    # Step 7 — write (live only)
    if DRY_RUN or not new_rows:
        if DRY_RUN:
            logger.info("DRY RUN — skipping format-cell + append + sort")
        total_after = len(existing)   # plus any in the sheet but blank-accounted (rare)
        # Read for accuracy:
        try:
            total_after = read_total_data_rows(svc)
        except Exception:
            pass
    else:
        sheet_id = get_sheet_id(svc, SHEET_TAB)
        logger.info("Setting col %s format → TEXT (account #s as plain text) …",
                    _col_letter(bindings["account"]))
        format_account_col_as_text(svc, sheet_id, bindings["account"])

        logger.info("Appending %d row(s) to %s …", len(new_rows), SHEET_TAB)
        aligned = [align_row_to_header(r, header) for r in new_rows]
        append_rows(svc, aligned)

        total_after = read_total_data_rows(svc)
        logger.info("Sorting SALES!2:%d ascending by col %s (Created Time) …",
                    total_after + 1, _col_letter(bindings["created_time"]))
        sort_by_column(svc, sheet_id, bindings["created_time"],
                       total_after, len(header))

    # Step 8 — email summary
    stats = {
        "source_file":       filename,
        "file_rows":         len(df),
        "existing_in_sales": len(existing),
        "new_rows":          len(new_rows),
        "total_after":       total_after,
        "min_created":       min_ct,
        "max_created":       max_ct,
        "mfr_counts":        mfr_counts,
        "dry_run":           DRY_RUN,
    }
    send_summary_email(stats, duration_seconds=time.monotonic() - started)

    logger.info("=" * 60)
    logger.info("DONE — %s", "would have appended" if DRY_RUN else "appended")
    logger.info("  new rows         : %d", len(new_rows))
    logger.info("  total after      : %d", total_after)
    logger.info("  Created Time     : %s → %s", min_ct, max_ct)
    if mfr_counts:
        logger.info("  by Manufacturer  : %s", mfr_counts)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
