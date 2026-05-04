#!/usr/bin/env python3
"""
scripts/export_accounts.py

Reads every Invoice Data drilldown tab from the VW Reporting Master Book
and writes docs/data/accounts.json for the GitHub Pages dashboard.

Drilldown tabs are auto-created 6 months ahead of their dashboard month
by sync_monthly_invoice.py. The canonical name format is "Abbr'YY Invoice
Data" (e.g. "Apr'26 Invoice Data" → dashboard month 2025-10). This script
discovers them at runtime so a new tab next month requires zero code
changes.

Three legacy tabs deviate from canonical:
  - Feb'26 Invoice data         (lowercase 'data')
  - April'26 Invoice Data       (full month name 'April')
  - May'26 Invoice data         (lowercase 'data')
The parser tolerates both case variants of Data/data and both 3-letter
and full month names. Anything else is logged as a warning and skipped
(does not abort the run).

If two tabs map to the same dashboard month (e.g. legacy + canonical
variant of the same month both present), the run aborts with exit 2,
sends a failure email to JD via the standard run-summary template, and
LEAVES accounts.json UNTOUCHED so the dashboard keeps serving the
previous-known-good data until the duplicate is resolved manually.

Run locally:
  export GOOGLE_SHEETS_CREDENTIALS=$(cat /path/to/service-account.json)
  python3 scripts/export_accounts.py

Or pass the file path directly:
  CREDS_FILE=/path/to/sa.json python3 scripts/export_accounts.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import smtplib
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

sys.path.insert(0, str(Path(__file__).resolve().parent))
from email_template import RunSummary, build_run_summary_email  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SHEET_ID    = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
OUTPUT_PATH = "docs/data/accounts.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Failures email JD only — collision is a debugging signal, not a team broadcast.
JD_RECIPIENT = "jd@projecthelp.co.za"
SAST = timezone(timedelta(hours=2))

# Both 3-letter abbreviations and full month names map to month numbers.
# This is the only place month-name knowledge lives.
MONTH_NAMES = {
    "Jan": 1,  "January":   1,
    "Feb": 2,  "February":  2,
    "Mar": 3,  "March":     3,
    "Apr": 4,  "April":     4,
    "May": 5,
    "Jun": 6,  "June":      6,
    "Jul": 7,  "July":      7,
    "Aug": 8,  "August":    8,
    "Sep": 9,  "September": 9,
    "Oct": 10, "October":   10,
    "Nov": 11, "November":  11,
    "Dec": 12, "December":  12,
}

# "{MonthAbbrOrFull}'{YY} Invoice {Data|data}". Strict whitespace —
# tab names in the master sheet use single spaces.
TAB_RE = re.compile(r"^([A-Za-z]+)'(\d{2}) Invoice (Data|data)$")


def get_client():
    creds_file = os.environ.get("CREDS_FILE", "")
    if creds_file:
        creds_info = json.load(open(creds_file))
    else:
        creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
        if not creds_json:
            raise ValueError(
                "Set CREDS_FILE=/path/to/sa.json  or  GOOGLE_SHEETS_CREDENTIALS=<json>"
            )
        creds_info = json.loads(creds_json)

    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def parse_tab_to_month_key(title: str) -> str | None:
    """Return 'YYYY-MM' dashboard month key for an Invoice Data tab title,
    or None if the title doesn't parse. Logs a warning for invoice-shaped
    titles whose month component isn't a recognised name (e.g. typo)."""
    m = TAB_RE.match(title)
    if not m:
        return None
    month_name, yy, _data = m.groups()
    invoice_month = MONTH_NAMES.get(month_name)
    if invoice_month is None:
        # Looks like an Invoice Data tab but month isn't recognised — likely a typo.
        logger.warning(
            "Tab %r matches Invoice-Data shape but month %r is unrecognised; skipping",
            title, month_name,
        )
        return None
    # Subtract 6 months from (year, month) without a relativedelta dep.
    total = (2000 + int(yy)) * 12 + (invoice_month - 1) - 6
    dash_year, dash_month = divmod(total, 12)
    dash_month += 1
    return f"{dash_year:04d}-{dash_month:02d}"


def discover_drilldown_tabs(spreadsheet) -> tuple[dict[str, str], list[tuple[str, str, str]]]:
    """Returns ({YYYY-MM: tab_title} sorted ascending, collisions[]).

    `collisions` is a list of (month_key, first_tab, duplicate_tab) tuples.
    Caller decides whether to abort. Discovery itself never raises."""
    discovered: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []
    for ws in spreadsheet.worksheets():
        key = parse_tab_to_month_key(ws.title)
        if key is None:
            continue
        if key in discovered:
            collisions.append((key, discovered[key], ws.title))
            continue
        discovered[key] = ws.title
    return dict(sorted(discovered.items())), collisions


def read_tab(spreadsheet, tab_name):
    try:
        ws   = spreadsheet.worksheet(tab_name)
        rows = ws.get_all_records()
    except Exception as e:
        logger.warning("Could not read tab '%s': %s", tab_name, e)
        return []

    logger.info("  Tab '%s': %d rows total", tab_name, len(rows))
    if rows:
        logger.info("  Columns: %s", list(rows[0].keys()))

    result = []
    for row in rows:
        norm = {str(k).strip().lower(): str(v).strip() for k, v in row.items()}

        account = norm.get("account number", "")
        status  = norm.get("status", "")

        if not account:
            continue

        logger.debug("    account=%s status=%r", account, status)

        if status.upper() != "YES":
            continue

        customer = ""
        for k, v in norm.items():
            if "customer" in k:
                customer = v
                break

        result.append({
            "account":  account,
            "type":     norm.get("membership type", norm.get("membership", "")),
            "customer": customer,
        })

    return result


def send_failure_email(collisions, duration_seconds):
    """Send a run-summary failure email to JD. Best-effort — never raises."""
    sender   = os.environ.get("EMAIL_SENDER", "")
    password = os.environ.get("EMAIL_PASSWORD", "")
    if not sender or not password:
        logger.warning(
            "EMAIL_SENDER / EMAIL_PASSWORD not set — cannot send collision alert"
        )
        return

    detail_lines = [
        f"{key}: tabs {first!r} AND {dup!r} both map here"
        for key, first, dup in collisions
    ]
    summary = (
        f"Discovered {len(collisions)} duplicate dashboard-month mapping(s) in the "
        "VW Reporting Master Book. accounts.json was NOT written; the live dashboard "
        "continues to serve the previous-known-good data. Most likely cause: a legacy "
        "and canonical variant of the same month both exist (e.g. \"April'26 Invoice "
        "Data\" alongside \"Apr'26 Invoice Data\"). Resolve by renaming or deleting the "
        "duplicate in the master sheet, then re-run the workflow."
    )
    rs = RunSummary(
        workflow_name="VW Drilldown Export",
        run_date=datetime.now(SAST),
        mode="production",
        outcome="failure",
        headline=f"Duplicate drilldown tab mapping — sync aborted ({len(collisions)} collision(s))",
        summary_paragraph=summary,
        numbers={"Collisions": len(collisions)},
        duration_seconds=duration_seconds,
        next_steps=[
            "Open the VW Reporting Master Book and rename or delete one of each duplicate pair:",
            *(f"  • {line}" for line in detail_lines),
            "Then re-run the Sync VW Audi Sales workflow.",
        ],
    )
    subject, html_body = build_run_summary_email(rs)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = JD_RECIPIENT
    msg.set_content("HTML email — see HTML part for the run summary.")
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        logger.info("Collision alert email sent → %s", JD_RECIPIENT)
    except Exception as e:
        logger.error("Failed to send collision alert email: %s", e)


def main():
    started = time.monotonic()
    logger.info("=" * 60)
    logger.info("EXPORT DRILLDOWN DATA TO JSON")
    logger.info("=" * 60)

    gc          = get_client()
    spreadsheet = gc.open_by_key(SHEET_ID)

    discovered, collisions = discover_drilldown_tabs(spreadsheet)
    logger.info("Discovered %d Invoice Data tabs", len(discovered))
    for key, tab in discovered.items():
        logger.info("  %s  →  %r", key, tab)

    if collisions:
        logger.error(
            "ABORTING: %d duplicate dashboard-month mapping(s) detected — "
            "accounts.json will NOT be overwritten:", len(collisions),
        )
        for key, first, dup in collisions:
            logger.error("  %s: %r AND %r both map here", key, first, dup)
        send_failure_email(collisions, duration_seconds=time.monotonic() - started)
        sys.exit(2)

    months = {}
    for key, tab in discovered.items():
        logger.info("Reading %s (%s)...", key, tab)
        rows = read_tab(spreadsheet, tab)
        months[key] = rows
        logger.info("  → %d active accounts", len(rows))

    # Dual-shape output for safe frontend rollback. The new frontend reads
    # the nested `months` / `tabs`; if it has to be reverted, the old code
    # reads the legacy top-level keys and works as before. Drop the
    # duplication ~2 weeks after merge once the new frontend is proven.
    output = {**months, "months": months, "tabs": discovered}

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in months.values())
    logger.info(
        "Written %s — %d accounts across %d months (dual-shape)",
        OUTPUT_PATH, total, len(months),
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
