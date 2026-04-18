#!/usr/bin/env python3
"""
scripts/export_accounts.py
Reads commission drilldown tabs from Google Sheets and writes
docs/data/accounts.json for the GitHub Pages dashboard.
Runs as part of the weekly sync workflow.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SHEET_ID     = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
OUTPUT_PATH  = "docs/data/accounts.json"

DRILLDOWN_TABS = {
    "Aug 2025": "Feb26 Data",
    "Sep 2025": "Mar26 Data",
    "Oct 2025": "April26 Data",
    "Nov 2025": "May26 Data",
    "Dec 2025": "Jun26 Data",
    "Jan 2026": "Jul26 Data",
    "Feb 2026": "Aug26 Data",
    "Mar 2026": "Sep26 Data",
}


def get_sheets_service():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS not set")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json), scopes=SHEET_SCOPES
    )
    return build("sheets", "v4", credentials=creds)


def read_tab(service, tab_name):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!A:E",
        ).execute()
    except Exception as e:
        logger.warning("Could not read tab '%s': %s", tab_name, e)
        return []

    values = result.get("values", [])
    if len(values) < 2:
        logger.warning("Tab '%s' is empty or has only headers", tab_name)
        return []

    headers = [str(h).strip().lower() for h in values[0]]
    logger.info("  Columns: %s", headers)

    def find_col(*terms):
        for t in terms:
            for i, h in enumerate(headers):
                if t.lower() in h:
                    return i
        return -1

    # Positional fallbacks match known sheet layout:
    # Sale Month | Account Number | Customer (VW/AUDI) | Membership Type | Status
    i_acct = find_col("account")          if find_col("account")                       >= 0 else 1
    i_cust = find_col("customer", "audi") if find_col("customer", "audi")              >= 0 else 2
    i_mem  = find_col("membership")       if find_col("membership")                    >= 0 else 3
    i_stat = find_col("status")           if find_col("status")                        >= 0 else 4

    def cell(row, idx):
        return str(row[idx]).strip() if 0 <= idx < len(row) else ""

    rows = []
    for row in values[1:]:
        account    = cell(row, i_acct)
        status_val = cell(row, i_stat)
        if account:
            rows.append({
                "account":    account,
                "customer":   cell(row, i_cust),
                "membership": cell(row, i_mem),
                "status":     status_val,
            })

    return rows


def main():
    logger.info("=" * 60)
    logger.info("EXPORT ACCOUNTS TO JSON")
    logger.info("=" * 60)

    service = get_sheets_service()
    months  = {}

    for display_month, tab_name in DRILLDOWN_TABS.items():
        logger.info("Reading %s (%s)...", display_month, tab_name)
        rows = read_tab(service, tab_name)
        months[display_month] = rows
        logger.info("  → %d active accounts", len(rows))

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "months": months,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    total = sum(len(v) for v in months.values())
    logger.info("Written %s — %d accounts across %d months", OUTPUT_PATH, total, len(months))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
