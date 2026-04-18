#!/usr/bin/env python3
"""
scripts/export_accounts.py
Reads monthly drilldown tabs from Google Sheets and writes
docs/data/accounts.json for the GitHub Pages dashboard.
"""

import os
import sys
import json
import logging

import gspread
from google.oauth2.service_account import Credentials

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

# JSON key → sheet tab name
DRILLDOWN_TABS = {
    "2025-08": "Feb26 Data",
    "2025-09": "Mar26 Data",
    "2025-10": "April26 Data",
    "2025-11": "May26 Data",
    "2025-12": "Jun26 Data",
    "2026-01": "Jul26 Data",
    "2026-02": "Aug26 Data",
    "2026-03": "Sep26 Data",
}


def get_client():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS not set")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json), scopes=SCOPES
    )
    return gspread.authorize(creds)


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
        # Normalise keys for safe lookup
        norm = {str(k).strip().lower(): str(v).strip() for k, v in row.items()}

        account = norm.get("account number", "")

        if not account:
            continue

        # Customer column is labelled "Customer (VW/AUDI)" — match loosely
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


def main():
    logger.info("=" * 60)
    logger.info("EXPORT DRILLDOWN DATA TO JSON")
    logger.info("=" * 60)

    gc          = get_client()
    spreadsheet = gc.open_by_key(SHEET_ID)
    output      = {}

    for month_key, tab_name in DRILLDOWN_TABS.items():
        logger.info("Reading %s (%s)...", month_key, tab_name)
        rows              = read_tab(spreadsheet, tab_name)
        output[month_key] = rows
        logger.info("  → %d active accounts", len(rows))

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in output.values())
    logger.info("Written %s — %d accounts across %d months", OUTPUT_PATH, total, len(output))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
