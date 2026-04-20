#!/usr/bin/env python3
"""
scripts/export_summary.py

Reads the SUMMARY tab of the VW All Road Report Google Sheet and writes
docs/data/summary.json with { lastUpdated, kpis, months } — consumed by
the dashboard (docs/index.html).

Env:
  GOOGLE_SHEETS_CREDENTIALS  service-account JSON (required)
  DRY_RUN=true               print JSON, write nothing

Safety guard: if the latest month's cumNet drops below the value already
in summary.json, the script exits non-zero without writing, so the live
dashboard never regresses from a partial/corrupt sheet read.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SHEET_ID    = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SUMMARY_TAB = "SUMMARY"
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]
OUT_PATH    = Path(__file__).resolve().parent.parent / "docs" / "data" / "summary.json"
DRY_RUN     = os.environ.get("DRY_RUN", "").lower() == "true"

MONTH_SHORT = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]

# Dashboard field → ordered list of aliases to search for in the SUMMARY header.
# Order matters: first match wins.
COLUMN_ALIASES = {
    "month":       ["month", "period"],
    "gross":       ["gross eligible", "gross policies", "gross"],
    "cancel":      ["cancellations", "cancelled", "cancel"],
    "reject":      ["rejections", "rejected", "reject"],
    "net":         ["net sub", "net subs", "net active", "net"],
    "netInd":      ["net individual", "net ind", "netind"],
    "netFam":      ["net family", "net fam", "netfam"],
    "cumNet":      ["cumulative net", "cum net", "cumnet", "cumulative"],
    "rev":         ["monthly revenue", "revenue", "rev"],
    "indRev":      ["individual revenue", "ind revenue", "indrev"],
    "famRev":      ["family revenue", "fam revenue", "famrev"],
    "totalRevCum": ["cumulative revenue", "total revenue cum", "total rev cum",
                    "totalrevcum", "revcum"],
    "collected":   ["collected", "amount collected"],
    "vwInvoice":   ["vw invoice", "vwinvoice", "invoice"],
    "vwLock":      ["vw lock", "vwlock", "lock"],
}


def _norm(s):
    return str(s or "").strip().lower().replace("_", " ")


def _to_num(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s in ("-", "—", "N/A", "n/a", "null", "None"):
        return None
    s = s.replace(",", "").replace("R", "").replace("\u202f", "").replace(" ", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        f = float(s)
        return int(f) if f.is_integer() else round(f, 2)
    except ValueError:
        return None


def _find_col(header, aliases):
    norm_header = [_norm(h) for h in header]
    for a in aliases:
        an = _norm(a)
        for i, h in enumerate(norm_header):
            if h == an:
                return i
    # Loose contains-match as a fallback
    for a in aliases:
        an = _norm(a)
        for i, h in enumerate(norm_header):
            if an and an in h:
                return i
    return None


def _auth():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS env var not set")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json), scopes=SCOPES,
    )
    return gspread.authorize(creds)


def _month_key(label):
    parts = label.split()
    if len(parts) != 2 or parts[0] not in MONTH_SHORT:
        return (9999, 99)
    return (int(parts[1]), MONTH_SHORT.index(parts[0]))


def _is_month_row(val):
    if not val:
        return False
    parts = str(val).strip().split()
    return (len(parts) == 2
            and parts[0] in MONTH_SHORT
            and parts[1].isdigit()
            and 2024 <= int(parts[1]) <= 2099)


def build_payload():
    gc = _auth()
    ws = gc.open_by_key(SHEET_ID).worksheet(SUMMARY_TAB)
    rows = ws.get_all_values()
    if not rows:
        raise RuntimeError("SUMMARY tab is empty")

    header = rows[0]
    logger.info("SUMMARY header: %s", header)

    col_idx = {f: _find_col(header, aliases) for f, aliases in COLUMN_ALIASES.items()}
    found   = {f: (header[i] if i is not None else None) for f, i in col_idx.items()}
    logger.info("Resolved column map: %s", found)

    missing = [f for f, i in col_idx.items() if i is None]
    if col_idx["month"] is None:
        raise RuntimeError(f"SUMMARY 'month' column not found. Header: {header}")
    if missing:
        logger.warning("Columns not found in SUMMARY (will be null in JSON): %s", missing)

    months = []
    for row in rows[1:]:
        if col_idx["month"] >= len(row):
            continue
        label = row[col_idx["month"]].strip()
        if not _is_month_row(label):
            continue
        record = {"month": label}
        for field, idx in col_idx.items():
            if field == "month" or idx is None:
                continue
            record[field] = _to_num(row[idx]) if idx < len(row) else None
        months.append(record)

    if not months:
        raise RuntimeError("No month rows parsed from SUMMARY")

    months.sort(key=lambda m: _month_key(m["month"]))

    latest = months[-1]
    net_policies = latest.get("cumNet") or 0
    revenue      = latest.get("totalRevCum") or 0

    total_gross  = sum((m.get("gross")  or 0) for m in months)
    total_cancel = sum((m.get("cancel") or 0) for m in months)
    total_reject = sum((m.get("reject") or 0) for m in months)
    eligible     = total_gross - total_reject

    churn_pct     = round((total_cancel / eligible) * 100, 1) if eligible else 0.0
    rejection_pct = round((total_reject / total_gross) * 100, 1) if total_gross else 0.0

    return {
        "lastUpdated": datetime.now(timezone.utc)
                        .isoformat(timespec="seconds")
                        .replace("+00:00", "Z"),
        "kpis": {
            "netPolicies":  net_policies,
            "revenue":      revenue,
            "churnPct":     churn_pct,
            "rejectionPct": rejection_pct,
        },
        "months": months,
    }


def _existing_cumnet():
    if not OUT_PATH.exists():
        return None
    try:
        prev = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        if prev.get("months"):
            return prev["months"][-1].get("cumNet")
    except Exception as e:
        logger.warning("Could not read existing summary.json: %s", e)
    return None


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    payload = build_payload()

    new_cum  = payload["months"][-1].get("cumNet")
    prev_cum = _existing_cumnet()
    if prev_cum is not None and new_cum is not None and new_cum < prev_cum:
        logger.error(
            "SAFETY GUARD: new cumNet (%s) is lower than existing (%s) — "
            "aborting to avoid regressing the dashboard",
            new_cum, prev_cum,
        )
        sys.exit(2)

    json_str = json.dumps(payload, indent=2, ensure_ascii=False)
    latest   = payload["months"][-1]
    logger.info(
        "Built summary — %d months, latest %s (gross=%s, cumNet=%s, totalRevCum=%s)",
        len(payload["months"]), latest["month"],
        latest.get("gross"), latest.get("cumNet"), latest.get("totalRevCum"),
    )

    if DRY_RUN:
        logger.info("DRY_RUN=true — not writing. Payload below:")
        print(json_str)
        return

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json_str + "\n", encoding="utf-8")
    logger.info("Wrote %s", OUT_PATH)


if __name__ == "__main__":
    main()
