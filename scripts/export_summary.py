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
SUMMARY_TAB = "DASHBOARD"
DATA_START_ROW = 10  # 1-indexed; headers are above this
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]
OUT_PATH    = Path(__file__).resolve().parent.parent / "docs" / "data" / "summary.json"
DRY_RUN     = os.environ.get("DRY_RUN", "").lower() == "true"

MONTH_SHORT = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]

# Dashboard field → ordered list of aliases to search for in the SUMMARY header.
# Order matters: first match wins.
COLUMN_ALIASES = {
    "month":       ["sale month", "month"],
    "gross":       ["gross eligible", "gross"],
    "cancel":      ["cancellations"],
    "reject":      ["rejections"],
    "net":         ["net subs"],
    "netInd":      ["net individual"],
    "netFam":      ["net family"],
    "cumNet":      ["cum net subs"],
    "indRev":      ["individual revenue"],
    "famRev":      ["family revenue"],
    "totalRevCum": ["cumulative revenue"],
    "collected":   ["collected revenue"],
    "vwInvoice":   ["vw total book -invoice", "vw total book invoice"],
    "vwLock":      ["vw billing lock"],
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


def _is_ym_row(val):
    """Match 'YYYY-MM' month format used in the DASHBOARD tab."""
    if not val:
        return False
    s = str(val).strip()
    if len(s) != 7 or s[4] != "-":
        return False
    try:
        y, m = int(s[:4]), int(s[5:])
        return 2024 <= y <= 2099 and 1 <= m <= 12
    except ValueError:
        return False


def _normalise_month(val):
    """Return 'Mon YYYY' regardless of input format."""
    s = str(val).strip()
    if _is_month_row(s):
        return s
    if _is_ym_row(s):
        y, m = int(s[:4]), int(s[5:])
        return f"{MONTH_SHORT[m - 1]} {y}"
    return s


def _find_worksheet(sh, target):
    wanted = _norm(target)
    matches = [w for w in sh.worksheets() if _norm(w.title) == wanted]
    if matches:
        return matches[0]
    titles = [w.title for w in sh.worksheets()]
    raise RuntimeError(
        f"Worksheet matching '{target}' not found. Existing tabs: {titles}"
    )


def build_payload():
    gc = _auth()
    sh = gc.open_by_key(SHEET_ID)
    ws = _find_worksheet(sh, SUMMARY_TAB)
    logger.info("Using worksheet: %r", ws.title)
    rows = ws.get_all_values()
    if not rows:
        raise RuntimeError(f"{SUMMARY_TAB} tab is empty")

    # Data starts at row DATA_START_ROW (1-indexed).
    # Headers live in the row directly above that.
    header_row_idx = DATA_START_ROW - 2   # zero-indexed
    if header_row_idx < 0 or header_row_idx >= len(rows):
        raise RuntimeError(f"Header row {header_row_idx+1} out of range")

    header = rows[header_row_idx]
    logger.info("DASHBOARD header (row %d): %s", header_row_idx + 1, header)
    # Dump the 3 rows above data + the first data row so we can inspect layout
    for idx in range(max(0, header_row_idx - 2), min(len(rows), DATA_START_ROW + 1)):
        logger.info("  row %d: %s", idx + 1, rows[idx])

    col_idx = {f: _find_col(header, aliases) for f, aliases in COLUMN_ALIASES.items()}
    found   = {f: (header[i] if i is not None else None) for f, i in col_idx.items()}
    logger.info("Resolved column map: %s", found)

    missing = [f for f, i in col_idx.items() if i is None]
    if col_idx["month"] is None:
        raise RuntimeError(f"SUMMARY 'month' column not found. Header: {header}")
    if missing:
        logger.warning("Columns not found in SUMMARY (will be null in JSON): %s", missing)

    months = []
    for row in rows[DATA_START_ROW - 1:]:
        if col_idx["month"] >= len(row):
            continue
        label = row[col_idx["month"]].strip()
        if not _is_month_row(label) and not _is_ym_row(label):
            continue
        record = {"month": _normalise_month(label)}
        for field, idx in col_idx.items():
            if field == "month" or idx is None:
                continue
            record[field] = _to_num(row[idx]) if idx < len(row) else None
        # Skip future months that have no data yet
        if record.get("gross") is None:
            continue
        # Dashboard uses `rev` = indRev + famRev (no source column for it)
        ind = record.get("indRev") or 0
        fam = record.get("famRev") or 0
        record["rev"] = ind + fam if (ind or fam) else None
        months.append(record)

    if not months:
        raise RuntimeError(f"No month rows with data parsed from {SUMMARY_TAB}")

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
