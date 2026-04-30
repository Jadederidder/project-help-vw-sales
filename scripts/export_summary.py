#!/usr/bin/env python3
"""
scripts/export_summary.py

Reads the SUMMARY tab of the VW All Road Report Google Sheet and writes
docs/data/summary.json with { lastUpdated, kpis, months } — consumed by
the dashboard (docs/index.html).

Env:
  GOOGLE_SHEETS_CREDENTIALS  service-account JSON (required)
  EMAIL_SENDER               Gmail sender for safety-guard alerts (optional)
  EMAIL_PASSWORD             Gmail app password (optional)
  EMAIL_RECIPIENT            Alert recipient (default jd@projecthelp.co.za)
  DRY_RUN=true               print JSON, write nothing, suppress alert email
  FORCE_OVERWRITE=true       bypass both safety guards (use only after a
                             manual sheet inspection has confirmed the drop)

Safety guards:
  1. Past-month gross regression — primary signal. If any past month had
     a positive gross before and now reads as 0 (or its row vanished),
     abort with exit code 2 and email JD. This is the actual signature
     of the 2026-04-28 silent-poisoning incident.
  2. cumNet drift — secondary backstop. Latest month's cumNet vs the
     on-disk value:
       drop > CUMNET_ABORT_THRESHOLD : abort + email JD
       0 < drop <= threshold         : warn + email JD, proceed
       no drop                       : silent
     Small drops (1-10 policies) are legitimate retroactive cancellations
     and do not block the write.

  FORCE_OVERWRITE=true skips both guards entirely.
"""

import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SHEET_ID    = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SUMMARY_TAB = "DASHBOARD"
DATA_START_ROW = 10  # 1-indexed; headers are above this
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]
OUT_PATH    = Path(__file__).resolve().parent.parent / "docs" / "data" / "summary.json"
DRY_RUN         = os.environ.get("DRY_RUN", "").lower() == "true"
FORCE_OVERWRITE = os.environ.get("FORCE_OVERWRITE", "").lower() == "true"

# cumNet drops larger than this trigger an abort. Drops at or below this
# threshold are treated as legitimate retroactive-cancellation drift —
# warn + email JD, but proceed with the write.
CUMNET_ABORT_THRESHOLD = 10

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


def _existing_grosses():
    """Return { 'Mon YYYY': gross } from the on-disk summary.json, or {}."""
    if not OUT_PATH.exists():
        return {}
    try:
        prev = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        return {
            m["month"]: m.get("gross")
            for m in prev.get("months", [])
            if m.get("month")
        }
    except Exception as e:
        logger.warning("Could not read existing summary.json: %s", e)
        return {}


def _gross_regressions(payload):
    """Months where previous gross was positive but new gross is 0/missing."""
    prev = _existing_grosses()
    if not prev:
        return []
    new_by_month = {m["month"]: m.get("gross") for m in payload["months"]}
    violations = []
    for month, prev_gross in prev.items():
        if not isinstance(prev_gross, (int, float)) or prev_gross <= 0:
            continue
        new_gross = new_by_month.get(month)
        if new_gross is None or new_gross == 0:
            violations.append((month, prev_gross, new_gross))
    return violations


def _email_cumnet_drift(prev_cum, new_cum, drop, *, aborted):
    sender    = os.environ.get("EMAIL_SENDER", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    recipient = os.environ.get("EMAIL_RECIPIENT", "jd@projecthelp.co.za")
    if not sender or not password:
        logger.warning(
            "EMAIL_SENDER / EMAIL_PASSWORD not set — skipping cumNet drift alert"
        )
        return

    if aborted:
        subject = (
            f"VW Dashboard — cumNet dropped {drop} policies, sync aborted"
        )
        body = (
            "Hi JD,\n\n"
            "export_summary.py refused to write docs/data/summary.json "
            f"because cumNet dropped by {drop} policies "
            f"({prev_cum} -> {new_cum}), exceeding the "
            f"{CUMNET_ABORT_THRESHOLD}-policy abort threshold.\n\n"
            "The dashboard has been left on its previous values. "
            "Inspect the DASHBOARD/MASTER_BOOK tab for missing or zeroed "
            "rows. If the drop is legitimate (e.g. a mass retroactive "
            "cancellation), re-run the workflow with force_overwrite=true "
            "to bypass the guard.\n\n"
            "— VW Sales Automation"
        )
    else:
        subject = (
            f"VW Dashboard — cumNet drifted -{drop} (informational)"
        )
        body = (
            "Hi JD,\n\n"
            "export_summary.py wrote summary.json normally, but cumNet "
            f"drifted down by {drop} policies ({prev_cum} -> {new_cum}). "
            f"Within the {CUMNET_ABORT_THRESHOLD}-policy tolerance, so "
            "proceeded automatically.\n\n"
            "Likely cause: retroactive cancellations being applied to "
            "past sale months. No action required — just visibility.\n\n"
            "— VW Sales Automation"
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.set_content(body)
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        logger.info("cumNet drift alert sent → %s", recipient)
    except Exception as e:
        logger.error("Failed to send cumNet drift alert: %s", e)


def _email_gross_regression(violations):
    sender    = os.environ.get("EMAIL_SENDER", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    recipient = os.environ.get("EMAIL_RECIPIENT", "jd@projecthelp.co.za")
    if not sender or not password:
        logger.warning(
            "EMAIL_SENDER / EMAIL_PASSWORD not set — skipping regression alert"
        )
        return

    lines = "\n".join(
        f"  - {m}: previous gross = {pg}, new gross = {ng if ng is not None else 'missing'}"
        for m, pg, ng in violations
    )
    body = (
        "Hi JD,\n\n"
        "export_summary.py refused to write docs/data/summary.json because "
        f"{len(violations)} past month(s) had positive gross drop to 0 or "
        "vanish from the DASHBOARD tab:\n\n"
        f"{lines}\n\n"
        "The dashboard has been left on its previous values. Likely cause: "
        "a formula or source row in DASHBOARD/MASTER_BOOK was edited or "
        "deleted. Inspect the affected cells and re-run the sync once "
        "they're repaired.\n\n"
        "— VW Sales Automation"
    )
    msg = EmailMessage()
    msg["Subject"] = (
        f"VW Dashboard — gross regression on "
        f"{len(violations)} month(s), sync aborted"
    )
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.set_content(body)
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        logger.info("Regression alert sent → %s", recipient)
    except Exception as e:
        logger.error("Failed to send regression alert: %s", e)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    payload = build_payload()

    if FORCE_OVERWRITE:
        logger.warning(
            "FORCE_OVERWRITE=true — bypassing safety guards (caller has "
            "manually confirmed the regression)"
        )
    else:
        # Guard 1: gross-regression on any past month (primary signal)
        violations = _gross_regressions(payload)
        if violations:
            detail = "; ".join(
                f"{m} ({pg} → {ng if ng is not None else 'missing'})"
                for m, pg, ng in violations
            )
            logger.error(
                "SAFETY GUARD: %d past month(s) had positive gross drop to "
                "0/missing — aborting to avoid poisoning the dashboard: %s",
                len(violations), detail,
            )
            if not DRY_RUN:
                _email_gross_regression(violations)
            sys.exit(2)

        # Guard 2: cumNet drift (secondary backstop, threshold-based)
        new_cum  = payload["months"][-1].get("cumNet")
        prev_cum = _existing_cumnet()
        if (prev_cum is not None and new_cum is not None
                and new_cum < prev_cum):
            drop = prev_cum - new_cum
            if drop > CUMNET_ABORT_THRESHOLD:
                logger.error(
                    "SAFETY GUARD: cumNet dropped %d policies (%s -> %s), "
                    "exceeds %d-policy abort threshold — aborting. Re-run "
                    "with force_overwrite=true if this drop is legitimate.",
                    drop, prev_cum, new_cum, CUMNET_ABORT_THRESHOLD,
                )
                if not DRY_RUN:
                    _email_cumnet_drift(prev_cum, new_cum, drop, aborted=True)
                sys.exit(2)
            else:
                logger.warning(
                    "cumNet dropped %d policies (%s -> %s) — within "
                    "%d-policy tolerance, proceeding",
                    drop, prev_cum, new_cum, CUMNET_ABORT_THRESHOLD,
                )
                if not DRY_RUN:
                    _email_cumnet_drift(prev_cum, new_cum, drop, aborted=False)

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
