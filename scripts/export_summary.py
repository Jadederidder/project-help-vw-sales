#!/usr/bin/env python3
"""
scripts/export_summary.py

Reads the SUMMARY tab of the VW All Road Report Google Sheet and writes
docs/data/summary.json with { lastUpdated, kpis, months } — consumed by
the dashboard (docs/index.html).

Env:
  GOOGLE_SHEETS_CREDENTIALS  service-account JSON (required)
  EMAIL_SENDER               Gmail sender for run-summary + alert emails
  EMAIL_PASSWORD             Gmail app password
  EMAIL_RECIPIENT            Recipient (default jd@projecthelp.co.za)
  DRY_RUN=true               print JSON, write nothing, send no emails
  FORCE_OVERWRITE=true       bypass the gross-regression guard

Behaviour:
  The dashboard is a pure replication of the source Google Sheet — the
  script writes whatever the sheet says. cumNet/totalRevCum can move in
  either direction without blocking; visibility comes from a run-summary
  email sent on every successful write.

Single safety guard (gross regression on a closed past month):
  If any month had a positive gross before and now reads as 0 or its
  row vanished, abort with exit code 2 and email JD. This is the only
  signature that cannot be produced by any legitimate process — closed
  past months don't lose sales. Set FORCE_OVERWRITE=true to bypass when
  a manual sheet inspection has confirmed the drop is intentional.
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

# Per-month fields surfaced in the run-summary email when their values change.
TRACKED_FIELDS = ["gross", "cancel", "reject", "netInd", "netFam"]

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


def _load_existing():
    """Return the on-disk summary.json as a dict, or None if absent/unreadable."""
    if not OUT_PATH.exists():
        return None
    try:
        return json.loads(OUT_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Could not read existing summary.json: %s", e)
        return None


def _gross_regressions(prev, payload):
    """Months where prev gross was positive but new gross is 0/missing."""
    if not prev:
        return []
    prev_by_month = {
        m["month"]: m.get("gross") for m in prev.get("months", []) if m.get("month")
    }
    new_by_month = {m["month"]: m.get("gross") for m in payload["months"]}
    violations = []
    for month, prev_gross in prev_by_month.items():
        if not isinstance(prev_gross, (int, float)) or prev_gross <= 0:
            continue
        new_gross = new_by_month.get(month)
        if new_gross is None or new_gross == 0:
            violations.append((month, prev_gross, new_gross))
    return violations


def _fmt_change(prev_v, new_v):
    """'a -> b (+delta)' / 'b (unchanged)' / '(none) -> b' style formatter."""
    if prev_v is None and new_v is None:
        return "(none)"
    if prev_v is None:
        return f"(none) -> {new_v}"
    if new_v is None:
        return f"{prev_v} -> (none)"
    if prev_v == new_v:
        return f"{new_v} (unchanged)"
    return f"{prev_v} -> {new_v} ({new_v - prev_v:+g})"


def _changed_rows(prev, payload):
    """List of (month, [(field, prev_v, new_v), ...]) where any tracked field differs."""
    prev_by_month = (
        {m["month"]: m for m in prev.get("months", [])} if prev else {}
    )
    rows = []
    for new_rec in payload["months"]:
        prev_rec = prev_by_month.get(new_rec["month"], {})
        diffs = [
            (f, prev_rec.get(f), new_rec.get(f))
            for f in TRACKED_FIELDS
            if prev_rec.get(f) != new_rec.get(f)
        ]
        if diffs:
            rows.append((new_rec["month"], diffs))
    return rows


def _email_run_summary(prev, payload):
    sender    = os.environ.get("EMAIL_SENDER", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    recipient = os.environ.get("EMAIL_RECIPIENT", "jd@projecthelp.co.za")
    if not sender or not password:
        logger.warning(
            "EMAIL_SENDER / EMAIL_PASSWORD not set — skipping run summary email"
        )
        return

    new_latest = payload["months"][-1]
    new_cum    = new_latest.get("cumNet")
    new_rev    = new_latest.get("totalRevCum")
    if prev and prev.get("months"):
        prev_latest = prev["months"][-1]
        prev_cum    = prev_latest.get("cumNet")
        prev_rev    = prev_latest.get("totalRevCum")
    else:
        prev_cum = prev_rev = None

    rows = _changed_rows(prev, payload)

    # Subject line
    if prev_cum is None or new_cum is None:
        cum_part = f"cumNet {new_cum if new_cum is not None else 'n/a'}"
    elif new_cum == prev_cum:
        cum_part = f"cumNet {new_cum} (unchanged)"
    else:
        cum_part = f"cumNet {prev_cum}→{new_cum} ({new_cum - prev_cum:+d})"
    subject = f"VW Dashboard sync — {cum_part}, {len(rows)} month rows changed"

    # Body
    lines = [
        "Hi JD,",
        "",
        f"Run timestamp: {payload['lastUpdated']}",
        "",
        f"cumNet:      {_fmt_change(prev_cum, new_cum)}",
        f"totalRevCum: {_fmt_change(prev_rev, new_rev)}",
        "",
        f"Months changed: {len(rows)}",
    ]
    for month, diffs in rows:
        lines.append("")
        lines.append(f"  {month}:")
        for f, p, n in diffs:
            lines.append(f"    {f}: {_fmt_change(p, n)}")
    lines += ["", "— VW Sales Automation"]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.set_content("\n".join(lines))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        logger.info("Run summary email sent → %s", recipient)
    except Exception as e:
        logger.error("Failed to send run summary email: %s", e)


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
    prev    = _load_existing()

    if FORCE_OVERWRITE:
        logger.warning(
            "FORCE_OVERWRITE=true — bypassing gross-regression guard"
        )
    else:
        violations = _gross_regressions(prev, payload)
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

    _email_run_summary(prev, payload)


if __name__ == "__main__":
    main()
