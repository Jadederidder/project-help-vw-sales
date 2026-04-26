#!/usr/bin/env python3
# ============================================================
# scripts/cancel_casi_revio.py
#
# Cancel Wesbank VW/Audi clients in Casi based on the CANCELLATIONS tab.
# Runs daily 30 min after sync_cancellations.py.
#
# - Reads the CANCELLATIONS tab; processes only rows where Processed Date is
#   empty (idempotent — never processes a row twice without manual intervention).
# - For known cancel reasons (or blank), DELETEs the client from the appropriate
#   Casi cover by phone number ('AUDI' in DEA_NAME → cover 8278, else 8277).
# - Casi DELETE payload is phone-only, matching the proven April 16 batch
#   (cancel_batch1_casi.py): [{"cellphone": "27XXXXXXXXX"}].
# - Writes Processed Date, Casi Status, and Notes back to the same row. Revio
#   Status is left blank — Wesbank VW clients are not in Revio by default;
#   the small VW-SN overlap is handled manually.
# - Per-row try/except so one bad row never stops the run.
# - Retry model: write Processed Date always; transient errors land in Casi
#   Status as "Error: …". To retry a row, manually clear its Processed Date.
# - DRY_RUN=true: no Casi calls, no sheet writes, no live email; preflight +
#   per-row plan logged; one [DRY RUN] summary email to jd@projecthelp.io.
# ============================================================

import json
import logging
import os
import re
import smtplib
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ─── Constants ───────────────────────────────────────────────────────────────
SHEET_ID = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
TAB = "CANCELLATIONS"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

CASI_BASE = "https://casi-live.liv.ninja/api/v1"
CASI_CLIENT_ID = "3"
COVER_VW = 8277
COVER_AUDI = 8278

CANCEL_REASONS = {
    "LEGAL STATUS",
    "CUSTOMER REQUESTS CANCELLATION",
    "ARREAR CANCELLATION",
    "VAP LOADED IN ERROR",
}

HEADER_VARIANTS = {
    "processed_date": ["processed date", "processed_date"],
    "revio_status":   ["revio status", "revio_status"],
    "casi_status":    ["casi status", "casi_status"],
    "notes":          ["notes", "note"],
    "reason":         ["vap_cancel_reason_desc", "vap cancel reason desc"],
    "id_number":      ["cus_identity_or_reg_num", "id_number", "id number", "id"],
    "phone":          ["phone", "phone_number", "phone number",
                       "cellphone", "cell_phone", "cell phone",
                       "cell", "mobile", "cellnumber", "cell_number", "cell number"],
    "dea_name":       ["dea_name", "dea name"],
    "account_number": ["account_number", "account number"],
}

# Phone is REQUIRED — without it the script can do nothing useful.
REQUIRED_FIELDS = {
    "processed_date", "casi_status", "notes",
    "reason", "phone", "dea_name", "account_number",
}
OPTIONAL_FIELDS = {"revio_status", "id_number"}

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
DRY_RUN_RECIPIENT = "jd@projecthelp.co.za"
SAST = timezone(timedelta(hours=2))

logger = logging.getLogger(__name__)
_casi_token = None


# ─── Pure helpers (unit-testable) ────────────────────────────────────────────
def _norm(s):
    """Normalise a header / value for case-and-separator-insensitive match."""
    return re.sub(r"[\s_]+", "", str(s or "").strip().lower())


def _col_letter(idx):
    """0-indexed column number → A1 letter (0=A, 25=Z, 26=AA)."""
    n = idx + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _quote_tab(name):
    return name.replace("'", "''")


def normalize_phone(raw):
    """Normalise to '27XXXXXXXXX' (11 digits, ZA international format).
    Returns '' for unrecoverable inputs."""
    if raw is None:
        return ""
    s = re.sub(r"[\s\-+()]", "", str(raw).strip())
    # Some sheets store phones as numbers and lose the leading 0.
    if s.endswith(".0"):
        s = s[:-2]
    if not s or not s.isdigit():
        return ""
    if len(s) == 10 and s.startswith("0"):
        return "27" + s[1:]
    if len(s) == 9:                   # leading zero already dropped
        return "27" + s
    if len(s) == 11 and s.startswith("27"):
        return s
    return ""


def bind_columns(headers):
    """Return {logical_field: column_index_or_None} for every key in
    HEADER_VARIANTS, using normalised matching."""
    norm_headers = {_norm(h): i for i, h in enumerate(headers)}
    bindings = {}
    for key, variants in HEADER_VARIANTS.items():
        idx = None
        for v in variants:
            if _norm(v) in norm_headers:
                idx = norm_headers[_norm(v)]
                break
        bindings[key] = idx
    return bindings


def decide_action(reason):
    """Return ('cancel', '') or ('skip', note_str)."""
    r = (reason or "").strip()
    if r == "":
        return ("cancel", "blank reason — treated as cancel per spec")
    if r.upper() in {x.upper() for x in CANCEL_REASONS}:
        return ("cancel", "")
    return ("skip", f"Unknown reason: '{r}' — manual review required")


def cover_for_dea(dea_name):
    return COVER_AUDI if "AUDI" in (dea_name or "").upper() else COVER_VW


def cover_label(cover_id):
    return "Audi" if cover_id == COVER_AUDI else "VW"


def _now_str():
    return datetime.now(SAST).strftime("%Y-%m-%d %H:%M:%S SAST")


# ─── Sheets ──────────────────────────────────────────────────────────────────
def _get_sheets():
    raw = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not raw:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS env var not set")
    creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def write_row_status(svc, row_num, bindings,
                     processed_date, casi_status, notes, revio_status=""):
    """Per-row write of up to four output cells via batchUpdate (1 API call)."""
    data = []
    for key, val in [
        ("processed_date", processed_date),
        ("revio_status",   revio_status),
        ("casi_status",    casi_status),
        ("notes",          notes),
    ]:
        idx = bindings.get(key)
        if idx is None:
            continue
        rng = f"'{_quote_tab(TAB)}'!{_col_letter(idx)}{row_num}"
        data.append({"range": rng, "values": [[val]]})
    if not data:
        return
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": data},
    ).execute()


# ─── Casi ────────────────────────────────────────────────────────────────────
def _casi_token_get():
    global _casi_token
    if _casi_token:
        return _casi_token
    secret = os.environ.get("CASI_CLIENT_SECRET", "")
    user = os.environ.get("CASI_USERNAME", "")
    pw = os.environ.get("CASI_PASSWORD", "")
    if not (secret and user and pw):
        raise RuntimeError("CASI_CLIENT_SECRET / CASI_USERNAME / CASI_PASSWORD not set")
    r = requests.post(
        f"{CASI_BASE}/oauth/access_token",
        json={"grant_type": "password", "client_id": CASI_CLIENT_ID,
              "client_secret": secret, "username": user, "password": pw},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Casi auth failed: {r.status_code} {r.text[:200]}")
    _casi_token = r.json().get("access_token")
    if not _casi_token:
        raise RuntimeError("No access_token in Casi response")
    return _casi_token


def casi_cancel_by_phone(phone, cover_id):
    """DELETE /covers/{cover_id}/users with phone-only payload (matching the
    April 16 batch script). Returns ('cancelled' | 'not_found' | 'error', detail)."""
    token = _casi_token_get()
    r = requests.delete(
        f"{CASI_BASE}/covers/{cover_id}/users",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=[{"cellphone": phone}],
        timeout=30,
    )
    if r.status_code == 200:
        body = r.json() if r.text else {}
        removed = body.get("removed", {}).get("results", 0)
        if removed > 0:
            return ("cancelled", f"removed {removed}")
        return ("not_found", "removed 0")
    return ("error", f"HTTP {r.status_code}: {r.text[:200]}")


# ─── Email ───────────────────────────────────────────────────────────────────
def send_summary_email(stats, rows_detail, dry_run):
    sender = os.environ.get("EMAIL_SENDER", "")
    pw = os.environ.get("EMAIL_PASSWORD", "")
    if not (sender and pw):
        logger.warning("EMAIL_SENDER/EMAIL_PASSWORD not set — skipping summary email")
        return
    if dry_run:
        recipients = [DRY_RUN_RECIPIENT]
        subject_prefix = "[DRY RUN] "
    else:
        rec_raw = os.environ.get("EMAIL_RECIPIENT", "")
        recipients = [r.strip() for r in rec_raw.split(",") if r.strip()] or [DRY_RUN_RECIPIENT]
        subject_prefix = ""
    subject = (
        f"{subject_prefix}VW Casi Cancellations — "
        f"{stats['cancelled']} cancelled, {stats['skipped']} skipped, "
        f"{stats['errors']} errors"
    )
    rows_html = "".join(
        f'<tr><td>{r["account"]}</td><td>{r["reason"]}</td>'
        f'<td>{r["cover"]}</td><td>{r["status"]}</td><td>{r["notes"]}</td></tr>'
        for r in rows_detail
    ) or '<tr><td colspan="5" style="text-align:center;color:#888;">'\
         'No new rows to process.</td></tr>'
    html = (
        f'<html><body style="font-family:Arial,sans-serif;color:#262626;">'
        f'<h2 style="color:#1F3864;">VW Casi Cancellations</h2>'
        f'<p style="font-size:13px;">'
        f'<b>Examined:</b> {stats["examined"]}<br>'
        f'<b>Already processed (skipped):</b> {stats["already_processed"]}<br>'
        f'<b>Cancelled (incl. Not-found):</b> {stats["cancelled"]}<br>'
        f'<b>Skipped — unknown reason:</b> {stats["skipped"]}<br>'
        f'<b>No phone available:</b> {stats["no_phone"]}<br>'
        f'<b>Errors:</b> {stats["errors"]}'
        f'</p>'
        f'<table border="1" cellpadding="6" cellspacing="0" '
        f'style="border-collapse:collapse;font-size:12px;">'
        f'<tr style="background:#1F3864;color:white;">'
        f'<th>Account</th><th>Reason</th><th>Cover</th>'
        f'<th>Casi Status</th><th>Notes</th></tr>'
        f'{rows_html}</table>'
        f'<p style="font-size:11px;color:#888;">'
        f'Generated {datetime.now(SAST).isoformat(timespec="seconds")}'
        f'</p></body></html>'
    )
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content("HTML email — see HTML part for the table.")
    msg.add_alternative(html, subtype="html")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(sender, pw)
        s.send_message(msg)
    logger.info(f"Summary email sent → {msg['To']}")


# ─── Main row loop ───────────────────────────────────────────────────────────
def process_rows(svc, rows, bindings):
    """Mutates the sheet via writes unless DRY_RUN is set. Returns (stats, detail)."""
    stats = {"examined": 0, "already_processed": 0, "cancelled": 0,
             "skipped": 0, "errors": 0, "no_phone": 0}
    detail = []

    pd_idx = bindings["processed_date"]
    reason_idx = bindings["reason"]
    phone_idx = bindings["phone"]
    dea_idx = bindings["dea_name"]
    acct_idx = bindings["account_number"]

    def cell(row, idx):
        return row[idx] if idx is not None and idx < len(row) else ""

    for offset, row in enumerate(rows):
        row_num = offset + 2  # 1-indexed; +1 for header row
        stats["examined"] += 1
        account = str(cell(row, acct_idx)).strip()

        try:
            if str(cell(row, pd_idx)).strip():
                stats["already_processed"] += 1
                continue

            reason = str(cell(row, reason_idx)).strip()
            action, note = decide_action(reason)
            dea = str(cell(row, dea_idx)).strip()
            cover_id = cover_for_dea(dea)

            if action == "skip":
                stats["skipped"] += 1
                detail.append({
                    "account": account, "reason": reason or "(blank)",
                    "cover": cover_label(cover_id), "status": "Skipped",
                    "notes": note,
                })
                logger.info(f"  row {row_num} acct={account}  SKIP — {note}")
                if not DRY_RUN:
                    write_row_status(svc, row_num, bindings,
                                     _now_str(), "Skipped — not processed", note)
                continue

            phone_raw = str(cell(row, phone_idx)).strip()
            phone = normalize_phone(phone_raw)
            if not phone:
                stats["no_phone"] += 1
                stats["skipped"] += 1
                detail.append({
                    "account": account, "reason": reason or "(blank)",
                    "cover": cover_label(cover_id), "status": "No phone available",
                    "notes": f"Raw phone: '{phone_raw}'",
                })
                logger.warning(f"  row {row_num} acct={account}  no phone "
                               f"(raw {phone_raw!r})")
                if not DRY_RUN:
                    write_row_status(svc, row_num, bindings,
                                     _now_str(), "No phone available",
                                     f"Raw phone: '{phone_raw}'")
                continue

            if DRY_RUN:
                detail.append({
                    "account": account, "reason": reason or "(blank)",
                    "cover": cover_label(cover_id), "status": "(would cancel)",
                    "notes": f"phone={phone}",
                })
                logger.info(f"  row {row_num} acct={account}  DRY-RUN cancel "
                            f"cover={cover_label(cover_id)} phone={phone}")
                stats["cancelled"] += 1
                continue

            casi_status, casi_detail = casi_cancel_by_phone(phone, cover_id)
            time.sleep(0.2)
            if casi_status == "cancelled":
                stats["cancelled"] += 1
                sheet_status = "Cancelled"
            elif casi_status == "not_found":
                stats["cancelled"] += 1   # row is fully processed
                sheet_status = "Not found"
            else:
                stats["errors"] += 1
                sheet_status = f"Error: {casi_detail}"
            detail.append({
                "account": account, "reason": reason or "(blank)",
                "cover": cover_label(cover_id), "status": sheet_status,
                "notes": casi_detail,
            })
            logger.info(f"  row {row_num} acct={account}  {sheet_status}  "
                        f"cover={cover_label(cover_id)} phone={phone}")
            write_row_status(svc, row_num, bindings,
                             _now_str(), sheet_status, casi_detail)

        except Exception as e:
            stats["errors"] += 1
            logger.exception(f"  row {row_num} acct={account}  unexpected error: {e}")
            detail.append({
                "account": account, "reason": "(error)",
                "cover": "(unknown)", "status": "Error",
                "notes": f"unexpected: {e}",
            })
            if not DRY_RUN:
                try:
                    write_row_status(svc, row_num, bindings,
                                     _now_str(), f"Error: {e}", "")
                except Exception:
                    pass  # last-ditch — don't fail the whole run

    return stats, detail


def main():
    logger.info("=" * 60)
    logger.info("CANCEL CASI REVIO")
    logger.info(f"Dry run : {DRY_RUN}")
    logger.info("=" * 60)

    svc = _get_sheets()
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{_quote_tab(TAB)}'!A1:ZZ",
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    values = res.get("values", [])
    if not values:
        logger.error("CANCELLATIONS tab is empty — aborting")
        sys.exit(2)

    headers = values[0]
    rows = values[1:]
    logger.info(f"Loaded {len(headers)} columns × {len(rows)} data rows")

    bindings = bind_columns(headers)

    # Preflight: log every header → bound logical field
    logger.info("─" * 60)
    logger.info("Header → logical-field bindings (every column listed):")
    bound_by_idx = {idx: key for key, idx in bindings.items() if idx is not None}
    for i, h in enumerate(headers):
        bound = bound_by_idx.get(i)
        logger.info(f"  col {_col_letter(i):>3}  {h!r:<42}  → "
                    f"{bound if bound else '(unused)'}")
    logger.info("─" * 60)

    missing_required = sorted(k for k in REQUIRED_FIELDS if bindings.get(k) is None)
    if missing_required:
        logger.error(f"Missing required column(s): {missing_required}")
        logger.error("Add them to the CANCELLATIONS tab and re-run.")
        if "phone" in missing_required:
            logger.error("Phone column is missing entirely — the script cannot do "
                         "anything useful without it. Add a phone column or extract "
                         "phones from another source first.")
        sys.exit(3)
    missing_optional = sorted(k for k in OPTIONAL_FIELDS if bindings.get(k) is None)
    if missing_optional:
        logger.warning(f"Optional columns not found (proceeding without): {missing_optional}")

    stats, detail = process_rows(svc, rows, bindings)

    logger.info("=" * 60)
    logger.info(f"Examined            : {stats['examined']}")
    logger.info(f"  Already processed : {stats['already_processed']}")
    logger.info(f"  Cancelled         : {stats['cancelled']}")
    logger.info(f"  Skipped           : {stats['skipped']}")
    logger.info(f"  No phone          : {stats['no_phone']}")
    logger.info(f"  Errors            : {stats['errors']}")
    logger.info("=" * 60)

    send_summary_email(stats, detail, dry_run=DRY_RUN)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()
