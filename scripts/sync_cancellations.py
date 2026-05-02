#!/usr/bin/env python3
"""
scripts/sync_cancellations.py

Daily VW/Audi cancellations sync.

Flow:
  1. Fetch last-7-days email matching the cancellation subject via IMAP
  2. Open the .zip attachment, parse its .csv (cp1252 encoding)
  3. Pass through the 36 raw CSV columns; rename ACC_NUM → ACCOUNT_NUMBER
  4. Read the CANCELLATIONS tab header + existing account numbers
  5. Align new rows to the sheet's column order, dedupe by ACCOUNT_NUMBER
  6. Append new rows and email a summary (Excel attached) to all recipients
  7. Track processed Message-IDs in state/processed_emails.json

Env:
  CANCEL_EMAIL_ADDRESS       IMAP mailbox username — full email address (e.g. jd@projecthelp.co.za)
  CANCEL_EMAIL_APP_PASSWORD  IMAP password for receiving mailbox — plain password from Afrihost, not a Gmail app password
  IMAP_HOST                  IMAP host (default: depalma.aserv.co.za)
  IMAP_PORT                  IMAP port (default: 993, SSL)
  EMAIL_SENDER               Gmail sender for the summary email (jd@projecthelp.io)
  EMAIL_PASSWORD             Gmail app password for the sender
  EMAIL_RECIPIENT            comma-separated recipient list
  GOOGLE_SHEETS_CREDENTIALS  service-account JSON
  DRY_RUN                    true → don't write to sheet, don't commit state,
                             email only jd, prefix subject "[DRY RUN]"
  DIAGNOSTIC                 true → print findings and exit (no email/writes)
"""

import csv
import email
import imaplib
import io
import json
import logging
import os
import smtplib
import sys
import zipfile
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
# Receiving mailbox lives on Afrihost, not Gmail. Override via env if needed.
IMAP_HOST      = os.environ.get("IMAP_HOST", "depalma.aserv.co.za")
IMAP_PORT      = int(os.environ.get("IMAP_PORT", "993"))
SEARCH_DAYS    = 7
SUBJECT_PREFIX = "Interface File - VAP FCO daily sold cancellations"
CSV_ENCODING   = "cp1252"

SHEET_ID = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
TAB_NAME = "CANCELLATIONS"
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]

STATE_PATH = Path(__file__).resolve().parent.parent / "state" / "processed_emails.json"

DRY_RUN           = os.environ.get("DRY_RUN", "").lower() == "true"
DIAGNOSTIC        = os.environ.get("DIAGNOSTIC", "").lower() == "true"
DRY_RUN_RECIPIENT = "jd@projecthelp.co.za"

# Source CSV → output column renames. Everything else is pass-through.
SRC_TO_OUT = {"ACC_NUM": "ACCOUNT_NUMBER"}

# The 36 target columns in the order the VW sheet expects.
CSV_COLUMNS = [
    "FCO_CODE", "FCO_NAME", "VAP_XVCA_CODE", "VAP_XVPS_CODE", "VAP_SUP_CODE",
    "VAP_PRODUCT_CODE", "ACCOUNT_NUMBER", "ACC_RELOAD_ACC_NUM",
    "ACC_EXPIRY_DATE", "VAP_DIV_CODE", "VAP_EFFECTIVE_DATE",
    "VAP_PREMIUM_AMT", "VAP_TERM_MONTHS", "VAP_STATUS_DATE",
    "VAP_CANCEL_REASON_CODE", "VAP_CANCEL_REASON_DESC", "PAYEE_SUP_CODE",
    "OLD_SYSTEM_ACCNUM", "VAP_PRODUCT_NAME", "VAP_SUP_PRODUCT_NAME",
    "DEA_CODE", "DEA_NAME", "VAP_XVST_CODE", "ACC_XOPC_CODE",
    "ACC_DATE_CLOSED", "CUS_IDENTITY_OR_REG_NUM", "VAP_CLAIM_XVCL_CODE",
    "VAP_CLAIM_XVCL_DESC", "LPAY_AMT", "LPAY_CREATE_DATE",
    "VAP_REFERENCE_NUM", "INSPECTION_FEE_AMT",
    "ACC_IN_DEBT_REVIEW_CYCLE_IND", "TOT_PREMIUM_COLLECTED",
    "VAP_XVOW_CODE", "VAP_XVAC_CODE",
]

# Parsed to ISO YYYY-MM-DD so Sheets (USER_ENTERED) treats them as dates.
DATE_FIELDS = {
    "ACC_EXPIRY_DATE", "VAP_EFFECTIVE_DATE", "VAP_STATUS_DATE",
    "LPAY_CREATE_DATE", "ACC_DATE_CLOSED",
}

# Sent as numeric so Sheets sorts them correctly. Identifier columns with
# leading zeros or letters (OLD_SYSTEM_ACCNUM, CUS_IDENTITY_OR_REG_NUM, code
# columns) stay as strings.
NUMERIC_FIELDS = {
    "ACCOUNT_NUMBER", "VAP_PREMIUM_AMT", "VAP_TERM_MONTHS",
    "DEA_CODE", "VAP_REFERENCE_NUM", "LPAY_AMT",
    "INSPECTION_FEE_AMT", "TOT_PREMIUM_COLLECTED",
}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def _norm(s):
    """Collapse whitespace/underscores; lowercase. Both-sides of header match."""
    return "".join(str(s or "").lower().split()).replace("_", "")


def _col_letter(idx_0):
    n = idx_0 + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _parse_date(v):
    """'YYYY/MM/DD' → 'YYYY-MM-DD'. Returns the raw string for anything else."""
    s = (v or "").strip()
    if not s:
        return ""
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return s


def _parse_number(v):
    """int for integer values, float for decimals, '' for blank/unparseable."""
    s = (v or "").strip().replace(",", "")
    if not s:
        return ""
    try:
        f = float(s)
    except ValueError:
        return s
    return int(f) if f.is_integer() else f


# ─── State file ──────────────────────────────────────────────────────────────
def load_state():
    if not STATE_PATH.exists():
        return set()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return set(data.get("processed", []))
    except Exception as e:
        logger.warning("Could not read state: %s", e)
        return set()


def save_state(processed_ids):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"processed": sorted(processed_ids)}
    STATE_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ─── Email fetch ─────────────────────────────────────────────────────────────
def fetch_cancellation_emails(username, password):
    """Returns [{msg_id, date, subject, zip_bytes, zip_name}, ...]."""
    logger.info("Connecting to %s:%d as %s", IMAP_HOST, IMAP_PORT, username)
    conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    conn.login(username, password)
    try:
        conn.select("INBOX")
        since = (datetime.now(timezone.utc) - timedelta(days=SEARCH_DAYS)).strftime("%d-%b-%Y")
        status, data = conn.search(
            None,
            f'SINCE {since} SUBJECT "{SUBJECT_PREFIX}"',
        )
        if status != "OK":
            logger.warning("IMAP search status=%s", status)
            return []
        eids = data[0].split()
        logger.info("Matching emails in last %d days: %d", SEARCH_DAYS, len(eids))

        results = []
        for eid in eids:
            status, msg_data = conn.fetch(eid, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            subject = (msg.get("Subject") or "").strip()
            msg_id  = (msg.get("Message-ID") or "").strip()
            try:
                msg_dt = parsedate_to_datetime(msg.get("Date"))
            except Exception:
                msg_dt = datetime.now(timezone.utc)

            zip_bytes = None
            zip_name  = ""
            for part in msg.walk():
                fn = part.get_filename() or ""
                if fn.lower().endswith(".zip"):
                    zip_bytes = part.get_payload(decode=True)
                    zip_name  = fn
                    break

            if not zip_bytes:
                logger.warning("No .zip attachment on msg-id=%s (subject=%r)",
                               msg_id, subject[:80])
                continue

            results.append({
                "msg_id":    msg_id,
                "date":      msg_dt,
                "subject":   subject,
                "zip_bytes": zip_bytes,
                "zip_name":  zip_name,
            })
        return results
    finally:
        try:
            conn.close()
        except Exception:
            pass
        conn.logout()


# ─── CSV parse + transform ───────────────────────────────────────────────────
def parse_zip(zip_bytes, zip_name):
    """Return (csv_name, list[dict]). Raises if no CSV is found."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise RuntimeError(f"No .csv in {zip_name}")
        csv_name = csvs[0]
        raw = zf.read(csv_name)
    text = raw.decode(CSV_ENCODING)   # cp1252 — en-dash via byte 0x96 → U+2013
    reader = csv.DictReader(io.StringIO(text))
    return csv_name, list(reader)


def transform_row(src):
    """Pass the 36 raw CSV columns through, renaming ACC_NUM → ACCOUNT_NUMBER.
    Numeric/date fields are typed so Sheets sorts them correctly."""
    out = {}
    for src_key, val in src.items():
        if src_key is None:
            continue
        key = SRC_TO_OUT.get(src_key, src_key)
        if key in DATE_FIELDS:
            out[key] = _parse_date(val)
        elif key in NUMERIC_FIELDS:
            out[key] = _parse_number(val)
        else:
            out[key] = (val or "").strip() if isinstance(val, str) else val
    return out


# ─── Google Sheets ───────────────────────────────────────────────────────────
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


def read_sheet_header(service):
    res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!1:1",
    ).execute()
    header = (res.get("values") or [[]])[0]
    if not header:
        raise RuntimeError(f"{TAB_NAME} header row is empty")
    return header


def account_number_col_idx(header):
    target = _norm("ACCOUNT_NUMBER")
    for i, col in enumerate(header):
        if _norm(col) == target:
            return i
    return None


def read_existing_account_numbers(service, header):
    idx = account_number_col_idx(header)
    if idx is None:
        raise RuntimeError(
            f"{TAB_NAME} has no ACCOUNT_NUMBER column (header={header!r})"
        )
    letter = _col_letter(idx)
    res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!{letter}2:{letter}",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    existing = set()
    for row in res.get("values", []):
        if not row or row[0] in (None, ""):
            continue
        v = row[0]
        if isinstance(v, float) and v.is_integer():
            v = str(int(v))
        else:
            v = str(v).strip()
            if v.endswith(".0"):
                v = v[:-2]
        if v:
            existing.add(v)
    return existing


def align_rows_to_sheet(rows, header):
    """Produce a 2-D list, one row per input dict, in sheet column order.
    Unknown header columns stay blank; unknown row keys are dropped."""
    norm_header = [_norm(c) for c in header]
    out = []
    for row in rows:
        norm_row = {_norm(k): v for k, v in row.items()}
        line = [norm_row.get(nh, "") for nh in norm_header]
        out.append(line)
    return out


def append_rows(service, aligned):
    if not aligned:
        return
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A:A",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": aligned},
    ).execute()


# ─── Summary email ───────────────────────────────────────────────────────────
def _derive_brand(dea_name):
    return "Audi" if "AUDI" in str(dea_name or "").upper() else "VW"


def _derive_membership(premium):
    try:
        p = float(premium)
    except (TypeError, ValueError):
        return ""
    if abs(p - 89)  < 0.5: return "Single"
    if abs(p - 159) < 0.5: return "Family"
    return ""


def build_summary_excel(new_rows):
    if not new_rows:
        return None
    import pandas as pd  # heavy — only needed when we actually build a summary
    df = pd.DataFrame(new_rows, columns=CSV_COLUMNS)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="New Cancellations")
    bio.seek(0)
    return bio.read()


def send_heartbeat_email(run_date, reason, dry_run=False):
    """Sent when there's nothing meaningful to process — distinguishable
    subject (`[HEARTBEAT]`) so JD can filter. `reason` is a one-line body
    explaining which heartbeat case fired."""
    sender  = os.environ.get("EMAIL_SENDER")
    pwd     = os.environ.get("EMAIL_PASSWORD")
    recip_s = os.environ.get("EMAIL_RECIPIENT", "")
    if not sender or not pwd:
        logger.warning("EMAIL_SENDER / EMAIL_PASSWORD not set — skipping heartbeat email")
        return
    if dry_run:
        recipients = [DRY_RUN_RECIPIENT]
    else:
        recipients = [r.strip() for r in recip_s.split(",") if r.strip()]
    if not recipients:
        logger.warning("No recipients — skipping heartbeat email")
        return

    date_label = run_date.strftime("%d %b %Y")
    subject = f"[HEARTBEAT] VW Cancellations — nothing to process ({date_label})"
    if dry_run:
        subject = f"[DRY RUN] {subject}"
    body = (
        f"VW/Audi Cancellations daily sync — {date_label}\n"
        "\n"
        f"  {reason}\n"
        f"  Lookback window: {SEARCH_DAYS} days.\n"
        "\n"
        "  Workflow exited cleanly (no sheet writes, no state update).\n"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, pwd)
        smtp.send_message(msg)
    logger.info("Heartbeat email sent → %s%s",
                ", ".join(recipients),
                " (DRY RUN)" if dry_run else "")


def send_summary_email(run_date, new_rows, emails_processed, dupes_skipped, dry_run=False):
    sender  = os.environ.get("EMAIL_SENDER")
    pwd     = os.environ.get("EMAIL_PASSWORD")
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

    n = len(new_rows)
    date_label = run_date.strftime("%d %b %Y")
    plural = "s" if n != 1 else ""
    if n == 0:
        subject = f"✅ VW Cancellations — clean ({date_label})"
    else:
        subject = f"📥 VW Cancellations — {n} new row{plural} ({date_label})"
    if dry_run:
        subject = f"[DRY RUN] {subject}"

    body_lines = [
        f"VW/Audi Cancellations daily sync — {date_label}",
        "",
    ]
    if n == 0:
        body_lines += ["  ✅ Run completed clean — nothing to action today.", ""]
    body_lines += [
        f"  Emails processed   : {emails_processed}",
        f"  New rows appended  : {n}",
        f"  Already-in-sheet   : {dupes_skipped}",
    ]
    if n:
        by_brand = {}
        by_type  = {}
        for r in new_rows:
            b = _derive_brand(r.get("DEA_NAME"))
            m = _derive_membership(r.get("VAP_PREMIUM_AMT"))
            by_brand[b] = by_brand.get(b, 0) + 1
            by_type[m]  = by_type.get(m, 0) + 1
        body_lines += [
            "",
            f"  By brand      : {by_brand}",
            f"  By membership : {by_type}",
        ]
    if dry_run:
        body_lines += ["", "DRY RUN — no rows were written and state was not committed."]
    body = "\n".join(body_lines) + "\n"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content(body)

    xlsx = build_summary_excel(new_rows)
    if xlsx:
        msg.add_attachment(
            xlsx,
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"VW_Cancellations_{run_date.strftime('%Y_%m_%d')}.xlsx",
        )

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
    logger.info("=" * 60)
    logger.info("VW/AUDI CANCELLATIONS SYNC")
    logger.info("Run date   : %s", run_date.isoformat(timespec="seconds"))
    logger.info("Dry run    : %s", DRY_RUN)
    logger.info("Diagnostic : %s", DIAGNOSTIC)
    logger.info("=" * 60)

    username = os.environ.get("CANCEL_EMAIL_ADDRESS")
    password = os.environ.get("CANCEL_EMAIL_APP_PASSWORD")
    if not username or not password:
        logger.error("CANCEL_EMAIL_ADDRESS / CANCEL_EMAIL_APP_PASSWORD not set")
        sys.exit(1)

    processed_ids = load_state()
    logger.info("State: %d previously-processed email(s)", len(processed_ids))

    mails = fetch_cancellation_emails(username, password)
    unseen = [m for m in mails if m["msg_id"] and m["msg_id"] not in processed_ids]
    logger.info("Matching emails: %d (of those, %d unprocessed)", len(mails), len(unseen))

    if not unseen and not DIAGNOSTIC:
        if not mails:
            reason = "No Wesbank email today — nothing to process."
            logger.info("No matching emails in last %d days — sending heartbeat and exiting cleanly",
                        SEARCH_DAYS)
        else:
            reason = "No new Wesbank emails — most recent already processed."
            logger.info("All %d matching email(s) already in processed_emails.json — "
                        "sending heartbeat and exiting cleanly", len(mails))
        send_heartbeat_email(run_date=run_date, reason=reason, dry_run=DRY_RUN)
        return

    if DIAGNOSTIC:
        for m in mails:
            seen = "seen" if m["msg_id"] in processed_ids else "new"
            logger.info("  [%s] %s  zip=%r  msg-id=%s",
                        seen, m["date"].strftime("%Y-%m-%d"),
                        m["zip_name"], m["msg_id"])
        logger.info("DIAGNOSTIC — exiting before parse / email / write.")
        return

    all_transformed = []
    newly_processed = []

    for m in unseen:
        try:
            csv_name, src_rows = parse_zip(m["zip_bytes"], m["zip_name"])
            logger.info("  parsed %d rows from %s / %s",
                        len(src_rows), m["zip_name"], csv_name)
            for r in src_rows:
                all_transformed.append(transform_row(r))
            newly_processed.append(m["msg_id"])
        except Exception as e:
            logger.error("  failed to parse %s: %s", m["zip_name"], e)

    logger.info("Total CSV rows parsed: %d", len(all_transformed))

    service  = get_sheets_service()
    header   = read_sheet_header(service)
    logger.info("Sheet header (%d cols): %s", len(header), header)

    existing = read_existing_account_numbers(service, header)
    logger.info("Existing ACCOUNT_NUMBERs in sheet: %d", len(existing))

    new_rows   = []
    dupes      = 0
    blank_acc  = 0
    for row in all_transformed:
        acc = row.get("ACCOUNT_NUMBER")
        if isinstance(acc, float) and acc.is_integer():
            acc_str = str(int(acc))
        elif acc not in (None, ""):
            acc_str = str(acc).strip()
        else:
            acc_str = ""
        if not acc_str:
            blank_acc += 1
            continue
        if acc_str in existing:
            dupes += 1
            continue
        new_rows.append(row)
        existing.add(acc_str)

    logger.info("Dedupe: %d new | %d already in sheet | %d blank ACCOUNT_NUMBER",
                len(new_rows), dupes, blank_acc)

    if DRY_RUN:
        logger.info("DRY RUN — skipping sheet append and state update")
    elif new_rows:
        aligned = align_rows_to_sheet(new_rows, header)
        append_rows(service, aligned)
        logger.info("Appended %d rows to %s", len(aligned), TAB_NAME)

    # Heartbeat email even when 0 new — silence = "did it run?"
    send_summary_email(
        run_date=run_date,
        new_rows=new_rows,
        emails_processed=len(unseen),
        dupes_skipped=dupes,
        dry_run=DRY_RUN,
    )

    # State: only touch on successful non-dry non-diagnostic runs
    if not DRY_RUN and newly_processed:
        processed_ids.update(newly_processed)
        save_state(processed_ids)
        logger.info("State updated: +%d message-id(s)", len(newly_processed))

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
