#!/usr/bin/env python3
"""
scripts/sync_cancellations.py

Daily VW/Audi cancellations sync.

Flow:
  1. Fetch last-7-days email matching the cancellation subject via IMAP
  2. Open the .zip attachment, parse its .csv (cp1252 encoding)
  3. Transform rows to our schema; derive brand + membership_type
  4. Read the CANCELLATIONS tab header + existing contract numbers
  5. Align new rows to the sheet's column order, dedupe by contract_number
  6. Append new rows and email a summary (Excel attached) to all recipients
  7. Track processed Gmail Message-IDs in state/processed_emails.json

Env:
  CANCEL_EMAIL_ADDRESS       Gmail mailbox (IMAP)
  CANCEL_EMAIL_APP_PASSWORD  Gmail app password
  EMAIL_SENDER               sender for the summary email
  EMAIL_PASSWORD             app password for the sender
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

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
IMAP_HOST      = "imap.gmail.com"
IMAP_PORT      = 993
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

# Schema → possible sheet column names. Normalised on both sides for matching.
SCHEMA_ALIASES = {
    "contract_number":    ["contract_number", "contract no", "contract",
                           "acc_num", "account number", "account_num",
                           "account no", "acc no"],
    "sale_date":          ["sale_date", "sale date",
                           "effective_date", "effective date"],
    "cancel_date":        ["cancel_date", "cancel date",
                           "cancellation_date", "cancellation date",
                           "status_date"],
    "cancel_reason_code": ["cancel_reason_code", "cancel reason code",
                           "reason_code", "reason code"],
    "cancel_reason_desc": ["cancel_reason_desc", "cancel reason desc",
                           "reason_desc", "reason description",
                           "cancellation reason", "reason"],
    "premium_amt":        ["premium_amt", "premium", "premium amount"],
    "membership_type":    ["membership_type", "membership type",
                           "membership", "type"],
    "dealer_code":        ["dealer_code", "dealer code", "dea_code"],
    "dealer_name":        ["dealer_name", "dealer name", "dea_name", "dealer"],
    "brand":              ["brand", "make"],
    "vehicle_reg":        ["vehicle_reg", "vehicle registration",
                           "registration", "reg", "old_system_accnum"],
    "customer_id":        ["customer_id", "customer id", "id_number",
                           "identity_number", "identity", "id"],
    "total_collected":    ["total_collected", "total collected",
                           "tot_premium_collected", "total premium collected"],
    "reference_num":      ["reference_num", "reference number", "reference",
                           "ref", "vap_reference_num"],
    "source_file":        ["source_file", "source file", "source", "file"],
    "processed_at":       ["processed_at", "processed at", "timestamp",
                           "synced_at", "sync timestamp", "run date"],
}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def _norm(s):
    """Collapse whitespace/underscores; lowercase. Both-sides of the match."""
    return "".join(str(s or "").lower().split()).replace("_", "")


def _col_letter(idx_0):
    n = idx_0 + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


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


def _to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def transform_row(src, source_file, processed_at):
    dea_name = (src.get("DEA_NAME") or "").strip()
    brand    = "Audi" if "AUDI" in dea_name.upper() else "VW"

    premium = _to_float(src.get("VAP_PREMIUM_AMT"))
    if premium is not None and abs(premium - 89) < 0.5:
        membership = "Single"
    elif premium is not None and abs(premium - 159) < 0.5:
        membership = "Family"
    else:
        membership = ""

    def g(k):
        return (src.get(k) or "").strip()

    return {
        "contract_number":    g("ACC_NUM"),
        "sale_date":          g("VAP_EFFECTIVE_DATE"),
        "cancel_date":        g("VAP_STATUS_DATE"),
        "cancel_reason_code": g("VAP_CANCEL_REASON_CODE"),
        "cancel_reason_desc": g("VAP_CANCEL_REASON_DESC"),
        "premium_amt":        g("VAP_PREMIUM_AMT"),
        "membership_type":    membership,
        "dealer_code":        g("DEA_CODE"),
        "dealer_name":        dea_name,
        "brand":              brand,
        "vehicle_reg":        g("OLD_SYSTEM_ACCNUM"),
        "customer_id":        g("CUS_IDENTITY_OR_REG_NUM"),
        "total_collected":    g("TOT_PREMIUM_COLLECTED"),
        "reference_num":      g("VAP_REFERENCE_NUM"),
        "source_file":        source_file,
        "processed_at":       processed_at,
    }


# ─── Google Sheets ───────────────────────────────────────────────────────────
def get_sheets_service():
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


def resolve_column_map(header):
    """Returns {sheet_col_index: schema_key} for columns we recognise."""
    out = {}
    for i, col in enumerate(header):
        norm_col = _norm(col)
        for key, aliases in SCHEMA_ALIASES.items():
            if any(_norm(a) == norm_col for a in aliases):
                out[i] = key
                break
    return out


def contract_number_col_idx(col_map):
    for i, key in col_map.items():
        if key == "contract_number":
            return i
    return None


def read_existing_contract_numbers(service, col_map):
    idx = contract_number_col_idx(col_map)
    if idx is None:
        raise RuntimeError(
            "CANCELLATIONS tab has no column matching 'contract_number' "
            f"(looked for any of {SCHEMA_ALIASES['contract_number']})"
        )
    letter = _col_letter(idx)
    res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!{letter}2:{letter}",
    ).execute()
    existing = set()
    for row in res.get("values", []):
        if row and row[0] not in (None, ""):
            v = str(row[0]).strip()
            if v.endswith(".0"):
                v = v[:-2]
            existing.add(v)
    return existing


def align_rows_to_sheet(rows, header, col_map):
    out = []
    for row in rows:
        line = []
        for i in range(len(header)):
            key = col_map.get(i)
            line.append(row.get(key, "") if key else "")
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
def build_summary_excel(new_rows):
    if not new_rows:
        return None
    df = pd.DataFrame(new_rows)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="New Cancellations")
    bio.seek(0)
    return bio.read()


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
    subject = f"VW Cancellations Sync — {date_label} — {n} new row{plural}"
    if dry_run:
        subject = f"[DRY RUN] {subject}"

    body_lines = [
        f"VW/Audi Cancellations daily sync — {date_label}",
        "",
        f"  Emails processed   : {emails_processed}",
        f"  New rows appended  : {n}",
        f"  Already-in-sheet   : {dupes_skipped}",
    ]
    if n:
        by_brand = {}
        by_type  = {}
        for r in new_rows:
            by_brand[r["brand"]]           = by_brand.get(r["brand"], 0) + 1
            by_type[r["membership_type"]]  = by_type.get(r["membership_type"], 0) + 1
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
    logger.info("Unprocessed matching emails: %d (of %d found)", len(unseen), len(mails))

    if DIAGNOSTIC:
        for m in mails:
            seen = "seen" if m["msg_id"] in processed_ids else "new"
            logger.info("  [%s] %s  zip=%r  msg-id=%s",
                        seen, m["date"].strftime("%Y-%m-%d"),
                        m["zip_name"], m["msg_id"])
        logger.info("DIAGNOSTIC — exiting before parse / email / write.")
        return

    # Parse every new email's zip
    all_transformed    = []
    newly_processed    = []
    processed_at_iso   = run_date.isoformat(timespec="seconds").replace("+00:00", "Z")

    for m in unseen:
        try:
            csv_name, src_rows = parse_zip(m["zip_bytes"], m["zip_name"])
            logger.info("  parsed %d rows from %s / %s",
                        len(src_rows), m["zip_name"], csv_name)
            source_tag = f"{m['zip_name']}::{csv_name}"
            for r in src_rows:
                all_transformed.append(transform_row(r, source_tag, processed_at_iso))
            newly_processed.append(m["msg_id"])
        except Exception as e:
            logger.error("  failed to parse %s: %s", m["zip_name"], e)

    logger.info("Total CSV rows parsed: %d", len(all_transformed))

    # Read sheet + dedupe
    service = get_sheets_service()
    header  = read_sheet_header(service)
    col_map = resolve_column_map(header)
    logger.info("Sheet header (%d cols): %s", len(header), header)

    unmatched = set(SCHEMA_ALIASES) - set(col_map.values())
    if unmatched:
        logger.warning("Schema keys with no matching sheet column: %s",
                       sorted(unmatched))

    existing = read_existing_contract_numbers(service, col_map)
    logger.info("Existing contract_numbers in sheet: %d", len(existing))

    new_rows   = []
    dupes      = 0
    blank_cn   = 0
    for row in all_transformed:
        cn = row["contract_number"]
        if not cn:
            blank_cn += 1
            continue
        if cn in existing:
            dupes += 1
            continue
        new_rows.append(row)
        existing.add(cn)   # dedupe within this batch too

    logger.info("Dedupe: %d new | %d already in sheet | %d blank contract_number",
                len(new_rows), dupes, blank_cn)

    if DRY_RUN:
        logger.info("DRY RUN — skipping sheet append and state update")
    elif new_rows:
        aligned = align_rows_to_sheet(new_rows, header, col_map)
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
