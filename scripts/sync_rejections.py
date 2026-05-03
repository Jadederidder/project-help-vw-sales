#!/usr/bin/env python3
"""
scripts/sync_rejections.py

Daily VW/Audi rejections sync.

Mirrors sync_cancellations.py architecture (same IMAP fetch, same dedupe-
and-append pattern, same SMTP summary). Differences:
  - Subject prefix is "Interface File - VAP Telesales Audittrail"
  - Source CSV has 14 columns; we keep 8 for the REJECTIONS tab
  - Filter: keep only rows where ACCEPT/REJECT IND='R' AND the ERROR MESSAGE
    does NOT start with "A VAP OF THIS CATEGORY ALREADY EXISTS"
  - ACCOUNT_NUMBER must be stored as text — long Wesbank account numbers
    render as scientific notation ("8,7E+10") if Sheets parses them as
    numbers. We use valueInputOption="RAW" for the entire append to keep
    everything text-faithful and side-step the issue cleanly.
  - No state file. Dedupe is by ACCOUNT_NUMBER against the existing
    REJECTIONS tab — re-running on the same email is a silent no-op.

Env:
  CANCEL_EMAIL_ADDRESS       IMAP mailbox username — full email address
  CANCEL_EMAIL_APP_PASSWORD  IMAP password
  IMAP_HOST                  IMAP host (default: depalma.aserv.co.za)
  IMAP_PORT                  IMAP port (default: 993, SSL)
  EMAIL_SENDER               Gmail sender for the summary email
  EMAIL_PASSWORD             Gmail app password for the sender
  EMAIL_RECIPIENT            comma-separated recipient list
  GOOGLE_SHEETS_CREDENTIALS  service-account JSON
  DRY_RUN                    true → don't write to sheet, email only jd
"""

import csv
import email
import imaplib
import io
import json
import logging
import os
import re
import smtplib
import sys
import time
import zipfile
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from email_template import RunSummary, build_run_summary_email  # noqa: E402

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
IMAP_HOST      = os.environ.get("IMAP_HOST", "depalma.aserv.co.za")
IMAP_PORT      = int(os.environ.get("IMAP_PORT", "993"))
SEARCH_DAYS    = 7
SUBJECT_PREFIX = "Interface File - VAP Telesales Audittrail"
CSV_ENCODING   = "cp1252"

SHEET_ID = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
TAB_NAME = "REJECTIONS"
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]

DRY_RUN           = os.environ.get("DRY_RUN", "").lower() == "true"
DRY_RUN_RECIPIENT = "jd@projecthelp.co.za"

# Excluded — these are CS errors (same client sold twice), not real rejections,
# and would skew the dashboard rejection rate. Match prefix, case-insensitive,
# whitespace-tolerant.
DUPLICATE_VAP_PREFIX = "a vap of this category already exists"

# Logical field → (CSV column header, sheet column header).
# Both sides are matched via _norm (case + whitespace + underscore + slash
# insensitive) so minor variants like "ACCEPT_REJECT_IND" still bind.
FIELDS = [
    ("accept_reject_ind",  "ACCEPT/REJECT IND",  "ACCEPT/REJECT IND"),
    ("vap_supplier",       "VAP SUPPLIER",       "VAP SUPPLIER"),
    ("product_term",       "PRODUCT TERM",       "PRODUCT TERM"),
    ("effective_date",     "EFFECTIVE DATE",     "EFFECTIVE DATE"),
    ("policy_cost",        "POLICY COST",        "POLICY COST"),
    ("accepted_rejected",  "ACCEPTED REJECTED",  "ACCEPTED REJECTED"),
    ("error_message",      "ERROR MESSAGE",      "ERROR MESSAGE"),
    ("account_number",     "ACCOUNT",            "ACCOUNT_NUMBER"),
]


# ─── Helpers ─────────────────────────────────────────────────────────────────
def _norm(s):
    """Lowercase + strip whitespace, underscores, and slashes."""
    return re.sub(r"[\s_/]", "", str(s or "").lower())


def _col_letter(idx_0):
    n = idx_0 + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _is_duplicate_vap(error_message):
    """True iff the message starts with the duplicate-VAP prefix
    (case-insensitive, leading whitespace tolerated)."""
    s = (error_message or "").strip().lower()
    return s.startswith(DUPLICATE_VAP_PREFIX)


def _normalise_account(v):
    """Coerce a sheet/CSV value to a comparable account-number string.
    Handles ints, floats with .0 suffix, and trims whitespace."""
    if v is None or v == "":
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


# ─── Email fetch ─────────────────────────────────────────────────────────────
def fetch_rejection_emails(username, password):
    """Returns [{msg_id, date, subject, zip_bytes, zip_name}, ...]."""
    logger.info("Connecting to %s:%d as %s", IMAP_HOST, IMAP_PORT, username)
    conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    conn.login(username, password)
    try:
        conn.select("INBOX")
        since = (datetime.now(timezone.utc) - timedelta(days=SEARCH_DAYS)).strftime("%d-%b-%Y")
        status, data = conn.search(
            None, f'SINCE {since} SUBJECT "{SUBJECT_PREFIX}"',
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


# ─── CSV parse + filter (pure) ───────────────────────────────────────────────
def parse_zip(zip_bytes, zip_name):
    """Return (csv_name, list[dict]). Raises if no CSV is found."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise RuntimeError(f"No .csv in {zip_name}")
        csv_name = csvs[0]
        raw = zf.read(csv_name)
    text = raw.decode(CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(text))
    return csv_name, list(reader)


def bind_csv_columns(reader_fieldnames):
    """Returns {logical_key: actual_csv_field_name_or_None}."""
    norm_to_orig = {_norm(f): f for f in (reader_fieldnames or [])}
    return {
        logical: norm_to_orig.get(_norm(csv_h))
        for logical, csv_h, _ in FIELDS
    }


def filter_and_transform(csv_rows, csv_bindings):
    """Apply the 'R'-only + not-duplicate-VAP filter. Returns
    (kept_rows, stats_dict). Each kept row is a dict keyed by logical field."""
    stats = {"examined": 0, "skipped_a": 0, "skipped_dup": 0, "malformed": 0}
    kept = []
    ar_field = csv_bindings.get("accept_reject_ind")
    err_field = csv_bindings.get("error_message")

    for src in csv_rows:
        stats["examined"] += 1
        try:
            ar_ind = (src.get(ar_field) or "").strip().upper()
            if ar_ind != "R":
                stats["skipped_a"] += 1
                continue
            err = src.get(err_field) or ""
            if _is_duplicate_vap(err):
                stats["skipped_dup"] += 1
                continue
            row = {}
            for logical, csv_field in csv_bindings.items():
                if csv_field is None:
                    row[logical] = ""
                    continue
                v = src.get(csv_field)
                row[logical] = (v or "").strip() if isinstance(v, str) else (v if v is not None else "")
            kept.append(row)
        except Exception as e:
            logger.warning("  malformed row skipped: %r src=%r", e, src)
            stats["malformed"] += 1
    return kept, stats


# ─── Sheet ───────────────────────────────────────────────────────────────────
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
        raise RuntimeError(f"{TAB_NAME} header row is empty (does the tab exist?)")
    return header


def bind_sheet_columns(sheet_headers):
    """Returns {logical_key: column_index_or_None}."""
    norm_to_idx = {_norm(h): i for i, h in enumerate(sheet_headers)}
    return {
        logical: norm_to_idx.get(_norm(sheet_h))
        for logical, _, sheet_h in FIELDS
    }


def read_existing_account_numbers(service, sheet_bindings):
    idx = sheet_bindings.get("account_number")
    if idx is None:
        raise RuntimeError(f"{TAB_NAME} has no ACCOUNT_NUMBER column")
    letter = _col_letter(idx)
    res = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!{letter}2:{letter}",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    return {
        v for v in (_normalise_account(row[0]) for row in res.get("values", []) if row)
        if v
    }


def align_to_sheet(rows, sheet_bindings, num_sheet_cols):
    """Build [num_rows × num_sheet_cols] with values placed at the bound
    indexes. ACCOUNT_NUMBER is force-stringified — every other field is
    left as-is so the caller can trust the cell content."""
    aligned = []
    for r in rows:
        line = [""] * num_sheet_cols
        for logical, idx in sheet_bindings.items():
            if idx is None:
                continue
            v = r.get(logical, "")
            if logical == "account_number":
                v = _normalise_account(v)   # always a clean string
            line[idx] = v
        aligned.append(line)
    return aligned


def append_rows(service, aligned):
    """Uses valueInputOption='RAW' so every cell is stored verbatim as text.
    This defeats the scientific-notation rendering Sheets applies to long
    numeric strings (which is how the CANCELLATIONS tab ended up with
    'ACCOUNT_NUMBER = 8,7E+10' for some rows)."""
    if not aligned:
        return
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": aligned},
    ).execute()


# ─── Summary email ───────────────────────────────────────────────────────────
def send_heartbeat_email(run_date, reason, dry_run=False):
    """Sent when there's nothing meaningful to process — distinguishable
    subject (`[HEARTBEAT]`) so JD can filter. `reason` is a one-line body
    explaining which heartbeat case fired.

    Deliberately plain-text and outside the RunSummary template — the
    [HEARTBEAT] prefix is a stable filter signal in JD's mailbox."""
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
    subject = f"[HEARTBEAT] VW Rejections — nothing to process ({date_label})"
    if dry_run:
        subject = f"[DRY RUN] {subject}"
    body = (
        f"VW/Audi Rejections daily sync — {date_label}\n"
        "\n"
        f"  {reason}\n"
        f"  Lookback window: {SEARCH_DAYS} days.\n"
        "\n"
        "  Workflow exited cleanly (no sheet writes).\n"
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


def _build_rejections_summary(run_date, source_email_dates, stats, dedupe_skipped,
                              new_count, dry_run, error_summary, duration_seconds):
    plural = "s" if new_count != 1 else ""
    src_label = ", ".join(d.strftime("%Y-%m-%d %H:%M") for d in source_email_dates) \
                or "(no source emails)"

    if error_summary:
        outcome = "failure"
        headline = "Error during rejections sync"
        summary = (
            f"Sync failed before completion. {error_summary}. "
            f"No rows appended; manual investigation needed."
        )
    else:
        outcome = "success"
        headline = f"{new_count} new rejection{plural} appended"
        summary = (
            f"Pulled Telesales Audittrail email(s) ({src_label}). "
            f"{stats.get('examined', 0)} CSV rows examined; after filtering "
            f"{stats.get('skipped_a', 0)} acceptance(s) and "
            f"{stats.get('skipped_dup', 0)} duplicate-VAP row(s), "
            f"{new_count} real rejection{plural} {'would be ' if dry_run else ''}appended."
        )

    numbers = {
        "Source email(s)": src_label,
        "CSV rows examined": stats.get("examined", 0),
        "A acceptances filtered": stats.get("skipped_a", 0),
        "Duplicate-VAP filtered": stats.get("skipped_dup", 0),
        "Already in REJECTIONS": dedupe_skipped,
        ("Would-be appended" if dry_run else "Real rejections appended"): new_count,
        "Malformed rows": stats.get("malformed", 0),
    }

    next_steps = []
    if error_summary:
        next_steps.append(f"Investigate: {error_summary}")

    sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"

    return RunSummary(
        workflow_name="VW Rejections Sync",
        run_date=run_date,
        mode="dry_run" if dry_run else "production",
        outcome=outcome,
        headline=headline,
        summary_paragraph=summary,
        numbers=numbers,
        duration_seconds=duration_seconds,
        next_steps=next_steps,
        sheet_url=sheet_url,
    )



def send_summary_email(run_date, source_email_dates, stats, dedupe_skipped,
                       new_count, dry_run=False, error_summary="",
                       duration_seconds=0.0):
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

    summary = _build_rejections_summary(
        run_date=run_date,
        source_email_dates=source_email_dates,
        stats=stats,
        dedupe_skipped=dedupe_skipped,
        new_count=new_count,
        dry_run=dry_run,
        error_summary=error_summary,
        duration_seconds=duration_seconds,
    )
    subject, html_body = build_run_summary_email(summary)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content("HTML email — see HTML part for the run summary.")
    msg.add_alternative(html_body, subtype="html")

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
    started = time.monotonic()
    logger.info("=" * 60)
    logger.info("VW/AUDI REJECTIONS SYNC")
    logger.info("Run date : %s", run_date.isoformat(timespec="seconds"))
    logger.info("Dry run  : %s", DRY_RUN)
    logger.info("=" * 60)

    username = os.environ.get("CANCEL_EMAIL_ADDRESS")
    password = os.environ.get("CANCEL_EMAIL_APP_PASSWORD")
    if not username or not password:
        logger.error("CANCEL_EMAIL_ADDRESS / CANCEL_EMAIL_APP_PASSWORD not set")
        sys.exit(1)

    mails = fetch_rejection_emails(username, password)
    logger.info("Matching emails: %d", len(mails))

    if not mails:
        logger.info("No matching emails in last %d days — sending heartbeat and exiting cleanly",
                    SEARCH_DAYS)
        send_heartbeat_email(
            run_date=run_date,
            reason="No Wesbank email today — nothing to process.",
            dry_run=DRY_RUN,
        )
        return

    # Parse all matching ZIPs and accumulate filtered rows
    all_kept = []
    combined_stats = {"examined": 0, "skipped_a": 0, "skipped_dup": 0, "malformed": 0}
    for m in mails:
        try:
            csv_name, src_rows = parse_zip(m["zip_bytes"], m["zip_name"])
            logger.info("  parsed %d rows from %s / %s",
                        len(src_rows), m["zip_name"], csv_name)
            if not src_rows:
                continue
            csv_bindings = bind_csv_columns(src_rows[0].keys() if src_rows else [])
            missing = [k for k, v in csv_bindings.items() if v is None]
            if missing:
                logger.warning("  CSV missing logical fields: %s "
                               "(headers seen: %s)",
                               missing, list(src_rows[0].keys()))
            kept, stats = filter_and_transform(src_rows, csv_bindings)
            for k, v in stats.items():
                combined_stats[k] = combined_stats.get(k, 0) + v
            logger.info("  kept %d rows after filter (skipped %d A, %d dup-VAP, %d malformed)",
                        len(kept), stats["skipped_a"], stats["skipped_dup"], stats["malformed"])
            all_kept.extend(kept)
        except Exception as e:
            logger.error("  ZIP extraction/parse failed for %s: %s", m["zip_name"], e)
            send_summary_email(
                run_date=run_date,
                source_email_dates=[m["date"] for m in mails],
                stats=combined_stats,
                dedupe_skipped=0,
                new_count=0,
                dry_run=DRY_RUN,
                error_summary=f"ZIP parse failed for {m['zip_name']}: {e}",
                duration_seconds=time.monotonic() - started,
            )
            sys.exit(1)

    logger.info("Total kept across all emails: %d", len(all_kept))

    service = get_sheets_service()
    header = read_sheet_header(service)
    logger.info("Sheet header (%d cols): %s", len(header), header)

    sheet_bindings = bind_sheet_columns(header)
    missing_sheet = [k for k, v in sheet_bindings.items() if v is None]
    if missing_sheet:
        logger.error("REJECTIONS tab missing required columns: %s "
                     "(headers seen: %s)", missing_sheet, header)
        sys.exit(2)

    existing = read_existing_account_numbers(service, sheet_bindings)
    logger.info("Existing ACCOUNT_NUMBERs in REJECTIONS: %d", len(existing))

    # Dedupe within this run AND against the sheet
    new_rows = []
    dupes = 0
    blank = 0
    for row in all_kept:
        acc = _normalise_account(row.get("account_number"))
        if not acc:
            blank += 1
            continue
        if acc in existing:
            dupes += 1
            continue
        new_rows.append(row)
        existing.add(acc)

    logger.info("Dedupe: %d new | %d already in sheet | %d blank ACCOUNT_NUMBER",
                len(new_rows), dupes, blank)

    # Heartbeat case 2: mails parsed but every R-row is already in the sheet.
    # Equivalent to sync_cancellations' "all msg-ids in processed_emails.json"
    # — the most recent Wesbank email predates today and we've seen it before.
    # Skip the summary email (which would otherwise read as `✅ clean`,
    # indistinguishable from a real today's-email-with-zero-rejections run).
    if not new_rows and dupes > 0:
        logger.info("All %d row(s) already in sheet — sending heartbeat and exiting cleanly",
                    dupes)
        send_heartbeat_email(
            run_date=run_date,
            reason="No new Wesbank emails — most recent already processed.",
            dry_run=DRY_RUN,
        )
        return

    if DRY_RUN:
        logger.info("DRY RUN — would append the following %d row(s):", len(new_rows))
        for i, r in enumerate(new_rows[:50], start=1):
            logger.info("    %2d  acct=%s  err=%s",
                        i, _normalise_account(r.get("account_number")),
                        (r.get("error_message", "") or "")[:80])
        if len(new_rows) > 50:
            logger.info("    … (%d more)", len(new_rows) - 50)
    elif new_rows:
        aligned = align_to_sheet(new_rows, sheet_bindings, len(header))
        append_rows(service, aligned)
        logger.info("Appended %d rows to %s", len(aligned), TAB_NAME)

    send_summary_email(
        run_date=run_date,
        source_email_dates=[m["date"] for m in mails],
        stats=combined_stats,
        dedupe_skipped=dupes,
        new_count=len(new_rows),
        dry_run=DRY_RUN,
        duration_seconds=time.monotonic() - started,
    )

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
