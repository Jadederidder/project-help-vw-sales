#!/usr/bin/env python3
"""
scripts/convert_account_expiry.py

Daily VW/Audi ACCOUNT EXPIRY → Revio debit-order conversion.

Wesbank rejects some VAP loads with reason
    "ACCOUNT EXPIRY DATE WITHIN MONTHS RANGE OF 12 acc:{account_number}"
because the customer's Wesbank finance is within 12 months of expiry.
The customer agreed to the policy and a sale was made — we just need to
switch collection method from Wesbank to Revio direct debit.

Read REJECTIONS tab → filter ACCOUNT EXPIRY rows that haven't been processed
(blank Conversion_Status column, EFFECTIVE DATE >= --backfill-from) → look up
SALES tab by WesBank Account Number (col Y) → if matched, create Revio client
and add to billing template; if unmatched, append to PENDING_CONVERSIONS.
Re-check PENDING rows every run; weekly maintenance ages out >= 2 weeks
unmatched.

Source of truth for rejections: the REJECTIONS tab (populated by
sync_rejections.py at 04:05 SAST). We run at 04:08 SAST so today's rejections
are already on the sheet by the time we read.

Run-locally example:
    DRY_RUN=true \\
    GOOGLE_SHEETS_CREDENTIALS=$(cat /path/to/sa.json) \\
    REVIO_API_KEY=… \\
    EMAIL_SENDER=… EMAIL_PASSWORD=… EMAIL_RECIPIENT=jd@projecthelp.co.za \\
    python3 scripts/convert_account_expiry.py --backfill-from 2026-04-29

Flags:
    --backfill-from YYYY-MM-DD  Filter REJECTIONS by EFFECTIVE DATE >= this.
                                Default 2026-04-29 (the live first-run date).
    --pending-recheck-only      Skip new-rejection scan; only re-check
                                PENDING_CONVERSIONS and (if Monday) age out.
                                Used by the weekly post-step on sync_sales.yml.
"""

import argparse
import json
import logging
import os
import re
import smtplib
import sys
from datetime import date, datetime, timezone
from email.message import EmailMessage

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.revio_subscription import (
    PRODUCT_CONFIG,
    add_subscriber,
    build_client_payload,
    compute_next_debit_date,
    create_client,
    resolve_template_id,
    _load_bt_client_map,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
SHEET_ID = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
REJECTIONS_TAB = "REJECTIONS"
SALES_TAB = "SALES"
PENDING_TAB = "PENDING_CONVERSIONS"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

EXPIRY_PREFIX = "ACCOUNT EXPIRY DATE WITHIN MONTHS RANGE OF 12"

DEFAULT_BACKFILL_FROM = "2026-04-29"
PENDING_TIMEOUT_WEEKS = 2

DRY_RUN = os.environ.get("DRY_RUN", "").lower() == "true"
DRY_RUN_RECIPIENT = "jd@projecthelp.co.za"

PENDING_HEADERS = [
    "Account_Number", "First_Seen_Date", "Last_Checked_Date",
    "Weeks_In_Pending", "Status", "Original_Reason",
    "Source_File_Date", "Notes",
]
# Two new columns are added to REJECTIONS on first run, in this order:
#   I = Conversion_Status        ("", PENDING, CONVERTED, ERROR: …, MOVED_TO_REJECTIONS)
#   J = Original_Account_Number  (preserves H value while H is blanked)
#
# The dashboard formula filters REJECTIONS!H:H<>"" to count rejections.
# By blanking H when a row is in flight (PENDING) or converted (CONVERTED),
# the dashboard count auto-updates with zero formula change. On age-out
# (MOVED_TO_REJECTIONS), H is restored from J so the row counts again.
REJECTIONS_CONVERSION_COL = "Conversion_Status"
REJECTIONS_ORIGINAL_ACCOUNT_COL = "Original_Account_Number"


# ─── Pure helpers ────────────────────────────────────────────────────────────
def _norm_account(v):
    """Strip non-digits — defeats sci-notation, .0 suffixes, whitespace."""
    if v is None or v == "":
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return re.sub(r"\D", "", s)


def is_account_expiry(error_message):
    """True iff the message is the ACCOUNT EXPIRY rejection.

    Substring/startswith match — every real message has the wesbank account
    suffix (' acc:NNNN'), so equality won't work. Case-insensitive,
    leading-whitespace tolerant; mirrors _is_duplicate_vap in sync_rejections.
    """
    s = (error_message or "").strip().upper()
    return s.startswith(EXPIRY_PREFIX)


def _col_letter(idx_0):
    n = idx_0 + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _parse_date_loose(s):
    """Parse the EFFECTIVE DATE column ('2026/04/29' / '2026-04-29' / blank)
    into a date, or None."""
    s = str(s or "").strip()
    if not s:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _today_iso():
    return date.today().isoformat()


# ─── Sheets ──────────────────────────────────────────────────────────────────
def get_sheets_service():
    raw = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
    if not raw:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS not set")
    creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def read_tab(svc, tab):
    """Returns (header_list, list_of_data_rows). Empty rows preserved as [].
    Each data row is padded to header length so column-index access is safe."""
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{tab}!A1:ZZ",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    rows = res.get("values", [])
    if not rows:
        return [], []
    header = rows[0]
    n = len(header)
    data = [list(r) + [""] * (n - len(r)) for r in rows[1:]]
    return header, data


def write_cell(svc, tab, row_1based, col_letter, value, dry_run=False):
    if dry_run:
        logger.info("[DRY RUN] would write %s!%s%d = %r",
                    tab, col_letter, row_1based, value)
        return
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!{col_letter}{row_1based}",
        valueInputOption="RAW",
        body={"values": [[value]]},
    ).execute()


def append_rows(svc, tab, rows, dry_run=False):
    if not rows:
        return
    if dry_run:
        for r in rows:
            logger.info("[DRY RUN] would append to %s: %s", tab, r)
        return
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def _ensure_one_column(svc, header, name):
    """Idempotent: append `name` to REJECTIONS header if not present.
    Returns (idx, new_header). Mutates the sheet only when missing."""
    for i, h in enumerate(header):
        if str(h or "").strip() == name:
            return i, header
    target_idx = len(header)
    new_header = list(header) + [name]
    if DRY_RUN:
        logger.info("[DRY RUN] would add %s column to %s at col %s",
                    name, REJECTIONS_TAB, _col_letter(target_idx))
        return target_idx, new_header
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{REJECTIONS_TAB}!{_col_letter(target_idx)}1",
        valueInputOption="RAW",
        body={"values": [[name]]},
    ).execute()
    logger.info("Added %s column to %s at col %s",
                name, REJECTIONS_TAB, _col_letter(target_idx))
    return target_idx, new_header


def ensure_rejections_columns(svc, header):
    """Idempotent: ensure both Conversion_Status (col I) and
    Original_Account_Number (col J) exist on REJECTIONS.
    Returns (i_idx, j_idx, new_header)."""
    i_idx, header = _ensure_one_column(svc, header, REJECTIONS_CONVERSION_COL)
    j_idx, header = _ensure_one_column(svc, header, REJECTIONS_ORIGINAL_ACCOUNT_COL)
    return i_idx, j_idx, header


def _is_in_flight_status(status):
    """True iff `status` represents an "in-flight retry pending" state that
    should blank H and save J. CONVERTED, PENDING, and any ERROR: ... message
    are all in-flight from the dashboard's perspective — they should not
    count as rejections. Only MOVED_TO_REJECTIONS and "" (blank, untouched)
    represent real rejections.
    """
    if not status:
        return False
    if status in ("CONVERTED", "PENDING"):
        return True
    if status.startswith("ERROR:"):
        return True
    return False


def compute_rejection_state_transition(current_h, current_j, new_status):
    """Pure: given current H + J cell values and the target Conversion_Status,
    return (target_h, target_j) to write. `None` means 'do not write'.

    Rules (per spec v1.5):
      - CONVERTED / PENDING / ERROR: save H→J if J blank, then blank H.
        ERROR rows are treated as in-flight retries (excluded from dashboard
        rejection count) so they self-heal silently if they later succeed.
      - MOVED_TO_REJECTIONS: restore H from J (J kept for audit). This is
        the one place H is restored.
      - Unknown status: H and J unchanged.
    """
    h = (current_h or "")
    j = (current_j or "")

    if new_status == "MOVED_TO_REJECTIONS":
        if j:
            return (j, None)  # restore H ← J; J kept
        return (None, None)   # nothing to restore

    if _is_in_flight_status(new_status):
        if j:
            # J already saved on a prior run — just ensure H is blank
            if h:
                return ("", None)
            return (None, None)
        if h:
            return ("", h)    # save H→J, blank H
        return (None, None)   # both blank — anomalous, no-op

    # Unknown status — leave H and J alone
    return (None, None)


def should_skip_conversion_status(status):
    """True iff a REJECTIONS row's Conversion_Status means "do not reprocess
    on the daily new-rejection scan". Per spec v1.5: only CONVERTED and
    MOVED_TO_REJECTIONS terminate retries. PENDING and ERROR rows are still
    retried each day so transient failures self-heal.
    """
    return (status or "").strip().upper() in ("CONVERTED", "MOVED_TO_REJECTIONS")


def apply_rejection_state(svc, row_num, h_idx, i_idx, j_idx,
                          current_h, current_j, new_status, dry_run):
    """Write the H / I / J cells for a REJECTIONS row state transition.
    Always writes I; H and J only when compute_rejection_state_transition
    says to."""
    target_h, target_j = compute_rejection_state_transition(
        current_h, current_j, new_status
    )
    write_cell(svc, REJECTIONS_TAB, row_num,
               _col_letter(i_idx), new_status, dry_run=dry_run)
    if target_h is not None:
        write_cell(svc, REJECTIONS_TAB, row_num,
                   _col_letter(h_idx), target_h, dry_run=dry_run)
    if target_j is not None and j_idx is not None:
        write_cell(svc, REJECTIONS_TAB, row_num,
                   _col_letter(j_idx), target_j, dry_run=dry_run)


def ensure_pending_headers(svc, header):
    """Idempotent: write PENDING_CONVERSIONS header row if blank."""
    if header:
        # Already set up — verify expected columns are present
        missing = [h for h in PENDING_HEADERS if h not in header]
        if missing:
            logger.warning("PENDING_CONVERSIONS missing headers: %s", missing)
        return header
    if DRY_RUN:
        logger.info("[DRY RUN] would write %s headers: %s", PENDING_TAB, PENDING_HEADERS)
        return PENDING_HEADERS
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{PENDING_TAB}!A1",
        valueInputOption="RAW",
        body={"values": [PENDING_HEADERS]},
    ).execute()
    logger.info("Wrote %s headers", PENDING_TAB)
    return PENDING_HEADERS


# ─── Conversion logic ────────────────────────────────────────────────────────
def build_sales_index(sales_header, sales_data):
    """Build {normalised_wesbank_account#: header-keyed sales_row dict}."""
    if "WesBank Account Number" not in sales_header:
        raise RuntimeError(
            f"SALES tab missing 'WesBank Account Number' column. "
            f"Headers: {sales_header}"
        )
    acc_idx = sales_header.index("WesBank Account Number")
    by_acc = {}
    for r in sales_data:
        acc = _norm_account(r[acc_idx])
        if acc:
            by_acc[acc] = dict(zip(sales_header, r))
    return by_acc


def find_rejection_indices(header):
    """Locate the columns we read on REJECTIONS by header name."""
    needed = {
        "ACCOUNT_NUMBER":               None,
        "ERROR MESSAGE":                None,
        "EFFECTIVE DATE":               None,
        "PRODUCT CODE":                 None,  # may be missing on legacy schema
        REJECTIONS_CONVERSION_COL:      None,
        REJECTIONS_ORIGINAL_ACCOUNT_COL: None,
    }
    for i, h in enumerate(header):
        h_clean = str(h or "").strip()
        if h_clean in needed:
            needed[h_clean] = i
    return needed


def process_one_rejection(svc, rejection_row, rejection_row_num,
                          h_idx, i_idx, j_idx, rej_idx,
                          sales_index, template_map, today, dry_run):
    """Returns one of:
        ('converted', sales_row, scheduled_date, revio_client_id)
        ('pending',   None, None, None)
        ('error',     error_msg, None, None)
    Side effect: applies the H/I/J state transition on the REJECTIONS row.
    """
    current_h = rejection_row[h_idx] if h_idx < len(rejection_row) else ""
    current_j = (rejection_row[j_idx]
                 if (j_idx is not None and j_idx < len(rejection_row))
                 else "")
    # Use H if populated; fall back to J if H was blanked on a prior run
    acc_num = _norm_account(current_h) or _norm_account(current_j)
    if not acc_num:
        return ("error", "blank ACCOUNT_NUMBER on rejection row", None, None)

    sales_row = sales_index.get(acc_num)
    if not sales_row:
        apply_rejection_state(svc, rejection_row_num,
                              h_idx, i_idx, j_idx,
                              current_h, current_j,
                              "PENDING", dry_run=dry_run)
        return ("pending", None, None, None)

    # Product code: prefer SALES col C; fall back to REJECTIONS PRODUCT CODE
    # if present (legacy 8-col REJECTIONS doesn't have it).
    product_code = (sales_row.get("VW/Audi Product") or "").strip()
    if not product_code and rej_idx.get("PRODUCT CODE") is not None:
        product_code = str(rejection_row[rej_idx["PRODUCT CODE"]]).strip()

    if product_code not in PRODUCT_CONFIG:
        msg = f"unknown product code {product_code!r} for acc {acc_num}"
        apply_rejection_state(svc, rejection_row_num,
                              h_idx, i_idx, j_idx,
                              current_h, current_j,
                              f"ERROR: {msg}"[:120], dry_run=dry_run)
        return ("error", msg, None, None)

    template_id = resolve_template_id(template_map, product_code)
    personal_code = "VW-" + str(sales_row.get("Policy Number") or "").strip()
    debit_date_str = sales_row.get("Debit_Order_Date") or ""
    scheduled_date = compute_next_debit_date(debit_date_str, today)
    invoice_reference = f"VW-{sales_row.get('Policy Number') or acc_num}-CONV"
    payload = build_client_payload(sales_row, personal_code)

    try:
        client_id = create_client(payload, dry_run=dry_run)
        add_subscriber(template_id, client_id, scheduled_date,
                       invoice_reference, dry_run=dry_run)
    except Exception as e:
        msg = str(e)[:200]
        logger.error("Conversion failed for acc %s: %s", acc_num, msg)
        apply_rejection_state(svc, rejection_row_num,
                              h_idx, i_idx, j_idx,
                              current_h, current_j,
                              f"ERROR: {msg}"[:120], dry_run=dry_run)
        return ("error", msg, None, None)

    apply_rejection_state(svc, rejection_row_num,
                          h_idx, i_idx, j_idx,
                          current_h, current_j,
                          "CONVERTED", dry_run=dry_run)
    return ("converted", sales_row, scheduled_date, client_id)


def recheck_pending_conversions(svc, sales_index, template_map, today,
                                bump_weeks_counter, dry_run):
    """Iterate PENDING_CONVERSIONS, re-attempt SALES match. If match found,
    convert and mark CONVERTED. If bump_weeks_counter (weekly run) and still
    PENDING, increment Weeks_In_Pending; if >= timeout, mark MOVED_TO_REJECTIONS
    and reset the REJECTIONS row Conversion_Status to '' so it counts again.

    Returns dict of counts: {converted, still_pending, moved}."""
    header, data = read_tab(svc, PENDING_TAB)
    if not header:
        return {"converted": 0, "still_pending": 0, "moved": 0, "errors": 0,
                "converted_rows": [], "moved_rows": [], "error_rows": []}

    idx = {h: i for i, h in enumerate(header)}
    required = {"Account_Number", "First_Seen_Date", "Last_Checked_Date",
                "Weeks_In_Pending", "Status"}
    if not required.issubset(idx):
        logger.warning("PENDING_CONVERSIONS schema unexpected: %s", header)
        return {"converted": 0, "still_pending": 0, "moved": 0, "errors": 0,
                "converted_rows": [], "moved_rows": [], "error_rows": []}

    # Need REJECTIONS for the H/I/J state transitions on convert + age-out
    rej_header, rej_data = read_tab(svc, REJECTIONS_TAB)
    rej_idx = find_rejection_indices(rej_header)
    if rej_idx[REJECTIONS_CONVERSION_COL] is None or \
       rej_idx[REJECTIONS_ORIGINAL_ACCOUNT_COL] is None:
        # columns were just inserted in main(); refresh once more
        i_idx, j_idx, rej_header = ensure_rejections_columns(svc, rej_header)
        rej_idx = find_rejection_indices(rej_header)
    h_idx = rej_idx["ACCOUNT_NUMBER"]
    i_idx = rej_idx[REJECTIONS_CONVERSION_COL]
    j_idx = rej_idx[REJECTIONS_ORIGINAL_ACCOUNT_COL]

    # Build account → (row_num, h_value, j_value) map.
    # Look up via H if present; fall back to J for already-processed rows
    # whose H has been blanked.
    rej_acc_to_meta = {}
    for ri, r in enumerate(rej_data, start=2):
        h_val = r[h_idx] if (h_idx is not None and h_idx < len(r)) else ""
        j_val = r[j_idx] if (j_idx is not None and j_idx < len(r)) else ""
        a = _norm_account(h_val) or _norm_account(j_val)
        if a:
            rej_acc_to_meta[a] = (ri, h_val, j_val)

    counts = {"converted": 0, "still_pending": 0, "moved": 0, "errors": 0,
              "converted_rows": [], "moved_rows": [], "error_rows": []}

    for i, row in enumerate(data, start=2):  # 1-based + header
        status = (row[idx["Status"]] or "").strip().upper()
        if status != "PENDING":
            continue

        acc = _norm_account(row[idx["Account_Number"]])
        if not acc:
            continue

        sales_row = sales_index.get(acc)
        if sales_row:
            # CONVERT
            product_code = (sales_row.get("VW/Audi Product") or "").strip()
            if product_code not in PRODUCT_CONFIG:
                logger.warning("PENDING %s has unknown product %r — skipping",
                               acc, product_code)
                continue
            template_id = resolve_template_id(template_map, product_code)
            personal_code = "VW-" + str(sales_row.get("Policy Number") or "").strip()
            scheduled_date = compute_next_debit_date(
                sales_row.get("Debit_Order_Date") or "", today
            )
            invoice_reference = f"VW-{sales_row.get('Policy Number') or acc}-CONV"
            payload = build_client_payload(sales_row, personal_code)
            try:
                client_id = create_client(payload, dry_run=dry_run)
                add_subscriber(template_id, client_id, scheduled_date,
                               invoice_reference, dry_run=dry_run)
            except Exception as e:
                err_msg = str(e)[:200]
                logger.error("PENDING re-conversion failed for %s: %s",
                             acc, err_msg)
                counts["errors"] += 1
                counts["error_rows"].append({
                    "personal_code": personal_code,
                    "account":       acc,
                    "error":         err_msg,
                })
                # Leave REJECTIONS row as PENDING — recheck retries again
                # next run; idempotent pre-checks make this safe.
                continue
            write_cell(svc, PENDING_TAB, i, _col_letter(idx["Status"]),
                       "CONVERTED", dry_run=dry_run)
            write_cell(svc, PENDING_TAB, i, _col_letter(idx["Last_Checked_Date"]),
                       _today_iso(), dry_run=dry_run)
            if "Notes" in idx:
                write_cell(svc, PENDING_TAB, i, _col_letter(idx["Notes"]),
                           f"converted on {_today_iso()} → Revio sub for "
                           f"{personal_code}, debit {scheduled_date}",
                           dry_run=dry_run)
            # Flip REJECTIONS row PENDING → CONVERTED (J already saved on
            # the original transition to PENDING; this just ensures H is
            # blank and writes the new status).
            r_row, r_h, r_j = rej_acc_to_meta.get(acc, (None, "", ""))
            if r_row:
                apply_rejection_state(
                    svc, r_row, h_idx, i_idx, j_idx,
                    r_h, r_j, "CONVERTED", dry_run=dry_run,
                )
            counts["converted"] += 1
            counts["converted_rows"].append({
                "account":         acc,
                "name":            f"{sales_row.get('FirstName','')} "
                                   f"{sales_row.get('Surname','')}".strip(),
                "product":         product_code,
                "price":           PRODUCT_CONFIG[product_code][1],
                "next_debit":      scheduled_date,
                "revio_client_id": client_id or "(dry-run)",
            })
            continue

        # Still PENDING — update Last_Checked, optionally bump weeks
        write_cell(svc, PENDING_TAB, i, _col_letter(idx["Last_Checked_Date"]),
                   _today_iso(), dry_run=dry_run)
        if bump_weeks_counter:
            current = row[idx["Weeks_In_Pending"]]
            try:
                weeks = int(current)
            except (TypeError, ValueError):
                weeks = 0
            weeks += 1
            write_cell(svc, PENDING_TAB, i, _col_letter(idx["Weeks_In_Pending"]),
                       weeks, dry_run=dry_run)

            if weeks >= PENDING_TIMEOUT_WEEKS:
                write_cell(svc, PENDING_TAB, i, _col_letter(idx["Status"]),
                           "MOVED_TO_REJECTIONS", dry_run=dry_run)
                if "Notes" in idx:
                    write_cell(svc, PENDING_TAB, i, _col_letter(idx["Notes"]),
                               f"timed out after {weeks} weeks — never matched "
                               f"in SALES; left as rejection on dashboard",
                               dry_run=dry_run)
                # Restore the REJECTIONS row's H from J so the dashboard
                # formula (filters H<>"") counts it as a rejection again.
                r_row, r_h, r_j = rej_acc_to_meta.get(acc, (None, "", ""))
                if r_row:
                    apply_rejection_state(
                        svc, r_row, h_idx, i_idx, j_idx,
                        r_h, r_j, "MOVED_TO_REJECTIONS", dry_run=dry_run,
                    )
                counts["moved"] += 1
                counts["moved_rows"].append({"account": acc, "weeks": weeks})
            else:
                counts["still_pending"] += 1
        else:
            counts["still_pending"] += 1

    return counts


def append_to_pending(svc, account, original_reason, source_file_date,
                      dry_run, notes=""):
    """Append one row to PENDING_CONVERSIONS.

    Used for both:
      - "no SALES match" rejections (notes="")
      - "SALES matched but Revio API failed" rejections (notes carries the
        API error so the audit trail is self-contained)
    Either way, the row enters the 2-week age-out timer.
    """
    today = _today_iso()
    src_date = source_file_date.isoformat() if source_file_date else ""
    row = [account, today, today, 0, "PENDING",
           original_reason, src_date, notes]
    append_rows(svc, PENDING_TAB, [row], dry_run=dry_run)


def existing_pending_accounts(svc):
    """Return the set of account numbers already in PENDING_CONVERSIONS
    (any status) so we don't double-add."""
    header, data = read_tab(svc, PENDING_TAB)
    if not header or "Account_Number" not in header:
        return set()
    idx = header.index("Account_Number")
    return {_norm_account(r[idx]) for r in data if r[idx]}


# ─── Email ───────────────────────────────────────────────────────────────────
def send_summary_email(stats, run_date_str, dry_run, error_summary=""):
    sender = os.environ.get("EMAIL_SENDER")
    pwd    = os.environ.get("EMAIL_PASSWORD")
    if not sender or not pwd:
        logger.warning("EMAIL_SENDER / EMAIL_PASSWORD not set — skipping email")
        return

    if dry_run:
        recipients = [DRY_RUN_RECIPIENT]
    else:
        recip_s = os.environ.get("EMAIL_RECIPIENT", "")
        recipients = [r.strip() for r in recip_s.split(",") if r.strip()]
    if not recipients:
        logger.warning("No recipients — skipping summary email")
        return

    n_conv  = stats["converted"]
    n_new_pending  = stats["new_pending"]
    n_still_pending = stats["still_pending"]
    n_moved = stats["moved"]
    n_err   = stats["errors"]

    if error_summary or n_err > 0:
        subject = f"⚠ VW ACCOUNT EXPIRY — error(s); {n_conv} partial"
    elif n_moved:
        subject = (f"⚠ VW ACCOUNT EXPIRY — {n_conv} converted, "
                   f"{n_moved} moved to REJECTIONS")
    elif n_conv or n_new_pending:
        subject = (f"📥 VW ACCOUNT EXPIRY — {n_conv} converted, "
                   f"{n_new_pending + n_still_pending} pending")
    else:
        subject = f"✅ VW ACCOUNT EXPIRY conversions — clean ({run_date_str})"
    if dry_run:
        subject = f"[DRY RUN] {subject}"

    lines = [
        f"VW ACCOUNT EXPIRY conversions — {run_date_str}",
        "",
        f"  ✓ {n_conv} converted to Revio debit order",
        f"  ⏳ {n_new_pending} added to PENDING (no SALES match yet)",
        f"  ⏳ {n_still_pending} still pending from previous days",
        f"  ⚠ {n_moved} moved to REJECTIONS (>={PENDING_TIMEOUT_WEEKS} weeks unmatched)",
        f"  ⚠ {n_err} errors",
        "",
    ]
    if stats["converted_rows"]:
        lines.append("Successful conversions:")
        for r in stats["converted_rows"]:
            lines.append(
                f"  - {r['name']} | acc {r['account']} | {r['product']} | "
                f"R{r['price']}/m | next debit {r['next_debit']} | "
                f"Revio sub {r['revio_client_id']}"
            )
        lines.append("")
    if stats["new_pending_rows"]:
        lines.append("Pending (no SALES match yet):")
        for r in stats["new_pending_rows"]:
            lines.append(
                f"  - {r['account']} | first seen {r['first_seen']} | "
                f"weeks pending {r['weeks']}"
            )
        lines.append("")
    if stats["moved_rows"]:
        lines.append("Moved to REJECTIONS this run:")
        for r in stats["moved_rows"]:
            lines.append(
                f"  - {r['account']} | weeks pending {r['weeks']} | "
                f"reason: never appeared in SALES"
            )
        lines.append("")
    if stats.get("error_rows"):
        lines.append(
            f"⚠ {len(stats['error_rows'])} row(s) in ERROR state "
            f"(will auto-retry tomorrow):"
        )
        for r in stats["error_rows"]:
            lines.append(
                f"  - {r['personal_code']} (acc {r['account']}) — "
                f"{r['error']}"
            )
        lines.append("")
    if error_summary:
        lines += ["", f"ERROR: {error_summary}"]
    if dry_run:
        lines += ["", "DRY RUN — no Revio calls and no sheet writes were made."]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content("\n".join(lines) + "\n")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(sender, pwd)
        s.send_message(msg)
    logger.info("Summary email sent → %s", ", ".join(recipients))


# ─── Main ────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--backfill-from", default=DEFAULT_BACKFILL_FROM,
                   help="Filter REJECTIONS by EFFECTIVE DATE >= this "
                        "(YYYY-MM-DD). Default 2026-04-29.")
    p.add_argument("--pending-recheck-only", action="store_true",
                   help="Skip new-rejection scan; only recheck PENDING and "
                        "(if Mon) age out. Used by weekly post-step.")
    return p.parse_args()


def main():
    args = parse_args()
    run_dt = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("VW ACCOUNT EXPIRY → REVIO CONVERSION")
    logger.info("Run date          : %s UTC", run_dt.isoformat(timespec="seconds"))
    logger.info("Dry run           : %s", DRY_RUN)
    logger.info("Mode              : %s", "pending-recheck-only"
                if args.pending_recheck_only else "full daily")
    logger.info("Backfill from     : %s", args.backfill_from)
    logger.info("=" * 60)

    backfill_from = _parse_date_loose(args.backfill_from)
    if not backfill_from:
        logger.error("--backfill-from is invalid: %r", args.backfill_from)
        sys.exit(1)

    today = date.today()
    is_monday = today.weekday() == 0
    bump_weeks = args.pending_recheck_only and is_monday

    logger.info("[1/6] Connecting to Google Sheets …")
    svc = get_sheets_service()

    logger.info("[2/6] Reading SALES + REJECTIONS + PENDING_CONVERSIONS …")
    sales_header, sales_data = read_tab(svc, SALES_TAB)
    sales_index = build_sales_index(sales_header, sales_data)
    logger.info("  SALES rows indexed by WesBank Account Number: %d", len(sales_index))

    rej_header, rej_data = read_tab(svc, REJECTIONS_TAB)
    if not rej_header:
        raise RuntimeError(f"{REJECTIONS_TAB} tab is empty")
    i_idx, j_idx, rej_header = ensure_rejections_columns(svc, rej_header)
    # Pad data rows to the new (possibly extended) header length so
    # column-index access is safe for the new I and J columns.
    new_n = len(rej_header)
    rej_data = [r + [""] * max(0, new_n - len(r)) for r in rej_data]
    rej_idx = find_rejection_indices(rej_header)
    h_idx = rej_idx["ACCOUNT_NUMBER"]
    logger.info("  REJECTIONS rows: %d (Conversion_Status=col %s, "
                "Original_Account_Number=col %s)",
                len(rej_data), _col_letter(i_idx), _col_letter(j_idx))

    pending_header, _ = read_tab(svc, PENDING_TAB)
    ensure_pending_headers(svc, pending_header)

    logger.info("[3/6] Loading Revio billing templates …")
    if DRY_RUN and not os.environ.get("REVIO_API_KEY"):
        # Offline dry-run convenience: skip the GET /billing_templates/ call.
        template_map = {name: f"DRY-{name}" for name, _ in PRODUCT_CONFIG.values()}
        logger.info("  [DRY RUN — no REVIO_API_KEY] using fake template ids: %s",
                    template_map)
    else:
        template_map = _load_bt_client_map()
        logger.info("  loaded %d billing templates", len(template_map))

    stats = {
        "converted": 0, "new_pending": 0, "still_pending": 0,
        "moved": 0, "errors": 0,
        "converted_rows": [], "new_pending_rows": [], "moved_rows": [],
        "error_rows": [],
    }
    error_summary = ""

    try:
        if not args.pending_recheck_only:
            logger.info("[4/6] Scanning REJECTIONS for ACCOUNT EXPIRY rows …")
            already_pending = existing_pending_accounts(svc)
            n_examined = 0
            n_skipped_old = 0
            n_skipped_already = 0
            for row_i, row in enumerate(rej_data, start=2):
                err = row[rej_idx["ERROR MESSAGE"]] if rej_idx["ERROR MESSAGE"] is not None else ""
                if not is_account_expiry(err):
                    continue
                eff_date = _parse_date_loose(
                    row[rej_idx["EFFECTIVE DATE"]] if rej_idx["EFFECTIVE DATE"] is not None else ""
                )
                if eff_date and eff_date < backfill_from:
                    n_skipped_old += 1
                    continue
                # Skip terminal-state rows. Per spec v1.5, only CONVERTED
                # and MOVED_TO_REJECTIONS are terminal. PENDING and ERROR
                # rows fall through and get retried — combined with the
                # idempotent pre-checks in revio_subscription.create_client
                # / add_subscriber, transient errors self-heal next run.
                cur_status = (row[i_idx] or "").strip() if i_idx < len(row) else ""
                if should_skip_conversion_status(cur_status):
                    n_skipped_already += 1
                    continue
                n_examined += 1
                cur_h = row[h_idx] if h_idx < len(row) else ""
                cur_j = row[j_idx] if (j_idx is not None and j_idx < len(row)) else ""
                acc = _norm_account(cur_h) or _norm_account(cur_j)
                if acc in already_pending:
                    # Already in PENDING_CONVERSIONS pipeline (possibly from
                    # a prior partial run); make REJECTIONS row consistent
                    # by applying the PENDING transition (saves H→J, blanks H).
                    apply_rejection_state(svc, row_i,
                                          h_idx, i_idx, j_idx,
                                          cur_h, cur_j, "PENDING",
                                          dry_run=DRY_RUN)
                    n_skipped_already += 1
                    continue

                outcome, info, sched, client_id = process_one_rejection(
                    svc, row, row_i, h_idx, i_idx, j_idx, rej_idx,
                    sales_index, template_map, today, DRY_RUN,
                )
                if outcome == "converted":
                    sales_row = info
                    pc = (sales_row.get("VW/Audi Product") or "").strip()
                    stats["converted"] += 1
                    stats["converted_rows"].append({
                        "account":         acc,
                        "name":            f"{sales_row.get('FirstName','')} "
                                           f"{sales_row.get('Surname','')}".strip(),
                        "product":         pc,
                        "price":           PRODUCT_CONFIG[pc][1] if pc in PRODUCT_CONFIG else "?",
                        "next_debit":      sched,
                        "revio_client_id": client_id or "(dry-run)",
                    })
                elif outcome == "pending":
                    append_to_pending(
                        svc,
                        account=acc,
                        original_reason=str(err)[:240],
                        source_file_date=eff_date,
                        dry_run=DRY_RUN,
                    )
                    already_pending.add(acc)
                    stats["new_pending"] += 1
                    stats["new_pending_rows"].append({
                        "account":    acc,
                        "first_seen": _today_iso(),
                        "weeks":      0,
                    })
                else:  # error — REJECTIONS row already marked ERROR by
                       # process_one_rejection. Surface in email + add to
                       # PENDING_CONVERSIONS so the 2-week age-out applies
                       # to errors too (per spec v1.5).
                    err_msg = info or ""
                    stats["errors"] += 1
                    sales_row_lookup = sales_index.get(acc)
                    pc_str = (
                        "VW-" + str(
                            sales_row_lookup.get("Policy Number") or ""
                        ).strip()
                        if sales_row_lookup else "(no SALES match)"
                    )
                    stats["error_rows"].append({
                        "personal_code": pc_str,
                        "account":       acc,
                        "error":         err_msg[:200],
                    })
                    if acc and acc not in already_pending:
                        append_to_pending(
                            svc, account=acc,
                            original_reason=str(err)[:240],
                            source_file_date=eff_date,
                            notes=f"convert attempt failed: {err_msg[:180]}",
                            dry_run=DRY_RUN,
                        )
                        already_pending.add(acc)

            logger.info("  examined=%d skipped_pre_backfill=%d skipped_already_processed=%d",
                        n_examined, n_skipped_old, n_skipped_already)

        logger.info("[5/6] Re-checking PENDING_CONVERSIONS%s …",
                    " (with weekly age-out)" if bump_weeks else "")
        pcounts = recheck_pending_conversions(
            svc, sales_index, template_map, today,
            bump_weeks_counter=bump_weeks, dry_run=DRY_RUN,
        )
        stats["converted"] += pcounts["converted"]
        stats["still_pending"] = pcounts["still_pending"]
        stats["moved"] += pcounts["moved"]
        stats["errors"] += pcounts.get("errors", 0)
        stats["converted_rows"].extend(pcounts["converted_rows"])
        stats["moved_rows"].extend(pcounts["moved_rows"])
        stats["error_rows"].extend(pcounts.get("error_rows", []))

    except Exception as e:
        logger.exception("CRITICAL: %s", e)
        error_summary = str(e)[:300]
        stats["errors"] += 1

    logger.info("[6/6] Sending summary email …")
    send_summary_email(
        stats=stats,
        run_date_str=today.strftime("%d %b %Y"),
        dry_run=DRY_RUN,
        error_summary=error_summary,
    )

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("  converted     : %d", stats["converted"])
    logger.info("  new pending   : %d", stats["new_pending"])
    logger.info("  still pending : %d", stats["still_pending"])
    logger.info("  moved out     : %d", stats["moved"])
    logger.info("  errors        : %d", stats["errors"])
    logger.info("=" * 60)

    if error_summary:
        sys.exit(1)


if __name__ == "__main__":
    main()
