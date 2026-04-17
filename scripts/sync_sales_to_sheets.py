#!/usr/bin/env python3
# ============================================================
# scripts/sync_sales_to_sheets.py
# Runs every Monday at 5pm
# 1. Connects to SFTP, downloads latest VW_Audi EOD file
# 2. Filters rows from last 14 days only
# 3. Deduplicates against existing sheet data by WesBank account number
# 4. Appends only NEW rows to Google Sheet SALES tab
# 5. Emails confirmation report
# ============================================================

import os
import sys
import logging
import smtplib
import io
import json
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import paramiko
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SFTP_HOST     = "eu-west-1.sftpcloud.io"
SFTP_PORT     = 22
SFTP_USER     = "projecthelp@projecthelp.co.za"
SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD", "")

SHEET_ID      = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SHEET_TAB     = "SALES"
SHEET_SCOPES  = ["https://www.googleapis.com/auth/spreadsheets"]

EMAIL_SENDER    = os.environ.get("EMAIL_SENDER", "")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "")

LOOKBACK_DAYS = 14

# Google Sheet column order — must match exactly
SHEET_HEADERS = [
    "call_date",        # A - Created Time
    "status",           # B - BLANK
    "user",             # C - BLANK
    "full_name",        # D - FirstName + Surname
    "brand",            # E - Manufacturer (VOLKSWAGEN/AUDI etc)
    "phone_number",     # F - Mobile Number
    "title",            # G - Title
    "first_name",       # H - FirstName
    "middle_initial",   # I - BLANK
    "last_name",        # J - Surname
    "address1",         # K - Physical Line1
    "address2",         # L - Physical Line2
    "address3",         # M - Physical Suburb
    "city",             # N - Physical City
    "state",            # O - BLANK
    "province",         # P - Physical Province
    "postal_code",      # Q - Physical Post Code
    "alt_phone",        # R - BLANK
    "email",            # S - Email Address
    "lead_id",          # T - VICI Lead ID
    "status_name",      # U - "SALE Premium Help" (hardcoded)
    "account number",   # V - WesBank Account Number (starts with 87)
    "CUSTOMER_NUMBER",  # W - Customer Number
    "CUST_TYPE_CODE",   # X - CUST_TYPE_CODE
    "IDENTITY_OR_REG_NUM", # Y - ID Number
    "BANK_NAME",        # Z - Bank
    "BANK_ACC_NUM",     # AA - Bank Account Number
    "BANK_BRANCH_CODE", # AB - Branch Code
    "VEHICLE_ID_NUM",   # AC - VEHICLE_ID_NUM
    "REGISTRATION_NUM", # AD - REGISTRATION_NUM
    "CHASSIS_NUM",      # AE - CHASSIS_NUM
    "ASSET_SHORT_DESCRIPTION", # AF
    "MM_MAKE_DESCRIPTION",     # AG - Manufacturer
    "MM_MODEL_DESCRIPTION",    # AH - Model
    "DATE_FIRST_LICENSED",     # AI - Date First Licensed
    "OPEN_DATE",               # AJ - Open Date
    "DATE_EXPIRY",             # AK - Expiry Date
    "CUST_TYPE_DESC",          # AL - CUST_TYPE_DESC
    "snapshot_date",           # AM - Snap Date
]


def get_sftp_client():
    logger.info("Connecting to SFTP...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD)
    sftp = ssh.open_sftp()
    logger.info("SFTP connected")
    return ssh, sftp


def find_latest_file(sftp):
    """Find the latest VW_Audi EOD file in ProjectHelp/VW & Audi Sales."""
    folder = "ProjectHelp/VW & Audi Sales"
    files = sftp.listdir(folder)
    xlsx_files = sorted([f for f in files if f.endswith(".xlsx") and "VW_Audi" in f])
    if not xlsx_files:
        raise RuntimeError("No VW_Audi files found in " + folder)
    latest = xlsx_files[-1]
    logger.info("Latest file: " + latest)
    return folder, latest


def download_file(sftp, folder, filename):
    remote_path = folder + "/" + filename
    logger.info("Downloading: " + remote_path)
    buf = io.BytesIO()
    sftp.getfo(remote_path, buf)
    buf.seek(0)
    return buf


def parse_and_map(buf):
    """Parse SFTP file and map to Google Sheet columns."""
    df = pd.read_excel(buf)
    logger.info("Total rows in file: " + str(len(df)))

    mapped = pd.DataFrame()

    # A: call_date — Created Time
    mapped["call_date"] = pd.to_datetime(
        df.get("Created Time (VW/Audi Campaign 1)"), errors="coerce"
    )

    # B: status — BLANK
    mapped["status"] = ""

    # C: user — BLANK
    mapped["user"] = ""

    # D: full_name — FirstName + Surname
    mapped["full_name"] = (
        df.get("FirstName", "").fillna("").astype(str) + " " +
        df.get("Surname", "").fillna("").astype(str)
    ).str.strip()

    # E: brand — Manufacturer (VOLKSWAGEN/AUDI/CHERY/SUZUKI etc)
    mapped["brand"] = df.get("Manufacturer", "").fillna("").astype(str).str.upper()

    # F: phone_number
    mapped["phone_number"] = df.get("Mobile Number (VW/Audi Campaign 1)", "").fillna("").astype(str)

    # G: title
    mapped["title"] = df.get("Title (VW/Audi Campaign 1)", "").fillna("").astype(str)

    # H: first_name
    mapped["first_name"] = df.get("FirstName", "").fillna("").astype(str)

    # I: middle_initial — BLANK
    mapped["middle_initial"] = ""

    # J: last_name
    mapped["last_name"] = df.get("Surname", "").fillna("").astype(str)

    # K: address1
    mapped["address1"] = df.get("Physical Line1", "").fillna("").astype(str)

    # L: address2
    mapped["address2"] = df.get("Physical Line2", "").fillna("").astype(str)

    # M: address3 — suburb
    mapped["address3"] = df.get("Physical Suburb", "").fillna("").astype(str)

    # N: city
    mapped["city"] = df.get("Physical City", "").fillna("").astype(str)

    # O: state — BLANK
    mapped["state"] = ""

    # P: province
    mapped["province"] = df.get("Physical Province", "").fillna("").astype(str)

    # Q: postal_code
    mapped["postal_code"] = df.get("Physical Post Code", "").fillna("").astype(str)

    # R: alt_phone — BLANK
    mapped["alt_phone"] = ""

    # S: email
    mapped["email"] = df.get("Email Address (VW/Audi Campaign 1)", "").fillna("").astype(str)

    # T: lead_id — VICI Lead ID
    mapped["lead_id"] = df.get("VICI Lead ID", "").fillna("").astype(str)

    # U: status_name — hardcoded
    mapped["status_name"] = "SALE Premium Help"

    # V: account number — WesBank Account Number (starts with 87) — MOST IMPORTANT
    def clean_account(val):
        s = str(val).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s.strip()

    mapped["account number"] = df.get("WesBank Account Number", "").fillna("").apply(clean_account)

    # W: CUSTOMER_NUMBER
    mapped["CUSTOMER_NUMBER"] = df.get("Customer Number", "").fillna("").astype(str)

    # X: CUST_TYPE_CODE
    mapped["CUST_TYPE_CODE"] = df.get("CUST_TYPE_CODE", "").fillna("").astype(str)

    # Y: IDENTITY_OR_REG_NUM
    mapped["IDENTITY_OR_REG_NUM"] = df.get("ID Number", "").fillna("").astype(str)

    # Z: BANK_NAME
    mapped["BANK_NAME"] = df.get("Bank", "").fillna("").astype(str)

    # AA: BANK_ACC_NUM
    mapped["BANK_ACC_NUM"] = df.get("Bank Account Number (VW/Audi)", "").fillna("").astype(str)

    # AB: BANK_BRANCH_CODE
    mapped["BANK_BRANCH_CODE"] = df.get("Branch Code (VW/Audi Campaign 1)", "").fillna("").astype(str)

    # AC: VEHICLE_ID_NUM
    mapped["VEHICLE_ID_NUM"] = df.get("VEHICLE_ID_NUM", "").fillna("").astype(str)

    # AD: REGISTRATION_NUM
    mapped["REGISTRATION_NUM"] = df.get("REGISTRATION_NUM", "").fillna("").astype(str)

    # AE: CHASSIS_NUM
    mapped["CHASSIS_NUM"] = df.get("CHASSIS_NUM", "").fillna("").astype(str)

    # AF: ASSET_SHORT_DESCRIPTION
    mapped["ASSET_SHORT_DESCRIPTION"] = df.get("ASSET_SHORT_DESCRIPTION", "").fillna("").astype(str)

    # AG: MM_MAKE_DESCRIPTION — Manufacturer
    mapped["MM_MAKE_DESCRIPTION"] = df.get("Manufacturer", "").fillna("").astype(str)

    # AH: MM_MODEL_DESCRIPTION — Model
    mapped["MM_MODEL_DESCRIPTION"] = df.get("Model", "").fillna("").astype(str)

    # AI: DATE_FIRST_LICENSED
    mapped["DATE_FIRST_LICENSED"] = df.get("Date First Licensed", "").fillna("").astype(str)

    # AJ: OPEN_DATE
    mapped["OPEN_DATE"] = df.get("Open Date", "").fillna("").astype(str)

    # AK: DATE_EXPIRY
    mapped["DATE_EXPIRY"] = df.get("Expiry Date", "").fillna("").astype(str)

    # AL: CUST_TYPE_DESC
    mapped["CUST_TYPE_DESC"] = df.get("CUST_TYPE_DESC", "").fillna("").astype(str)

    # AM: snapshot_date
    mapped["snapshot_date"] = df.get("Snap Date", "").fillna("").astype(str)

    # Filter to last 14 days only
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    mapped["call_date"] = pd.to_datetime(mapped["call_date"], utc=True, errors="coerce")
    before_filter = len(mapped)
    mapped = mapped[mapped["call_date"] >= cutoff].copy()
    logger.info("Rows in last " + str(LOOKBACK_DAYS) + " days: " + str(len(mapped)) + " (filtered from " + str(before_filter) + ")")

    return mapped[SHEET_HEADERS]


def get_sheets_service():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS not set")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SHEET_SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_existing_accounts(service):
    """Get all existing WesBank account numbers from column V."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_TAB + "!V:V"
    ).execute()

    values = result.get("values", [])
    if not values or len(values) < 2:
        return set()

    existing = set()
    for row in values[1:]:  # skip header
        if row and row[0]:
            val = str(row[0]).strip()
            # Remove .0 suffix if present (Google Sheets stores numbers as floats)
            if val.endswith(".0"):
                val = val[:-2]
            val = val.strip()
            if val and val != "account number":
                existing.add(val)

    logger.info("Existing account numbers in sheet: " + str(len(existing)))
    if existing:
        sample = list(existing)[:3]
        logger.info("Sample existing accounts: " + str(sample))
    return existing


def append_new_rows(service, df):
    if df.empty:
        logger.info("No new rows to append")
        return 0

    rows = []
    for _, row in df.iterrows():
        formatted = []
        for val in row:
            try:
                if pd.isna(val):
                    formatted.append("")
                    continue
            except (TypeError, ValueError):
                pass
            if hasattr(val, 'strftime'):
                formatted.append(val.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                s = str(val) if val is not None else ""
                formatted.append("" if s in ["nan", "NaT", "None"] else s)
        rows.append(formatted)

    body = {"values": rows}
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=SHEET_TAB + "!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

    logger.info("Appended " + str(len(rows)) + " new rows")
    return len(rows)


def send_email(new_count, total_file_rows, filtered_rows, filename):
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT]):
        logger.warning("Email credentials not set — skipping email")
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECIPIENT
    msg["Subject"] = "VW/Audi Sales Sync — " + str(new_count) + " new rows added"

    body = "<html><body style='font-family:Arial,sans-serif;max-width:500px;'>"
    body += "<div style='background:#1F3864;padding:20px;border-radius:8px 8px 0 0;'>"
    body += "<h2 style='color:white;margin:0;'>VW/Audi Sales Dashboard Updated</h2></div>"
    body += "<div style='padding:20px;background:#EBF3FB;'><table width='100%' cellpadding='8'>"
    body += "<tr><td>Source file</td><td align='right'><b>" + filename + "</b></td></tr>"
    body += "<tr><td>Total rows in file</td><td align='right'><b>" + str(total_file_rows) + "</b></td></tr>"
    body += "<tr><td>Rows in last 14 days</td><td align='right'><b>" + str(filtered_rows) + "</b></td></tr>"
    body += "<tr><td>New rows added</td><td align='right'><b style='color:#375623;'>" + str(new_count) + "</b></td></tr>"
    body += "</table>"
    body += "<p style='font-size:12px;color:#595959;'><a href='https://docs.google.com/spreadsheets/d/" + SHEET_ID + "'>Open Google Sheet</a></p>"
    body += "</div></body></html>"

    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
    logger.info("Email sent to " + EMAIL_RECIPIENT)


def main():
    run_date = datetime.now()
    logger.info("=" * 60)
    logger.info("VW/AUDI SALES SYNC")
    logger.info("Date: " + run_date.strftime("%d %B %Y %H:%M"))
    logger.info("Lookback: last " + str(LOOKBACK_DAYS) + " days")
    logger.info("=" * 60)

    # Step 1: Download latest file from SFTP
    ssh, sftp = get_sftp_client()
    try:
        folder, filename = find_latest_file(sftp)
        buf = download_file(sftp, folder, filename)
    finally:
        sftp.close()
        ssh.close()

    # Step 2: Parse and map columns, filter to last 14 days
    df_mapped = parse_and_map(buf)
    logger.info("Mapped rows after date filter: " + str(len(df_mapped)))

    # Step 3: Get existing account numbers from sheet
    service = get_sheets_service()
    existing_accounts = get_existing_accounts(service)

    # Step 4: Filter to new rows only (dedupe by WesBank account number)
    df_new = df_mapped[
        ~df_mapped["account number"].astype(str).str.strip().isin(existing_accounts)
    ].copy()

    logger.info("New rows to add: " + str(len(df_new)))
    logger.info("Already in sheet: " + str(len(df_mapped) - len(df_new)))

    # Step 5: Append
    added = append_new_rows(service, df_new)

    # Step 6: Email
    send_email(added, 0, len(df_mapped), filename)

    logger.info("=" * 60)
    logger.info("Done. Added " + str(added) + " new rows.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
