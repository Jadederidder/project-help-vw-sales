#!/usr/bin/env python3
# ============================================================
# scripts/sync_sales_to_sheets.py
# Runs every Monday at 5pm
# 1. Connects to SFTP and downloads latest VW_Audi EOD file
# 2. Reads all rows from the file
# 3. Compares against existing rows in Google Sheet SALES tab
# 4. Appends only NEW rows (based on Policy Number)
# 5. Emails confirmation report
# ============================================================

import os
import sys
import logging
import smtplib
import io
import json
from datetime import datetime
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
SFTP_USER     = "Projecthelp"
SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD", "")

SHEET_ID      = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SHEET_TAB     = "SALES"
SHEET_SCOPES  = ["https://www.googleapis.com/auth/spreadsheets"]

EMAIL_SENDER    = os.environ.get("EMAIL_SENDER", "")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "")

SHEET_HEADERS = [
    "call_date", "status", "user", "full_name", "brand", "phone_number",
    "title", "first_name", "middle_initial", "last_name",
    "address1", "address2", "address3", "city", "state", "province",
    "postal_code", "alt_phone", "email", "lead_id", "status_name",
    "account number", "CUSTOMER_NUMBER", "CUST_TYPE_CODE",
    "IDENTITY_OR_REG_NUM", "BANK_NAME", "BANK_ACC_NUM", "BANK_BRANCH_CODE",
    "VEHICLE_ID_NUM", "REGISTRATION_NUM", "CHASSIS_NUM",
    "ASSET_SHORT_DESCRIPTION", "MM_MAKE_DESCRIPTION", "MM_MODEL_DESCRIPTION",
    "DATE_FIRST_LICENSED", "OPEN_DATE", "DATE_EXPIRY",
    "CUST_TYPE_DESC", "snapshot_date"
]


def get_sftp_client():
    logger.info("Connecting to SFTP...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD)
    sftp = ssh.open_sftp()
    logger.info("SFTP connected")
    return ssh, sftp


def find_sales_folder(sftp):
    """Find the VW & Audi Sales folder by listing directories."""
    logger.info("Exploring SFTP structure...")

    # List root
    root_items = sftp.listdir(".")
    logger.info("Root items: " + str(root_items))

    # Try ProjectHelp folder
    for root_item in root_items:
        try:
            sub_items = sftp.listdir(root_item)
            logger.info(root_item + "/: " + str(sub_items))
            for sub_item in sub_items:
                path = root_item + "/" + sub_item
                try:
                    files = sftp.listdir(path)
                    logger.info(path + "/: " + str(files))
                    # Check if this folder has VW_Audi files
                    xlsx_files = [f for f in files if f.endswith(".xlsx") and ("VW_Audi" in f or "VW Audi" in f)]
                    if xlsx_files:
                        logger.info("Found sales files in: " + path)
                        return path, xlsx_files
                except Exception:
                    pass
        except Exception:
            pass

    return None, []


def get_latest_file(sftp):
    folder, files = find_sales_folder(sftp)
    if not folder:
        raise RuntimeError("Could not find VW_Audi sales folder on SFTP")

    files.sort()
    latest = files[-1]
    logger.info("Latest file: " + latest)
    return folder, latest


def download_file(sftp, folder, filename):
    remote_path = folder + "/" + filename
    logger.info("Downloading: " + remote_path)
    buf = io.BytesIO()
    sftp.getfo(remote_path, buf)
    buf.seek(0)
    return buf


def parse_sftp_file(buf):
    df = pd.read_excel(buf)
    logger.info("SFTP file rows: " + str(len(df)))
    return df


def map_to_sheet_columns(df):
    col_map = {
        "Created Time (VW/Audi Campaign 1)": "call_date",
        "Stage": "status",
        "Created By (VW/Audi Campaign 1)": "user",
        "FirstName": "first_name",
        "Surname": "last_name",
        "Title (VW/Audi Campaign 1)": "title",
        "VW/Audi Product": "brand",
        "Mobile Number (VW/Audi Campaign 1)": "phone_number",
        "Email Address (VW/Audi Campaign 1)": "email",
        "Physical Line1": "address1",
        "Physical Line2": "address2",
        "Physical Suburb": "address3",
        "Physical City": "city",
        "Physical Province": "province",
        "Physical Post Code": "postal_code",
        "VICI Lead ID": "lead_id",
        "Policy Number": "account number",
        "Customer Number": "CUSTOMER_NUMBER",
        "CUST_TYPE_CODE": "CUST_TYPE_CODE",
        "ID Number": "IDENTITY_OR_REG_NUM",
        "Bank": "BANK_NAME",
        "Bank Account Number (VW/Audi)": "BANK_ACC_NUM",
        "Branch Code (VW/Audi Campaign 1)": "BANK_BRANCH_CODE",
        "VEHICLE_ID_NUM": "VEHICLE_ID_NUM",
        "REGISTRATION_NUM": "REGISTRATION_NUM",
        "CHASSIS_NUM": "CHASSIS_NUM",
        "ASSET_SHORT_DESCRIPTION": "ASSET_SHORT_DESCRIPTION",
        "Manufacturer": "MM_MAKE_DESCRIPTION",
        "Model": "MM_MODEL_DESCRIPTION",
        "Date First Licensed": "DATE_FIRST_LICENSED",
        "Open Date": "OPEN_DATE",
        "Expiry Date": "DATE_EXPIRY",
        "CUST_TYPE_DESC": "CUST_TYPE_DESC",
        "Snap Date": "snapshot_date",
    }

    mapped = pd.DataFrame()
    for src_col, dst_col in col_map.items():
        if src_col in df.columns:
            mapped[dst_col] = df[src_col]

    if "first_name" in mapped.columns and "last_name" in mapped.columns:
        mapped["full_name"] = (
            mapped["first_name"].fillna("") + " " + mapped["last_name"].fillna("")
        ).str.strip()

    for h in SHEET_HEADERS:
        if h not in mapped.columns:
            mapped[h] = ""

    return mapped[SHEET_HEADERS]


def get_sheets_service():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "")
    if not creds_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS secret not set")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SHEET_SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_existing_policy_numbers(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_TAB + "!A:AO"
    ).execute()

    values = result.get("values", [])
    if not values or len(values) < 2:
        logger.info("Sheet is empty or has only headers")
        return set(), 0

    headers = values[0]
    logger.info("Sheet has " + str(len(values) - 1) + " existing rows")

    try:
        acct_col = headers.index("account number")
        existing = set()
        for row in values[1:]:
            if len(row) > acct_col and row[acct_col]:
                existing.add(str(row[acct_col]).strip())
        logger.info("Existing policy numbers: " + str(len(existing)))
        return existing, len(values) - 1
    except ValueError:
        logger.warning("account number column not found — will add all rows")
        return set(), 0


def append_new_rows(service, new_rows_df):
    if new_rows_df.empty:
        logger.info("No new rows to append")
        return 0

    rows = []
    for _, row in new_rows_df.iterrows():
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
                formatted.append(str(val) if val is not None else "")
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


def send_email(new_count, total_count, filename):
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT]):
        logger.warning("Email credentials not set")
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
    body += "<tr><td>Total rows in file</td><td align='right'><b>" + str(total_count) + "</b></td></tr>"
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
    logger.info("=" * 60)

    ssh, sftp = get_sftp_client()
    try:
        folder, filename = get_latest_file(sftp)
        buf = download_file(sftp, folder, filename)
    finally:
        sftp.close()
        ssh.close()

    df_raw = parse_sftp_file(buf)
    df_mapped = map_to_sheet_columns(df_raw)

    service = get_sheets_service()
    existing_policies, existing_count = get_existing_policy_numbers(service)

    df_new = df_mapped[
        ~df_mapped["account number"].astype(str).str.strip().isin(existing_policies)
    ].copy()

    logger.info("New rows to add: " + str(len(df_new)))
    logger.info("Already in sheet: " + str(len(df_mapped) - len(df_new)))

    added = append_new_rows(service, df_new)
    send_email(added, len(df_raw), filename)

    logger.info("=" * 60)
    logger.info("Done. Added " + str(added) + " new rows.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
