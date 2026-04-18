#!/usr/bin/env python3
import os
import json
import logging
from datetime import date
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-10s %(message)s")
logger = logging.getLogger(__name__)

SHEET_ID   = "1nzDkzva7wZO0lDFBDctNQdqxvOU-uexyUkxmex6xGgs"
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]
DASH_TAB   = "DASHBOARD"
DATA_START = 10

MONTH_ABBR = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

def get_service():
    creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"]), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def last_month(override=None):
    if override:
        d = date.fromisoformat(override + "-01")
    else:
        d = date.today().replace(day=1) - relativedelta(months=1)
    return d.strftime("%Y-%m"), d

def find_row(service, target_month):
    res = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=f"{DASH_TAB}!A{DATA_START}:A50").execute()
    for i, row in enumerate(res.get("values", [])):
        if row and row[0] == target_month:
            return DATA_START + i
    return None

def get_sheet_id(service, tab_name):
    meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]
    return None

def build_formulas(r):
    p = r - 1
    cancel = (
        f"=COUNTIF(ARRAYFORMULA(IF(TEXT(MASTER_BOOK!C:C,\"yyyy-mm\")=$A{r},REGEXREPLACE(TO_TEXT(MASTER_BOOK!A:A),\"\\D\",\"\"),\"\")),\"*\")"
        f"-COUNTIF(ARRAYFORMULA(IF(TEXT(MASTER_BOOK!C:C,\"yyyy-mm\")=$A{r},IF(ISNUMBER(MATCH(REGEXREPLACE(TO_TEXT(MASTER_BOOK!A:A),\"\\D\",\"\"),REGEXREPLACE(TO_TEXT(CANCELLATIONS!G:G),\"\\D\",\"\"),0)),REGEXREPLACE(TO_TEXT(MASTER_BOOK!A:A),\"\\D\",\"\"),\"\")),\"\"))"
    )
    reject = (
        f"=COUNTIF(ARRAYFORMULA(IF(TEXT(MASTER_BOOK!C:C,\"yyyy-mm\")=$A{r},REGEXREPLACE(TO_TEXT(MASTER_BOOK!A:A),\"\\D\",\"\"),\"\")),\"*\")"
        f"-COUNTIF(ARRAYFORMULA(IF(TEXT(MASTER_BOOK!C:C,\"yyyy-mm\")=$A{r},IF(ISNUMBER(MATCH(REGEXREPLACE(TO_TEXT(MASTER_BOOK!A:A),\"\\D\",\"\"),FILTER(REGEXREPLACE(TO_TEXT(REJECTIONS!P:P),\"\\D\",\"\"),REJECTIONS!P:P<>\"\",LOWER(TO_TEXT(REJECTIONS!P:P))<>\"account_number\"),0)),REGEXREPLACE(TO_TEXT(MASTER_BOOK!A:A),\"\\D\",\"\"),\"\")),\"\"))"
    )
    return {
        2:  f"=COUNTIFS(MASTER_BOOK!D:D,$A{r})",
        3:  cancel,
        4:  reject,
        6:  f"=C{r}+D{r}+E{r}",
        7:  f"=SUMPRODUCT((TEXT(MASTER_BOOK!$D$2:$D,\"yyyy-mm\")=$A{r})*(UPPER(TRIM(MASTER_BOOK!$G$2:$G))=\"NO\")*(UPPER(TRIM(MASTER_BOOK!$H$2:$H))=\"NO\")*(UPPER(TRIM(MASTER_BOOK!$J$2:$J))=\"INDIVIDUAL\"))",
        8:  f"=SUMPRODUCT((TEXT(MASTER_BOOK!$D$2:$D,\"yyyy-mm\")=$A{r})*(UPPER(TRIM(MASTER_BOOK!$G$2:$G))=\"NO\")*(UPPER(TRIM(MASTER_BOOK!$H$2:$H))=\"NO\")*(UPPER(TRIM(MASTER_BOOK!$J$2:$J))=\"FAMILY\"))",
        9:  f"=G{r}+H{r}",
        10: f"=J{p}+I{r}",
        11: f"=G{r}*PRICING!B2",
        12: f"=H{r}*PRICING!B3",
        13: f"=M{p}+K{r}+L{r}",
        15: f"=K{r}*0.2067",
        16: f"=L{r}*0.2063",
        17: f"=O{r}+P{r}+Q{p}",
        19: f"=K{r}*$S$8",
        20: f"=L{r}*$T$8",
        21: f"=S{r}+T{r}",
    }

def write_formulas(service, dash_gid, row, formulas):
    requests = []
    for col, formula in formulas.items():
        requests.append({"updateCells": {"range": {"sheetId": dash_gid,"startRowIndex": row-1,"endRowIndex": row,"startColumnIndex": col-1,"endColumnIndex": col},"rows": [{"values": [{"userEnteredValue": {"formulaValue": formula}}]}],"fields": "userEnteredValue"}})
    service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": requests}).execute()
    logger.info(f"Wrote {len(requests)} formulas → DASHBOARD row {row}")

def create_invoice_tab(service, tab_name, dash_row):
    res = service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}).execute()
    new_gid = res["replies"][0]["addSheet"]["properties"]["sheetId"]
    logger.info(f"Created tab '{tab_name}'  gid={new_gid}")
    service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=f"'{tab_name}'!A1:E1", valueInputOption="USER_ENTERED", body={"values": [["Sale month","Account Number","Customer","Membership Type","Status"]]}).execute()
    formula = (f"=SORT(FILTER({{MASTER_BOOK!D:D,MASTER_BOOK!A:A,MASTER_BOOK!B:B,MASTER_BOOK!J:J,MASTER_BOOK!I:I}},TEXT(MASTER_BOOK!D:D,\"yyyy-mm\")<=DASHBOARD!$A{dash_row},UPPER(TRIM(MASTER_BOOK!G:G))=\"NO\",UPPER(TRIM(MASTER_BOOK!H:H))=\"NO\"),1,TRUE)")
    service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=f"'{tab_name}'!A2", valueInputOption="USER_ENTERED", body={"values": [[formula]]}).execute()
    logger.info(f"Formula written to '{tab_name}'!A2")
    return new_gid

def write_drilldown(service, dash_gid, row, new_gid, tab_label):
    hyperlink = f'=HYPERLINK("#gid={new_gid}","🔍 SHEET {tab_label}")'
    service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": [{"updateCells": {"range": {"sheetId": dash_gid,"startRowIndex": row-1,"endRowIndex": row,"startColumnIndex": 23,"endColumnIndex": 24},"rows": [{"values": [{"userEnteredValue": {"formulaValue": hyperlink}}]}],"fields": "userEnteredValue"}}]}).execute()
    logger.info(f"Drilldown written → row {row}  label='{tab_label}'")

def main():
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    logger.info("=" * 60)
    logger.info("VW MONTHLY INVOICE SYNC")
    logger.info(f"Run date : {date.today()}")
    logger.info(f"Dry run  : {dry_run}")
    logger.info("=" * 60)
    service = get_service()
    target_month_override = os.environ.get("TARGET_MONTH") or None
    month_str, month_date = last_month(target_month_override)
    logger.info(f"Processing : {month_str}")
    row = find_row(service, month_str)
    if not row:
        logger.error(f"Row for {month_str} not found in DASHBOARD — aborting")
        return
    logger.info(f"DASHBOARD row : {row}")
    invoice_date = month_date + relativedelta(months=6)
    abbr     = MONTH_ABBR[invoice_date.month]
    yr       = str(invoice_date.year)[2:]
    tab_name  = f"{abbr}'{yr} Invoice Data"
    tab_label = f"{abbr}'{yr} Data"
    logger.info(f"Invoice tab : {tab_name}")
    if dry_run:
        formulas = build_formulas(row)
        logger.info(f"DRY RUN — would write {len(formulas)} formulas to row {row}")
        logger.info(f"DRY RUN — would create tab '{tab_name}'")
        logger.info(f"DRY RUN — would link Drilldown in col X row {row} → '{tab_label}'")
        return
    dash_gid = get_sheet_id(service, DASH_TAB)

    # Skip if row already has data (e.g. manually filled)
    existing = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{DASH_TAB}!B{row}:B{row}"
    ).execute()
    if existing.get("values"):
        logger.info(f"Row {row} already has data — skipping formula write")
    else:
        formulas = build_formulas(row)
        write_formulas(service, dash_gid, row, formulas)

    if get_sheet_id(service, tab_name):
        logger.warning(f"Tab '{tab_name}' already exists — skipping creation")
        new_gid = get_sheet_id(service, tab_name)
    else:
        new_gid = create_invoice_tab(service, tab_name, row)

    write_drilldown(service, dash_gid, row, new_gid, tab_label)
    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
