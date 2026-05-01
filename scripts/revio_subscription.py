#!/usr/bin/env python3
"""
scripts/revio_subscription.py

Revio API wrappers for the ACCOUNT EXPIRY → debit-order conversion flow.

Three responsibilities:
  1. Pure helpers:  compute_next_debit_date, build_full_name, build_street_address.
  2. Revio reads:   _load_bt_client_map (template name → template_id).
  3. Revio writes:  create_client (POST /clients/),
                    add_subscriber  (POST /billing_templates/{id}/add_subscriber/).

Endpoint shapes are per the official docs JD shared on 2026-05-01:
  POST /clients/                                   → returns Client {id, ...}
  POST /billing_templates/{id}/add_subscriber/     → returns BillingTemplateClient

Imported by scripts/convert_account_expiry.py. Not run directly.

Run-locally example (smoke test of helpers only — no network):
    python3 -c "
    from scripts.revio_subscription import compute_next_debit_date
    from datetime import date
    print(compute_next_debit_date('2026/05/21', date(2026, 5, 1)))
    "
"""

import calendar
import logging
import os
import time
from datetime import date, datetime, timedelta

import requests

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
REVIO_API_BASE_URL = os.environ.get(
    "REVIO_API_BASE_URL", "https://gate.reviopay.com/api/v1"
)
REVIO_API_KEY = os.environ.get("REVIO_API_KEY", "")

# ALLRHLP / ALLRHFM → (env var holding the Revio billing-template UUID,
# monthly price in ZAR). Template IDs come from per-environment GitHub
# secrets — JD pulled the exact UUIDs from the Revio portal so we no
# longer enumerate /billing_templates/ at runtime (which was both
# fragile across template-name suffix drift and slow).
PRODUCT_CONFIG = {
    "ALLRHLP": ("REVIO_TEMPLATE_VW_SINGLE_ID", 89),
    "ALLRHFM": ("REVIO_TEMPLATE_VW_FAMILY_ID", 159),
}


def _brand_id():
    """Revio docs require brand_id on POST /clients/ and on
    POST /billing_templates/{id}/add_subscriber/. Read at call-time so
    tests can patch via os.environ."""
    return os.environ.get("REVIO_BRAND_ID", "")

DAY_28_FALLBACK = 28

# Retry policy for transient failures. Per spec:
#   - Max 3 attempts
#   - Backoff: 5s before attempt 2, 30s before attempt 3
#     (a third 120s value is documented for future tuning to 4 attempts;
#     unused at MAX_RETRY_ATTEMPTS=3)
#   - Retry on: connection errors, 5xx, 408 (timeout), 429 (rate limit)
#   - Do NOT retry on: 400 (validation), 401 (auth), 403 (forbidden),
#     404 (not found) — these are persistent and would just consume time.
MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFFS_SECONDS = (5, 30, 120)
NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404}
RETRYABLE_STATUS_CODES = {408, 429}  # plus all 5xx


# ─── Pure helpers (unit-testable, no network) ────────────────────────────────
def _extract_day_of_month(debit_order_date_str):
    """SALES col X is a date string like '2026/05/21'. Return the day component
    (int 1..31) or None if the cell is blank / unparseable. JD notes blank
    is rare (~1 in 20,000)."""
    s = str(debit_order_date_str or "").strip()
    if not s:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).day
        except ValueError:
            continue
    # Bare integer day-of-month (defensive — spec wording suggested this shape)
    try:
        n = int(float(s))
        if 1 <= n <= 31:
            return n
    except (TypeError, ValueError):
        pass
    return None


def _clamp_day_to_month(year, month, day):
    """Clamp `day` to the last valid day of (year, month). Always-future-safe
    for short months: April 31 → April 30, Feb 30 → Feb 28/29."""
    last = calendar.monthrange(year, month)[1]
    return min(day, last)


def compute_next_debit_date(debit_order_date_str, today):
    """Return ISO 8601 YYYY-MM-DD string for the next debit.

    Rule:
      day = day-of-month from SALES col X (fallback 28 if blank/invalid)
      if today.day < day → this month, else next month
      always future-dated; never today, never past

    today must be a date — caller passes date.today() in production.
    """
    day = _extract_day_of_month(debit_order_date_str)
    if day is None:
        day = DAY_28_FALLBACK

    if today.day < day:
        target_year, target_month = today.year, today.month
    else:
        if today.month == 12:
            target_year, target_month = today.year + 1, 1
        else:
            target_year, target_month = today.year, today.month + 1

    target_day = _clamp_day_to_month(target_year, target_month, day)
    return date(target_year, target_month, target_day).isoformat()


def build_full_name(first_name, surname):
    """Concat with single space; trim. Falls back to whichever side is present."""
    parts = [str(first_name or "").strip(), str(surname or "").strip()]
    return " ".join(p for p in parts if p)[:128]


def build_street_address(line1, line2):
    """Single-string address per Revio's max-128 string field. Drops blank parts."""
    parts = [str(line1 or "").strip(), str(line2 or "").strip()]
    joined = ", ".join(p for p in parts if p)
    return joined[:128]


def normalise_phone(raw):
    """Coerce ZA mobile to 11-digit '27XXXXXXXXX'. Returns '' if unrecoverable."""
    if raw is None:
        return ""
    if isinstance(raw, float) and raw.is_integer():
        raw = int(raw)
    s = str(raw).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(c for c in s if c.isdigit())
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("27"):
        return digits
    if len(digits) == 10 and digits.startswith("0"):
        return "27" + digits[1:]
    if len(digits) == 9:
        return "27" + digits
    return digits  # let Revio reject if truly malformed; we logged the raw


def build_client_payload(sales_row, personal_code):
    """Map a SALES-row dict (header-keyed) → POST /clients/ payload.

    Caller is responsible for sales_row keys matching the live SALES headers.
    `brand_id` is sourced from REVIO_BRAND_ID env var (required by Revio docs).
    """
    city = (sales_row.get("Physical City") or "").strip() \
        or (sales_row.get("Physical Suburb") or "").strip()
    return {
        "email":          (sales_row.get("Email Address (VW/Audi Campaign 1)") or "").strip()[:254],
        "full_name":      build_full_name(sales_row.get("FirstName"), sales_row.get("Surname")),
        "personal_code":  personal_code[:32],
        "bank_account":   str(sales_row.get("Bank Account Number (VW/Audi)") or "").strip()[:34],
        "bank_code":      str(sales_row.get("Branch Code (VW/Audi Campaign 1)") or "").strip()[:11],
        "phone":          normalise_phone(sales_row.get("Mobile Number (VW/Audi Campaign 1)"))[:32],
        "city":           city[:128],
        "country":        "ZA",
        "zip_code":       str(sales_row.get("Physical Post Code") or "").strip()[:32],
        "street_address": build_street_address(
            sales_row.get("Physical Line1"), sales_row.get("Physical Line2")
        ),
        "brand_id":       _brand_id(),
    }


# ─── Revio API ──────────────────────────────────────────────────────────────
def _get_headers():
    if not REVIO_API_KEY:
        raise RuntimeError("REVIO_API_KEY is not set")
    return {
        "Authorization": "Bearer " + REVIO_API_KEY,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _is_retryable_response(response):
    sc = response.status_code
    if sc in NON_RETRYABLE_STATUS_CODES:
        return False
    if 200 <= sc < 300:
        return False
    if sc >= 500:
        return True
    if sc in RETRYABLE_STATUS_CODES:
        return True
    return False


def _do_request_with_retry(method, url, **kwargs):
    """Wrap requests.<method> in the retry policy.

    Returns the final Response. Raises the last network exception only if
    every attempt died at the transport layer. A non-2xx Response with a
    non-retryable status code is returned immediately so the caller can
    handle it (e.g. surface as an ERROR row).

    `time.sleep` is used between attempts and is mockable in tests.
    """
    last_exc = None
    last_response = None
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            r = getattr(requests, method)(url, **kwargs)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            last_response = None
            logger.warning("Network error on %s %s (attempt %d/%d): %s",
                           method.upper(), url, attempt, MAX_RETRY_ATTEMPTS, e)
            retryable = True
        else:
            last_exc = None
            last_response = r
            retryable = _is_retryable_response(r)
            if not retryable:
                return r
            logger.warning(
                "%s %s returned %d (attempt %d/%d): %s",
                method.upper(), url, r.status_code,
                attempt, MAX_RETRY_ATTEMPTS, (r.text or "")[:200],
            )
        if attempt < MAX_RETRY_ATTEMPTS:
            wait = RETRY_BACKOFFS_SECONDS[attempt - 1]
            time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return last_response


def find_client_by_personal_code(personal_code):
    """GET /clients/?personal_code=… — returns the existing client_id if any,
    else None. Used to make POST /clients/ idempotent across partial failures.

    A non-200 lookup returns None (not an error) so that a flaky GET doesn't
    block the actual conversion — the subsequent POST will either succeed or
    surface its own error.
    """
    if not personal_code:
        return None
    r = _do_request_with_retry(
        "get",
        REVIO_API_BASE_URL + "/clients/",
        headers=_get_headers(),
        params={"personal_code": personal_code},
        timeout=30,
    )
    if r is None or r.status_code != 200:
        sc = r.status_code if r is not None else "transport-error"
        logger.warning("Personal-code lookup for %r returned %s — proceeding "
                       "to create", personal_code, sc)
        return None
    data = r.json()
    results = data.get("results", data) if isinstance(data, dict) else data
    for c in results or []:
        if c.get("personal_code") == personal_code:
            return c.get("id")
    return None


def is_already_subscriber(template_id, client_id):
    """GET /billing_templates/{template_id}/clients/ paginated — True iff
    client_id is already a subscriber. Used to make add_subscriber idempotent.
    """
    if not client_id:
        return False
    url = REVIO_API_BASE_URL + f"/billing_templates/{template_id}/clients/"
    while url:
        r = _do_request_with_retry(
            "get", url, headers=_get_headers(), timeout=30,
        )
        if r is None or r.status_code != 200:
            sc = r.status_code if r is not None else "transport-error"
            logger.warning("Subscriber lookup on template %s returned %s — "
                           "proceeding to add", template_id, sc)
            return False
        data = r.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        for tc in results or []:
            cid = tc.get("client_id") or tc.get("id")
            if cid == client_id:
                return True
        url = data.get("next") if isinstance(data, dict) else None
    return False


def resolve_template_id(product_code):
    """ALLRHLP / ALLRHFM → Revio billing-template UUID from env var.

    Replaces the old "GET /billing_templates/ + match by name" pattern,
    which was fragile across template-name suffix drift. JD pulled the
    exact UUIDs from the Revio portal; they live in GitHub secrets.
    """
    cfg = PRODUCT_CONFIG.get(product_code)
    if not cfg:
        raise RuntimeError(f"Unknown product code: {product_code!r}")
    env_var, _price = cfg
    tid = os.environ.get(env_var, "")
    if not tid:
        raise RuntimeError(
            f"{env_var} is not set — required to resolve Revio billing "
            f"template for product {product_code!r}"
        )
    return tid


def create_client(client_data, dry_run=False):
    """Idempotent client creation.

    1. Pre-check: GET /clients/?personal_code=… — if a client already exists
       with this personal_code, return its id without POSTing. This handles
       the "partial-failure re-run" case where create_client succeeded
       previously but add_subscriber failed.
    2. POST /clients/ with retry on transient errors (5xx / 408 / 429 /
       network). 400/401/403/404 are non-retryable.

    Returns Revio client_id (uuid) or None in dry_run.
    """
    if dry_run:
        logger.info("[DRY RUN] POST /clients/ payload=%s", client_data)
        return None

    personal_code = client_data.get("personal_code")
    existing_id = find_client_by_personal_code(personal_code)
    if existing_id:
        logger.info("Client already exists for personal_code=%s — reusing "
                    "id=%s, skipping create", personal_code, existing_id)
        return existing_id

    r = _do_request_with_retry(
        "post",
        REVIO_API_BASE_URL + "/clients/",
        headers=_get_headers(),
        json=client_data,
        timeout=30,
    )
    if r is None or r.status_code not in (200, 201):
        sc = r.status_code if r is not None else "transport-error"
        body = (r.text if r is not None else "(no response)")[:400]
        raise RuntimeError(
            f"POST /clients/ failed after retries: {sc} {body} "
            f"payload={client_data}"
        )
    body = r.json()
    cid = body.get("id")
    if not cid:
        raise RuntimeError(f"POST /clients/ returned no id: {body}")
    logger.info("Created Revio client: id=%s personal_code=%s",
                cid, personal_code)
    return cid


def add_subscriber(template_id, client_id, scheduled_date,
                   invoice_reference, dry_run=False):
    """Idempotent subscriber addition.

    1. Pre-check: GET /billing_templates/{id}/clients/ — if our client_id is
       already on the template, skip POST. This handles the "create succeeded
       but add_subscriber failed last time" case.
    2. POST add_subscriber with retry on transient errors.
    """
    payload = {
        "client_id":                            client_id,
        "subscription_billing_scheduled_on":    scheduled_date,  # YYYY-MM-DD
        "invoice_reference":                    invoice_reference,
        "send_invoice_on_add_subscriber":       True,
        "brand_id":                             _brand_id(),
    }
    if dry_run:
        logger.info("[DRY RUN] POST /billing_templates/%s/add_subscriber/ payload=%s",
                    template_id, payload)
        return None

    if is_already_subscriber(template_id, client_id):
        logger.info("Client %s is already a subscriber on template %s — "
                    "skipping add", client_id, template_id)
        return {"status": "already_subscriber", "client_id": client_id,
                "template_id": template_id}

    url = REVIO_API_BASE_URL + f"/billing_templates/{template_id}/add_subscriber/"
    r = _do_request_with_retry(
        "post", url, headers=_get_headers(), json=payload, timeout=30,
    )
    if r is None or r.status_code not in (200, 201):
        sc = r.status_code if r is not None else "transport-error"
        body = (r.text if r is not None else "(no response)")[:400]
        raise RuntimeError(
            f"POST add_subscriber failed after retries: {sc} {body} "
            f"template_id={template_id} payload={payload}"
        )
    body = r.json()
    logger.info("Added subscriber: template_id=%s client_id=%s scheduled=%s",
                template_id, client_id, scheduled_date)
    return body
