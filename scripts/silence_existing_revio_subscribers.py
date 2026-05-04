#!/usr/bin/env python3
"""
scripts/silence_existing_revio_subscribers.py

One-off backfill — silences customer-facing comms on every existing
BillingTemplateClient (BTC) across every billing template the API key
has access to. Mirrors the new add_subscriber default in
scripts/revio_subscription.py (master doc §4.2):

    send_invoice_on_add_subscriber  → false
    send_invoice_on_charge_failure  → false
    send_receipt                    → false

WHY: Revio's API defaults send a welcome invoice on add, a failure
invoice on declined debits, and a monthly receipt on each successful
debit. Combined those drive monthly emails to subscribers reminding
them money is leaving their account, which the SA market reads as a
cancellation cue. We want all three OFF by default. This script
backfills the change for every subscriber added before the contract
flip in revio_subscription.py.

WHAT IT DOES:
  1. GET /billing_templates/?is_subscription=true — paginate fully.
     Optionally scoped to one brand via REVIO_BRAND_ID.
  2. For each template: GET /billing_templates/{id}/clients/ — paginate.
  3. For each BTC where any of the 3 flags ≠ false:
       PATCH /billing_templates/{tid}/clients/{btc_id}/
       form-data: send_invoice_on_add_subscriber=false,
                  send_invoice_on_charge_failure=false,
                  send_receipt=false
     A BTC already silent is skipped (idempotent — no wasted call).
  4. Sends a RunSummary email at the end (same template as
     cleanup_rejections_duplicates.py).

ENV:
  REVIO_API_KEY              required
  REVIO_BRAND_ID             optional — if set, scope to this brand only
  EMAIL_SENDER               Gmail sender for the summary email
  EMAIL_PASSWORD             Gmail app password
  EMAIL_RECIPIENT            comma-separated recipient list (production only)
  DRY_RUN                    "true" (default) → no PATCHes, email JD only
"""

import logging
import os
import smtplib
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from email_template import RunSummary, build_run_summary_email  # noqa: E402
from revio_subscription import (  # noqa: E402
    REVIO_API_BASE_URL,
    REVIO_API_KEY,
    _do_request_with_retry,
    _get_headers,
)

logger = logging.getLogger(__name__)

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"
DRY_RUN_RECIPIENT = "jd@projecthelp.co.za"

# Three flags we want set to False on every active BTC. Order matches the
# master-doc §4.2 ordering for log readability.
COMM_FLAGS = (
    "send_invoice_on_add_subscriber",
    "send_invoice_on_charge_failure",
    "send_receipt",
)

# The PATCH payload is locked to exactly these three keys — defensive
# belt-and-braces against any future code path accidentally injecting a
# `status` field or anything else that could mutate subscriber state.
# patch_client_silent() asserts on this set before each PATCH call.
ALLOWED_PATCH_FIELDS = frozenset(COMM_FLAGS)

# Per Revio API spec, BillingTemplateClient.status has four documented
# values. We patch only "active" — see master doc §4.2 / PR #26 follow-up:
# patching paused clients risks unintended side-effects (e.g. resuming a
# paused debit). inactive + pending are read-only per API.
ACTIVE_STATUS = "active"
KNOWN_INACTIVE_STATUSES = frozenset({
    "subscription_paused",
    "inactive",
    "pending",
})


# ─── Pure: identify which clients need patching ──────────────────────────────
def identify_clients_to_patch(clients):
    """Pure function. Bucket every BTC by what we should do with it.

    Returns a dict with six lists of {"client": <full BTC dict>,
    "non_silent_flags": {flag: value}}:

      to_patch                  — status=active AND ≥1 comm flag not False
      skipped_already_silent    — status=active AND all 3 comm flags already
                                  False (idempotent skip — no wasted PATCH)
      skipped_paused            — status=subscription_paused (any flag state)
      skipped_inactive          — status=inactive             (any flag state)
      skipped_pending           — status=pending              (any flag state)
      skipped_unknown_status    — anything else, including missing/None status

    Why we restrict to active: PR #26 dry-run surfaced that the raw BTC
    count (8,071) was inflated by paused/inactive subscribers across
    OLD BILLING templates. JD's call: patching a paused client risks
    unintended re-activation if a flag flip is interpreted as a state
    change downstream. Active subscribers are the only ones whose comm
    flags fire in production anyway, so the others are pure noise to
    touch.

    Strictly tests `is not False` for flag values — missing keys / truthy
    non-bool values are flagged for patching. The status filter is exact
    string equality (case-sensitive) per Revio API spec.
    """
    buckets = {
        "to_patch":                 [],
        "skipped_already_silent":   [],
        "skipped_paused":           [],
        "skipped_inactive":         [],
        "skipped_pending":          [],
        "skipped_unknown_status":   [],
    }

    for c in clients:
        non_silent = {f: c.get(f) for f in COMM_FLAGS if c.get(f) is not False}
        entry = {"client": c, "non_silent_flags": non_silent}
        status = c.get("status")

        if status == ACTIVE_STATUS:
            if non_silent:
                buckets["to_patch"].append(entry)
            else:
                buckets["skipped_already_silent"].append(entry)
        elif status == "subscription_paused":
            buckets["skipped_paused"].append(entry)
        elif status == "inactive":
            buckets["skipped_inactive"].append(entry)
        elif status == "pending":
            buckets["skipped_pending"].append(entry)
        else:
            # Unknown status: anything not in the four documented values,
            # including None / missing. Logged + counted but never patched.
            buckets["skipped_unknown_status"].append(entry)

    return buckets


# ─── Revio reads ─────────────────────────────────────────────────────────────
def list_subscription_templates():
    """GET /billing_templates/?is_subscription=true — paginated. Returns a
    list of template dicts (each has at least id + title). Optionally scoped
    by REVIO_BRAND_ID."""
    params = {"is_subscription": "true"}
    brand_id = os.environ.get("REVIO_BRAND_ID", "")
    if brand_id:
        params["brand_id"] = brand_id

    url = REVIO_API_BASE_URL + "/billing_templates/"
    out = []
    while url:
        # `params` only on the first call; pagination URL already has them.
        r = _do_request_with_retry(
            "get", url, headers=_get_headers(),
            params=params if url == REVIO_API_BASE_URL + "/billing_templates/" else None,
            timeout=30,
        )
        if r is None or r.status_code != 200:
            sc = r.status_code if r is not None else "transport-error"
            body = (r.text if r is not None else "")[:300]
            raise RuntimeError(
                f"GET /billing_templates/ failed: {sc} {body}"
            )
        data = r.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        out.extend(results or [])
        url = data.get("next") if isinstance(data, dict) else None
    return out


def list_clients_for_template(template_id):
    """GET /billing_templates/{id}/clients/ — paginated. Returns list of
    BillingTemplateClient dicts."""
    url = REVIO_API_BASE_URL + f"/billing_templates/{template_id}/clients/"
    out = []
    while url:
        r = _do_request_with_retry(
            "get", url, headers=_get_headers(), timeout=30,
        )
        if r is None or r.status_code != 200:
            sc = r.status_code if r is not None else "transport-error"
            body = (r.text if r is not None else "")[:300]
            raise RuntimeError(
                f"GET /billing_templates/{template_id}/clients/ failed: "
                f"{sc} {body}"
            )
        data = r.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        out.extend(results or [])
        url = data.get("next") if isinstance(data, dict) else None
    return out


# ─── Revio write ─────────────────────────────────────────────────────────────
def _get_form_headers():
    """Auth headers without Content-Type — requests will set
    application/x-www-form-urlencoded automatically when `data=` is passed.
    Mirrors the auth pattern from _get_headers but for form-data PATCH."""
    if not REVIO_API_KEY:
        raise RuntimeError("REVIO_API_KEY is not set")
    return {
        "Authorization": "Bearer " + REVIO_API_KEY,
        "Accept":        "application/json",
    }


def _assert_patch_payload_safe(payload):
    """Pre-flight: PATCH payload must contain ONLY the three comm-flag
    keys. Belt-and-braces against any future code path accidentally
    injecting a `status` field or any other key that would mutate
    subscriber state. Raises RuntimeError on violation."""
    extra = set(payload.keys()) - ALLOWED_PATCH_FIELDS
    if extra:
        raise RuntimeError(
            f"PATCH payload contains unexpected keys: {sorted(extra)}. "
            f"Only the 3 comm flags are permitted to prevent accidental "
            f"modification of subscriber state (e.g. status field). "
            f"Allowed: {sorted(ALLOWED_PATCH_FIELDS)}."
        )


def patch_client_silent(template_id, btc_id):
    """PATCH /billing_templates/{tid}/clients/{btc_id}/ with the three
    comm flags set to false. Returns True on success.

    Form-data per Revio API spec (not JSON). On any failure the caller
    decides how to surface — this function returns False rather than
    raising, so one bad client doesn't block the rest of the run.

    Payload is built locally from COMM_FLAGS only — _assert_payload_safe
    is the safety net even if a future change accidentally adds a key."""
    url = (REVIO_API_BASE_URL
           + f"/billing_templates/{template_id}/clients/{btc_id}/")
    data = {flag: "false" for flag in COMM_FLAGS}
    _assert_patch_payload_safe(data)
    r = _do_request_with_retry(
        "patch", url,
        headers=_get_form_headers(),
        data=data,
        timeout=30,
    )
    if r is None:
        logger.error("PATCH %s — transport error after retries", url)
        return False
    if r.status_code not in (200, 202, 204):
        body = (r.text or "")[:300]
        logger.error("PATCH %s returned %d: %s", url, r.status_code, body)
        return False
    return True


# ─── Email ───────────────────────────────────────────────────────────────────
def _build_summary(run_date, templates_examined, clients_examined,
                   patched, would_patch, skipped_already_silent,
                   skipped_paused, skipped_inactive, skipped_pending,
                   skipped_unknown_status, errors, patched_detail,
                   dry_run, error_summary, duration_seconds):
    n_action = would_patch if dry_run else patched
    skipped_total_inactive = (skipped_paused + skipped_inactive
                              + skipped_pending + skipped_unknown_status)

    if error_summary:
        outcome = "failure"
        headline = "Error during silence backfill"
        summary = (
            f"Backfill failed before completion. {error_summary}. "
            f"Manual investigation needed."
        )
    elif n_action == 0:
        outcome = "noop"
        headline = "No active subscribers need patching"
        summary = (
            f"Examined {clients_examined} subscriber(s) across "
            f"{templates_examined} template(s). "
            f"{skipped_already_silent} active subscriber(s) already silent. "
            f"{skipped_total_inactive} non-active subscriber(s) skipped "
            f"(paused/inactive/pending/unknown). Nothing to patch."
        )
    else:
        outcome = "success"
        verb = "would be silenced" if dry_run else "silenced"
        headline = f"{n_action} active subscriber(s) {verb}"
        summary = (
            f"Examined {clients_examined} subscriber(s) across "
            f"{templates_examined} template(s). "
            f"{n_action} active subscriber(s) {verb}, "
            f"{skipped_already_silent} active already silent, "
            f"{skipped_total_inactive} non-active skipped "
            f"({skipped_paused} paused / {skipped_inactive} inactive / "
            f"{skipped_pending} pending / {skipped_unknown_status} unknown), "
            f"{errors} error(s)."
        )

    numbers = {
        "Templates examined": templates_examined,
        "Subscribers examined": clients_examined,
        ("Active — would be patched" if dry_run
         else "Active — patched"): n_action,
        "Active — already silent": skipped_already_silent,
        "Skipped (subscription_paused)": skipped_paused,
        "Skipped (inactive)": skipped_inactive,
        "Skipped (pending)": skipped_pending,
        "Skipped (unknown status)": skipped_unknown_status,
        "Errors": errors,
    }

    next_steps = []
    if patched_detail:
        # Up to 25 detail lines so the email stays readable
        sample = patched_detail[:25]
        bullets = []
        for item in sample:
            c = item["client"]
            flags = item["non_silent_flags"]
            bullets.append(
                f"template={item['template_title']!r} "
                f"personal_code={c.get('personal_code') or '∅'} "
                f"flags_to_flip={list(flags.keys())}"
            )
        if len(patched_detail) > 25:
            bullets.append(
                f"… and {len(patched_detail) - 25} more "
                f"(see workflow logs for full list)"
            )
        next_steps.append("Active subscribers targeted: " + " | ".join(bullets))
        if dry_run:
            next_steps.append(
                "Re-run with DRY_RUN=false to apply the patches."
            )

    return RunSummary(
        workflow_name="Revio — Silence Existing Subscribers",
        run_date=run_date,
        mode="dry_run" if dry_run else "production",
        outcome=outcome,
        headline=headline,
        summary_paragraph=summary,
        numbers=numbers,
        duration_seconds=duration_seconds,
        next_steps=next_steps,
    )


def send_summary_email(run_date, *, templates_examined, clients_examined,
                       patched, would_patch, skipped_already_silent,
                       skipped_paused, skipped_inactive, skipped_pending,
                       skipped_unknown_status, errors, patched_detail,
                       dry_run, error_summary="", duration_seconds=0.0):
    sender = os.environ.get("EMAIL_SENDER")
    pwd = os.environ.get("EMAIL_PASSWORD")
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

    summary = _build_summary(
        run_date=run_date,
        templates_examined=templates_examined,
        clients_examined=clients_examined,
        patched=patched,
        would_patch=would_patch,
        skipped_already_silent=skipped_already_silent,
        skipped_paused=skipped_paused,
        skipped_inactive=skipped_inactive,
        skipped_pending=skipped_pending,
        skipped_unknown_status=skipped_unknown_status,
        errors=errors,
        patched_detail=patched_detail,
        dry_run=dry_run,
        error_summary=error_summary,
        duration_seconds=duration_seconds,
    )
    subject, html_body = build_run_summary_email(summary)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
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
    brand_scope = os.environ.get("REVIO_BRAND_ID", "")
    logger.info("=" * 60)
    logger.info("REVIO — SILENCE EXISTING SUBSCRIBERS")
    logger.info("Run date    : %s", run_date.isoformat(timespec="seconds"))
    logger.info("Dry run     : %s", DRY_RUN)
    logger.info("Brand scope : %s", brand_scope or "(all brands)")
    logger.info("=" * 60)

    templates_examined = 0
    clients_examined = 0
    patched = 0
    would_patch = 0
    skipped_already_silent = 0
    skipped_paused = 0
    skipped_inactive = 0
    skipped_pending = 0
    skipped_unknown_status = 0
    errors = 0
    patched_detail = []  # list of {"client": ..., "non_silent_flags": ..., "template_title": ...}
    by_template_counts = defaultdict(lambda: {
        "examined": 0, "to_patch": 0, "active_silent": 0,
        "paused": 0, "inactive": 0, "pending": 0, "unknown": 0,
    })

    try:
        templates = list_subscription_templates()
        templates_examined = len(templates)
        logger.info("Found %d subscription template(s)", templates_examined)

        for tmpl in templates:
            tid = tmpl.get("id") or tmpl.get("uuid")
            ttitle = tmpl.get("title") or tmpl.get("name") or str(tid)
            if not tid:
                logger.warning("Template missing id; skipping: %s", tmpl)
                continue

            clients = list_clients_for_template(tid)
            clients_examined += len(clients)
            by_template_counts[ttitle]["examined"] += len(clients)

            buckets = identify_clients_to_patch(clients)
            t_to_patch        = len(buckets["to_patch"])
            t_active_silent   = len(buckets["skipped_already_silent"])
            t_paused          = len(buckets["skipped_paused"])
            t_inactive        = len(buckets["skipped_inactive"])
            t_pending         = len(buckets["skipped_pending"])
            t_unknown         = len(buckets["skipped_unknown_status"])

            skipped_already_silent  += t_active_silent
            skipped_paused          += t_paused
            skipped_inactive        += t_inactive
            skipped_pending         += t_pending
            skipped_unknown_status  += t_unknown
            by_template_counts[ttitle]["to_patch"]      += t_to_patch
            by_template_counts[ttitle]["active_silent"] += t_active_silent
            by_template_counts[ttitle]["paused"]        += t_paused
            by_template_counts[ttitle]["inactive"]      += t_inactive
            by_template_counts[ttitle]["pending"]       += t_pending
            by_template_counts[ttitle]["unknown"]       += t_unknown

            logger.info(
                "Template %s (%s): examined=%d active_to_patch=%d "
                "active_silent=%d paused=%d inactive=%d pending=%d unknown=%d",
                tid, ttitle, len(clients), t_to_patch, t_active_silent,
                t_paused, t_inactive, t_pending, t_unknown,
            )

            # Log a one-line trace for each non-active skip so the audit
            # trail in the workflow log shows exactly what was bypassed.
            for bucket_name, label in (
                ("skipped_paused", "paused"),
                ("skipped_inactive", "inactive"),
                ("skipped_pending", "pending"),
                ("skipped_unknown_status", "unknown_status"),
            ):
                for item in buckets[bucket_name]:
                    c = item["client"]
                    logger.info(
                        "  skip[%s] btc_id=%s personal_code=%s status=%r",
                        label,
                        c.get("id") or c.get("client_id"),
                        c.get("personal_code") or "∅",
                        c.get("status"),
                    )

            for item in buckets["to_patch"]:
                c = item["client"]
                btc_id = c.get("id") or c.get("client_id")
                pcode = c.get("personal_code") or "∅"
                flags = item["non_silent_flags"]
                detail_entry = {**item, "template_title": ttitle,
                                "template_id": tid}

                if DRY_RUN:
                    logger.info(
                        "[DRY RUN] would PATCH template=%s btc_id=%s "
                        "personal_code=%s status=active flags_to_flip=%s "
                        "current_values=%s",
                        tid, btc_id, pcode, list(flags.keys()), flags,
                    )
                    would_patch += 1
                    patched_detail.append(detail_entry)
                    continue

                if not btc_id:
                    logger.error(
                        "Cannot PATCH — BTC has no id field: %s", c,
                    )
                    errors += 1
                    continue

                ok = patch_client_silent(tid, btc_id)
                if ok:
                    logger.info(
                        "Patched template=%s btc_id=%s personal_code=%s "
                        "flags_flipped=%s",
                        tid, btc_id, pcode, list(flags.keys()),
                    )
                    patched += 1
                    patched_detail.append(detail_entry)
                else:
                    errors += 1

        logger.info("─" * 60)
        logger.info("Per-template summary (active_to_patch / active_silent / "
                    "paused / inactive / pending / unknown):")
        for ttitle, counts in by_template_counts.items():
            logger.info(
                "  %s: examined=%d active_to_patch=%d active_silent=%d "
                "paused=%d inactive=%d pending=%d unknown=%d",
                ttitle, counts["examined"], counts["to_patch"],
                counts["active_silent"], counts["paused"],
                counts["inactive"], counts["pending"], counts["unknown"],
            )
        logger.info("─" * 60)
        logger.info(
            "Totals: templates=%d examined=%d active_to_patch=%d "
            "active_silent=%d paused=%d inactive=%d pending=%d unknown=%d",
            templates_examined, clients_examined,
            would_patch if DRY_RUN else patched,
            skipped_already_silent, skipped_paused, skipped_inactive,
            skipped_pending, skipped_unknown_status,
        )

        send_summary_email(
            run_date=run_date,
            templates_examined=templates_examined,
            clients_examined=clients_examined,
            patched=patched,
            would_patch=would_patch,
            skipped_already_silent=skipped_already_silent,
            skipped_paused=skipped_paused,
            skipped_inactive=skipped_inactive,
            skipped_pending=skipped_pending,
            skipped_unknown_status=skipped_unknown_status,
            errors=errors,
            patched_detail=patched_detail,
            dry_run=DRY_RUN,
            duration_seconds=time.monotonic() - started,
        )

    except Exception as e:
        logger.exception("Backfill failed: %s", e)
        try:
            send_summary_email(
                run_date=run_date,
                templates_examined=templates_examined,
                clients_examined=clients_examined,
                patched=patched,
                would_patch=would_patch,
                skipped_already_silent=skipped_already_silent,
                skipped_paused=skipped_paused,
                skipped_inactive=skipped_inactive,
                skipped_pending=skipped_pending,
                skipped_unknown_status=skipped_unknown_status,
                errors=errors,
                patched_detail=patched_detail,
                dry_run=DRY_RUN,
                error_summary=str(e),
                duration_seconds=time.monotonic() - started,
            )
        except Exception as e2:
            logger.error("Summary email also failed: %s", e2)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("DONE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
