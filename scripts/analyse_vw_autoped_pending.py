#!/usr/bin/env python3
"""
scripts/analyse_vw_autoped_pending.py

DIAGNOSTIC ONE-OFF — answer JD's question: why are the VW + Auto
Pedigree pending subscribers stuck? Banking? Email? Something else?

The non-active dump (PR #27) surfaced ~205 pending subscribers across
the live VW + Auto Pedigree templates (the R59 OLD BILLING backlog of
~1,876 is a separate conversation — JD wants R59 deferred). This
script fetches just those 205, runs a completeness scan on every
field that could plausibly block Revio from progressing the BTC out
of `pending`, and emails JD a markdown breakdown.

Read-only against Revio (GETs only). Always emails JD only — no
production-distribution fan-out, no Excel attachment. Designed to be
deletable once the question is answered.

ENV:
  REVIO_API_KEY     required
  EMAIL_SENDER      Gmail sender
  EMAIL_PASSWORD    Gmail app password
"""

import logging
import os
import re
import smtplib
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from email.message import EmailMessage
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from revio_subscription import (  # noqa: E402
    REVIO_API_BASE_URL,
    _do_request_with_retry,
    _get_headers,
    normalise_phone,
)
from silence_existing_revio_subscribers import (  # noqa: E402
    list_clients_for_template,
    list_subscription_templates,
)

logger = logging.getLogger(__name__)

JD_RECIPIENT = "jd@projecthelp.co.za"

# Target templates — exact title match against the live API. AUDI
# templates were 0-pending in the dry-run so excluded; R59 OLD BILLING
# is the 1,876-record legacy backlog deferred per JD's call.
TARGET_TEMPLATE_TITLES = {
    "VW Premium HELP Single R89",
    "VW Premium HELP Family R159",
    "Auto Ped Embedded Family",
    "Auto Pedigree Family R159",
    "Auto Pedigree R89",
}

PENDING_STATUS = "pending"

# ZA-shaped sanity bounds. Loose deliberately — we flag values that
# look wrong, not values that are formally invalid.
ZA_BANK_ACCOUNT_MIN_DIGITS = 9
ZA_BANK_ACCOUNT_MAX_DIGITS = 11
ZA_BANK_CODE_DIGITS = 6
ZA_PERSONAL_CODE_DIGITS = 13   # full ID number; passport-holders may be shorter
ZA_PHONE_LEN = 11              # after normalise_phone → 27XXXXXXXXX

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ─── Field probes ────────────────────────────────────────────────────────────
def _digits_only(s):
    return "".join(c for c in str(s or "") if c.isdigit())


def probe_email(value):
    """Returns one of: blank, malformed, ok."""
    s = (value or "").strip()
    if not s:
        return "blank"
    if not EMAIL_RE.match(s):
        return "malformed"
    return "ok"


def probe_phone(value):
    """Phone normaliser already lives in revio_subscription. We probe
    by running it + length-checking the result."""
    s = (value or "").strip() if isinstance(value, str) else value
    if not s:
        return "blank"
    n = normalise_phone(s)
    if not n:
        return "unrecoverable"
    if len(n) != ZA_PHONE_LEN:
        return f"wrong_length_{len(n)}"
    return "ok"


def probe_bank_account(value):
    s = str(value or "").strip()
    if not s:
        return "blank"
    digits = _digits_only(s)
    if not digits:
        return "non_numeric"
    if len(digits) < ZA_BANK_ACCOUNT_MIN_DIGITS:
        return f"too_short_{len(digits)}d"
    if len(digits) > ZA_BANK_ACCOUNT_MAX_DIGITS:
        return f"too_long_{len(digits)}d"
    return "ok"


def probe_bank_code(value):
    s = str(value or "").strip()
    if not s:
        return "blank"
    digits = _digits_only(s)
    if len(digits) != ZA_BANK_CODE_DIGITS:
        return f"wrong_length_{len(digits)}d"
    return "ok"


def probe_personal_code(value):
    s = str(value or "").strip()
    if not s:
        return "blank"
    digits = _digits_only(s)
    if not digits:
        return "non_numeric"
    if len(digits) == ZA_PERSONAL_CODE_DIGITS:
        return "ok_id_number"
    return f"non_id_shape_{len(digits)}d"


def probe_scheduled_debit(value, today):
    """`subscription_billing_scheduled_on` is the most diagnostic field
    for "pending" specifically — if it's in the past, the BTC is overdue
    (Revio should have debited but didn't); if it's in the future, the
    record is just waiting; if blank, no debit was ever scheduled."""
    if value is None or value == "":
        return "blank"
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            n = float(value)
            if n < 1:
                return "blank"
            if n < 100_000:
                from openpyxl.utils.datetime import from_excel
                d = from_excel(n).date()
            else:
                d = datetime.fromtimestamp(n, tz=timezone.utc).date()
        else:
            s = str(value).strip()
            if not s:
                return "blank"
            d = datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return "unparseable"
    delta = (d - today).days
    if delta > 0:
        return f"future_{delta}d"
    if delta == 0:
        return "today"
    return f"overdue_{-delta}d"


# ─── Revio reads ─────────────────────────────────────────────────────────────
def fetch_client(client_id, cache):
    if not client_id:
        return None
    if client_id in cache:
        return cache[client_id]
    url = REVIO_API_BASE_URL + f"/clients/{client_id}/"
    r = _do_request_with_retry("get", url, headers=_get_headers(), timeout=30)
    if r is None or r.status_code != 200:
        cache[client_id] = None
        return None
    try:
        data = r.json()
    except ValueError:
        cache[client_id] = None
        return None
    cache[client_id] = data
    return data


# ─── Analysis ────────────────────────────────────────────────────────────────
def analyse(records, today):
    """records: list of dicts {template_title, btc, client}.
    Returns:
      {
        "by_template_count": {title: n},
        "field_buckets": {field: Counter({outcome: n})},
        "cross_tabs": {
          "no_email_AND_no_bank_account": n,
          "no_phone_AND_no_email":         n,
          "no_bank_account_AND_no_bank_code": n,
        },
        "scheduled_buckets": Counter({outcome: n}),  # roll-up of
                                                     # blank/today/future_*/overdue_*
        "oldest_pending": [(days_since_created, template, personal_code), ...],
      }
    """
    by_template = Counter()
    field_buckets = defaultdict(Counter)
    cross = Counter()
    sched_rollup = Counter()
    age_rows = []

    for r in records:
        ttitle = r["template_title"]
        btc = r["btc"]
        client = r["client"] or {}
        by_template[ttitle] += 1

        # Probes per field
        email_o = probe_email(client.get("email"))
        phone_o = probe_phone(client.get("phone"))
        bacc_o  = probe_bank_account(client.get("bank_account"))
        bcode_o = probe_bank_code(client.get("bank_code"))
        pcode_o = probe_personal_code(client.get("personal_code"))
        sched_o = probe_scheduled_debit(
            btc.get("subscription_billing_scheduled_on"), today,
        )

        field_buckets["email"][email_o]                 += 1
        field_buckets["phone"][phone_o]                 += 1
        field_buckets["bank_account"][bacc_o]           += 1
        field_buckets["bank_code"][bcode_o]             += 1
        field_buckets["personal_code"][pcode_o]         += 1
        field_buckets["scheduled_debit_raw"][sched_o]   += 1

        # Roll up scheduled into 4 buckets for the headline table.
        if sched_o == "blank":
            sched_rollup["blank"]    += 1
        elif sched_o == "unparseable":
            sched_rollup["unparseable"] += 1
        elif sched_o == "today":
            sched_rollup["today"]    += 1
        elif sched_o.startswith("future_"):
            sched_rollup["future"]   += 1
        elif sched_o.startswith("overdue_"):
            sched_rollup["overdue"]  += 1

        # Cross-tabs (the "tell me the combinations" table).
        if email_o in ("blank", "malformed") and bacc_o == "blank":
            cross["no_email_AND_no_bank_account"] += 1
        if phone_o in ("blank", "unrecoverable") and email_o in ("blank", "malformed"):
            cross["no_phone_AND_no_email"]        += 1
        if bacc_o == "blank" and bcode_o == "blank":
            cross["no_bank_account_AND_no_bank_code"] += 1

        # Age tracking for the "oldest stuck pending" callout.
        try:
            created = btc.get("created_on")
            if isinstance(created, (int, float)) and not isinstance(created, bool):
                n = float(created)
                if n >= 100_000:
                    cd = datetime.fromtimestamp(n, tz=timezone.utc).date()
                elif n >= 1:
                    from openpyxl.utils.datetime import from_excel
                    cd = from_excel(n).date()
                else:
                    cd = None
            elif isinstance(created, str) and created.strip():
                cd = datetime.fromisoformat(
                    created.strip().replace("Z", "+00:00")
                ).date()
            else:
                cd = None
            if cd is not None:
                age_rows.append((
                    (today - cd).days,
                    ttitle,
                    client.get("personal_code") or "∅",
                ))
        except Exception:
            pass

    age_rows.sort(reverse=True)
    return {
        "by_template_count": dict(by_template),
        "field_buckets":     {k: dict(v) for k, v in field_buckets.items()},
        "cross_tabs":        dict(cross),
        "scheduled_buckets": dict(sched_rollup),
        "oldest_pending":    age_rows[:10],
    }


# ─── Report rendering ────────────────────────────────────────────────────────
def _pct(n, total):
    return f"{(100.0 * n / total):.1f}%" if total else "—"


def render_report(analysis, total_records, today):
    """Markdown report → HTML email body."""
    lines = []
    lines.append("# VW + Auto Pedigree — Pending Diagnostic\n")
    lines.append(f"_Run date: {today.isoformat()} • {total_records} pending "
                 f"subscriber(s) examined across "
                 f"{len(analysis['by_template_count'])} template(s)._\n")

    # Per-template count
    lines.append("## Pending count by template\n")
    lines.append("| Template | Pending |")
    lines.append("|---|---|")
    for t, n in sorted(analysis["by_template_count"].items(),
                       key=lambda kv: -kv[1]):
        lines.append(f"| {t} | {n} |")
    lines.append("")

    # Field completeness
    lines.append("## Field completeness (lower = more problems)\n")
    lines.append("| Field | OK | Blank | Malformed / wrong-shape | OK % |")
    lines.append("|---|---:|---:|---:|---:|")
    for field in ("email", "phone", "bank_account", "bank_code",
                  "personal_code"):
        b = analysis["field_buckets"].get(field, {})
        ok = b.get("ok", 0) + b.get("ok_id_number", 0)
        blank = b.get("blank", 0)
        bad = total_records - ok - blank
        lines.append(
            f"| {field} | {ok} | {blank} | {bad} | {_pct(ok, total_records)} |"
        )
    lines.append("")

    # Scheduled-debit roll-up — the most diagnostic field for `pending`
    lines.append("## `subscription_billing_scheduled_on` — the pending-state "
                 "diagnostic\n")
    lines.append("If a pending record's scheduled debit is **overdue**, "
                 "Revio should have debited but didn't (likely a banking-"
                 "details problem). **Future** = simply waiting. **Blank** "
                 "= no debit was ever scheduled (likely a sign-up that "
                 "never completed).\n")
    sb = analysis["scheduled_buckets"]
    lines.append("| Bucket | Count | % |")
    lines.append("|---|---:|---:|")
    for k in ("blank", "overdue", "today", "future", "unparseable"):
        n = sb.get(k, 0)
        lines.append(f"| {k} | {n} | {_pct(n, total_records)} |")
    lines.append("")

    # Cross-tabs
    lines.append("## Cross-tabs (the \"combination\" patterns)\n")
    lines.append("| Combination | Count | % |")
    lines.append("|---|---:|---:|")
    for k in ("no_email_AND_no_bank_account",
              "no_phone_AND_no_email",
              "no_bank_account_AND_no_bank_code"):
        n = analysis["cross_tabs"].get(k, 0)
        lines.append(f"| {k.replace('_', ' ')} | {n} | "
                     f"{_pct(n, total_records)} |")
    lines.append("")

    # Detailed buckets per field
    lines.append("## Detailed outcome buckets per field\n")
    for field, b in analysis["field_buckets"].items():
        lines.append(f"**{field}**")
        for outcome, n in sorted(b.items(), key=lambda kv: -kv[1]):
            lines.append(f"- {outcome}: {n}")
        lines.append("")

    # Oldest stuck pending
    lines.append("## Oldest pending records (top 10 by Days Since Created)\n")
    lines.append("| Days | Template | Personal Code |")
    lines.append("|---:|---|---|")
    for days, t, pc in analysis["oldest_pending"]:
        lines.append(f"| {days} | {t} | `{pc}` |")
    lines.append("")

    return "\n".join(lines)


def _markdown_to_html(md):
    """Tiny converter — good enough for Gmail and avoids a markdown
    dependency. Tables render as <pre> blocks; everything else stays
    legible. The diagnostic is one-off, not worth pulling in markdown2."""
    out = ["<html><body><pre style='font-family:monospace;font-size:13px'>"]
    out.append(md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
    out.append("</pre></body></html>")
    return "".join(out)


def send_report_email(markdown_body, total_records):
    sender = os.environ.get("EMAIL_SENDER")
    pwd    = os.environ.get("EMAIL_PASSWORD")
    if not sender or not pwd:
        logger.warning("EMAIL_SENDER / EMAIL_PASSWORD not set — skipping email")
        return
    msg = EmailMessage()
    msg["Subject"] = (f"[Diagnostic] VW + Auto Ped pending — "
                      f"{total_records} record(s) analysed")
    msg["From"]    = sender
    msg["To"]      = JD_RECIPIENT
    msg.set_content(markdown_body)
    msg.add_alternative(_markdown_to_html(markdown_body), subtype="html")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, pwd)
        smtp.send_message(msg)
    logger.info("Diagnostic email sent → %s", JD_RECIPIENT)


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    today = date.today()
    started = time.monotonic()
    logger.info("=" * 60)
    logger.info("VW + AUTO PED — PENDING DIAGNOSTIC")
    logger.info("Run date: %s", today.isoformat())
    logger.info("Targets : %s", ", ".join(sorted(TARGET_TEMPLATE_TITLES)))
    logger.info("=" * 60)

    templates = list_subscription_templates()
    targeted = [
        t for t in templates
        if (t.get("title") or t.get("name") or "") in TARGET_TEMPLATE_TITLES
    ]
    found_titles = {t.get("title") or t.get("name") or "" for t in targeted}
    missing = TARGET_TEMPLATE_TITLES - found_titles
    if missing:
        logger.warning("Target template(s) not found in live API: %s",
                       sorted(missing))

    pending_records = []
    for tmpl in targeted:
        tid    = tmpl.get("id") or tmpl.get("uuid")
        ttitle = tmpl.get("title") or tmpl.get("name")
        btcs = list_clients_for_template(tid)
        pending_btcs = [b for b in btcs if b.get("status") == PENDING_STATUS]
        logger.info("Template %s: %d total, %d pending",
                    ttitle, len(btcs), len(pending_btcs))
        for b in pending_btcs:
            pending_records.append({
                "template_title": ttitle,
                "template_id":    tid,
                "btc":            b,
                "client":         None,  # filled in next pass
            })

    logger.info("Total pending across targets: %d", len(pending_records))

    client_cache = {}
    unique_cids = {r["btc"].get("client_id") for r in pending_records
                   if r["btc"].get("client_id")}
    logger.info("Fetching %d unique Client record(s)", len(unique_cids))
    for n, cid in enumerate(unique_cids, 1):
        fetch_client(cid, client_cache)
        if n % 50 == 0:
            logger.info("  …fetched %d / %d", n, len(unique_cids))
    for r in pending_records:
        r["client"] = client_cache.get(r["btc"].get("client_id"))

    analysis = analyse(pending_records, today)
    report_md = render_report(analysis, len(pending_records), today)

    # Mirror the report into stdout so the workflow log carries it too.
    logger.info("─" * 60)
    for line in report_md.splitlines():
        logger.info("%s", line)
    logger.info("─" * 60)

    send_report_email(report_md, len(pending_records))

    logger.info("Done in %.1fs", time.monotonic() - started)


if __name__ == "__main__":
    main()
