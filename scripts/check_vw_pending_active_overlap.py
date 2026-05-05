#!/usr/bin/env python3
"""
scripts/check_vw_pending_active_overlap.py

DIAGNOSTIC ONE-OFF — answers the single question:

  Of the 118 VW pending subscribers, how many already have an
  ACTIVE subscription on some other Revio template (matched by
  normalised phone)?

If the answer is "lots", the actionable VW pending pile shrinks
considerably — those clients are already paying via a different
sign-up path; the pending BTC is just an orphaned duplicate.

The original dump (PR #27) collected this data as the "Currently
Active Elsewhere" column, but never sliced it specifically to VW
pending. This script reads live Revio (no stale data) and reports
the per-template / per-status overlap.

Read-only. Always emails JD only. Designed to be deletable after
the question is answered.

ENV:
  REVIO_API_KEY     required
  EMAIL_SENDER      Gmail sender
  EMAIL_PASSWORD    Gmail app password
"""

import logging
import os
import smtplib
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
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

VW_TEMPLATE_TITLES = frozenset({
    "VW Premium HELP Single R89",
    "VW Premium HELP Family R159",
})
ACTIVE_STATUS  = "active"
PENDING_STATUS = "pending"


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


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    started = time.monotonic()
    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    logger.info("=" * 60)
    logger.info("VW PENDING × ACTIVE OVERLAP CHECK")
    logger.info("Run ts: %s", run_ts)
    logger.info("=" * 60)

    templates = list_subscription_templates()
    logger.info("Found %d subscription templates", len(templates))

    # ── Pass 1: enumerate every BTC across every template, bucket ──────────
    # active_btcs:      [(template_title, btc), ...] — every active across
    #                   ALL templates, used to build the cross-ref index
    # vw_pending_btcs:  [(template_title, btc), ...] — the question scope
    active_btcs = []
    vw_pending_btcs = []
    for tmpl in templates:
        tid    = tmpl.get("id") or tmpl.get("uuid")
        ttitle = tmpl.get("title") or tmpl.get("name") or str(tid)
        if not tid:
            continue
        btcs = list_clients_for_template(tid)
        for b in btcs:
            status = b.get("status")
            if status == ACTIVE_STATUS:
                active_btcs.append((ttitle, b))
            elif status == PENDING_STATUS and ttitle in VW_TEMPLATE_TITLES:
                vw_pending_btcs.append((ttitle, b))
    logger.info("Active BTCs (all templates): %d", len(active_btcs))
    logger.info("VW pending BTCs            : %d", len(vw_pending_btcs))

    # ── Pass 2: fetch Clients (cached) — need .phone for both groups ───────
    client_cache = {}
    unique_cids = (
        {b.get("client_id") for _, b in active_btcs if b.get("client_id")}
        | {b.get("client_id") for _, b in vw_pending_btcs if b.get("client_id")}
    )
    logger.info("Fetching %d unique Client record(s)", len(unique_cids))
    for n, cid in enumerate(unique_cids, 1):
        fetch_client(cid, client_cache)
        if n % 250 == 0:
            logger.info("  …fetched %d / %d", n, len(unique_cids))

    # ── Build active phone index ───────────────────────────────────────────
    # {normalised_phone: [(active_template_title, status), ...]}
    # We track template title only (status is implicit "active") for
    # readability when JD scans the per-record breakdown.
    active_phone_index = defaultdict(list)
    for ttitle, btc in active_btcs:
        client = client_cache.get(btc.get("client_id"))
        if not client:
            continue
        phone = normalise_phone(client.get("phone"))
        if not phone:
            continue
        active_phone_index[phone].append(ttitle)
    logger.info("Active phone index: %d unique phones across %d active BTCs",
                len(active_phone_index), len(active_btcs))

    # ── Cross-reference VW pending against active phone index ──────────────
    has_active_elsewhere = []  # [{pending_template, phone, active_templates, ...}]
    truly_pending        = []  # same shape, empty active_templates
    no_phone             = 0   # VW pending records with no phone on Client

    by_overlap_template = Counter()  # which active templates overlap the most
    by_pending_template = Counter()  # VW pending bucket — Single vs Family

    for ttitle, btc in vw_pending_btcs:
        by_pending_template[ttitle] += 1
        client = client_cache.get(btc.get("client_id")) or {}
        phone = normalise_phone(client.get("phone"))
        record = {
            "pending_template":  ttitle,
            "client_id":         btc.get("client_id"),
            "personal_code":     client.get("personal_code") or "",
            "full_name":         client.get("full_name") or "",
            "phone":             phone,
            "active_templates":  [],
        }
        if not phone:
            no_phone += 1
            truly_pending.append(record)
            continue
        active_titles = sorted(set(active_phone_index.get(phone, [])))
        if active_titles:
            record["active_templates"] = active_titles
            for at in active_titles:
                by_overlap_template[at] += 1
            has_active_elsewhere.append(record)
        else:
            truly_pending.append(record)

    n_total = len(vw_pending_btcs)
    n_overlap = len(has_active_elsewhere)
    n_truly = len(truly_pending)
    pct = (100.0 * n_overlap / n_total) if n_total else 0.0

    # Per-pending-template overlap rate
    overlap_by_pending_template = Counter()
    for r in has_active_elsewhere:
        overlap_by_pending_template[r["pending_template"]] += 1

    # ── Build report ───────────────────────────────────────────────────────
    md = []
    md.append("# VW Pending × Active Overlap Check\n")
    md.append(f"_Run: {run_ts} UTC_\n")
    md.append("## Headline\n")
    md.append(f"**{n_overlap} of {n_total} VW pending clients ({pct:.1f}%) "
              f"already have at least one ACTIVE subscription on another "
              f"template (phone-matched).**\n")
    md.append(f"**Truly pending (no active anywhere): {n_truly}.** "
              f"_(Of these, {no_phone} had no phone on file — "
              f"impossible to cross-ref.)_\n")
    md.append("## VW pending split by template\n")
    md.append("| Pending template | Total | Has active elsewhere | %% |")
    md.append("|---|---:|---:|---:|")
    for t in sorted(by_pending_template):
        total = by_pending_template[t]
        ovr   = overlap_by_pending_template.get(t, 0)
        p     = (100.0 * ovr / total) if total else 0.0
        md.append(f"| {t} | {total} | {ovr} | {p:.1f}% |")
    md.append("")
    md.append("## Where the active subs live\n")
    md.append("(For each VW-pending client whose phone hits an active sub, "
              "this counts the active templates by frequency. Same client "
              "active on N templates contributes N counts.)\n")
    md.append("| Active template | Hits |")
    md.append("|---|---:|")
    for t, c in by_overlap_template.most_common():
        md.append(f"| {t} | {c} |")
    md.append("")

    # Sample 10 of each side for sanity-check
    def _fmt_record(r):
        actives = "; ".join(r["active_templates"]) if r["active_templates"] \
            else "(none)"
        return (f"- pending=`{r['pending_template']}` · "
                f"phone=`{r['phone'] or '∅'}` · "
                f"personal_code=`{r['personal_code'] or '∅'}` · "
                f"name=`{r['full_name'] or '∅'}` · active_elsewhere={actives}")

    md.append("## Sample: VW pending WITH active elsewhere (first 10)\n")
    if has_active_elsewhere:
        for r in has_active_elsewhere[:10]:
            md.append(_fmt_record(r))
    else:
        md.append("_(none)_")
    md.append("")
    md.append("## Sample: VW pending TRULY pending — actionable pile (first 10)\n")
    if truly_pending:
        for r in truly_pending[:10]:
            md.append(_fmt_record(r))
    else:
        md.append("_(none)_")
    md.append("")

    report = "\n".join(md)
    logger.info("─" * 60)
    for line in report.splitlines():
        logger.info("%s", line)
    logger.info("─" * 60)

    # Email to JD
    sender = os.environ.get("EMAIL_SENDER")
    pwd    = os.environ.get("EMAIL_PASSWORD")
    if sender and pwd:
        msg = EmailMessage()
        msg["Subject"] = (f"[Diagnostic] VW pending × active overlap — "
                          f"{n_overlap}/{n_total} ({pct:.1f}%) already active")
        msg["From"]    = sender
        msg["To"]      = JD_RECIPIENT
        msg.set_content(report)
        # Cheap markdown→<pre> wrapper, same approach as the
        # analyse_vw_autoped_pending.py diagnostic.
        html = (
            "<html><body><pre style='font-family:monospace;font-size:13px'>"
            + report.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            + "</pre></body></html>"
        )
        msg.add_alternative(html, subtype="html")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, pwd)
            smtp.send_message(msg)
        logger.info("Email sent → %s", JD_RECIPIENT)
    else:
        logger.warning("EMAIL_SENDER / EMAIL_PASSWORD not set — skipping email")

    logger.info("Done in %.1fs", time.monotonic() - started)


if __name__ == "__main__":
    main()
