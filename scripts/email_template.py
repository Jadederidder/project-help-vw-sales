"""Standard run-summary email template shared by every workflow.

Pure function: take a `RunSummary`, return `(subject, html_body)`.
No SMTP code — callers pass the returned strings to their existing
send-email helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

OUTCOME_EMOJI = {"success": "✅", "noop": "ℹ️", "failure": "❌"}
OUTCOME_LABEL = {
    "success": "Success",
    "noop": "Nothing to do",
    "failure": "Failed",
}


@dataclass
class RunSummary:
    workflow_name: str                 # e.g. "VW Cancellations Sync"
    run_date: datetime                 # SAST
    mode: Literal["production", "dry_run"]
    outcome: Literal["success", "noop", "failure"]
    headline: str                      # one-line outcome, goes in subject
    summary_paragraph: str             # 2–3 sentence plain-English
    numbers: dict                      # {"Source rows examined": 14, ...}
    duration_seconds: float = 0.0
    next_steps: list = field(default_factory=list)
    workflow_run_url: str | None = None
    sheet_url: str | None = None
    attachments_note: str | None = None  # e.g. "4 Excel reports attached"


def build_run_summary_email(s: RunSummary) -> tuple[str, str]:
    emoji = OUTCOME_EMOJI[s.outcome]
    mode_tag = "[DRY RUN] " if s.mode == "dry_run" else ""
    subject = f"{mode_tag}{emoji} {s.workflow_name} — {s.headline}"

    duration = _format_duration(s.duration_seconds)
    date_str = s.run_date.strftime("%d %b %Y, %H:%M SAST")

    numbers_rows = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:#555'>{k}</td>"
        f"<td style='padding:4px 0;font-weight:600'>{v}</td></tr>"
        for k, v in s.numbers.items()
    )

    next_steps_html = ""
    if s.next_steps:
        items = "".join(f"<li>{step}</li>" for step in s.next_steps)
        next_steps_html = (
            "<h3 style='margin:24px 0 8px;color:#1F3864'>Next steps</h3>"
            f"<ul style='margin:0 0 0 18px;padding:0'>{items}</ul>"
        )

    footer_links = []
    if s.workflow_run_url:
        footer_links.append(f"<a href='{s.workflow_run_url}'>Workflow run</a>")
    if s.sheet_url:
        footer_links.append(f"<a href='{s.sheet_url}'>Open sheet</a>")
    footer = " · ".join(footer_links) if footer_links else ""

    attach_line = (
        f"<p style='margin:8px 0 0;color:#555;font-size:13px'>"
        f"📎 {s.attachments_note}</p>"
        if s.attachments_note else ""
    )

    body = f"""
    <div style="font-family:Arial,sans-serif;max-width:640px;color:#1a1a1a">
      <div style="background:#1F3864;color:#fff;padding:16px 20px;border-radius:6px 6px 0 0">
        <div style="font-size:18px;font-weight:600">{emoji} {s.workflow_name}</div>
        <div style="font-size:13px;opacity:0.85;margin-top:2px">{s.headline}</div>
      </div>
      <div style="background:#EBF3FB;padding:14px 20px;font-size:13px;color:#333">
        <strong>Run date:</strong> {date_str} &nbsp;·&nbsp;
        <strong>Mode:</strong> {s.mode.replace('_',' ').title()} &nbsp;·&nbsp;
        <strong>Outcome:</strong> {OUTCOME_LABEL[s.outcome]} &nbsp;·&nbsp;
        <strong>Duration:</strong> {duration}
      </div>
      <div style="padding:20px;background:#fff;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 6px 6px">
        <h3 style="margin:0 0 8px;color:#1F3864">What happened</h3>
        <p style="margin:0 0 16px;line-height:1.5">{s.summary_paragraph}</p>
        <h3 style="margin:0 0 8px;color:#1F3864">Numbers</h3>
        <table style="border-collapse:collapse;font-size:14px">{numbers_rows}</table>
        {attach_line}
        {next_steps_html}
        <p style="margin:24px 0 0;font-size:12px;color:#888;border-top:1px solid #eee;padding-top:12px">
          {footer}
        </p>
      </div>
    </div>
    """.strip()

    return subject, body


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s"
