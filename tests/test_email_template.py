"""Pure-function tests for scripts/email_template.py."""

import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))

from email_template import (  # noqa: E402
    RunSummary,
    _format_duration,
    build_run_summary_email,
)


def _base_summary(**overrides) -> RunSummary:
    defaults = dict(
        workflow_name="VW Cancellations Sync",
        run_date=datetime(2026, 5, 3, 4, 0),
        mode="production",
        outcome="success",
        headline="14 new cancellations queued for Casi",
        summary_paragraph="Pulled today's email. 14 new rows appended.",
        numbers={"CSV rows": 14, "Appended": 2, "Errors": 0},
        duration_seconds=12.5,
    )
    defaults.update(overrides)
    return RunSummary(**defaults)


def test_subject_contains_workflow_name_and_headline():
    s = _base_summary()
    subject, _ = build_run_summary_email(s)
    assert "VW Cancellations Sync" in subject
    assert "14 new cancellations queued for Casi" in subject


def test_dry_run_prefix_present_in_dry_run_mode_only():
    dry_subject, _ = build_run_summary_email(_base_summary(mode="dry_run"))
    prod_subject, _ = build_run_summary_email(_base_summary(mode="production"))
    assert dry_subject.startswith("[DRY RUN] ")
    assert not prod_subject.startswith("[DRY RUN] ")


def test_emoji_matches_outcome():
    success_subject, _ = build_run_summary_email(_base_summary(outcome="success"))
    noop_subject, _ = build_run_summary_email(_base_summary(outcome="noop"))
    failure_subject, _ = build_run_summary_email(_base_summary(outcome="failure"))
    assert "✅" in success_subject
    assert "ℹ️" in noop_subject
    assert "❌" in failure_subject


def test_duration_formatting():
    assert _format_duration(12) == "12.0s"
    assert _format_duration(59.4) == "59.4s"
    assert _format_duration(90) == "1m 30s"
    assert _format_duration(605) == "10m 05s"


def test_next_steps_render_as_unordered_list():
    s = _base_summary(next_steps=["Investigate account 12345", "Retry tomorrow"])
    _, body = build_run_summary_email(s)
    assert "Next steps" in body
    assert "<ul" in body
    assert "<li>Investigate account 12345</li>" in body
    assert "<li>Retry tomorrow</li>" in body


def test_next_steps_omitted_when_empty():
    _, body = build_run_summary_email(_base_summary(next_steps=[]))
    assert "Next steps" not in body


def test_numbers_dict_renders_all_pairs():
    s = _base_summary(numbers={
        "Source rows examined": 50,
        "Already in tab": 3,
        "Appended": 47,
    })
    _, body = build_run_summary_email(s)
    assert "Source rows examined" in body
    assert ">50<" in body
    assert "Already in tab" in body
    assert ">3<" in body
    assert "Appended" in body
    assert ">47<" in body


def test_workflow_run_url_and_sheet_url_render_as_anchors():
    s = _base_summary(
        workflow_run_url="https://github.com/me/repo/actions/runs/1",
        sheet_url="https://docs.google.com/spreadsheets/d/abc",
    )
    _, body = build_run_summary_email(s)
    assert "<a href='https://github.com/me/repo/actions/runs/1'>Workflow run</a>" in body
    assert "<a href='https://docs.google.com/spreadsheets/d/abc'>Open sheet</a>" in body


def test_attachments_note_renders_when_provided():
    s = _base_summary(attachments_note="Excel of 14 new rows attached")
    _, body = build_run_summary_email(s)
    assert "📎 Excel of 14 new rows attached" in body


def test_attachments_line_omitted_when_no_note():
    _, body = build_run_summary_email(_base_summary())
    assert "📎" not in body
