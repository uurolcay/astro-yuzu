"""Structured interpretation feedback persistence and validation helpers."""

from __future__ import annotations

import json
from typing import Any

import database as db_mod

VALID_FEEDBACK_LABELS = {
    "accurate",
    "unclear",
    "too_generic",
    "very_helpful",
    "not_relevant",
}

VALID_DOMAINS = {"career", "money", "relationships", "inner_state", "growth"}
VALID_SECTIONS = {"summary", "career", "money", "relationships", "inner_state", "growth", "key_advice", "risk_areas"}


def serialize_feedback_entry(entry) -> dict[str, Any]:
    return {
        "id": entry.id,
        "user_id": entry.user_id,
        "report_id": entry.report_id,
        "section_name": entry.section_name,
        "anchor_rank": entry.anchor_rank,
        "anchor_title": entry.anchor_title,
        "anchor_type": entry.anchor_type,
        "domain": entry.domain,
        "user_rating": entry.user_rating,
        "feedback_label": entry.feedback_label,
        "free_text_comment": entry.free_text_comment,
        "created_at": entry.created_at.isoformat() if entry.created_at else None,
    }


def load_feedback_history(db, *, user_id=None, report_id=None, limit=100) -> list[dict[str, Any]]:
    query = db.query(db_mod.InterpretationFeedback)
    if user_id is not None:
        query = query.filter(db_mod.InterpretationFeedback.user_id == user_id)
    if report_id is not None:
        query = query.filter(db_mod.InterpretationFeedback.report_id == report_id)
    rows = query.order_by(db_mod.InterpretationFeedback.created_at.desc()).limit(limit).all()
    return [serialize_feedback_entry(row) for row in rows]


def save_interpretation_feedback(db, payload, *, report=None, user=None) -> dict[str, Any]:
    normalized = _validate_feedback_payload(payload)
    target_report = report
    if target_report is None:
        target_report = db.query(db_mod.GeneratedReport).filter(db_mod.GeneratedReport.id == normalized["report_id"]).first()
    if not target_report:
        raise ValueError("report_not_found")

    if normalized["anchor_rank"] is not None:
        anchor = _resolve_anchor_reference(target_report, normalized["anchor_rank"])
        normalized["anchor_title"] = anchor["title"]
        normalized["anchor_type"] = anchor["anchor_type"]
        normalized["domain"] = anchor["domains"][0] if anchor.get("domains") else normalized["domain"]
    elif normalized["domain"] is None and normalized["section_name"] in VALID_DOMAINS:
        normalized["domain"] = normalized["section_name"]

    entry = db_mod.InterpretationFeedback(
        user_id=getattr(user, "id", None),
        report_id=normalized["report_id"],
        section_name=normalized["section_name"],
        anchor_rank=normalized["anchor_rank"],
        anchor_title=normalized["anchor_title"],
        anchor_type=normalized["anchor_type"],
        domain=normalized["domain"],
        user_rating=normalized["user_rating"],
        feedback_label=normalized["feedback_label"],
        free_text_comment=normalized["free_text_comment"],
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return serialize_feedback_entry(entry)


def _validate_feedback_payload(payload) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("invalid_payload")

    report_id = payload.get("report_id") or payload.get("reading_id")
    try:
        report_id = int(report_id)
    except (TypeError, ValueError):
        raise ValueError("invalid_report_id") from None

    anchor_rank = payload.get("anchor_rank")
    if anchor_rank in ("", None):
        anchor_rank = None
    else:
        try:
            anchor_rank = int(anchor_rank)
        except (TypeError, ValueError):
            raise ValueError("invalid_anchor_rank") from None
        if anchor_rank not in {1, 2, 3}:
            raise ValueError("invalid_anchor_rank")

    user_rating = payload.get("user_rating")
    try:
        user_rating = int(user_rating)
    except (TypeError, ValueError):
        raise ValueError("invalid_user_rating") from None
    if user_rating < 1 or user_rating > 5:
        raise ValueError("invalid_user_rating")

    feedback_label = str(payload.get("feedback_label") or "").strip().lower()
    if feedback_label not in VALID_FEEDBACK_LABELS:
        raise ValueError("invalid_feedback_label")

    section_name = str(payload.get("section_name") or payload.get("interpretation_section") or "").strip().lower() or None
    if section_name and section_name not in VALID_SECTIONS:
        raise ValueError("invalid_section_name")

    domain = str(payload.get("domain") or "").strip().lower() or None
    if domain and domain not in VALID_DOMAINS:
        raise ValueError("invalid_domain")

    free_text_comment = str(payload.get("free_text_comment") or "").strip() or None
    if free_text_comment and len(free_text_comment) > 1000:
        free_text_comment = free_text_comment[:1000]

    return {
        "report_id": report_id,
        "section_name": section_name,
        "anchor_rank": anchor_rank,
        "anchor_title": None,
        "anchor_type": None,
        "domain": domain,
        "user_rating": user_rating,
        "feedback_label": feedback_label,
        "free_text_comment": free_text_comment,
    }


def _resolve_anchor_reference(report, anchor_rank: int) -> dict[str, Any]:
    try:
        interpretation_context = json.loads(report.interpretation_context_json or "{}")
    except Exception as exc:
        raise ValueError("invalid_report_context") from exc

    anchors = (
        interpretation_context.get("signal_layer", {}).get("top_anchors")
        or interpretation_context.get("top_anchors")
        or []
    )
    for anchor in anchors:
        if int(anchor.get("rank", 0)) == anchor_rank:
            return anchor
    raise ValueError("invalid_anchor_reference")
