import json
from datetime import datetime

import ai_interpreter as ai_logic
import database as db_mod
from sqlalchemy import inspect as sa_inspect
from services import ai_behavior_rules
from services import admin_astro_workspace as workspace


CHAT_MODES = {
    "grounded": "Grounded Analysis",
    "consultant": "Consultant Narrative",
}


SYSTEM_SAFEGUARDS = [
    "Do not assume planetary positions.",
    "Use only provided or computed astrology data.",
    "If data is missing, say so instead of inventing.",
    "Swiss Ephemeris-backed calculations and existing engines are the authority for chart facts.",
]


def normalize_chat_mode(value):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in CHAT_MODES else "grounded"


def _safe_model_id(instance):
    if instance is None:
        return None
    try:
        identity = sa_inspect(instance).identity
        if identity:
            return identity[0]
    except Exception:
        pass
    try:
        return getattr(instance, "id", None)
    except Exception:
        return None


def create_chat_session(db, *, profile=None, secondary_profile=None, title=None, report_type=None, mode="grounded", admin_user=None):
    session = db_mod.InternalChatSession(
        profile=profile,
        secondary_profile=secondary_profile,
        title=(str(title or "").strip() or _default_session_title(profile, secondary_profile)),
        report_type=workspace.normalize_workspace_report_type(report_type),
        mode=normalize_chat_mode(mode),
        created_by_user_id=_safe_model_id(admin_user),
    )
    db.add(session)
    return session


def _default_session_title(profile=None, secondary_profile=None):
    primary_name = getattr(profile, "full_name", None) or "Internal profile"
    secondary_name = getattr(secondary_profile, "full_name", None)
    return f"{primary_name} + {secondary_name}" if secondary_name else primary_name


def append_chat_message(db, session, *, role, message_text, tool_payload=None, context_snapshot=None):
    message = db_mod.InternalChatMessage(
        session=session,
        role=str(role or "").strip().lower(),
        message_text=str(message_text or "").strip(),
        tool_payload_json=json.dumps(tool_payload, ensure_ascii=False, default=str) if tool_payload is not None else None,
        context_snapshot_json=json.dumps(context_snapshot, ensure_ascii=False, default=str) if context_snapshot is not None else None,
    )
    db.add(message)
    session.updated_at = datetime.utcnow()
    return message


def get_saved_internal_interpretations(db, profile_id, secondary_profile_id=None, limit=3):
    if not profile_id:
        return []
    query = db.query(db_mod.InternalInterpretation).filter(
        (db_mod.InternalInterpretation.profile_id == profile_id)
        | (db_mod.InternalInterpretation.secondary_profile_id == profile_id)
    )
    if secondary_profile_id:
        query = query.filter(
            (db_mod.InternalInterpretation.profile_id == secondary_profile_id)
            | (db_mod.InternalInterpretation.secondary_profile_id == secondary_profile_id)
            | (db_mod.InternalInterpretation.secondary_profile_id.is_(None))
        )
    rows = query.order_by(db_mod.InternalInterpretation.created_at.desc()).limit(limit).all()
    return [
        {
            "id": item.id,
            "report_type": item.report_type,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "interpretation_text": item.interpretation_text,
        }
        for item in rows
    ]


def build_chat_context(*, session, question, chart_payload, saved_interpretations=None, recent_messages=None):
    chart_payload = chart_payload or {}
    behavior_rule_block = ai_behavior_rules.build_prompt_rule_blocks()
    astro_signal_context = chart_payload.get("astro_signal_context") or {}
    return {
        "workflow": "admin_astro_workspace_chat",
        "source": "admin_astro_workspace",
        "language": chart_payload.get("language") or "tr",
        "mode": normalize_chat_mode(getattr(session, "mode", None)),
        "mode_label": CHAT_MODES[normalize_chat_mode(getattr(session, "mode", None))],
        "safeguards": list(dict.fromkeys(SYSTEM_SAFEGUARDS + behavior_rule_block["immutable_rules"])),
        "ai_behavior_rules": behavior_rule_block,
        "admin_question": str(question or "").strip(),
        "profile": {
            "id": getattr(getattr(session, "profile", None), "id", None),
            "full_name": getattr(getattr(session, "profile", None), "full_name", None),
        },
        "secondary_profile": {
            "id": getattr(getattr(session, "secondary_profile", None), "id", None),
            "full_name": getattr(getattr(session, "secondary_profile", None), "full_name", None),
        },
        "chart_context": chart_payload,
        "astro_signal_context": astro_signal_context,
        "saved_internal_interpretations": saved_interpretations or [],
        "recent_messages": recent_messages or [],
        "instructions": _mode_instructions(normalize_chat_mode(getattr(session, "mode", None))),
    }


def _mode_instructions(mode):
    if mode == "consultant":
        return (
            "Answer in client-friendly consultation language, but stay grounded in the provided computed chart data. "
            "You may rewrite or soften previous interpretation text, but do not introduce unsupported chart claims."
        )
    return (
        "Answer as a strict chart-grounded astrology analyst. Prefer concrete computed signals, timing data, "
        "natal/transit/dasha context, and relationship context supplied in the payload."
    )


def build_chat_prompt_payload(chat_context, *, behavior_rules=None, runtime_overrides=None):
    return ai_behavior_rules.inject_rules_into_payload(
        chat_context,
        active_rules=behavior_rules,
        runtime_overrides=runtime_overrides,
        task_instruction="Answer the admin's follow-up question using only the computed chart context and saved internal context.",
    )


def generate_workspace_chat_reply(chat_context, *, generator=None, behavior_rules=None, runtime_overrides=None):
    prompt_payload = build_chat_prompt_payload(
        chat_context,
        behavior_rules=behavior_rules,
        runtime_overrides=runtime_overrides,
    )
    return (generator or ai_logic.generate_interpretation)(prompt_payload)


def create_grounded_reply(db, *, session, question, chart_payload, admin_user=None, generator=None, behavior_rules=None):
    saved = get_saved_internal_interpretations(
        db,
        getattr(session, "profile_id", None),
        getattr(session, "secondary_profile_id", None),
    )
    recent_messages = [
        {"role": item.role, "message_text": item.message_text}
        for item in (getattr(session, "messages", []) or [])[-8:]
    ]
    chat_context = build_chat_context(
        session=session,
        question=question,
        chart_payload=chart_payload,
        saved_interpretations=saved,
        recent_messages=recent_messages,
    )
    user_message = append_chat_message(db, session, role="user", message_text=question, context_snapshot=chat_context)
    reply_text = generate_workspace_chat_reply(chat_context, generator=generator, behavior_rules=behavior_rules)
    assistant_message = append_chat_message(
        db,
        session,
        role="assistant",
        message_text=reply_text,
        tool_payload={"used_tools": resolve_chat_tools(chat_context)},
        context_snapshot=chat_context,
    )
    return {
        "reply_text": reply_text,
        "context": chat_context,
        "user_message": user_message,
        "assistant_message": assistant_message,
    }


def resolve_chat_tools(chat_context):
    tools = ["get_profile_context", "get_natal_summary", "get_saved_internal_interpretations"]
    if (chat_context.get("chart_context") or {}).get("workspace_report_type") == "parent_child":
        tools.append("get_parent_child_context")
    question = str(chat_context.get("admin_question") or "").lower()
    if any(term in question for term in ("timing", "transit", "2026", "window", "zaman")):
        tools.append("get_transit_context")
    if any(term in question for term in ("career", "work", "meslek", "kariyer")):
        tools.append("get_career_signals")
    return tools
