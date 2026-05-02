from __future__ import annotations

from copy import deepcopy
from typing import Any


VALID_REPORT_TYPES = {"birth_chart_karma", "career", "annual_transit", "parent_child"}

SECTION_TITLES = {
    "tr": {
        "identity_layer": "Temel Kimlik",
        "core_drivers": "Ana Yönlendiriciler",
        "dominant_signals": "Baskın Sinyaller",
        "interaction_layer": "Dinamikler ve Etkileşimler",
        "risk_opportunity_map": "Risk ve Fırsatlar",
        "timing_engine": "Zamanlama Analizi",
        "action_engine": "Önerilen Aksiyonlar",
        "strategic_summary": "Stratejik Özet",
        "child_nature": "Çocuk Doğası",
        "parent_influence": "Ebeveyn Etkisi",
        "interaction_loops": "Etkileşim Döngüleri",
        "parenting_actions": "Ebeveynlik Aksiyonları",
    },
    "en": {
        "identity_layer": "Identity Layer",
        "core_drivers": "Core Drivers",
        "dominant_signals": "Dominant Signals",
        "interaction_layer": "Interaction Layer",
        "risk_opportunity_map": "Risk & Opportunity Map",
        "timing_engine": "Timing Engine",
        "action_engine": "Action Engine",
        "strategic_summary": "Strategic Summary",
        "child_nature": "Child Nature",
        "parent_influence": "Parent Influence",
        "interaction_loops": "Interaction Loops",
        "parenting_actions": "Parenting Actions",
    },
}


def build_report_structure_v3(signal_context, report_type, language):
    normalized_type = str(report_type or "birth_chart_karma").strip().lower()
    if normalized_type not in VALID_REPORT_TYPES:
        normalized_type = "birth_chart_karma"
    normalized_language = "tr" if str(language or "tr").strip().lower() == "tr" else "en"
    context = deepcopy(signal_context or {})
    if not _has_signal_content(context):
        return {}

    if normalized_type == "parent_child":
        structure = _build_parent_child_structure(context, normalized_language)
    else:
        structure = _build_single_profile_structure(context, normalized_type, normalized_language)

    structure["report_type"] = normalized_type
    structure["language"] = normalized_language
    structure["section_titles"] = SECTION_TITLES[normalized_language]
    structure["v3_enabled"] = True
    return structure


def _has_signal_content(context):
    return any(
        bool(context.get(key))
        for key in (
            "dominant_signals",
            "risk_signals",
            "opportunity_signals",
            "nakshatra_signals",
            "yoga_signals",
            "atmakaraka_signals",
            "dasha_signal_bundle",
            "transit_trigger_signals",
            "parent_child_interaction_signals",
        )
    )


def _build_single_profile_structure(context, report_type, language):
    identity_layer = _build_identity_layer(context, language)
    core_drivers = _build_core_drivers(context, language)[:4]
    dominant_signals = _normalize_signal_rows(context.get("dominant_signals") or [], language, limit=6)
    interaction_layer = _derive_internal_interactions(context, language)
    risk_opportunity_map = {
        "risks": _normalize_signal_rows(context.get("risk_signals") or [], language, limit=4),
        "opportunities": _normalize_signal_rows(context.get("opportunity_signals") or [], language, limit=4),
    }
    prediction_fusion = _prediction_fusion_context(context)
    if prediction_fusion.get("available"):
        risk_opportunity_map["risk_windows"] = list(prediction_fusion.get("risk_windows") or [])[:4]
        risk_opportunity_map["opportunity_windows"] = list(prediction_fusion.get("opportunity_windows") or [])[:4]
    timing_engine = _build_timing_engine(context, language)
    action_engine = _build_action_engine(
        context,
        language,
        report_type=report_type,
        interaction_layer=interaction_layer,
        timing_engine=timing_engine,
    )
    strategic_summary = _build_strategic_summary(
        context,
        language,
        report_type=report_type,
        identity_layer=identity_layer,
        core_drivers=core_drivers,
        interaction_layer=interaction_layer,
        timing_engine=timing_engine,
    )

    if report_type == "annual_transit":
        dominant_signals = dominant_signals[:3]
        identity_layer["short_reference_only"] = True
        strategic_summary["decision_frame"] = (
            "Bu yapı esas olarak zamanlama ve tetiklenme pencerelerini öne çıkarır."
            if language == "tr"
            else "This structure primarily emphasizes timing and delivery windows."
        )
    elif report_type == "career":
        interaction_layer = [row for row in interaction_layer if "career" in (row.get("domains") or [])] or interaction_layer[:2]
        risk_opportunity_map["career_focus"] = True
        action_engine = [row for row in action_engine if row.get("domain") == "career"] or action_engine

    return {
        "identity_layer": identity_layer,
        "core_drivers": core_drivers,
        "dominant_signals": dominant_signals,
        "interaction_layer": interaction_layer,
        "risk_opportunity_map": risk_opportunity_map,
        "timing_engine": timing_engine,
        "action_engine": action_engine,
        "strategic_summary": strategic_summary,
    }


def _build_parent_child_structure(context, language):
    child_context = context.get("child_profile_signals") or context.get("child_astro_signal_context") or {}
    parent_context = context.get("parent_profile_signals") or context.get("parent_astro_signal_context") or {}
    interaction_bundle = context.get("parent_child_interaction_signals") or {}

    child_identity = _build_identity_layer(child_context or context, language)
    parent_influence = {
        "atmakaraka": _extract_atmakaraka_row(parent_context, language),
        "dominant_signals": _normalize_signal_rows((parent_context.get("dominant_signals") or []), language, limit=3),
    }
    interaction_layer = _normalize_interaction_rows(interaction_bundle.get("interaction_patterns") or [], language, limit=6)
    risk_loops = _normalize_interaction_rows(interaction_bundle.get("risk_loops") or [], language, limit=4, kind="risk_loop")
    support_patterns = _normalize_interaction_rows(interaction_bundle.get("support_patterns") or [], language, limit=4, kind="support")
    timing_engine = _build_timing_engine(child_context or context, language)
    action_engine = _normalize_parenting_actions(interaction_bundle.get("recommended_parent_actions") or [], language, limit=5)
    strategic_summary = {
        "headline": (
            "Çocuğun temel doğasını, ebeveyn etkisini ve tekrar eden ilişki döngülerini birlikte okuyan yapı."
            if language == "tr"
            else "A structure that reads child nature, parent influence, and repeating relational loops together."
        ),
        "decision_frame": (
            "Öncelik, sabit uyum etiketi değil; tetikleyici, yanlış okuma ve onarım ritmini görmektir."
            if language == "tr"
            else "The priority is not a fixed compatibility label but the trigger, misreading, and repair rhythm."
        ),
        "confidence_notes": list(interaction_bundle.get("confidence_notes") or []),
    }
    return {
        "identity_layer": child_identity,
        "core_drivers": _build_core_drivers(child_context or context, language)[:4],
        "dominant_signals": _normalize_signal_rows((child_context.get("dominant_signals") or context.get("dominant_signals") or []), language, limit=5),
        "interaction_layer": interaction_layer,
        "risk_opportunity_map": {
            "risks": _normalize_signal_rows((child_context.get("risk_signals") or []), language, limit=4),
            "opportunities": _normalize_signal_rows((child_context.get("opportunity_signals") or []), language, limit=4),
            "risk_loops": risk_loops,
            "support_patterns": support_patterns,
        },
        "timing_engine": timing_engine,
        "action_engine": action_engine,
        "strategic_summary": strategic_summary,
        "child_nature": child_identity,
        "parent_influence": parent_influence,
        "interaction_loops": interaction_layer,
        "parenting_actions": action_engine,
    }


def _build_identity_layer(context, language):
    nakshatra_signals = context.get("nakshatra_signals") or {}
    moon = nakshatra_signals.get("moon_nakshatra") or {}
    lagna = nakshatra_signals.get("lagna_nakshatra") or {}
    atmakaraka = _extract_atmakaraka_row(context, language)
    dominant_nakshatra = moon or lagna
    return {
        "lagna": _label_and_summary(lagna, language),
        "moon": _label_and_summary(moon, language),
        "atmakaraka": atmakaraka,
        "dominant_nakshatra": _label_and_summary(dominant_nakshatra, language),
    }


def _build_core_drivers(context, language):
    rows = []
    atmakaraka = _extract_atmakaraka_row(context, language)
    if atmakaraka:
        rows.append(atmakaraka)
    yoga_signals = context.get("yoga_signals") or {}
    for item in (yoga_signals.get("detected_yogas") or [])[:3]:
        rows.append(
            {
                "label": item.get("yoga_name") or item.get("label") or _text("Yoga pattern", "Yoga örüntüsü", language),
                "summary": item.get("explanation") or item.get("base_condition") or "",
                "strength": item.get("strength") or item.get("confidence") or "",
                "source": "yoga",
            }
        )
    return rows


def _build_timing_engine(context, language):
    dasha_bundle = context.get("dasha_signal_bundle") or {}
    transit_bundle = context.get("transit_trigger_signals") or {}
    prediction_fusion = _prediction_fusion_context(context)
    triggers = []
    for item in (transit_bundle.get("delivery_events") or transit_bundle.get("transit_triggers") or [])[:5]:
        triggers.append(
            {
                "label": item.get("planet") or item.get("label") or _text("Trigger", "Tetik", language),
                "effect": item.get("effect") or "",
                "time_window": item.get("duration") or "",
                "explanation": item.get("explanation") or "",
            }
        )
    timing_engine = {
        "active_period": dasha_bundle.get("dasha_lord") or ((context.get("dasha_activation_signals") or {}).get("active_period") or {}).get("planet"),
        "active_nakshatra_patterns": list(dasha_bundle.get("active_nakshatra_patterns") or [])[:4],
        "transit_triggers": triggers,
        "blocked_events": list((transit_bundle.get("blocked_events") or []))[:3],
        "summary": (
            "Zamanlama, yalnızca natal vaat ve dasha desteği olan alanlarda ağırlık kazanır."
            if language == "tr"
            else "Timing is emphasized only where natal promise and dasha support are already present."
        ),
    }
    if prediction_fusion.get("available"):
        timing_engine["fusion_available"] = True
        timing_engine["fusion_signals"] = list(prediction_fusion.get("fusion_signals") or [])[:4]
        timing_engine["risk_windows"] = list(prediction_fusion.get("risk_windows") or [])[:3]
        timing_engine["opportunity_windows"] = list(prediction_fusion.get("opportunity_windows") or [])[:3]
        timing_engine["timing_summary"] = list(prediction_fusion.get("timing_summary") or [])[:4]
        timing_engine["summary"] = (
            "Zamanlama, natal vaat ile dasha/transit kesişiminin aktiflestirdigi temalar uzerinden dikkatle okunmali."
            if language == "tr"
            else "Timing should be read carefully through the themes activated where natal promise intersects with dasha and transit."
        )
    return timing_engine


def _build_action_engine(context, language, *, report_type, interaction_layer, timing_engine):
    actions = []
    if report_type == "parent_child":
        return actions
    for item in _normalize_signal_rows(context.get("opportunity_signals") or [], language, limit=2):
        actions.append(
            {
                "title": item.get("label"),
                "reason": item.get("summary") or item.get("explanation") or "",
                "time_window": timing_engine.get("active_period") or "",
                "domain": "career" if report_type == "career" else report_type,
            }
        )
    for item in _normalize_signal_rows(context.get("risk_signals") or [], language, limit=2):
        actions.append(
            {
                "title": _text("Slow down around", "Yavaşlatılması gereken alan", language) + f": {item.get('label')}",
                "reason": item.get("summary") or item.get("explanation") or "",
                "time_window": timing_engine.get("active_period") or "",
                "domain": "career" if report_type == "career" else report_type,
            }
        )
    for item in interaction_layer[:2]:
        if item.get("outcome"):
            actions.append(
                {
                    "title": item.get("label") or _text("Interaction adjustment", "Etkileşim ayarı", language),
                    "reason": item.get("outcome"),
                    "time_window": timing_engine.get("active_period") or "",
                    "domain": report_type,
                }
            )
    for signal in (timing_engine.get("fusion_signals") or [])[:2]:
        actions.append(
            {
                "title": (
                    f"Bu pencereyi kullan: {signal.get('title')}"
                    if language == "tr"
                    else f"Use this window for: {signal.get('title')}"
                ),
                "reason": signal.get("interpretation_hint") or "",
                "time_window": signal.get("dasha_driver") or timing_engine.get("active_period") or "",
                "domain": signal.get("domain") or ("career" if report_type == "career" else report_type),
            }
        )
    return actions[:5]


def _build_strategic_summary(context, language, *, report_type, identity_layer, core_drivers, interaction_layer, timing_engine):
    headline = (
        "Bu yapı, kimlik ekseni ile aktif sinyaller arasındaki karar kalitesini öne çıkarır."
        if language == "tr"
        else "This structure brings decision quality into focus through the identity axis and active signals."
    )
    if report_type == "career":
        headline = (
            "Kariyer kararı, görünürlük, otorite ve kaynak yönetimini birlikte okuyan sinyal yapısı."
            if language == "tr"
            else "A signal structure that reads career decisions, visibility, authority, and resource management together."
        )
    elif report_type == "annual_transit":
        headline = (
            "Ana vurgu, hangi dönemin gerçekten çalıştığını gösteren zamanlama ve tetiklenme mantığıdır."
            if language == "tr"
            else "The main emphasis is the timing and trigger logic showing which period is truly active."
        )
    strategic_summary = {
        "headline": headline,
        "identity_focus": identity_layer.get("dominant_nakshatra") or {},
        "driver_count": len(core_drivers),
        "interaction_count": len(interaction_layer),
        "timing_focus": timing_engine.get("active_period") or "",
        "confidence_notes": list(context.get("confidence_notes") or [])[:4],
    }
    if timing_engine.get("fusion_available"):
        strategic_summary["timing_confidence"] = (
            "Yumusak zamanlama baglami mevcut"
            if language == "tr"
            else "Soft timing context available"
        )
        strategic_summary["timing_focus"] = (
            (timing_engine.get("fusion_signals") or [{}])[0].get("theme")
            or strategic_summary["timing_focus"]
        )
    return strategic_summary


def _prediction_fusion_context(context):
    fusion = context.get("prediction_fusion") or {}
    return fusion if isinstance(fusion, dict) else {"available": False}


def _extract_atmakaraka_row(context, language):
    atmakaraka = context.get("atmakaraka_signals") or {}
    if not atmakaraka:
        return {}
    signals = atmakaraka.get("signals") or []
    first_signal = signals[0] if signals else {}
    return {
        "label": atmakaraka.get("atmakaraka_planet") or first_signal.get("label") or _text("Atmakaraka", "Atmakaraka", language),
        "summary": atmakaraka.get("soul_lesson") or first_signal.get("explanation") or "",
        "risk": atmakaraka.get("risk_pattern") or "",
        "evolution_path": atmakaraka.get("evolution_path") or "",
        "house_domain": atmakaraka.get("house_domain") or "",
        "source": "atmakaraka",
    }


def _derive_internal_interactions(context, language):
    dominant = _normalize_signal_rows(context.get("dominant_signals") or [], language, limit=6)
    interactions = []
    for left, right in zip(dominant, dominant[1:]):
        shared = sorted(set(left.get("categories") or []) & set(right.get("categories") or []))
        if not shared:
            continue
        interactions.append(
            {
                "label": f"{left.get('label')} + {right.get('label')}",
                "domains": shared,
                "outcome": (
                    f"{left.get('label')} ile {right.get('label')} aynı alanlarda birlikte çalışıyor."
                    if language == "tr"
                    else f"{left.get('label')} and {right.get('label')} are colliding in the same life domains."
                ),
            }
        )
    return interactions[:4]


def _normalize_signal_rows(rows, language, *, limit=5):
    normalized = []
    for row in rows[:limit]:
        normalized.append(
            {
                "key": row.get("key"),
                "label": row.get("label") or row.get("key") or _text("Signal", "Sinyal", language),
                "summary": row.get("explanation") or row.get("summary") or "",
                "strength": row.get("strength") or "",
                "categories": list(row.get("categories") or []),
                "tone": row.get("tone") or "",
                "source": row.get("source") or "",
            }
        )
    return normalized


def _normalize_interaction_rows(rows, language, *, limit=5, kind="interaction"):
    normalized = []
    for row in rows[:limit]:
        normalized.append(
            {
                "label": row.get("trigger_pair") or row.get("label") or _text("Interaction pattern", "Etkileşim örüntüsü", language),
                "trigger": row.get("trigger") or row.get("pattern") or row.get("loop") or "",
                "child_response": row.get("child_response") or "",
                "misinterpretation": row.get("misinterpretation") or "",
                "loop": row.get("loop") or row.get("pattern") or "",
                "outcome": row.get("outcome") or "",
                "intensity": row.get("intensity") or "",
                "kind": kind,
            }
        )
    return normalized


def _normalize_parenting_actions(rows, language, *, limit=5):
    normalized = []
    for row in rows[:limit]:
        normalized.append(
            {
                "title": row.get("do") or _text("Recommended parenting move", "Önerilen ebeveynlik adımı", language),
                "avoid": row.get("avoid") or "",
                "reason": row.get("reason") or "",
                "domain": "parenting",
            }
        )
    return normalized


def _label_and_summary(row, language):
    if not row:
        return {}
    if isinstance(row, dict):
        return {
            "label": row.get("label") or row.get("nakshatra") or row.get("planet") or _text("Signal", "Sinyal", language),
            "summary": row.get("explanation") or row.get("psychological_pattern") or row.get("summary") or "",
        }
    return {"label": str(row), "summary": ""}


def _text(en_value, tr_value, language):
    return tr_value if language == "tr" else en_value
