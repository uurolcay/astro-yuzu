"""Interpretation builder on top of prioritized astrological signals."""

from __future__ import annotations

from typing import Any

from core.anchors import assemble_anchor_payload, build_interpretation_anchors
from core.calibration import apply_calibration, build_personalization_summary, compute_calibration_adjustments
from core.domains import map_signals_to_domains
from core.recommendations import build_recommendations
from core.scoring import prioritize_signals
from core.signals import extract_signals


def build_interpretation_layer(natal_data, dasha_data=None, personalization=None, transit_data=None):
    personalization = personalization or {}
    feedback_history = personalization.get("user_feedback", [])
    raw_signals = extract_signals(natal_data, dasha_data)
    prioritized_signals = prioritize_signals(raw_signals, natal_data=natal_data, dasha_data=dasha_data, limit=6)
    domain_mapping = map_signals_to_domains(prioritized_signals)
    prioritized_signals = _attach_domains(prioritized_signals, domain_mapping)
    interpretation = build_interpretation(prioritized_signals, domain_mapping, personalization=personalization)
    anchors = build_interpretation_anchors(
        prioritized_signals,
        domain_mapping.get("domain_scores", {}),
        premium_interpretation=interpretation,
        personalization=personalization,
    )
    calibration_summary = compute_calibration_adjustments(feedback_history)
    personalization_summary = {
        **build_personalization_summary(feedback_history),
        **(personalization.get("recommendation_feedback_summary") or {}),
    }
    calibrated = apply_calibration(prioritized_signals, anchors.get("top_anchors", []), calibration_summary)
    calibrated_signals = calibrated.get("prioritized_signals", prioritized_signals)
    calibrated_anchors = assemble_anchor_payload(
        calibrated.get("anchors", {}).get("top_anchors", anchors.get("top_anchors", [])),
        anchors.get("confidence_notes", []),
    )
    calibrated_interpretation = build_interpretation(calibrated_signals, domain_mapping, personalization=personalization)
    recommendation_layer = build_recommendations(
        calibrated_signals,
        calibrated_anchors.get("top_anchors", []),
        dasha_data or [],
        transit_data=transit_data,
        calibration_summary=calibration_summary,
        personalization_summary=personalization_summary,
        anchor_calibration_notes=calibrated.get("anchor_calibration_notes", []),
    )
    return {
        "signals": raw_signals,
        "prioritized_signals": calibrated_signals,
        "domain_mapping": domain_mapping,
        "interpretation": calibrated_interpretation,
        "anchors": calibrated_anchors,
        "recommendation_layer": recommendation_layer,
        "feedback_ready": True,
        "calibration_summary": calibration_summary,
        "personalization_summary": personalization_summary,
        "anchor_calibration_notes": calibrated.get("anchor_calibration_notes", []),
        "personalization": {
            "past_readings": personalization.get("past_readings", []),
            "user_feedback": feedback_history,
            "dominant_patterns": personalization.get("dominant_patterns", []),
        },
    }


def build_interpretation(prioritized_signals, domain_mapping, personalization=None):
    personalization = personalization or {}
    summary = _build_summary(prioritized_signals, personalization)
    return {
        "summary": summary,
        "career": _build_domain_paragraph("career", domain_mapping),
        "money": _build_domain_paragraph("money", domain_mapping),
        "relationships": _build_domain_paragraph("relationships", domain_mapping),
        "inner_state": _build_domain_paragraph("inner_state", domain_mapping),
        "growth": _build_domain_paragraph("growth", domain_mapping),
        "key_advice": _build_key_advice(prioritized_signals),
        "risk_areas": _build_risk_areas(prioritized_signals),
    }


def _build_summary(prioritized_signals, personalization):
    top_signals = prioritized_signals[:2]
    if not top_signals:
        return "The chart is readable, but no dominant interpretation signal is strong enough to rank as a premium lead insight yet."
    lead = top_signals[0]
    secondary = top_signals[1] if len(top_signals) > 1 else None
    memory_note = ""
    if personalization.get("dominant_patterns"):
        memory_note = f" This also repeats an existing pattern around {personalization['dominant_patterns'][0]}."
    summary = (
        f"The chart is currently led by {lead['planet']} through {lead['type'].replace('_', ' ')}, "
        f"which makes this a high-priority theme around {', '.join(lead.get('tags', [])[:2])}."
    )
    if secondary:
        summary += (
            f" A second strong signal comes from {secondary['planet']}"
            f"{' and ' + secondary['other_planet'] if secondary.get('other_planet') else ''}, "
            f"adding emphasis to {', '.join(secondary.get('tags', [])[:2])}."
        )
    return summary + memory_note


def _build_domain_paragraph(domain, domain_mapping):
    signals = domain_mapping.get("domain_signals", {}).get(domain, [])
    if not signals:
        return "No dominant premium signal is concentrated in this area right now."
    lead = signals[0]
    domain_label = domain.replace("_", " ")
    emphasis = ", ".join(lead.get("tags", [])[:2]) or domain_label
    sentence = (
        f"In {domain_label}, the strongest signal comes from {lead['planet']} via {lead['type'].replace('_', ' ')}, "
        f"suggesting a period shaped by {emphasis}."
    )
    if lead.get("house"):
        sentence += f" House {lead['house']} placement makes this effect concrete rather than abstract."
    return sentence


def _build_key_advice(prioritized_signals):
    if not prioritized_signals:
        return "Wait for a clearer signal cluster before making a strong interpretation claim."
    lead = prioritized_signals[0]
    if "pressure" in lead.get("tags", []) or "friction" in lead.get("tags", []):
        return "Move with structure, not urgency. The chart rewards disciplined sequencing over reactive decisions."
    if "growth" in lead.get("tags", []) or "opportunity" in lead.get("tags", []):
        return "Use the active opening deliberately. The chart favors timely action when the opportunity is already visible."
    return "Follow the strongest repeating theme, not the loudest short-term distraction."


def _build_risk_areas(prioritized_signals):
    risk_tags = []
    for signal in prioritized_signals:
        for tag in signal.get("tags", []):
            if tag in {"pressure", "stress", "conflict", "loss", "obsession", "friction"} and tag not in risk_tags:
                risk_tags.append(tag)
    if not risk_tags:
        return "No severe concentration of risk signals stands above the rest. The main risk is dilution rather than collapse."
    return f"The main risk concentration sits around {', '.join(risk_tags[:3])}. This is where overreaction or under-clarity would cost the most."


def _attach_domains(prioritized_signals, domain_mapping):
    signal_domains = {}
    for domain, signals in domain_mapping.get("domain_signals", {}).items():
        for signal in signals:
            key = _signal_key(signal)
            signal_domains.setdefault(key, [])
            if domain not in signal_domains[key]:
                signal_domains[key].append(domain)

    enriched = []
    for signal in prioritized_signals:
        enriched.append({**signal, "domains": signal_domains.get(_signal_key(signal), [])})
    return enriched


def _signal_key(signal):
    return (
        signal.get("type"),
        signal.get("planet"),
        signal.get("other_planet"),
        signal.get("house"),
        signal.get("sign"),
    )
