"""Deterministic, feedback-aware recommendation layer for interpretation guidance."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

VALID_RECOMMENDATION_FEEDBACK_LABELS = {
    "very_useful",
    "makes_sense",
    "not_useful",
    "not_relevant",
}
VALID_RECOMMENDATION_FOLLOWUP_LABELS = {
    "useful",
    "helped_me",
    "not_relevant",
    "did_not_act",
}

ACTION_TEMPLATES = {
    "career": {
        "type": "action",
        "title": "Prioritize deliberate career positioning",
        "reasoning": "This recommendation is driven by current dasha emphasis and reinforced by chart themes around structured progress in work decisions.",
        "priority": "high",
        "verb": "prioritize",
    },
    "money": {
        "type": "avoidance",
        "title": "Delay major financial commitments",
        "reasoning": "This recommendation is driven by current dasha emphasis and reinforced by chart themes around caution in money decisions.",
        "priority": "high",
        "verb": "delay",
    },
    "relationships": {
        "type": "action",
        "title": "Have important conversations with more clarity",
        "reasoning": "This recommendation is driven by the current chart emphasis on relational honesty and cleaner emotional boundaries.",
        "priority": "medium",
        "verb": "have",
    },
    "inner_state": {
        "type": "focus",
        "title": "Rebuild routine and inner steadiness",
        "reasoning": "This recommendation is driven by a concentration of signals that reward steadier pacing, reflection, and better emotional regulation.",
        "priority": "high",
        "verb": "rebuild",
    },
    "growth": {
        "type": "timing",
        "title": "Use the current opening for targeted growth",
        "reasoning": "This recommendation is driven by the current dasha opening and reinforced by broader chart themes around visible expansion.",
        "priority": "medium",
        "verb": "use",
    },
}


def build_recommendations(
    prioritized_signals,
    anchors,
    dasha_data,
    transit_data=None,
    calibration_summary=None,
    personalization_summary=None,
    anchor_calibration_notes=None,
):
    calibration_summary = calibration_summary or {"anchor_type_boosts": {}, "domain_penalties": {}, "narrative_flags": {}}
    personalization_summary = personalization_summary or {}
    anchor_calibration_notes = anchor_calibration_notes or []
    transit_data = transit_data or []

    anchor_index = {anchor.get("rank"): anchor for anchor in anchors or []}
    grouped = _group_signals_by_domain(prioritized_signals or [])
    current_period = _active_dasha(dasha_data or [])
    next_period = dasha_data[1] if len(dasha_data or []) > 1 else None

    recommendations = []
    for domain, signals in grouped:
        template = ACTION_TEMPLATES.get(domain, ACTION_TEMPLATES["growth"])
        strongest_signal = signals[0]
        related_anchor = _find_related_anchor(anchors or [], domain)
        evidence_score = _evidence_score(signals, current_period, related_anchor)
        calibration_note, adjustment = _calibration_note(
            domain,
            related_anchor,
            calibration_summary,
            personalization_summary,
            anchor_calibration_notes,
        )
        confidence = round(_bounded(evidence_score + adjustment, 0.55, 0.92), 2)
        time_window = _recommendation_time_window(domain, current_period, next_period, transit_data)
        recommendations.append(
            {
                "type": template["type"],
                "title": template["title"],
                "time_window": time_window,
                "confidence": confidence,
                "reasoning": template["reasoning"],
                "linked_anchors": [
                    {"rank": related_anchor.get("rank"), "title": related_anchor.get("title")}
                ] if related_anchor else [],
                "priority": template["priority"],
                "domains": list(dict.fromkeys([domain] + (related_anchor.get("domains", []) if related_anchor else [])))[:2],
                "supporting_signals": [
                    {
                        "type": signal.get("type"),
                        "planet": signal.get("planet"),
                        "score": signal.get("score"),
                    }
                    for signal in signals[:2]
                ],
                "calibration_note": calibration_note,
            }
        )

    recommendations.sort(key=lambda item: (item["confidence"], _priority_rank(item["priority"])), reverse=True)
    top_recommendations = recommendations[:5]

    opportunity_windows = _window_rows("opportunity", top_recommendations)
    risk_windows = _window_rows("risk", top_recommendations)
    recommendation_notes = _recommendation_notes(top_recommendations, current_period, personalization_summary)

    return {
        "top_recommendations": top_recommendations[:5],
        "opportunity_windows": opportunity_windows,
        "risk_windows": risk_windows,
        "recommendation_notes": recommendation_notes,
    }


def derive_followup_time(recommendation, *, base_time=None):
    recommendation = recommendation or {}
    base_time = base_time or datetime.utcnow()
    time_window = str(recommendation.get("time_window") or "").strip().lower()
    if "4-6 weeks" in time_window or "4-8 weeks" in time_window:
        return base_time + timedelta(days=30)
    if "2-3 months" in time_window:
        return base_time + timedelta(days=60)
    if "current" in time_window and "phase" in time_window:
        return base_time + timedelta(days=45)
    if "transition" in time_window:
        return base_time + timedelta(days=21)
    return base_time + timedelta(days=45)


def compute_recommendation_feedback_summary(feedback_rows) -> dict[str, Any]:
    rows = list(feedback_rows or [])
    if not rows:
        return {
            "preferred_recommendation_types": [],
            "low_performing_domains": [],
            "action_rate": 0.0,
            "average_usefulness": 0.0,
            "followup_usefulness_rate": 0.0,
            "followup_action_rate": 0.0,
        }

    type_scores = defaultdict(list)
    domain_scores = defaultdict(list)
    acted_count = 0
    rating_values = []
    followup_rows = []

    for row in rows:
        recommendation_type = str(row.get("recommendation_type") or "").strip()
        domain = str(row.get("domain") or "").strip()
        label = str(row.get("user_feedback_label") or "").strip().lower()
        rating = _normalized_usefulness_score(row.get("user_rating"), label)
        if recommendation_type:
            type_scores[recommendation_type].append(rating)
        if domain:
            domain_scores[domain].append(rating)
        if row.get("acted_on"):
            acted_count += 1
        rating_values.append(rating)
        if str(row.get("feedback_source") or "").strip().lower() == "followup":
            followup_rows.append({"rating": rating, "acted_on": bool(row.get("acted_on"))})

    preferred_types = [
        key for key, value in sorted(type_scores.items(), key=lambda item: sum(item[1]) / len(item[1]), reverse=True)
        if (sum(value) / len(value)) >= 3.5
    ][:3]
    low_domains = [
        key for key, value in sorted(domain_scores.items(), key=lambda item: sum(item[1]) / len(item[1]))
        if (sum(value) / len(value)) <= 2.5
    ][:3]

    return {
        "preferred_recommendation_types": preferred_types,
        "low_performing_domains": low_domains,
        "action_rate": round(acted_count / len(rows), 4),
        "average_usefulness": round(sum(rating_values) / len(rating_values), 2) if rating_values else 0.0,
        "followup_usefulness_rate": round(sum(1 for item in followup_rows if item["rating"] >= 3.5) / len(followup_rows), 4) if followup_rows else 0.0,
        "followup_action_rate": round(sum(1 for item in followup_rows if item["acted_on"]) / len(followup_rows), 4) if followup_rows else 0.0,
    }


def _group_signals_by_domain(prioritized_signals):
    grouped = {}
    for signal in prioritized_signals:
        domains = signal.get("domains") or ["growth"]
        primary_domain = domains[0]
        grouped.setdefault(primary_domain, [])
        grouped[primary_domain].append(signal)
    ordered = []
    for domain, signals in grouped.items():
        sorted_signals = sorted(signals, key=lambda item: item.get("score", 0.0), reverse=True)
        ordered.append((domain, sorted_signals))
    ordered.sort(key=lambda item: item[1][0].get("score", 0.0), reverse=True)
    return ordered[:5]


def _find_related_anchor(anchors, domain):
    for anchor in anchors:
        if domain in (anchor.get("domains") or []):
            return anchor
    return anchors[0] if anchors else None


def _evidence_score(signals, current_period, related_anchor):
    lead_score = float(signals[0].get("score", 0.0)) if signals else 0.0
    anchor_weight = 0.05 * len((related_anchor or {}).get("supporting_signals", []))
    dasha_bonus = 0.0
    if current_period:
        active_planet = current_period.get("planet")
        if any(signal.get("planet") == active_planet for signal in signals):
            dasha_bonus = 0.08
    base = 0.58 + min(lead_score * 0.06, 0.22) + anchor_weight + dasha_bonus
    return round(_bounded(base, 0.55, 0.9), 4)


def _recommendation_time_window(domain, current_period, next_period, transit_data):
    if transit_data:
        top_transit = max(transit_data, key=lambda item: float(item.get("score", 0.0)), default=None)
        if top_transit and float(top_transit.get("score", 0.0)) >= 7:
            return "next 4-6 weeks"
    if current_period and current_period.get("start") and current_period.get("end"):
        return f"during the current {current_period.get('planet')} phase"
    if next_period and domain in {"career", "money"}:
        return "next 2-3 months"
    if domain in {"relationships", "inner_state"}:
        return "next 4-8 weeks"
    return "next 2-3 months"


def _calibration_note(domain, related_anchor, calibration_summary, personalization_summary, anchor_calibration_notes):
    adjustment = 0.0
    anchor_type = (related_anchor or {}).get("anchor_type")
    if anchor_type:
        adjustment += calibration_summary.get("anchor_type_boosts", {}).get(anchor_type, 0.0) * 0.35
    adjustment += calibration_summary.get("domain_penalties", {}).get(domain, 0.0) * 0.4
    adjustment = round(_bounded(adjustment, -0.03, 0.04), 4)

    note = ""
    if personalization_summary.get("preferred_tone") == "clear_direct" and adjustment >= 0:
        note = "Slightly prioritized due to stronger user response to clear_direct guidance."
    elif adjustment < 0:
        note = "Slightly softened because this domain has recently been rated as too generic."
    elif anchor_calibration_notes and anchor_type:
        note = anchor_calibration_notes[0]
    return note, adjustment


def _window_rows(kind, recommendations):
    rows = []
    for item in recommendations[:3]:
        if kind == "opportunity" and item["type"] in {"action", "focus", "timing"}:
            rows.append({"title": item["title"], "time_window": item["time_window"], "priority": item["priority"]})
        if kind == "risk" and item["type"] in {"avoidance", "timing"}:
            rows.append({"title": item["title"], "time_window": item["time_window"], "priority": item["priority"]})
    return rows


def _recommendation_notes(recommendations, current_period, personalization_summary):
    notes = []
    if current_period and current_period.get("planet"):
        notes.append(f"Current dasha emphasis is centered on {current_period['planet']}, so timing-sensitive guidance is weighted more heavily.")
    if personalization_summary.get("preferred_tone") == "clear_direct":
        notes.append("Recommendation wording stays more direct because recent feedback favored clearer guidance.")
    if personalization_summary.get("action_rate", 0.0) >= 0.6:
        notes.append("Recent recommendation feedback suggests users respond well to direct, action-oriented guidance.")
    if not recommendations:
        notes.append("No recommendation could be formed without a stable evidence cluster.")
    return notes[:3]


def _active_dasha(dasha_data):
    if not dasha_data:
        return None
    return dasha_data[0]


def _priority_rank(priority):
    return {"high": 3, "medium": 2, "low": 1}.get(priority, 0)


def _normalized_usefulness_score(user_rating, label):
    try:
        rating = int(user_rating)
        if 1 <= rating <= 5:
            return float(rating)
    except (TypeError, ValueError):
        pass
    if label in {"very_useful", "makes_sense"}:
        return 4.0 if label == "very_useful" else 3.5
    if label in {"useful", "helped_me"}:
        return 4.0 if label == "helped_me" else 3.5
    if label == "not_useful":
        return 2.0
    if label in {"not_relevant", "did_not_act"}:
        return 1.5
    return 3.0


def _bounded(value, lower, upper):
    return max(lower, min(upper, value))
