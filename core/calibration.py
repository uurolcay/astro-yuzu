"""Deterministic interpretation calibration based on structured feedback."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from core.anchors import ANCHOR_TYPE_LOOKUP, assemble_anchor_payload


def compute_calibration_adjustments(feedback_history) -> dict[str, Any]:
    rows = list(feedback_history or [])
    if not rows:
        return {
            "anchor_type_boosts": {},
            "domain_penalties": {},
            "narrative_flags": {},
        }

    anchor_ratings = defaultdict(list)
    domain_events = defaultdict(list)

    for row in rows:
        rating = _safe_float(row.get("user_rating"))
        label = str(row.get("feedback_label") or "").strip().lower()
        anchor_type = str(row.get("anchor_type") or "").strip()
        domain = str(row.get("domain") or "").strip()
        if anchor_type:
            anchor_ratings[anchor_type].append((rating, label))
        if domain:
            domain_events[domain].append((rating, label))

    anchor_type_boosts = {}
    for anchor_type, samples in anchor_ratings.items():
        avg_rating = sum(item[0] for item in samples) / max(len(samples), 1)
        helpful_ratio = _label_ratio(samples, {"very_helpful", "accurate"})
        boost = ((avg_rating - 3.0) / 2.0) * 0.08 + helpful_ratio * 0.05
        anchor_type_boosts[anchor_type] = round(_bounded(boost, -0.1, 0.15), 4)

    domain_penalties = {}
    narrative_flags = {}
    for domain, samples in domain_events.items():
        generic_ratio = _label_ratio(samples, {"too_generic", "not_relevant"})
        unclear_ratio = _label_ratio(samples, {"unclear"})
        penalty = -((generic_ratio * 0.08) + (unclear_ratio * 0.04))
        if abs(penalty) > 0:
            domain_penalties[domain] = round(_bounded(penalty, -0.12, 0.0), 4)
        if generic_ratio >= 0.4 and len(samples) >= 2:
            narrative_flags[f"too_generic_{domain}_language"] = True

    return {
        "anchor_type_boosts": anchor_type_boosts,
        "domain_penalties": domain_penalties,
        "narrative_flags": narrative_flags,
    }


def build_personalization_summary(feedback_history) -> dict[str, Any]:
    rows = list(feedback_history or [])
    if not rows:
        return {
            "strongest_positive_anchor_types": [],
            "weakest_domains": [],
            "preferred_tone": "premium_clear",
            "feedback_volume": 0,
        }

    adjustments = compute_calibration_adjustments(rows)
    strongest_positive_anchor_types = [
        key for key, value in sorted(adjustments["anchor_type_boosts"].items(), key=lambda item: item[1], reverse=True) if value > 0
    ][:3]
    weakest_domains = [
        key for key, value in sorted(adjustments["domain_penalties"].items(), key=lambda item: item[1]) if value < 0
    ][:3]
    preferred_tone = "clear_direct" if any("too_generic" in str(row.get("feedback_label")) for row in rows) else "premium_clear"
    return {
        "strongest_positive_anchor_types": strongest_positive_anchor_types,
        "weakest_domains": weakest_domains,
        "preferred_tone": preferred_tone,
        "feedback_volume": len(rows),
    }


def apply_calibration(prioritized_signals, anchors, calibration_adjustments):
    adjustments = calibration_adjustments or {
        "anchor_type_boosts": {},
        "domain_penalties": {},
        "narrative_flags": {},
    }

    calibrated_signals = []
    for signal in prioritized_signals:
        domains = signal.get("domains") or []
        anchor_type = _infer_anchor_type(domains)
        domain_adjustment = min((adjustments["domain_penalties"].get(domain, 0.0) for domain in domains), default=0.0)
        anchor_adjustment = adjustments["anchor_type_boosts"].get(anchor_type, 0.0)
        net_adjustment = round(_bounded(anchor_adjustment + domain_adjustment, -0.12, 0.15), 4)
        calibrated_signals.append({
            **signal,
            "calibration_adjustment": net_adjustment,
            "calibrated_score": round(float(signal.get("score", 0.0)) * (1.0 + net_adjustment), 4),
        })
    calibrated_signals.sort(key=lambda item: item.get("calibrated_score", item.get("score", 0.0)), reverse=True)

    calibrated_anchors = []
    anchor_notes = []
    sorted_anchors = []
    for anchor in anchors:
        domains = anchor.get("domains", [])
        domain_adjustment = min((adjustments["domain_penalties"].get(domain, 0.0) for domain in domains), default=0.0)
        anchor_adjustment = adjustments["anchor_type_boosts"].get(anchor.get("anchor_type"), 0.0)
        net_adjustment = round(_bounded(anchor_adjustment + domain_adjustment, -0.12, 0.15), 4)
        signal_score = sum(_safe_float(item.get("score")) for item in anchor.get("supporting_signals", [])) or 1.0
        sorted_anchors.append({
            **anchor,
            "calibration_adjustment": net_adjustment,
            "calibrated_weight": round(signal_score * (1.0 + net_adjustment), 4),
        })
        if net_adjustment:
            anchor_notes.append(
                f"{anchor.get('title')} adjusted by {net_adjustment:+.2f} from feedback on {anchor.get('anchor_type')} and {', '.join(domains) or 'general'}."
            )

    sorted_anchors.sort(key=lambda item: item.get("calibrated_weight", 0.0), reverse=True)
    for rank, anchor in enumerate(sorted_anchors, start=1):
        calibrated_anchors.append({**anchor, "rank": rank})

    return {
        "prioritized_signals": calibrated_signals,
        "anchors": assemble_anchor_payload(calibrated_anchors[:3], []),
        "anchor_calibration_notes": anchor_notes,
    }


def _infer_anchor_type(domains):
    primary_domain = domains[0] if domains else "growth"
    return ANCHOR_TYPE_LOOKUP.get(primary_domain, "core_life_theme")


def _label_ratio(samples, labels):
    if not samples:
        return 0.0
    match_count = sum(1 for _, label in samples if label in labels)
    return match_count / len(samples)


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _bounded(value, lower, upper):
    return max(lower, min(upper, value))
