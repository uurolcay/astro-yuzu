from __future__ import annotations

from core.child_guidance import (
    build_child_core_nature,
    build_child_growth_path,
    build_child_timing_notes,
    build_learning_school_pattern,
)
from core.parent_child_matching import (
    build_parent_child_anchors,
    build_parent_child_dynamics,
    build_parent_child_recommendations,
    build_parenting_guidance,
)


def build_parent_child_interpretation(parent_bundle, child_bundle):
    child_profile = build_child_core_nature(child_bundle)
    school_guidance = build_learning_school_pattern(child_bundle)
    growth_guidance = build_child_growth_path(child_bundle)
    timing_notes = build_child_timing_notes(child_bundle)
    relationship_dynamics = build_parent_child_dynamics(parent_bundle, child_bundle)
    parenting_guidance = build_parenting_guidance(parent_bundle, child_bundle)
    top_anchors = build_parent_child_anchors(child_profile, relationship_dynamics, parenting_guidance)
    recommendation_layer = build_parent_child_recommendations(
        child_profile,
        school_guidance,
        parenting_guidance,
        timing_notes,
    )
    watch_areas = [
        *child_profile.get("sensitivity_areas", []),
        *relationship_dynamics.get("friction_points", []),
        *parenting_guidance.get("avoid_patterns", []),
    ]
    watch_areas = [item for idx, item in enumerate(watch_areas) if item and item not in watch_areas[:idx]]
    child_name = child_bundle.get("name") or "Child"
    parent_name = parent_bundle.get("name") or "Parent"
    summary = (
        f"{child_name} shows a clear inner pattern that responds strongly to emotional tone and pacing. "
        f"The relationship with {parent_name} grows best when guidance stays calm, specific, and emotionally attuned."
    )
    return {
        "report_type": "parent_child",
        "primary_focus": "parent_child_guidance",
        "secondary_focus": "child_growth",
        "confidence_level": "high",
        "decision_posture": "support",
        "timing_strategy": "advisory",
        "dominant_narratives": ["parent_child_guidance"],
        "dominant_life_areas": ["family"],
        "summary": summary,
        "child_profile": child_profile,
        "relationship_dynamics": relationship_dynamics,
        "school_guidance": school_guidance,
        "parenting_guidance": parenting_guidance,
        "watch_areas": watch_areas[:5],
        "growth_guidance": growth_guidance,
        "timing_notes": timing_notes,
        "signal_layer": {
            "top_anchors": top_anchors,
            "recommendation_layer": recommendation_layer,
        },
        "recommendation_layer": recommendation_layer,
        "top_anchors": top_anchors,
        "parent_profile": {
            "name": parent_name,
            "birth_summary": parent_bundle.get("birth_summary"),
        },
        "child_profile_meta": {
            "name": child_name,
            "birth_summary": child_bundle.get("birth_summary"),
        },
    }


def build_parent_child_ai_summary(interpretation_context):
    child_profile = interpretation_context.get("child_profile") or {}
    relationship_dynamics = interpretation_context.get("relationship_dynamics") or {}
    school_guidance = interpretation_context.get("school_guidance") or {}
    parenting_guidance = interpretation_context.get("parenting_guidance") or {}
    growth_guidance = interpretation_context.get("growth_guidance") or {}
    watch_areas = interpretation_context.get("watch_areas") or []
    timing_notes = interpretation_context.get("timing_notes") or []
    lines = [
        "### Child Core Nature",
        child_profile.get("temperament") or "",
        child_profile.get("emotional_needs") or "",
        "",
        "### Parent-Child Dynamic",
        relationship_dynamics.get("emotional_compatibility") or "",
        relationship_dynamics.get("communication_style_difference") or "",
        "",
        "### School & Growth Guidance",
        school_guidance.get("learning_style") or "",
        school_guidance.get("attention_tendency") or "",
        school_guidance.get("expression_pattern") or "",
        "",
        "### Recommended Approach",
        parenting_guidance.get("best_support") or "",
        parenting_guidance.get("communication_guidance") or "",
    ]
    if watch_areas:
        lines.extend(["", "### Watch Areas"])
        lines.extend(f"- {item}" for item in watch_areas[:4])
    if growth_guidance:
        lines.extend([
            "",
            "### Growth Path",
            growth_guidance.get("thrive_support") or "",
            growth_guidance.get("relationship_strengthening") or "",
            growth_guidance.get("next_growth_theme") or "",
        ])
    if timing_notes:
        lines.extend([
            "",
            "### Timing Notes",
            f"- {timing_notes[0].get('title')}: {timing_notes[0].get('note')} ({timing_notes[0].get('time_window')})",
        ])
    return "\n".join(item for item in lines if item is not None)
