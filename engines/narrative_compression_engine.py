from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any


NARRATIVE_ARCHETYPES = {
    "career_transition": {
        "life_areas": {"career", "life_direction", "finances"},
        "themes": {"career_pressure", "career_growth", "responsibility_phase", "stability_building"},
        "summary": "A period of professional restructuring, increased responsibility, and strategic redirection.",
        "psychological_meaning": "identity maturation through work and responsibility",
        "external_manifestation": "career restructuring",
        "recommended_focus": "long term planning",
        "risk_factor": "burnout risk",
        "growth_potential": "high stability potential",
    },
    "relationship_transition": {
        "life_areas": {"relationships", "family", "personal_growth"},
        "themes": {"relationship_testing", "relationship_opportunity", "closure_cycle", "emotional_reset"},
        "summary": "Relationship patterns are shifting, requiring honesty, emotional processing, and relational clarity.",
        "psychological_meaning": "relational re-patterning",
        "external_manifestation": "relationship transition",
        "recommended_focus": "clear communication",
        "risk_factor": "emotional reactivity",
        "growth_potential": "deeper intimacy potential",
    },
    "financial_restructuring": {
        "life_areas": {"finances", "career", "home"},
        "themes": {"financial_focus", "financial_pressure", "stability_building"},
        "summary": "Financial priorities are reorganizing, pushing stronger structure, realism, and resource management.",
        "psychological_meaning": "security recalibration",
        "external_manifestation": "financial restructuring",
        "recommended_focus": "budget discipline",
        "risk_factor": "scarcity stress",
        "growth_potential": "stronger foundation potential",
    },
    "identity_reinvention": {
        "life_areas": {"personal_growth", "life_direction", "career"},
        "themes": {"identity_transformation", "independence_drive", "life_direction_shift"},
        "summary": "Identity is being reshaped, pushing reinvention, autonomy, and a new direction.",
        "psychological_meaning": "deep self-redefinition",
        "external_manifestation": "identity reinvention",
        "recommended_focus": "aligned choices",
        "risk_factor": "fragmented direction",
        "growth_potential": "high authenticity potential",
    },
    "emotional_healing": {
        "life_areas": {"personal_growth", "family", "relationships"},
        "themes": {"emotional_reset", "closure_cycle", "spiritual_search"},
        "summary": "Emotional material is surfacing for release, reflection, and healing integration.",
        "psychological_meaning": "emotional processing",
        "external_manifestation": "healing cycle",
        "recommended_focus": "rest and reflection",
        "risk_factor": "emotional overwhelm",
        "growth_potential": "deep healing potential",
    },
    "responsibility_cycle": {
        "life_areas": {"career", "life_direction", "personal_growth"},
        "themes": {"responsibility_phase", "career_pressure", "stability_building"},
        "summary": "A responsibility-heavy chapter is asking for discipline, patience, and durable commitments.",
        "psychological_meaning": "maturity under pressure",
        "external_manifestation": "responsibility cycle",
        "recommended_focus": "structured effort",
        "risk_factor": "fatigue accumulation",
        "growth_potential": "long-term authority building",
    },
    "growth_opportunity": {
        "life_areas": {"career", "finances", "social_network"},
        "themes": {"career_growth", "opportunity_window", "new_beginning"},
        "summary": "Expansion signals are active, opening opportunities for progress, visibility, and growth.",
        "psychological_meaning": "confidence expansion",
        "external_manifestation": "opportunity phase",
        "recommended_focus": "timely action",
        "risk_factor": "overextension",
        "growth_potential": "high opportunity yield",
    },
    "life_redirection": {
        "life_areas": {"life_direction", "personal_growth", "career"},
        "themes": {"life_direction_shift", "new_beginning", "closure_cycle"},
        "summary": "Life direction is reorienting, asking for closure, decisions, and a revised path forward.",
        "psychological_meaning": "path correction",
        "external_manifestation": "life redirection",
        "recommended_focus": "decision clarity",
        "risk_factor": "indecision drag",
        "growth_potential": "clearer trajectory potential",
    },
    "inner_transformation": {
        "life_areas": {"personal_growth", "spirituality", "life_direction"},
        "themes": {"spiritual_search", "identity_transformation", "closure_cycle"},
        "summary": "A deep inner transformation is underway, shifting values, meaning, and psychological orientation.",
        "psychological_meaning": "inner reorganization",
        "external_manifestation": "inner transformation",
        "recommended_focus": "inner alignment",
        "risk_factor": "dissolution confusion",
        "growth_potential": "profound renewal potential",
    },
    "stability_building": {
        "life_areas": {"home", "finances", "career"},
        "themes": {"stability_building", "responsibility_phase", "financial_focus"},
        "summary": "This period emphasizes building stability through consistent choices and practical structure.",
        "psychological_meaning": "foundation reinforcement",
        "external_manifestation": "stability building",
        "recommended_focus": "steady consistency",
        "risk_factor": "rigidity",
        "growth_potential": "durable security potential",
    },
    "release_and_closure": {
        "life_areas": {"relationships", "personal_growth", "life_direction"},
        "themes": {"closure_cycle", "emotional_reset", "relationship_testing"},
        "summary": "Release and closure themes are active, helping old cycles complete and make room for change.",
        "psychological_meaning": "completion and release",
        "external_manifestation": "ending cycle",
        "recommended_focus": "conscious letting go",
        "risk_factor": "clinging to expired patterns",
        "growth_potential": "renewal through completion",
    },
    "expansion_period": {
        "life_areas": {"career", "social_network", "education"},
        "themes": {"career_growth", "opportunity_window", "spiritual_search"},
        "summary": "An expansion period is opening wider possibilities through growth, learning, and visibility.",
        "psychological_meaning": "horizon broadening",
        "external_manifestation": "expansion period",
        "recommended_focus": "strategic openness",
        "risk_factor": "scattered priorities",
        "growth_potential": "broad advancement potential",
    },
    "pressure_test_phase": {
        "life_areas": {"career", "relationships", "finances"},
        "themes": {"career_pressure", "relationship_testing", "financial_pressure", "power_struggle"},
        "summary": "A pressure test phase is exposing weak points and demanding resilience, strategy, and restraint.",
        "psychological_meaning": "stress testing of structures",
        "external_manifestation": "high-pressure phase",
        "recommended_focus": "prioritization and resilience",
        "risk_factor": "conflict escalation",
        "growth_potential": "stronger systems after correction",
    },
}


def compress_ai_narratives(
    scored_events: list[dict[str, Any]],
    psychological_themes: dict[str, Any],
    life_area_analysis: dict[str, Any],
) -> dict[str, Any]:
    important_events = [event for event in scored_events if float(event.get("importance_score", 0)) >= 55]
    theme_lookup = _theme_lookup(psychological_themes)
    area_lookup = _area_lookup(life_area_analysis)

    narratives: list[dict[str, Any]] = []

    for narrative_type, config in NARRATIVE_ARCHETYPES.items():
        matched_areas = [area for area in config["life_areas"] if area in area_lookup]
        matched_themes = [theme for theme in config["themes"] if theme in theme_lookup]
        key_events = _matching_events(important_events, config["life_areas"], config["themes"])

        if len(matched_areas) < 1 or len(matched_themes) < 2 or len(key_events) < 1:
            continue

        primary_area = max(matched_areas, key=lambda area: area_lookup[area]["score"])
        top_theme = max(matched_themes, key=lambda theme: theme_lookup[theme]["score"])
        top_event = max(key_events, key=lambda event: float(event.get("importance_score", 0)))

        narrative_score = min(
            100,
            round(
                area_lookup[primary_area]["score"]
                + theme_lookup[top_theme]["score"]
                + float(top_event.get("importance_score", 0)) / 2
            ),
        )
        if narrative_score < 60:
            continue

        merged_dates = _merged_dates(
            [area_lookup[area] for area in matched_areas],
            [theme_lookup[theme] for theme in matched_themes],
            key_events,
        )

        narratives.append(
            {
                "narrative_type": narrative_type,
                "narrative_score": narrative_score,
                "level": _narrative_level(narrative_score),
                "primary_life_area": primary_area,
                "supporting_life_areas": [area for area in matched_areas if area != primary_area][:2],
                "dominant_themes": sorted(matched_themes, key=lambda theme: theme_lookup[theme]["score"], reverse=True)[:3],
                "key_events": [event.get("event_id") for event in sorted(key_events, key=lambda item: float(item.get("importance_score", 0)), reverse=True)[:3]],
                "start": merged_dates["start"],
                "peak": merged_dates["peak"],
                "resolution": merged_dates["resolution"],
                "narrative_summary": _truncate_words(config["summary"], 25),
                "narrative_psychological_meaning": config["psychological_meaning"],
                "narrative_external_manifestation": config["external_manifestation"],
                "recommended_focus": config["recommended_focus"],
                "risk_factor": config["risk_factor"],
                "growth_potential": config["growth_potential"],
                "intensity": _narrative_intensity(key_events, life_area_analysis, psychological_themes),
            }
        )

    merged_narratives = _merge_narratives(narratives)
    merged_narratives.sort(key=lambda item: item["narrative_score"], reverse=True)

    weights = _story_weights(merged_narratives)
    primary_narratives = merged_narratives[:3]
    secondary_narratives = merged_narratives[3:6]
    emerging_narratives = [narrative for narrative in merged_narratives if 52 <= narrative["narrative_score"] < 60][:3]

    return {
        "primary_narratives": primary_narratives,
        "secondary_narratives": secondary_narratives,
        "emerging_narratives": emerging_narratives,
        "narrative_weights": weights,
        "life_period_summary": _life_period_summary(primary_narratives),
        "interpretation_strategy": "story_based" if merged_narratives else "mixed",
    }


def _theme_lookup(psychological_themes: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for bucket in ("dominant_themes", "active_themes"):
        for theme in psychological_themes.get(bucket, []):
            lookup[theme["theme"]] = theme
    return lookup


def _area_lookup(life_area_analysis: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for bucket in ("dominant_life_areas", "active_life_areas"):
        for area in life_area_analysis.get(bucket, []):
            lookup[area["life_area"]] = area
    return lookup


def _matching_events(
    events: list[dict[str, Any]],
    life_areas: set[str],
    themes: set[str],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    theme_names = {theme.replace("_", " ") for theme in themes}
    for event in events:
        event_text = " ".join(
            str(event.get(key, ""))
            for key in ("event_type", "dominant_activation_type", "event_id")
        ).lower()
        house = event.get("house") or event.get("moon_house") or event.get("new_house") or event.get("sun_house")

        if "career" in life_areas and house in {10, 2, 11}:
            matches.append(event)
            continue
        if "relationships" in life_areas and house == 7:
            matches.append(event)
            continue
        if "finances" in life_areas and house in {2, 8, 11}:
            matches.append(event)
            continue
        if "personal_growth" in life_areas and house in {1, 8, 12}:
            matches.append(event)
            continue
        if any(theme_name in event_text for theme_name in theme_names):
            matches.append(event)
            continue
        if event.get("importance_level") in {"critical", "major"}:
            matches.append(event)
    return _unique_events(matches)


def _merged_dates(
    areas: list[dict[str, Any]],
    themes: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, str]:
    starts = [_parse_date(area.get("start_date")) for area in areas]
    starts.extend(_parse_date(theme.get("start_date")) for theme in themes)
    starts.extend(_parse_date(event.get("date")) for event in events)

    peaks = [_parse_date(area.get("peak_date")) for area in areas]
    peaks.extend(_parse_date(theme.get("peak_date")) for theme in themes)
    peaks.append(_parse_date(max(events, key=lambda item: float(item.get("importance_score", 0))).get("date")))

    ends = [_parse_date(area.get("projected_stabilization")) for area in areas]
    ends.extend(_parse_date(theme.get("projected_end")) for theme in themes)
    ends.extend(_parse_date(event.get("date")) for event in events)

    return {
        "start": min(starts).strftime("%Y-%m-%d"),
        "peak": max(peaks).strftime("%Y-%m-%d"),
        "resolution": max(ends).strftime("%Y-%m-%d"),
    }


def _narrative_level(score: int) -> str:
    if score >= 90:
        return "dominant"
    if score >= 80:
        return "major"
    return "active"


def _narrative_intensity(
    events: list[dict[str, Any]],
    life_area_analysis: dict[str, Any],
    psychological_themes: dict[str, Any],
) -> str:
    signal = 0
    if any((event.get("dominant_activation_type") == "outer_planet_event") for event in events):
        signal += 2
    if life_area_analysis.get("life_area_clusters"):
        signal += 1
    if psychological_themes.get("change_intensity") in {"high", "extreme"}:
        signal += 2
    if signal >= 4:
        return "defining life period"
    if signal == 3:
        return "major life chapter"
    if signal == 2:
        return "active story"
    return "background story"


def _merge_narratives(narratives: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for narrative in narratives:
        found = None
        for existing in merged:
            if _should_merge(existing, narrative):
                found = existing
                break
        if found is None:
            merged.append(narrative)
            continue

        found["narrative_score"] = min(100, max(found["narrative_score"], narrative["narrative_score"]) + 5)
        found["supporting_life_areas"] = sorted(set(found["supporting_life_areas"] + narrative["supporting_life_areas"]))
        found["dominant_themes"] = sorted(set(found["dominant_themes"] + narrative["dominant_themes"]))[:4]
        found["key_events"] = sorted(set(found["key_events"] + narrative["key_events"]))[:4]
        found["start"] = min(found["start"], narrative["start"])
        found["resolution"] = max(found["resolution"], narrative["resolution"])
        found["peak"] = max(found["peak"], narrative["peak"])
        found["level"] = _narrative_level(found["narrative_score"])

    return merged


def _should_merge(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    if existing["primary_life_area"] != incoming["primary_life_area"]:
        return False
    shared_themes = set(existing["dominant_themes"]) & set(incoming["dominant_themes"])
    if not shared_themes:
        return False
    existing_start = _parse_date(existing["start"])
    incoming_start = _parse_date(incoming["start"])
    return abs((existing_start - incoming_start).days) <= 30


def _story_weights(narratives: list[dict[str, Any]]) -> dict[str, int]:
    total = sum(narrative["narrative_score"] for narrative in narratives)
    if total <= 0:
        return {}
    return {
        narrative["narrative_type"]: round((narrative["narrative_score"] / total) * 100)
        for narrative in narratives
    }


def _life_period_summary(primary_narratives: list[dict[str, Any]]) -> str:
    if not primary_narratives:
        return "No dominant life storyline is currently active."
    fragments = []
    for narrative in primary_narratives[:2]:
        fragments.append(narrative["narrative_type"].replace("_", " "))
    summary = f"This period centers on {' and '.join(fragments)}."
    return _truncate_words(summary, 40)


def _truncate_words(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).rstrip(",.") + "."


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(value, "%Y-%m-%d")
    return datetime.max


def _unique_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for event in events:
        event_id = str(event.get("event_id", ""))
        if event_id and event_id not in seen:
            seen.add(event_id)
            unique.append(event)
    return unique
