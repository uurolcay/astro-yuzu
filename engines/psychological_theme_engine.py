from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any


PLANET_THEME_SIGNALS = {
    "Saturn": {"responsibility_phase": 12, "career_pressure": 10, "stability_building": 8},
    "Jupiter": {"opportunity_window": 12, "career_growth": 10, "financial_focus": 8},
    "Mars": {"independence_drive": 10, "power_struggle": 8},
    "Venus": {"relationship_opportunity": 10, "financial_focus": 6},
    "Mercury": {"life_direction_shift": 7},
    "Moon": {"emotional_reset": 10, "closure_cycle": 8},
    "Sun": {"identity_transformation": 10, "life_direction_shift": 8},
    "Uranus": {"identity_transformation": 15, "independence_drive": 10, "life_direction_shift": 12},
    "Neptune": {"spiritual_search": 12, "emotional_reset": 8},
    "Pluto": {"identity_transformation": 18, "power_struggle": 14, "closure_cycle": 10},
}

HOUSE_THEME_SIGNALS = {
    10: {"career_pressure": 12, "career_growth": 10},
    7: {"relationship_testing": 12, "relationship_opportunity": 10},
    2: {"financial_focus": 10, "financial_pressure": 8},
    8: {"identity_transformation": 12, "financial_pressure": 8, "closure_cycle": 8},
    1: {"identity_transformation": 12, "life_direction_shift": 10},
    4: {"emotional_reset": 10, "stability_building": 8},
    6: {"health_restructuring": 10, "responsibility_phase": 6},
    9: {"spiritual_search": 10, "life_direction_shift": 8},
    11: {"opportunity_window": 8, "financial_focus": 6},
    5: {"new_beginning": 8, "opportunity_window": 6},
}

ASPECT_THEME_SIGNALS = {
    "square": {"responsibility_phase": 6, "career_pressure": 6},
    "opposition": {"relationship_testing": 8, "power_struggle": 6},
    "trine": {"opportunity_window": 8, "career_growth": 6},
    "sextile": {"new_beginning": 6, "relationship_opportunity": 5},
    "conjunction": {"life_direction_shift": 8, "identity_transformation": 6},
}

CLUSTERS = {
    "career_restructuring_phase": ["career_pressure", "responsibility_phase", "stability_building"],
    "relationship_transition_phase": ["relationship_testing", "emotional_reset", "closure_cycle"],
    "growth_window_phase": ["career_growth", "opportunity_window", "new_beginning"],
    "identity_shift_phase": ["identity_transformation", "life_direction_shift", "independence_drive"],
    "financial_rebalancing_phase": ["financial_focus", "financial_pressure", "stability_building"],
}

THEME_LIFE_AREAS = {
    "career_pressure": "career",
    "career_growth": "career",
    "relationship_testing": "relationships",
    "relationship_opportunity": "relationships",
    "identity_transformation": "personal",
    "emotional_reset": "personal",
    "financial_focus": "finances",
    "financial_pressure": "finances",
    "health_restructuring": "health",
    "spiritual_search": "spiritual",
    "life_direction_shift": "personal",
    "responsibility_phase": "career",
    "opportunity_window": "career",
    "closure_cycle": "personal",
    "new_beginning": "personal",
    "power_struggle": "relationships",
    "independence_drive": "personal",
    "stability_building": "career",
}

GROWTH_THEMES = {
    "career_growth",
    "relationship_opportunity",
    "financial_focus",
    "spiritual_search",
    "opportunity_window",
    "new_beginning",
    "stability_building",
    "independence_drive",
}

STRESS_THEMES = {
    "career_pressure",
    "relationship_testing",
    "financial_pressure",
    "health_restructuring",
    "responsibility_phase",
    "closure_cycle",
    "power_struggle",
}

# Outer planets may appear in transit event data but are not calculated
# in natal_data by default. Theme signals from these planets apply
# only when they appear in scored transit events.
OUTER_PLANETS = {"Uranus", "Neptune", "Pluto"}


def extract_psychological_themes(scored_events: list[dict[str, Any]]) -> dict[str, Any]:
    relevant_events = [event for event in scored_events if float(event.get("importance_score", 0)) >= 40]

    theme_scores: dict[str, float] = defaultdict(float)
    theme_meta: dict[str, dict[str, Any]] = {}
    cluster_input: dict[str, set[str]] = defaultdict(set)
    stress_score = 0.0
    growth_score = 0.0
    change_signal = 0.0

    for event in relevant_events:
        multiplier = _importance_multiplier(float(event.get("importance_score", 0)))
        event_themes = _collect_theme_signals(event, multiplier)
        event_date = _sort_date(event.get("date"))

        hard_aspect = str(event.get("aspect_type", "")).lower() in {"square", "opposition"}
        soft_aspect = str(event.get("aspect_type", "")).lower() in {"trine", "sextile"}

        if hard_aspect:
            stress_score += 8
        if soft_aspect:
            growth_score += 8
        if _event_planet(event) in OUTER_PLANETS:
            change_signal += 14
        elif _event_planet(event) in {"Saturn", "Jupiter"}:
            change_signal += 8

        for theme, signal in event_themes.items():
            theme_scores[theme] += signal
            cluster_input[theme].add(event.get("event_id", ""))

            meta = theme_meta.setdefault(
                theme,
                {
                    "contributing_events": set(),
                    "start_date": event_date,
                    "peak_date": event_date,
                    "peak_importance": float(event.get("importance_score", 0)),
                    "end_date": event_date,
                },
            )
            meta["contributing_events"].add(event.get("event_id", ""))
            meta["start_date"] = min(meta["start_date"], event_date)
            meta["end_date"] = max(meta["end_date"], event_date)
            if float(event.get("importance_score", 0)) > meta["peak_importance"]:
                meta["peak_importance"] = float(event.get("importance_score", 0))
                meta["peak_date"] = event_date

            if theme in STRESS_THEMES:
                stress_score += signal
            elif theme in GROWTH_THEMES:
                growth_score += signal

    theme_objects = [_build_theme_object(theme, score, theme_meta[theme]) for theme, score in theme_scores.items()]
    theme_objects.sort(key=lambda item: item["score"], reverse=True)

    dominant_themes = theme_objects[:3]
    active_themes = [theme for theme in theme_objects if theme["score"] >= 25][:5]
    emerging_themes = [theme for theme in theme_objects if 15 <= theme["score"] < 25]
    clusters = _build_clusters(theme_scores, cluster_input)

    total_ratio = stress_score + growth_score
    if total_ratio > 0:
        stress_pct = round((stress_score / total_ratio) * 100)
        growth_pct = 100 - stress_pct
    else:
        stress_pct = 50
        growth_pct = 50

    psychological_focus = _psychological_focus(dominant_themes)
    life_area_focus = _life_area_focus(theme_objects)
    change_intensity = _change_intensity(change_signal, clusters, dominant_themes)

    return {
        "dominant_themes": dominant_themes,
        "active_themes": active_themes,
        "emerging_themes": emerging_themes,
        "theme_scores": {theme: round(score, 2) for theme, score in sorted(theme_scores.items(), key=lambda item: item[1], reverse=True)},
        "theme_clusters": clusters,
        "psychological_focus": psychological_focus,
        "life_area_focus": life_area_focus,
        "stress_vs_growth_ratio": {"stress": stress_pct, "growth": growth_pct},
        "change_intensity": change_intensity,
    }


def _collect_theme_signals(event: dict[str, Any], multiplier: float) -> dict[str, float]:
    signals: dict[str, float] = defaultdict(float)
    planet = _event_planet(event)
    house = _event_house(event)
    aspect_type = str(event.get("aspect_type", "")).lower()

    for theme, score in PLANET_THEME_SIGNALS.get(planet, {}).items():
        signals[theme] += score * multiplier

    if house in HOUSE_THEME_SIGNALS:
        for theme, score in HOUSE_THEME_SIGNALS[house].items():
            signals[theme] += score * multiplier

    if aspect_type in ASPECT_THEME_SIGNALS:
        for theme, score in ASPECT_THEME_SIGNALS[aspect_type].items():
            signals[theme] += score * multiplier

    if event.get("event_type") == "FULL_MOON":
        signals["closure_cycle"] += 8 * multiplier
        signals["emotional_reset"] += 6 * multiplier
    elif event.get("event_type") == "NEW_MOON":
        signals["new_beginning"] += 10 * multiplier
        signals["life_direction_shift"] += 5 * multiplier
    elif event.get("event_type") in {"RETROGRADE_EVENT", "DIRECT_EVENT"}:
        signals["life_direction_shift"] += 5 * multiplier
        signals["responsibility_phase"] += 4 * multiplier

    return signals


def _build_theme_object(theme: str, score: float, meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "theme": theme,
        "score": round(score, 2),
        "level": _theme_level(score),
        "contributing_events": len(meta["contributing_events"]),
        "start_date": meta["start_date"].strftime("%Y-%m-%d"),
        "peak_date": meta["peak_date"].strftime("%Y-%m-%d"),
        "projected_end": meta["end_date"].strftime("%Y-%m-%d"),
    }


def _build_clusters(theme_scores: dict[str, float], cluster_input: dict[str, set[str]]) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    for cluster_type, themes in CLUSTERS.items():
        active_members = [theme for theme in themes if theme_scores.get(theme, 0) >= 25]
        if len(active_members) < 3:
            continue
        cluster_score = sum(theme_scores[theme] for theme in active_members)
        event_ids = set()
        for theme in active_members:
            event_ids.update(cluster_input.get(theme, set()))
        clusters.append(
            {
                "cluster_type": cluster_type,
                "cluster_strength": _theme_level(cluster_score),
                "cluster_score": round(cluster_score, 2),
                "active_themes": active_members,
                "contributing_events": len([event_id for event_id in event_ids if event_id]),
            }
        )
    clusters.sort(key=lambda item: item["cluster_score"], reverse=True)
    return clusters


def _theme_level(score: float) -> str:
    if score >= 70:
        return "dominant"
    if score >= 45:
        return "major"
    if score >= 25:
        return "active"
    return "emerging"


def _importance_multiplier(score: float) -> float:
    if score >= 80:
        return 1.5
    if score >= 65:
        return 1.3
    if score >= 50:
        return 1.15
    return 1.0


def _psychological_focus(dominant_themes: list[dict[str, Any]]) -> str:
    if not dominant_themes:
        return "no dominant psychological phase"
    labels = [theme["theme"].replace("_", " ") for theme in dominant_themes[:2]]
    return " + ".join(labels)


def _life_area_focus(theme_objects: list[dict[str, Any]]) -> str:
    area_scores: dict[str, float] = defaultdict(float)
    for theme in theme_objects[:8]:
        area_scores[THEME_LIFE_AREAS.get(theme["theme"], "personal")] += theme["score"]
    ordered = sorted(area_scores.items(), key=lambda item: item[1], reverse=True)
    top_areas = [area for area, _ in ordered[:2]]
    return " + ".join(top_areas) if top_areas else "personal"


def _change_intensity(change_signal: float, clusters: list[dict[str, Any]], dominant_themes: list[dict[str, Any]]) -> str:
    if clusters:
        change_signal += 12
    if dominant_themes and dominant_themes[0]["score"] >= 70:
        change_signal += 10
    if change_signal >= 60:
        return "extreme"
    if change_signal >= 40:
        return "high"
    if change_signal >= 20:
        return "moderate"
    return "low"


def _event_planet(event: dict[str, Any]) -> str:
    if event.get("transit_planet"):
        return str(event["transit_planet"])
    if event.get("planet"):
        return str(event["planet"])
    transit_planets = event.get("transit_planets") or []
    if transit_planets:
        return str(transit_planets[0])
    return ""


def _event_house(event: dict[str, Any]) -> int | None:
    for key in ("moon_house", "house", "new_house", "sun_house"):
        if event.get(key) is not None:
            try:
                return int(event[key])
            except (TypeError, ValueError):
                return None
    return None


# _sort_date is duplicated intentionally pending a shared utils module.
def _sort_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(value, "%Y-%m-%d")
    return datetime.max
