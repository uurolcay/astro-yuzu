from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


THEME_TO_LIFE_AREAS = {
    "career_pressure": {"career": 15, "finances": 6, "life_direction": 8},
    "career_growth": {"career": 15, "finances": 8, "social_network": 5},
    "relationship_testing": {"relationships": 15, "personal_growth": 6, "family": 8},
    "relationship_opportunity": {"relationships": 15, "social_network": 6},
    "identity_transformation": {"personal_growth": 15, "life_direction": 10, "career": 6},
    "financial_focus": {"finances": 15, "career": 6, "home": 8},
    "financial_pressure": {"finances": 15, "career": 8, "health": 6},
    "emotional_reset": {"personal_growth": 10, "relationships": 6, "family": 6},
    "responsibility_phase": {"career": 12, "life_direction": 10, "personal_growth": 8},
    "spiritual_search": {"spirituality": 15, "personal_growth": 10},
    "closure_cycle": {"relationships": 10, "personal_growth": 8, "life_direction": 6},
    "new_beginning": {"life_direction": 15, "career": 6, "personal_growth": 8},
    "power_struggle": {"career": 10, "relationships": 10, "personal_growth": 6},
    "stability_building": {"finances": 10, "career": 8, "home": 6},
    "independence_drive": {"personal_growth": 12, "career": 6, "relationships": 5},
    "health_restructuring": {"health": 15, "personal_growth": 7},
    "opportunity_window": {"career": 10, "finances": 6, "life_direction": 6},
}

PRESSURE_THEMES = {
    "career_pressure",
    "financial_pressure",
    "relationship_testing",
    "responsibility_phase",
    "power_struggle",
    "closure_cycle",
}

OPPORTUNITY_THEMES = {
    "career_growth",
    "relationship_opportunity",
    "new_beginning",
    "opportunity_window",
    "stability_building",
}

LIFE_AREA_CLUSTERS = {
    "professional_transition_phase": ["career", "finances", "life_direction"],
    "relationship_evolution_phase": ["relationships", "personal_growth", "family"],
    "inner_transformation_phase": ["personal_growth", "spirituality", "life_direction"],
    "stability_rebuild_phase": ["finances", "home", "career"],
}


def analyze_life_area_impact(psychological_themes: dict[str, Any]) -> dict[str, Any]:
    theme_objects = _relevant_themes(psychological_themes)

    area_scores: dict[str, float] = defaultdict(float)
    area_meta: dict[str, dict[str, Any]] = {}
    pressure_score = 0.0
    opportunity_score = 0.0

    for theme in theme_objects:
        theme_name = theme.get("theme")
        theme_score = float(theme.get("score", 0))
        multiplier = _theme_strength_multiplier(theme_score)
        mapped_areas = THEME_TO_LIFE_AREAS.get(theme_name, {})
        start_date = _parse_date(theme.get("start_date"))
        peak_date = _parse_date(theme.get("peak_date"))
        end_date = _parse_date(theme.get("projected_end"))

        if theme_name in PRESSURE_THEMES:
            pressure_score += theme_score
        if theme_name in OPPORTUNITY_THEMES:
            opportunity_score += theme_score

        for life_area, weight in mapped_areas.items():
            contribution = weight * multiplier
            area_scores[life_area] += contribution

            meta = area_meta.setdefault(
                life_area,
                {
                    "contributing_themes": [],
                    "start_date": start_date,
                    "peak_date": peak_date,
                    "peak_score": theme_score,
                    "cooldown_date": end_date,
                },
            )
            meta["contributing_themes"].append(theme_name)
            meta["start_date"] = min(meta["start_date"], start_date)
            meta["cooldown_date"] = max(meta["cooldown_date"], end_date)
            if theme_score >= meta["peak_score"]:
                meta["peak_score"] = theme_score
                meta["peak_date"] = peak_date

    area_objects = [
        _build_life_area_object(area, score, area_meta[area]) for area, score in area_scores.items()
    ]
    area_objects.sort(key=lambda item: item["score"], reverse=True)

    dominant_life_areas = area_objects[:2]
    active_life_areas = [area for area in area_objects if area["score"] >= 30][:4]
    emerging_life_areas = [area for area in area_objects if 15 <= area["score"] < 30]
    clusters = _build_life_area_clusters(area_scores, area_meta)

    pressure_total = pressure_score + opportunity_score
    if pressure_total > 0:
        pressure_pct = round((pressure_score / pressure_total) * 100)
        opportunity_pct = 100 - pressure_pct
    else:
        pressure_pct = 50
        opportunity_pct = 50

    change_distribution = _change_distribution(area_scores)
    life_stability_index = _life_stability_index(
        psychological_themes,
        clusters,
        pressure_pct,
    )

    return {
        "life_area_scores": {area: round(score, 2) for area, score in sorted(area_scores.items(), key=lambda item: item[1], reverse=True)},
        "dominant_life_areas": dominant_life_areas,
        "active_life_areas": active_life_areas,
        "emerging_life_areas": emerging_life_areas,
        "life_area_clusters": clusters,
        "pressure_vs_opportunity_ratio": {
            "pressure": pressure_pct,
            "opportunity": opportunity_pct,
        },
        "primary_life_focus": _focus_label(dominant_life_areas, default_label="general transition"),
        "secondary_life_focus": _focus_label(area_objects[1:3], default_label="secondary adjustment"),
        "change_distribution": change_distribution,
        "life_stability_index": life_stability_index,
        "requires_decision_period": pressure_pct >= 60 or "life_direction" in [area["life_area"] for area in dominant_life_areas],
        "high_growth_window": opportunity_pct >= 55,
        "stress_management_period": pressure_pct >= 55,
        "long_term_build_phase": any(
            theme.get("theme") in {"responsibility_phase", "stability_building"} for theme in theme_objects
        ),
    }


def _relevant_themes(psychological_themes: dict[str, Any]) -> list[dict[str, Any]]:
    themes = []
    themes.extend(psychological_themes.get("dominant_themes", []))
    themes.extend(psychological_themes.get("active_themes", []))

    unique: dict[str, dict[str, Any]] = {}
    for theme in themes:
        if float(theme.get("score", 0)) >= 25:
            unique[theme["theme"]] = theme
    return list(unique.values())


def _theme_strength_multiplier(score: float) -> float:
    if score >= 70:
        return 1.4
    if score >= 50:
        return 1.25
    if score >= 35:
        return 1.1
    return 1.0


def _build_life_area_object(life_area: str, score: float, meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "life_area": life_area,
        "score": round(score, 2),
        "level": _life_area_level(score),
        "contributing_themes": meta["contributing_themes"],
        "start_date": meta["start_date"].strftime("%Y-%m-%d"),
        "peak_date": meta["peak_date"].strftime("%Y-%m-%d"),
        "projected_stabilization": meta["cooldown_date"].strftime("%Y-%m-%d"),
    }


def _life_area_level(score: float) -> str:
    if score >= 70:
        return "dominant impact"
    if score >= 50:
        return "strong impact"
    if score >= 30:
        return "active impact"
    if score >= 15:
        return "background influence"
    return "minor"


def _build_life_area_clusters(
    area_scores: dict[str, float],
    area_meta: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    for cluster_type, areas in LIFE_AREA_CLUSTERS.items():
        active_areas = [area for area in areas if area_scores.get(area, 0) >= 30]
        if len(active_areas) < 3:
            continue

        dominant_themes: list[str] = []
        for area in active_areas:
            dominant_themes.extend(area_meta.get(area, {}).get("contributing_themes", []))

        cluster_score = sum(area_scores[area] for area in active_areas)
        clusters.append(
            {
                "cluster_type": cluster_type,
                "cluster_score": round(cluster_score, 2),
                "cluster_life_areas": active_areas,
                "cluster_dominant_themes": sorted(set(dominant_themes)),
            }
        )

    clusters.sort(key=lambda item: item["cluster_score"], reverse=True)
    return clusters


def _change_distribution(area_scores: dict[str, float]) -> dict[str, int]:
    total = sum(area_scores.values())
    if total <= 0:
        return {}
    return {
        area: round((score / total) * 100)
        for area, score in sorted(area_scores.items(), key=lambda item: item[1], reverse=True)
    }


def _life_stability_index(
    psychological_themes: dict[str, Any],
    clusters: list[dict[str, Any]],
    pressure_pct: int,
) -> str:
    change_signal = 0
    if clusters:
        change_signal += 1
    if pressure_pct >= 65:
        change_signal += 1
    if psychological_themes.get("change_intensity") in {"high", "extreme"}:
        change_signal += 2
    elif psychological_themes.get("change_intensity") == "moderate":
        change_signal += 1

    if change_signal >= 4:
        return "major life restructuring"
    if change_signal == 3:
        return "high change period"
    if change_signal == 2:
        return "moderate volatility"
    if change_signal == 1:
        return "slight change"
    return "stable"


def _focus_label(area_objects: list[dict[str, Any]], default_label: str) -> str:
    if not area_objects:
        return default_label
    top = area_objects[0]
    label_map = {
        "career": "career transition",
        "finances": "financial restructuring",
        "relationships": "relationship evolution",
        "personal_growth": "personal reinvention",
        "health": "health recalibration",
        "family": "family rebalancing",
        "education": "learning expansion",
        "spirituality": "spiritual deepening",
        "social_network": "network expansion",
        "life_direction": "life direction reset",
        "home": "home foundation shift",
        "creativity": "creative activation",
    }
    return label_map.get(top["life_area"], default_label)


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(value, "%Y-%m-%d")
    return datetime.max
