from __future__ import annotations

from datetime import date, datetime
from typing import Any

from engines.engines_lunations import score_lunation_events


PLANET_SIGN_CHANGE_WEIGHTS = {
    "Sun": 20,
    "Moon": 10,
    "Mercury": 25,
    "Venus": 25,
    "Mars": 30,
    "Jupiter": 35,
    "Saturn": 40,
    "Uranus": 45,
    "Neptune": 45,
    "Pluto": 50,
}

OUTER_PLANETS = {"Uranus", "Neptune", "Pluto"}
SOCIAL_PLANETS = {"Jupiter", "Saturn"}
PERSONAL_PLANETS = {"Sun", "Moon", "Mercury", "Venus", "Mars"}
NODES = {"Rahu", "Ketu", "North Node", "South Node"}
ANGLE_POINTS = {"ASC", "MC", "IC", "DESC", "Vertex"}

PLANET_MULTIPLIERS = {
    "Sun": 1.3,
    "Moon": 1.1,
    "Mercury": 1.0,
    "Venus": 1.0,
    "Mars": 1.1,
    "Jupiter": 1.2,
    "Saturn": 1.3,
    "Uranus": 1.4,
    "Neptune": 1.4,
    "Pluto": 1.5,
}

RARITY_SCORES = {
    ("PLANET_SIGN_CHANGE", "Pluto"): 25,
    ("PLANET_SIGN_CHANGE", "Neptune"): 20,
    ("PLANET_SIGN_CHANGE", "Uranus"): 18,
    ("PLANET_SIGN_CHANGE", "Saturn"): 15,
    ("PLANET_SIGN_CHANGE", "Jupiter"): 10,
    ("RETROGRADE_EVENT", "Mercury"): 5,
    ("RETROGRADE_EVENT", "Venus"): 12,
    ("RETROGRADE_EVENT", "Mars"): 15,
}


def score_events(
    events: list[dict[str, Any]],
    natal_data: dict[str, Any],
    activation_lists: dict[str, Any] | None = None,
    transit_strengths: dict[str, Any] | None = None,
    aspect_lists: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Unified Phase 2.4 global importance scorer for all transit events.

    Existing callers can keep using score_events(events, natal_data). Additional
    precomputed activation data is optional and only enriches scoring.
    """
    return score_global_events(
        events=events,
        natal_data=natal_data,
        activation_lists=activation_lists,
        transit_strengths=transit_strengths,
        aspect_lists=aspect_lists,
    )


def score_global_events(
    events: list[dict[str, Any]],
    natal_data: dict[str, Any],
    activation_lists: dict[str, Any] | None = None,
    transit_strengths: dict[str, Any] | None = None,
    aspect_lists: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    activation_lists = activation_lists or {}
    transit_strengths = transit_strengths or {}
    aspect_lists = aspect_lists or {}

    lunation_events = [event for event in events if event.get("event_type") in {"FULL_MOON", "NEW_MOON"}]
    non_lunation_events = [event for event in events if event.get("event_type") not in {"FULL_MOON", "NEW_MOON"}]

    phase23_lunations = score_lunation_events(
        lunation_events,
        transit_aspects=aspect_lists.get("lunations"),
        natal_sensitive_points=activation_lists.get("lunation_sensitive_points"),
        activated_planets=activation_lists.get("lunation_planets"),
        transit_strength_indicators=transit_strengths.get("lunations"),
    )
    phase23_by_id = {event["event_id"]: event for event in phase23_lunations}

    cluster_map = _build_cluster_map(events)
    scored_events: list[dict[str, Any]] = []

    for event in non_lunation_events + phase23_lunations:
        base_weight = _base_weight(event)
        natal_activation = _natal_activation_score(event, aspect_lists, activation_lists)
        precision = _precision_score(event)
        reinforcement = _reinforcement_score(event, activation_lists, aspect_lists, transit_strengths)
        rarity = _rarity_score(event)
        clustering = _cluster_bonus(cluster_map.get(event.get("event_id"), 1))
        multiplier = _planet_multiplier(event)

        weighted_activation = round((natal_activation + reinforcement) * multiplier)
        raw_score = min(100, base_weight + weighted_activation + precision + rarity + clustering)

        phase23_floor = 0
        if event.get("event_id") in phase23_by_id:
            phase23_floor = int(phase23_by_id[event["event_id"]].get("importance_score", 0))
        final_score = min(100, max(raw_score, phase23_floor))

        factors = {
            "base_weight": base_weight,
            "natal_activation": natal_activation,
            "precision": precision,
            "reinforcement": reinforcement,
            "rarity": rarity,
            "clustering": clustering,
            "multiplier_applied": multiplier,
        }
        if phase23_floor:
            factors["phase23_floor"] = phase23_floor

        enriched_event = dict(event)
        enriched_event.update(
            {
                "importance_score": final_score,
                "importance_level": _importance_level(final_score),
                "importance_factors": factors,
                "dominant_activation_type": _dominant_activation_type(
                    natal_activation=natal_activation,
                    precision=precision,
                    reinforcement=reinforcement,
                    rarity=rarity,
                    clustering=clustering,
                    event=event,
                ),
                "interpretation_priority": _interpretation_priority(final_score),
                "event_weight_class": _event_weight_class(final_score),
                "should_interpret": final_score >= 55,
                "interpretation_complexity": _interpretation_complexity(final_score),
            }
        )
        scored_events.append(enriched_event)

    return sorted(scored_events, key=lambda item: (-item.get("importance_score", 0), _sort_date(item.get("date"))))


def calculate_base_importance(event: dict[str, Any]) -> int:
    return _base_weight(event)


def _base_weight(event: dict[str, Any]) -> int:
    event_type = event.get("event_type")
    planet = _event_planet(event)

    if event_type == "FULL_MOON":
        return 50
    if event_type == "NEW_MOON":
        return 45
    if event_type == "TRANSIT_ASPECT":
        aspect_type = str(event.get("aspect_type", "")).lower()
        return 40 if aspect_type in {"conjunction", "opposition", "square", "trine"} else 25
    if event_type == "RETROGRADE_EVENT":
        return 35
    if event_type == "DIRECT_EVENT":
        return 30
    if event_type == "PLANET_SIGN_CHANGE":
        return PLANET_SIGN_CHANGE_WEIGHTS.get(planet, 20)
    if event_type == "PLANET_HOUSE_CHANGE":
        return 30 if planet in OUTER_PLANETS or planet in {"Jupiter", "Saturn"} else 15
    return 10


def _natal_activation_score(
    event: dict[str, Any],
    aspect_lists: dict[str, Any],
    activation_lists: dict[str, Any],
) -> int:
    total = 0
    seen_planets: set[str] = set()

    planet_hits = []
    if event.get("hits_natal_planet"):
        hits = event.get("hits_natal_planet")
        if isinstance(hits, list):
            planet_hits.extend(hits)
        else:
            planet_hits.append(hits)
    planet_hits.extend(event.get("aspect_to_natal") or [])
    planet_hits.extend(event.get("conjunction_to_natal") or [])
    planet_hits.extend(aspect_lists.get(event.get("event_id"), {}).get("natal_hits", []))

    for hit in planet_hits:
        hit_name = hit.get("planet") if isinstance(hit, dict) else str(hit)
        if not hit_name or hit_name in seen_planets:
            continue
        seen_planets.add(hit_name)
        if hit_name in PERSONAL_PLANETS:
            total += 10
        elif hit_name in SOCIAL_PLANETS:
            total += 7
        # Outer planets (Uranus, Neptune, Pluto) are not in natal_data by default.
        # If a transit event involves them, natal activation score will be 0,
        # which is correct — they act as background/transpersonal indicators only.
        elif hit_name in OUTER_PLANETS:
            total += 5
        else:
            total += 7

    sensitive_hits = set(event.get("activated_sensitive_points") or [])
    sensitive_hits.update(event.get("activated_points") or [])
    sensitive_hits.update(activation_lists.get(event.get("event_id"), {}).get("sensitive_points", []))

    for hit in sensitive_hits:
        label = str(hit).upper()
        if label in ANGLE_POINTS:
            total += 15
        elif label in {point.upper() for point in NODES}:
            total += 12

    return min(total, 35)


def _precision_score(event: dict[str, Any]) -> int:
    event_type = event.get("event_type")
    orb = event.get("orb")
    if orb is None:
        orb = event.get("exact_orb")

    if orb is not None:
        orb = float(orb)
        if orb <= 0.5:
            return 15
        if orb <= 1.0:
            return 12
        if orb <= 2.0:
            return 8
        if orb <= 3.0:
            return 4

    if event_type == "PLANET_HOUSE_CHANGE" and event.get("is_exact_cusp_crossing"):
        return 10
    if event_type in {"RETROGRADE_EVENT", "DIRECT_EVENT"} and event.get("is_station_exact_day"):
        return 15
    return 0


def _reinforcement_score(
    event: dict[str, Any],
    activation_lists: dict[str, Any],
    aspect_lists: dict[str, Any],
    transit_strengths: dict[str, Any],
) -> int:
    confirmations = 0

    if event.get("hits_natal_planet") or event.get("aspect_to_natal") or event.get("conjunction_to_natal"):
        confirmations += 1
    if event.get("activated_sensitive_points") or event.get("activated_points"):
        confirmations += 1
    if event.get("orb") is not None or event.get("exact_orb") is not None or event.get("is_station_exact_day"):
        confirmations += 1
    if event.get("reinforcing_transits") or transit_strengths.get(event.get("event_id"), {}).get("reinforcing_aspects"):
        confirmations += 1
    if activation_lists.get(event.get("event_id"), {}).get("secondary_confirmations"):
        confirmations += 1
    if aspect_lists.get(event.get("event_id"), {}).get("confirmations"):
        confirmations += 1

    if confirmations >= 4:
        return 25
    if confirmations == 3:
        return 18
    if confirmations == 2:
        return 10
    return 0


def _rarity_score(event: dict[str, Any]) -> int:
    return RARITY_SCORES.get((event.get("event_type"), _event_planet(event)), 0)


def _planet_multiplier(event: dict[str, Any]) -> float:
    planet = _event_planet(event)
    return PLANET_MULTIPLIERS.get(planet, 1.0)


def _build_cluster_map(events: list[dict[str, Any]]) -> dict[str, int]:
    dated_events = []
    for event in events:
        event_date = _sort_date(event.get("date"))
        if event_date is not None:
            dated_events.append((event.get("event_id"), event_date))

    dated_events.sort(key=lambda item: item[1])
    cluster_counts: dict[str, int] = {}
    left = 0
    for right, (event_id, event_date) in enumerate(dated_events):
        while (event_date - dated_events[left][1]).days > 5:
            left += 1
        count = right - left + 1
        cluster_counts[event_id] = max(cluster_counts.get(event_id, 1), count)
        for idx in range(left, right):
            other_id = dated_events[idx][0]
            cluster_counts[other_id] = max(cluster_counts.get(other_id, 1), count)
    return cluster_counts


def _cluster_bonus(cluster_size: int) -> int:
    if cluster_size >= 4:
        return 22
    if cluster_size == 3:
        return 15
    if cluster_size == 2:
        return 8
    return 0


def _importance_level(score: int) -> str:
    if score >= 85:
        return "critical"
    if score >= 70:
        return "major"
    if score >= 55:
        return "strong"
    if score >= 40:
        return "moderate"
    return "minor"


def _interpretation_priority(score: int) -> str:
    if score >= 85:
        return "critical"
    if score >= 70:
        return "high"
    if score >= 55:
        return "high"
    if score >= 40:
        return "normal"
    return "low"


def _event_weight_class(score: int) -> str:
    if score >= 85:
        return "critical"
    if score >= 70:
        return "major"
    if score >= 55:
        return "primary"
    if score >= 40:
        return "supporting"
    return "background"


def _interpretation_complexity(score: int) -> str:
    weight_class = _event_weight_class(score)
    mapping = {
        "critical": "deep analysis",
        "major": "full interpretation",
        "primary": "normal interpretation",
        "supporting": "short interpretation",
        "background": "skip",
    }
    return mapping[weight_class]


def _dominant_activation_type(
    natal_activation: int,
    precision: int,
    reinforcement: int,
    rarity: int,
    clustering: int,
    event: dict[str, Any],
) -> str:
    candidates = {
        "natal_activation": natal_activation,
        "precision_event": precision,
        "transit_cluster": clustering,
        "outer_planet_event": rarity if _event_planet(event) in OUTER_PLANETS else 0,
        "retrograde_station": precision if event.get("event_type") in {"RETROGRADE_EVENT", "DIRECT_EVENT"} else 0,
        "multi_activation": reinforcement,
    }
    strongest = max(candidates.values()) if candidates else 0
    if strongest == 0:
        return "balanced"
    winners = [label for label, value in candidates.items() if value == strongest]
    if len(winners) > 1:
        return "balanced"
    return winners[0]


def _event_planet(event: dict[str, Any]) -> str:
    if event.get("transit_planet"):
        return str(event["transit_planet"])
    if event.get("planet"):
        return str(event["planet"])
    transit_planets = event.get("transit_planets") or []
    if transit_planets:
        return str(transit_planets[0])
    return ""


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
