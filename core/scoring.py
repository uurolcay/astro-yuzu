"""Signal scoring and prioritization for premium interpretation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

BASE_WEIGHTS = {
    "planet_in_house": 1.0,
    "planet_in_sign": 0.8,
    "major_aspect": 1.25,
    "conjunction_cluster": 1.4,
    "node_placement": 1.2,
    "dasha_activation": 1.5,
}

EXALTED_SIGNS = {
    "Sun": "Aries",
    "Moon": "Taurus",
    "Mars": "Capricorn",
    "Mercury": "Virgo",
    "Jupiter": "Cancer",
    "Venus": "Pisces",
    "Saturn": "Libra",
}

OWN_SIGNS = {
    "Sun": {"Leo"},
    "Moon": {"Cancer"},
    "Mars": {"Aries", "Scorpio"},
    "Mercury": {"Gemini", "Virgo"},
    "Jupiter": {"Sagittarius", "Pisces"},
    "Venus": {"Taurus", "Libra"},
    "Saturn": {"Capricorn", "Aquarius"},
}


def prioritize_signals(signals: list[dict[str, Any]], *, natal_data=None, dasha_data=None, limit=6) -> list[dict[str, Any]]:
    prioritized = []
    for signal in signals:
        base_weight = BASE_WEIGHTS.get(signal.get("type"), 1.0)
        strength_modifier = _strength_modifier(signal)
        timing_boost = _timing_boost(signal, dasha_data or [])
        final_score = round(base_weight * strength_modifier * timing_boost, 4)
        prioritized.append(
            {
                **signal,
                "base_weight": base_weight,
                "strength_modifier": round(strength_modifier, 4),
                "timing_boost": round(timing_boost, 4),
                "score": final_score,
            }
        )
    prioritized.sort(key=lambda item: item["score"], reverse=True)
    return prioritized[:limit]


def _strength_modifier(signal: dict[str, Any]) -> float:
    modifier = 1.0
    house = signal.get("house")
    sign = signal.get("sign")
    planet = signal.get("planet")

    if house in {1, 4, 7, 10}:
        modifier += 0.35
    elif house in {5, 9, 11}:
        modifier += 0.2
    elif house in {6, 8, 12}:
        modifier += 0.12

    if planet and sign and EXALTED_SIGNS.get(planet) == sign:
        modifier += 0.35
    elif planet and sign in OWN_SIGNS.get(planet, set()):
        modifier += 0.2

    if signal.get("type") == "major_aspect":
        aspect = signal.get("aspect")
        if aspect in {"square", "opposition"}:
            modifier += 0.2
        elif aspect in {"conjunction", "trine"}:
            modifier += 0.15

    if signal.get("type") == "conjunction_cluster":
        modifier += min(0.3, (len(signal.get("cluster_planets", [])) - 2) * 0.1)

    if signal.get("planet") in {"Rahu", "Ketu"}:
        modifier += 0.18

    return modifier


def _timing_boost(signal: dict[str, Any], dasha_data: list[dict[str, Any]]) -> float:
    if not dasha_data:
        return 1.0
    active_period = _active_dasha(dasha_data)
    next_period = dasha_data[1] if len(dasha_data) > 1 else None
    if signal.get("type") == "dasha_activation":
        return 1.35 if signal.get("context", {}).get("is_active") else 1.1
    if active_period and signal.get("planet") == active_period.get("planet"):
        return 1.3
    if next_period and signal.get("planet") == next_period.get("planet"):
        return 1.12
    if signal.get("other_planet") and active_period and signal.get("other_planet") == active_period.get("planet"):
        return 1.15
    return 1.0


def _active_dasha(dasha_data):
    today = datetime.utcnow().date()
    for period in dasha_data:
        start = _parse_date(period.get("start"))
        end = _parse_date(period.get("end"))
        if start and end and start <= today <= end:
            return period
    return dasha_data[0] if dasha_data else None


def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None
