from __future__ import annotations

from datetime import date, datetime
from typing import Any


SIGN_NAMES = [
    "Aries",
    "Taurus",
    "Gemini",
    "Cancer",
    "Leo",
    "Virgo",
    "Libra",
    "Scorpio",
    "Sagittarius",
    "Capricorn",
    "Aquarius",
    "Pisces",
]

HOUSE_IMPORTANCE = {
    1: 12,
    2: 6,
    3: 3,
    4: 12,
    5: 6,
    6: 3,
    7: 12,
    8: 6,
    9: 3,
    10: 15,
    11: 6,
    12: 3,
}

ASPECT_SCORES = {
    "conjunction": 12,
    "opposition": 10,
    "square": 9,
    "trine": 7,
    "sextile": 5,
}

RULER_MAP = {
    "Aries": ["Mars"],
    "Taurus": ["Venus"],
    "Gemini": ["Mercury"],
    "Cancer": ["Moon"],
    "Leo": ["Sun"],
    "Virgo": ["Mercury"],
    "Libra": ["Venus"],
    "Scorpio": ["Mars"],
    "Sagittarius": ["Jupiter"],
    "Capricorn": ["Saturn"],
    "Aquarius": ["Saturn"],
    "Pisces": ["Jupiter"],
}

MODERN_CO_RULER_MAP = {
    "Scorpio": ["Mars", "Pluto"],
    "Aquarius": ["Saturn", "Uranus"],
    "Pisces": ["Jupiter", "Neptune"],
}


def generate_lunation_events(
    transit_window: list[dict[str, Any]],
    natal_data: dict[str, Any],
    personal_points: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Generate structured FULL_MOON and NEW_MOON events from existing transit data.

    Expected transit_window shape:
    [
        {
            "date": "2026-04-01",
            "transits": [{"name": "Sun", ...}, {"name": "Moon", ...}]
        },
        ...
    ]

    The function does not run ephemeris calculations. It consumes already-computed
    Sun and Moon transit positions and returns deduplicated lunation events.
    """
    sorted_days = sorted(transit_window, key=lambda item: _normalize_date(item.get("date")))
    events: list[dict[str, Any]] = []
    active_type: str | None = None
    best_candidate: dict[str, Any] | None = None

    for day_data in sorted_days:
        candidate = _build_lunation_candidate(day_data, natal_data, personal_points)
        candidate_type = candidate["event_type"] if candidate else None

        if candidate_type and active_type == candidate_type:
            if best_candidate is None or candidate["orb"] < best_candidate["orb"]:
                best_candidate = candidate
            continue

        if best_candidate is not None:
            events.append(_finalize_lunation_event(best_candidate))
            best_candidate = None

        active_type = candidate_type
        if candidate is not None:
            best_candidate = candidate

    if best_candidate is not None:
        events.append(_finalize_lunation_event(best_candidate))

    unique_events: dict[str, dict[str, Any]] = {}
    for event in events:
        unique_events[event["event_id"]] = event

    return list(unique_events.values())


def score_lunation_events(
    lunation_events: list[dict[str, Any]],
    transit_aspects: dict[str, Any] | None = None,
    natal_sensitive_points: dict[str, Any] | None = None,
    activated_planets: dict[str, Any] | None = None,
    transit_strength_indicators: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Enrich existing lunation events with deterministic importance metadata only.

    No ephemeris, aspect, house, or dignity calculations are performed here.
    The function consumes precomputed activation/interaction fields when present.
    """
    scored_events: list[dict[str, Any]] = []

    for event in lunation_events:
        factors = _calculate_importance_factors(
            event,
            transit_aspects or {},
            natal_sensitive_points or {},
            activated_planets or {},
            transit_strength_indicators or {},
        )
        importance_score = min(100, sum(factors.values()))
        dominant_activation_type = _dominant_activation_type(factors)

        enriched_event = dict(event)
        enriched_event.update(
            {
                "importance_score": importance_score,
                "importance_level": _importance_level(importance_score),
                "importance_factors": factors,
                "dominant_activation_type": dominant_activation_type,
                "is_major_lunation": importance_score >= 70,
                "interpretation_priority": _interpretation_priority(importance_score),
            }
        )
        scored_events.append(enriched_event)

    return scored_events


def _build_lunation_candidate(
    day_data: dict[str, Any],
    natal_data: dict[str, Any],
    personal_points: dict[str, Any] | None,
) -> dict[str, Any] | None:
    date_value = day_data.get("date")
    transits = day_data.get("transits") or day_data.get("planets") or []
    sun = _find_planet(transits, "Sun")
    moon = _find_planet(transits, "Moon")
    if not sun or not moon or not date_value:
        return None

    sun_lon = _planet_longitude(sun)
    moon_lon = _planet_longitude(moon)
    separation = _separation(sun_lon, moon_lon)

    event_type: str | None = None
    orb: float | None = None

    full_orb = abs(separation - 180.0)
    new_orb = min(separation, 360.0 - separation)

    if full_orb <= 3.0:
        event_type = "FULL_MOON"
        orb = full_orb
    elif new_orb <= 3.0:
        event_type = "NEW_MOON"
        orb = new_orb

    if event_type is None or orb is None:
        return None

    moon_sign_idx = int(moon.get("sign_idx", int(moon_lon / 30)))
    sun_sign_idx = int(sun.get("sign_idx", int(sun_lon / 30)))
    lagna_sign = int(natal_data.get("ascendant", {}).get("sign_idx", 0))

    return {
        "event_type": event_type,
        "date": _date_string(date_value),
        "orb": round(orb, 4),
        "moon_sign": SIGN_NAMES[moon_sign_idx],
        "sun_sign": SIGN_NAMES[sun_sign_idx],
        "moon_degree": round(moon_lon % 30, 4),
        "sun_degree": round(sun_lon % 30, 4),
        "moon_house": ((moon_sign_idx - lagna_sign + 12) % 12) + 1,
        "sun_house": ((sun_sign_idx - lagna_sign + 12) % 12) + 1,
        "axis": f"{SIGN_NAMES[sun_sign_idx]}-{SIGN_NAMES[moon_sign_idx]}",
        "duration_days": 3 if event_type == "FULL_MOON" else 2,
        "duration": 3 if event_type == "FULL_MOON" else 2,
        "transit_planets": ["Sun", "Moon"],
        "activated_sensitive_points": _activated_sensitive_points(
            moon_lon,
            personal_points or natal_data.get("personal_points", {}),
        ),
        "activated_points": _activated_sensitive_points(
            moon_lon,
            personal_points or natal_data.get("personal_points", {}),
        ),
    }


def _finalize_lunation_event(candidate: dict[str, Any]) -> dict[str, Any]:
    event_date = candidate["date"].replace("-", "_")
    prefix = "FULLMOON" if candidate["event_type"] == "FULL_MOON" else "NEWMOON"

    return {
        "event_id": f"{prefix}_{event_date}",
        "event_type": candidate["event_type"],
        "date": candidate["date"],
        "orb": candidate["orb"],
        "moon_sign": candidate["moon_sign"],
        "sun_sign": candidate["sun_sign"],
        "moon_degree": candidate["moon_degree"],
        "sun_degree": candidate["sun_degree"],
        "moon_house": candidate["moon_house"],
        "sun_house": candidate["sun_house"],
        "axis": candidate["axis"],
        "duration": candidate["duration"],
        "duration_days": candidate["duration_days"],
        "transit_planets": candidate["transit_planets"],
        "activated_sensitive_points": candidate["activated_sensitive_points"],
        "activated_points": candidate["activated_points"],
    }


def _calculate_importance_factors(
    event: dict[str, Any],
    transit_aspects: dict[str, Any],
    natal_sensitive_points: dict[str, Any],
    activated_planets: dict[str, Any],
    transit_strength_indicators: dict[str, Any],
) -> dict[str, int]:
    exactness = _exactness_score(float(event.get("orb", 99.0)))
    natal_activation = _natal_activation_score(event, transit_aspects, activated_planets)
    sensitive_points = _sensitive_point_score(event, natal_sensitive_points)
    house_strength = HOUSE_IMPORTANCE.get(int(event.get("moon_house", 0) or 0), 0)
    transit_reinforcement = _transit_reinforcement_score(event, transit_strength_indicators)
    stellium = _stellium_score(event, activated_planets)
    ruler_activation = _ruler_activation_score(event, transit_strength_indicators, activated_planets)

    active_system_count = sum(
        1
        for value in [
            natal_activation > 0,
            sensitive_points > 0,
            house_strength > 0,
            transit_reinforcement > 0,
            stellium > 0,
            ruler_activation > 0,
        ]
        if value
    )
    multi_activation = _multi_activation_bonus(active_system_count)

    return {
        "exactness": exactness,
        "natal_activation": natal_activation,
        "house_strength": house_strength,
        "sensitive_points": sensitive_points,
        "transit_reinforcement": transit_reinforcement,
        "multi_activation": multi_activation,
        "stellium": stellium,
        "ruler_activation": ruler_activation,
    }


def _exactness_score(orb: float) -> int:
    if orb <= 0.5:
        return 20
    if orb <= 1.0:
        return 15
    if orb <= 2.0:
        return 10
    if orb <= 3.0:
        return 5
    return 0


def _natal_activation_score(
    event: dict[str, Any],
    transit_aspects: dict[str, Any],
    activated_planets: dict[str, Any],
) -> int:
    aspect_entries = list(event.get("natal_planet_aspects") or [])
    aspect_entries.extend(transit_aspects.get(event.get("event_id"), []))
    aspect_entries.extend(activated_planets.get(event.get("event_id"), []))

    total = 0
    for entry in aspect_entries:
        aspect_type = str(entry.get("aspect_type", "")).lower()
        total += ASPECT_SCORES.get(aspect_type, 0)
        if total >= 30:
            return 30
    return min(total, 30)


def _sensitive_point_score(event: dict[str, Any], natal_sensitive_points: dict[str, Any]) -> int:
    direct_points = set(event.get("activated_sensitive_points") or [])
    direct_points.update(event.get("activated_points") or [])
    additional = natal_sensitive_points.get(event.get("event_id"), {})
    direct_points.update(additional.get("exact", []))
    secondary_points = set(additional.get("secondary", []))

    score = min(len(direct_points) * 15 + len(secondary_points) * 8, 30)
    return score


def _transit_reinforcement_score(event: dict[str, Any], transit_strength_indicators: dict[str, Any]) -> int:
    reinforcing = list(event.get("reinforcing_transits") or [])
    reinforcing.extend(transit_strength_indicators.get(event.get("event_id"), {}).get("reinforcing_aspects", []))
    return min(len(reinforcing) * 6, 24)


def _stellium_score(event: dict[str, Any], activated_planets: dict[str, Any]) -> int:
    stellium_count = event.get("stellium_count")
    if stellium_count is None:
        stellium_count = activated_planets.get(event.get("event_id"), {}).get("stellium_count", 0)
    stellium_count = int(stellium_count or 0)
    if stellium_count >= 5:
        return 20
    if stellium_count == 4:
        return 15
    if stellium_count == 3:
        return 10
    return 0


def _ruler_activation_score(
    event: dict[str, Any],
    transit_strength_indicators: dict[str, Any],
    activated_planets: dict[str, Any],
) -> int:
    sign_rulers = RULER_MAP.get(event.get("moon_sign", ""), [])
    ruler_data = transit_strength_indicators.get(event.get("event_id"), {}).get("ruler_activation")
    if ruler_data is None:
        ruler_data = event.get("ruler_activation")
    if ruler_data is None:
        active_names = {
            entry.get("planet")
            for entry in activated_planets.get(event.get("event_id"), [])
            if isinstance(entry, dict)
        }
        if any(ruler in active_names for ruler in sign_rulers):
            return 5
        return 0
    if ruler_data in {"strong", "major", "major_activation"} or ruler_data is True:
        return 10
    if ruler_data in {"minor", "secondary", "minor_activation"}:
        return 5
    return 0


def _multi_activation_bonus(active_system_count: int) -> int:
    if active_system_count >= 4:
        return 22
    if active_system_count == 3:
        return 15
    if active_system_count == 2:
        return 8
    return 0


def _importance_level(score: int) -> str:
    if score >= 76:
        return "major"
    if score >= 51:
        return "strong"
    if score >= 26:
        return "moderate"
    return "minor"


def _interpretation_priority(score: int) -> str:
    if score >= 80:
        return "critical"
    if score >= 65:
        return "high"
    if score >= 45:
        return "normal"
    return "low"


def _dominant_activation_type(factors: dict[str, int]) -> str:
    mapped = {
        "natal_activation": "natal_planet_activation",
        "house_strength": "angular_house_activation",
        "sensitive_points": "node_activation",
        "multi_activation": "multi_system_activation",
        "transit_reinforcement": "transit_reinforcement",
        "stellium": "stellium_trigger",
    }
    strongest_key = max(mapped, key=lambda key: factors.get(key, 0))
    strongest_value = factors.get(strongest_key, 0)
    if strongest_value == 0:
        return "balanced"

    tied = [key for key in mapped if factors.get(key, 0) == strongest_value]
    if len(tied) > 1:
        return "balanced"
    return mapped[strongest_key]


def _activated_sensitive_points(moon_lon: float, personal_points: dict[str, Any]) -> list[str]:
    activated: list[str] = []
    lookup = {
        "atmakaraka": personal_points.get("chara_karakas", {}).get("atmakaraka"),
        "darakaraka": personal_points.get("chara_karakas", {}).get("darakaraka"),
        "amatyakaraka": personal_points.get("chara_karakas", {}).get("amatyakaraka"),
        "arudha_lagna": personal_points.get("arudha", {}).get("arudha_lagna"),
        "upapada_lagna": personal_points.get("upapada", {}).get("ul"),
    }

    for label, point_data in lookup.items():
        point_lon = _point_longitude(point_data)
        if point_lon is not None and _orb_distance(moon_lon, point_lon) <= 3.0:
            activated.append(label)

    return activated


def _point_longitude(point_data: dict[str, Any] | None) -> float | None:
    if not point_data:
        return None
    if "longitude" in point_data:
        return float(point_data["longitude"])
    sign = point_data.get("sign")
    degree = point_data.get("degree", 0.0)
    if sign in SIGN_NAMES:
        return SIGN_NAMES.index(sign) * 30.0 + float(degree)
    return None


def _find_planet(planets: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for planet in planets:
        if planet.get("name") == name:
            return planet
    return None


def _planet_longitude(planet: dict[str, Any]) -> float:
    if "abs_longitude" in planet:
        return float(planet["abs_longitude"])
    sign_idx = int(planet.get("sign_idx", 0))
    degree = float(planet.get("degree", 0.0))
    return sign_idx * 30.0 + degree


def _separation(lon_a: float, lon_b: float) -> float:
    return (lon_a - lon_b) % 360.0


def _orb_distance(lon_a: float, lon_b: float) -> float:
    diff = abs((lon_a - lon_b) % 360.0)
    return min(diff, 360.0 - diff)


def _normalize_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(value, "%Y-%m-%d")
    raise ValueError("Unsupported date format in transit window.")


def _date_string(value: Any) -> str:
    return _normalize_date(value).strftime("%Y-%m-%d")
