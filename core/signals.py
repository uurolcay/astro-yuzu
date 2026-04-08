"""Structured signal extraction layer for premium interpretation building."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
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

PLANET_TAGS = {
    "Sun": ["identity", "visibility", "purpose"],
    "Moon": ["emotions", "inner_state", "security"],
    "Mars": ["drive", "conflict", "action"],
    "Mercury": ["thinking", "communication", "analysis"],
    "Jupiter": ["growth", "wisdom", "expansion"],
    "Venus": ["relationships", "pleasure", "attraction"],
    "Saturn": ["pressure", "discipline", "maturity"],
    "Rahu": ["ambition", "obsession", "acceleration"],
    "Ketu": ["detachment", "release", "inner_work"],
}

HOUSE_TAGS = {
    1: ["identity", "self_direction"],
    2: ["money", "resources"],
    3: ["skills", "effort"],
    4: ["home", "inner_state"],
    5: ["creativity", "romance"],
    6: ["work", "stress"],
    7: ["relationships", "partnerships"],
    8: ["crisis", "transformation"],
    9: ["belief", "growth"],
    10: ["career", "status"],
    11: ["gains", "network"],
    12: ["retreat", "loss", "spirituality"],
}

SIGN_TAGS = {
    "Aries": ["initiative", "directness"],
    "Taurus": ["stability", "material_grounding"],
    "Gemini": ["adaptability", "learning"],
    "Cancer": ["sensitivity", "protection"],
    "Leo": ["confidence", "visibility"],
    "Virgo": ["precision", "correction"],
    "Libra": ["balance", "relating"],
    "Scorpio": ["intensity", "depth"],
    "Sagittarius": ["expansion", "vision"],
    "Capricorn": ["discipline", "ambition"],
    "Aquarius": ["independence", "future_focus"],
    "Pisces": ["intuition", "surrender"],
}

ASPECTS = {
    "conjunction": {"angle": 0, "orb": 8, "tags": ["fusion", "intensity"]},
    "sextile": {"angle": 60, "orb": 4, "tags": ["opportunity", "cooperation"]},
    "square": {"angle": 90, "orb": 6, "tags": ["friction", "effort"]},
    "trine": {"angle": 120, "orb": 6, "tags": ["flow", "support"]},
    "opposition": {"angle": 180, "orb": 8, "tags": ["polarity", "relationship_axis"]},
}


def extract_signals(natal_data: dict[str, Any], dasha_data: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    planets = natal_data.get("planets", [])
    signals: list[dict[str, Any]] = []

    for planet in planets:
        sign = SIGN_NAMES[planet["sign_idx"]]
        house = planet.get("house")
        signals.append(
            {
                "type": "planet_in_house",
                "planet": planet["name"],
                "house": house,
                "sign": sign,
                "tags": _dedupe_tags(PLANET_TAGS.get(planet["name"], []) + HOUSE_TAGS.get(house, [])),
                "context": {"abs_longitude": planet.get("abs_longitude")},
            }
        )
        signals.append(
            {
                "type": "planet_in_sign",
                "planet": planet["name"],
                "house": house,
                "sign": sign,
                "tags": _dedupe_tags(PLANET_TAGS.get(planet["name"], []) + SIGN_TAGS.get(sign, [])),
                "context": {"abs_longitude": planet.get("abs_longitude")},
            }
        )

        if planet["name"] in {"Rahu", "Ketu"}:
            signals.append(
                {
                    "type": "node_placement",
                    "planet": planet["name"],
                    "house": house,
                    "sign": sign,
                    "tags": _dedupe_tags(["nodes"] + PLANET_TAGS.get(planet["name"], []) + HOUSE_TAGS.get(house, [])),
                    "context": {"abs_longitude": planet.get("abs_longitude")},
                }
            )

    signals.extend(_extract_major_aspects(planets))
    signals.extend(_extract_conjunction_clusters(planets))
    signals.extend(_extract_dasha_signals(dasha_data or []))
    return signals


def _extract_major_aspects(planets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    signals = []
    for index, left in enumerate(planets):
        for right in planets[index + 1 :]:
            delta = abs(float(left.get("abs_longitude", 0.0)) - float(right.get("abs_longitude", 0.0)))
            delta = min(delta, 360.0 - delta)
            for aspect_name, config in ASPECTS.items():
                orb = abs(delta - config["angle"])
                if orb <= config["orb"]:
                    signals.append(
                        {
                            "type": "major_aspect",
                            "planet": left["name"],
                            "other_planet": right["name"],
                            "aspect": aspect_name,
                            "tags": _dedupe_tags(config["tags"] + PLANET_TAGS.get(left["name"], [])[:1] + PLANET_TAGS.get(right["name"], [])[:1]),
                            "context": {"delta": round(delta, 4), "orb": round(orb, 4)},
                        }
                    )
                    break
    return signals


def _extract_conjunction_clusters(planets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    house_clusters: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for planet in planets:
        if planet.get("house"):
            house_clusters[planet["house"]].append(planet)

    signals = []
    for house, members in house_clusters.items():
        if len(members) < 2:
            continue
        if len({planet["sign_idx"] for planet in members}) != 1:
            continue
        sorted_members = sorted(members, key=lambda item: item.get("degree", 0.0))
        spread = sorted_members[-1].get("degree", 0.0) - sorted_members[0].get("degree", 0.0)
        if spread > 10:
            continue
        signals.append(
            {
                "type": "conjunction_cluster",
                "planet": sorted_members[0]["name"],
                "house": house,
                "sign": SIGN_NAMES[sorted_members[0]["sign_idx"]],
                "cluster_planets": [planet["name"] for planet in sorted_members],
                "tags": _dedupe_tags(["concentration", "amplification"] + HOUSE_TAGS.get(house, [])),
                "context": {"spread": round(spread, 4)},
            }
        )
    return signals


def _extract_dasha_signals(dasha_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not dasha_data:
        return []
    today = datetime.utcnow().date()
    signals = []
    for index, period in enumerate(dasha_data[:3]):
        start = _parse_date(period.get("start"))
        end = _parse_date(period.get("end"))
        planet = period.get("planet")
        is_active = start <= today <= end if start and end else index == 0
        if is_active or index == 0:
            signals.append(
                {
                    "type": "dasha_activation",
                    "planet": planet,
                    "tags": _dedupe_tags(["timing", "activation"] + PLANET_TAGS.get(planet, [])),
                    "context": {
                        "start": period.get("start"),
                        "end": period.get("end"),
                        "is_active": is_active,
                    },
                }
            )
    return signals


def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def _dedupe_tags(tags):
    unique = []
    for tag in tags:
        if tag and tag not in unique:
            unique.append(tag)
    return unique
