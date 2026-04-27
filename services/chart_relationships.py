from __future__ import annotations

from copy import deepcopy


SIGN_RULERS = {
    0: "Mars",
    1: "Venus",
    2: "Mercury",
    3: "Moon",
    4: "Sun",
    5: "Mercury",
    6: "Venus",
    7: "Mars",
    8: "Jupiter",
    9: "Saturn",
    10: "Saturn",
    11: "Jupiter",
}

OWN_SIGNS = {
    "Sun": {4},
    "Moon": {3},
    "Mars": {0, 7},
    "Mercury": {2, 5},
    "Jupiter": {8, 11},
    "Venus": {1, 6},
    "Saturn": {9, 10},
}

EXALTATION_SIGNS = {
    "Sun": 0,
    "Moon": 1,
    "Mars": 9,
    "Mercury": 5,
    "Jupiter": 3,
    "Venus": 11,
    "Saturn": 6,
}

DEBILITATION_SIGNS = {
    "Sun": 6,
    "Moon": 7,
    "Mars": 3,
    "Mercury": 11,
    "Jupiter": 9,
    "Venus": 5,
    "Saturn": 0,
}

NATURAL_MALEFICS = {"Saturn", "Mars", "Rahu", "Ketu", "Sun"}
NATURAL_BENEFICS = {"Jupiter", "Venus", "Mercury"}


def build_chart_relationships(natal_data, navamsa_data=None):
    natal_data = natal_data or {}
    navamsa_data = navamsa_data or {}
    planets = _planet_map(natal_data.get("planets") or [])
    ascendant = natal_data.get("ascendant") or {}
    asc_sign = ascendant.get("sign_idx")
    confidence_notes = []

    if asc_sign is None:
        confidence_notes.append("Ascendant sign is missing; whole-sign house derivations are limited.")
        asc_sign = 0

    planet_houses = _planet_houses(planets, int(asc_sign))
    house_lords, lorded_houses_by_planet = _house_lords(int(asc_sign))
    planet_dignities = _planet_dignities(planets)
    functional_nature = _functional_nature(lorded_houses_by_planet)
    vedic_aspects = _vedic_aspects(planets, planet_houses)
    moon_relative_houses = _relative_houses(planets, "Moon", confidence_notes, label="Moon")
    sun_relative_houses = _relative_houses(planets, "Sun", confidence_notes, label="Sun")
    afflictions = _afflictions(planets, planet_houses, planet_dignities, vedic_aspects)
    navamsa_strength = _navamsa_strength(navamsa_data, confidence_notes)

    if not navamsa_data:
        confidence_notes.append("Navamsa data is unavailable; navamsa strength remains empty.")

    return {
        "planet_houses": planet_houses,
        "house_lords": house_lords,
        "lorded_houses_by_planet": lorded_houses_by_planet,
        "planet_dignities": planet_dignities,
        "functional_nature": functional_nature,
        "vedic_aspects": vedic_aspects,
        "moon_relative_houses": moon_relative_houses,
        "sun_relative_houses": sun_relative_houses,
        "afflictions": afflictions,
        "navamsa_strength": navamsa_strength,
        "confidence_notes": confidence_notes,
    }


def _planet_map(planets):
    return {str(item.get("name") or "").strip(): deepcopy(item) for item in planets if str(item.get("name") or "").strip()}


def _planet_houses(planets, asc_sign):
    houses = {}
    for name, planet in planets.items():
        if planet.get("sign_idx") is None:
            continue
        houses[name] = ((int(planet["sign_idx"]) - asc_sign + 12) % 12) + 1
    return houses


def _house_lords(asc_sign):
    house_lords = {}
    lorded_houses_by_planet = {}
    for house in range(1, 13):
        sign_idx = (asc_sign + house - 1) % 12
        lord = SIGN_RULERS[sign_idx]
        house_lords[str(house)] = lord
        lorded_houses_by_planet.setdefault(lord, []).append(house)
    return house_lords, lorded_houses_by_planet


def _planet_dignities(planets):
    dignities = {}
    for name, planet in planets.items():
        if name in {"Rahu", "Ketu"}:
            dignities[name] = "node"
            continue
        sign_idx = planet.get("sign_idx")
        if sign_idx is None:
            dignities[name] = "not_evaluated"
            continue
        sign_idx = int(sign_idx)
        if sign_idx == EXALTATION_SIGNS.get(name):
            dignities[name] = "exalted"
        elif sign_idx == DEBILITATION_SIGNS.get(name):
            dignities[name] = "debilitated"
        elif sign_idx in OWN_SIGNS.get(name, set()):
            dignities[name] = "own"
        else:
            dignities[name] = "neutral"
    return dignities


def _functional_nature(lorded_houses_by_planet):
    results = {}
    for planet, houses in lorded_houses_by_planet.items():
        house_set = set(houses)
        if house_set & {5, 9} and house_set & {4, 7, 10}:
            results[planet] = "yogakaraka"
        elif house_set & {6, 8, 12} and house_set & {5, 9}:
            results[planet] = "mixed"
        elif house_set & {6, 8, 12}:
            results[planet] = "malefic"
        elif house_set & {1, 5, 9}:
            results[planet] = "benefic"
        elif house_set & {4, 7, 10}:
            results[planet] = "neutral"
        else:
            results[planet] = "mixed"
    for planet in ("Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"):
        results.setdefault(planet, "not_evaluated" if planet in {"Rahu", "Ketu"} else "neutral")
    return results


def _vedic_aspects(planets, planet_houses):
    aspects = []
    for from_planet, from_house in planet_houses.items():
        offsets = [(7, f"{from_planet.lower()}_7th")]
        if from_planet == "Mars":
            offsets.extend([(4, "mars_4th"), (8, "mars_8th")])
        elif from_planet == "Jupiter":
            offsets.extend([(5, "jupiter_5th"), (9, "jupiter_9th")])
        elif from_planet == "Saturn":
            offsets.extend([(3, "saturn_3rd"), (10, "saturn_10th")])
        for offset, aspect_type in offsets:
            target_house = ((from_house + offset - 2) % 12) + 1
            for to_planet, to_house in planet_houses.items():
                if to_planet == from_planet:
                    continue
                if to_house == target_house:
                    aspects.append(
                        {
                            "from_planet": from_planet,
                            "to_planet": to_planet,
                            "aspect_type": aspect_type,
                            "from_house": from_house,
                            "to_house": to_house,
                            "strength": "standard",
                        }
                    )
    return aspects


def _relative_houses(planets, reference_name, confidence_notes, *, label):
    reference = planets.get(reference_name)
    if not reference or reference.get("sign_idx") is None:
        confidence_notes.append(f"{label} sign is missing; relative houses from {label} are unavailable.")
        return {}
    reference_sign = int(reference["sign_idx"])
    relative = {}
    for name, planet in planets.items():
        if name == reference_name or planet.get("sign_idx") is None:
            continue
        relative[name] = ((int(planet["sign_idx"]) - reference_sign + 12) % 12) + 1
    return relative


def _afflictions(planets, planet_houses, planet_dignities, vedic_aspects):
    afflictions = {}
    conjunction_map = {}
    for name, planet in planets.items():
        sign_idx = planet.get("sign_idx")
        if sign_idx is None:
            continue
        conjunction_map.setdefault(int(sign_idx), []).append(name)

    malefic_aspect_lookup = {}
    for aspect in vedic_aspects:
        if aspect["from_planet"] in NATURAL_MALEFICS:
            malefic_aspect_lookup.setdefault(aspect["to_planet"], []).append(f"aspected_by_{aspect['from_planet'].lower()}")

    for name, planet in planets.items():
        reasons = []
        sign_idx = planet.get("sign_idx")
        if sign_idx is not None:
            others = [item for item in conjunction_map.get(int(sign_idx), []) if item != name]
            if any(item in NATURAL_MALEFICS for item in others):
                reasons.append("conjunct_with_malefic")
            if any(item in {"Rahu", "Ketu"} for item in others):
                reasons.append("with_rahu_or_ketu")
        reasons.extend(malefic_aspect_lookup.get(name, []))
        if planet_dignities.get(name) == "debilitated":
            reasons.append("debilitated")
        afflictions[name] = {
            "is_afflicted": bool(reasons),
            "reasons": reasons,
        }
    return afflictions


def _navamsa_strength(navamsa_data, confidence_notes):
    planets = _planet_map((navamsa_data or {}).get("planets") or [])
    strength = {}
    for name, planet in planets.items():
        sign_idx = planet.get("sign_idx")
        if sign_idx is None:
            continue
        sign_idx = int(sign_idx)
        if name in {"Rahu", "Ketu"}:
            strength[name] = "node"
        elif sign_idx == EXALTATION_SIGNS.get(name):
            strength[name] = "exalted"
        elif sign_idx == DEBILITATION_SIGNS.get(name):
            strength[name] = "debilitated"
        elif sign_idx in OWN_SIGNS.get(name, set()):
            strength[name] = "own"
        else:
            strength[name] = "neutral"
    return strength
