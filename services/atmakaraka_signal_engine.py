from __future__ import annotations

from core.karaka_utils import karaka_degree_value, select_atmakaraka


ATMAKARAKA_PLANET_PATTERNS = {
    "Sun": {
        "desire_pattern": "to become fully responsible for one's direction and visible purpose",
        "soul_lesson": "lead from integrity rather than identity inflation",
        "risk_pattern": "over-identifying with recognition, pride, or authority pressure",
        "evolution_path": "move from validation-seeking into conscious responsibility and steady inner authority",
        "base_karmic_intensity": "high",
        "report_usage": ["life_direction", "authority", "karmic"],
    },
    "Moon": {
        "desire_pattern": "to find emotional truth, belonging, and trustworthy inner regulation",
        "soul_lesson": "learn not to organize life around fluctuating emotional comfort alone",
        "risk_pattern": "attachment to safety, mood-led choices, or emotional over-absorption",
        "evolution_path": "move from reactivity into emotional wisdom, containment, and relational maturity",
        "base_karmic_intensity": "high",
        "report_usage": ["emotional", "life_direction", "karmic"],
    },
    "Mars": {
        "desire_pattern": "to act decisively, cut through stagnation, and test strength through effort",
        "soul_lesson": "use force in service of truth instead of conflict habit or self-protection",
        "risk_pattern": "reactivity, conflict cycles, impatience, or over-identifying with struggle",
        "evolution_path": "move from friction-based identity into disciplined courageous action",
        "base_karmic_intensity": "very_high",
        "report_usage": ["discipline", "risk", "life_direction"],
    },
    "Mercury": {
        "desire_pattern": "to understand, connect, interpret, and master pattern through language and intelligence",
        "soul_lesson": "stop mistaking analysis for resolution",
        "risk_pattern": "overthinking, strategic avoidance, or living from mental control alone",
        "evolution_path": "move from restless cognition into grounded discernment and skillful communication",
        "base_karmic_intensity": "medium",
        "report_usage": ["communication", "learning_style", "life_direction"],
    },
    "Jupiter": {
        "desire_pattern": "to align life with meaning, truth, guidance, and inner moral coherence",
        "soul_lesson": "embody wisdom instead of performing certainty or righteousness",
        "risk_pattern": "dogma, overconfidence, inflated hope, or spiritual bypassing",
        "evolution_path": "move from borrowed belief into lived wisdom and generous orientation",
        "base_karmic_intensity": "high",
        "report_usage": ["spiritual", "guidance", "life_direction"],
    },
    "Venus": {
        "desire_pattern": "to find value, harmony, affection, and a life that feels relationally meaningful",
        "soul_lesson": "choose value over comfort and intimacy over aesthetic control",
        "risk_pattern": "comfort dependence, approval-seeking, or compromise for emotional ease",
        "evolution_path": "move from pleasing and attachment into self-worth and clean relational reciprocity",
        "base_karmic_intensity": "medium",
        "report_usage": ["relationship", "wealth", "life_direction"],
    },
    "Saturn": {
        "desire_pattern": "to mature through endurance, duty, structure, and contact with reality",
        "soul_lesson": "build meaning through sustained responsibility instead of fear-driven control",
        "risk_pattern": "hardness, isolation, delay fixation, or identity fused with burden",
        "evolution_path": "move from defensive contraction into stable mastery and earned inner authority",
        "base_karmic_intensity": "very_high",
        "report_usage": ["discipline", "karmic", "life_direction"],
    },
    "Rahu": {
        "desire_pattern": "to chase growth through unmastered hunger, amplification, and unconventional appetite",
        "soul_lesson": "separate soul direction from compulsive acceleration",
        "risk_pattern": "obsession, destabilization, hunger without integration, or destiny inflation",
        "evolution_path": "move from compulsive craving into conscious innovation and integrated appetite",
        "base_karmic_intensity": "very_high",
        "report_usage": ["risk", "visibility", "life_direction", "karmic"],
    },
}

HOUSE_DOMAIN_MAP = {
    1: "identity and embodied direction",
    2: "value, resources, and speech karma",
    3: "effort, courage, and self-generated initiative",
    4: "emotional roots, home, and inner stability",
    5: "creativity, intelligence, children, and merit",
    6: "friction, discipline, labor, and karmic correction",
    7: "partnership, mirroring, and relational contracts",
    8: "upheaval, hidden karma, and deep transformation",
    9: "belief, dharma, mentors, and meaning",
    10: "career, duty, visibility, and public consequence",
    11: "gains, networks, and desire fulfillment",
    12: "release, loss, surrender, and spiritual closure",
}


def detect_atmakaraka(natal_data, mode=None):
    karaka_mode = mode
    if karaka_mode is None:
        karaka_mode = ((natal_data or {}).get("karakas") or {}).get("karaka_mode")
    if karaka_mode is None:
        # Legacy direct callers often pass a minimal natal payload without karakas metadata.
        # Keep those ad hoc calls Rahu-aware for backward compatibility, while full natal
        # payloads honor the explicit mode set by engines_natal.
        karaka_mode = "8"
    normalized_mode = "8" if str(karaka_mode or "7").strip() == "8" else "7"
    return select_atmakaraka((natal_data or {}).get("planets") or [], mode=normalized_mode)


def build_atmakaraka_signal(natal_data, chart_relationships=None):
    detected = detect_atmakaraka(natal_data)
    if not detected:
        return {
            "source": "atmakaraka_signal_engine",
            "signals": [],
            "confidence_notes": ["Atmakaraka could not be determined because usable planet degree data is missing."],
        }
    planet = detected["planet"]
    pattern = ATMAKARAKA_PLANET_PATTERNS.get(planet)
    if not pattern:
        return {
            "source": "atmakaraka_signal_engine",
            "signals": [],
            "confidence_notes": [f"No Atmakaraka pattern mapping is defined for {planet}."],
        }
    chart_relationships = chart_relationships or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    afflictions = chart_relationships.get("afflictions") or {}
    house_domain = HOUSE_DOMAIN_MAP.get(int(planet_houses.get(planet) or 0), "an unclassified karmic life area")
    affliction_info = afflictions.get(planet) or {}
    affliction_level = _affliction_level(affliction_info)
    karmic_intensity = _karmic_intensity(pattern["base_karmic_intensity"], affliction_level)
    signal = {
        "key": f"atmakaraka:{planet.lower()}",
        "label": f"Atmakaraka - {planet}",
        "source": "atmakaraka",
        "planet": planet,
        "categories": _categories_for_atmakaraka(pattern["report_usage"]),
        "tone": "risk" if affliction_level in {"moderate", "high"} else "opportunity",
        "keywords": ["atmakaraka", planet.lower(), "soul_direction", house_domain],
        "strength": _strength_value(karmic_intensity),
        "explanation": (
            f"{planet} is Atmakaraka, so the chart's soul-direction is filtered through {pattern['desire_pattern']}. "
            f"The core lesson centers on {pattern['soul_lesson']}, especially through {house_domain}."
        ),
        "report_usage": list(pattern["report_usage"]),
    }
    summary = {
        "source": "atmakaraka_signal_engine",
        "atmakaraka_planet": planet,
        "desire_pattern": pattern["desire_pattern"],
        "soul_lesson": pattern["soul_lesson"],
        "risk_pattern": pattern["risk_pattern"],
        "evolution_path": pattern["evolution_path"],
        "house_domain": house_domain,
        "affliction_level": affliction_level,
        "karmic_intensity": karmic_intensity,
        "signals": [signal],
        "confidence_notes": [],
    }
    if affliction_level in {"moderate", "high"}:
        summary["confidence_notes"].append(
            f"Atmakaraka {planet} is afflicted, so the soul lesson tends to intensify through pressure rather than comfort."
        )
    if detected.get("rahu_adjusted"):
        summary["confidence_notes"].append("Rahu Atmakaraka degree was adjusted using reverse-within-sign logic.")
    return summary


def _atmakaraka_degree_value(row):
    return karaka_degree_value(row, mode="8")


def _affliction_level(affliction_info):
    reasons = list((affliction_info or {}).get("reasons") or [])
    if not reasons:
        return "low"
    if len(reasons) >= 2 or any(reason in {"with_rahu_or_ketu", "debilitated"} for reason in reasons):
        return "high"
    return "moderate"


def _karmic_intensity(base_intensity, affliction_level):
    if affliction_level == "high":
        return "very_high"
    if affliction_level == "moderate" and base_intensity == "medium":
        return "high"
    return base_intensity


def _strength_value(karmic_intensity):
    return {
        "medium": 3.0,
        "high": 4.2,
        "very_high": 5.0,
    }.get(karmic_intensity, 3.2)


def _categories_for_atmakaraka(report_usage):
    categories = ["life_direction", "karmic"]
    categories.extend(str(item or "").strip().lower().replace(" ", "_") for item in report_usage or [])
    return sorted(set(categories))
