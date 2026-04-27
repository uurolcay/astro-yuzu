from __future__ import annotations

from copy import deepcopy

try:
    from services.chart_relationships import (
        SIGN_RULERS,
        build_chart_relationships,
    )
except Exception:  # pragma: no cover - safe fallback
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
    build_chart_relationships = None


YOGA_CATALOG = {
    "Raj Yoga": {
        "yoga_name": "Raj Yoga",
        "base_condition": "Kendra and trikona lord interaction",
        "involved_planets": [],
        "involved_houses": [1, 4, 5, 7, 9, 10],
        "domain": "authority",
        "positive_signals": ["leadership potential", "recognition through merit"],
        "risk_signals": ["status pressure", "inflated expectations"],
        "functional_requirements": ["house_lord_data"],
        "strength_rules": ["lord dignity", "kendra/trikona reinforcement"],
        "house_field_mapping": {"1": "identity", "5": "merit", "9": "fortune", "10": "status"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["stronger during career and visibility cycles"],
        "interaction_rules": ["strengthens with Dhan and Lakshmi patterns"],
        "report_usage": ["career", "authority", "visibility"],
    },
    "Laxmi Yoga": {
        "yoga_name": "Laxmi Yoga",
        "base_condition": "Strong 9th lord and Venus/Jupiter wealth support",
        "involved_planets": ["Venus", "Jupiter"],
        "involved_houses": [1, 5, 9],
        "domain": "wealth",
        "positive_signals": ["resource support", "grace and prosperity potential"],
        "risk_signals": ["comfort dependence"],
        "functional_requirements": ["house_lord_data"],
        "strength_rules": ["9th lord dignity", "benefic support"],
        "house_field_mapping": {"5": "merit", "9": "fortune"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["becomes visible in prosperity windows"],
        "interaction_rules": ["pairs well with Dhan Yoga"],
        "report_usage": ["wealth", "opportunity"],
    },
    "Gaj Kesari Yoga": {
        "yoga_name": "Gaj Kesari Yoga",
        "base_condition": "Jupiter in kendra from Moon",
        "involved_planets": ["Moon", "Jupiter"],
        "involved_houses": [1, 4, 7, 10],
        "domain": "guidance",
        "positive_signals": ["emotional intelligence", "wise support", "stabilizing judgment"],
        "risk_signals": ["self-satisfaction if unsupported"],
        "functional_requirements": ["moon_sign", "jupiter_sign"],
        "strength_rules": ["Moon stability", "Jupiter dignity", "kendra distance"],
        "house_field_mapping": {"1": "identity", "4": "emotional base", "7": "relational judgment", "10": "public intelligence"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["stronger in emotional and guidance contexts"],
        "interaction_rules": ["supports Adhi and Hamsa patterns"],
        "report_usage": ["emotional", "support_strategy", "career"],
    },
    "Vipareeta Raj Yoga": {
        "yoga_name": "Vipareeta Raj Yoga",
        "base_condition": "Dusthana lords placed in dusthanas",
        "involved_planets": [],
        "involved_houses": [6, 8, 12],
        "domain": "resilience",
        "positive_signals": ["success through adversity", "reversal strength"],
        "risk_signals": ["stress-heavy growth path"],
        "functional_requirements": ["house_lord_data"],
        "strength_rules": ["dusthana lord mapping", "house support"],
        "house_field_mapping": {"6": "struggle", "8": "upheaval", "12": "release"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["shows most under pressure cycles"],
        "interaction_rules": ["can coexist with Raj Yoga"],
        "report_usage": ["risk", "life_direction"],
    },
    "Dhan Yoga": {
        "yoga_name": "Dhan Yoga",
        "base_condition": "2nd/11th wealth factors tied to benefic support",
        "involved_planets": ["Jupiter", "Venus", "Mercury"],
        "involved_houses": [2, 5, 9, 11],
        "domain": "wealth",
        "positive_signals": ["earning power", "asset accumulation potential"],
        "risk_signals": ["resource volatility if unsupported"],
        "functional_requirements": ["house_lord_data"],
        "strength_rules": ["2nd and 11th connection", "benefic dignity"],
        "house_field_mapping": {"2": "assets", "11": "gains"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["visible in money cycles"],
        "interaction_rules": ["strengthens with Laxmi Yoga"],
        "report_usage": ["wealth", "career"],
    },
    "Neechabhanga Raj Yoga": {
        "yoga_name": "Neechabhanga Raj Yoga",
        "base_condition": "Debility cancellation through dispositor or exaltation link",
        "involved_planets": [],
        "involved_houses": [],
        "domain": "resilience",
        "positive_signals": ["comeback strength", "recovered confidence"],
        "risk_signals": ["delayed stabilization"],
        "functional_requirements": ["debilitation_status", "house_lord_data"],
        "strength_rules": ["debility cancellation rules"],
        "house_field_mapping": {},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["visible after setbacks"],
        "interaction_rules": ["can amplify Raj Yoga"],
        "report_usage": ["life_direction", "authority"],
    },
    "Mahabhagya Yoga": {
        "yoga_name": "Mahabhagya Yoga",
        "base_condition": "Birth time, gender, and luminary sign parity alignment",
        "involved_planets": ["Sun", "Moon", "Lagna"],
        "involved_houses": [1],
        "domain": "fortune",
        "positive_signals": ["high natural support", "life-lift potential"],
        "risk_signals": ["entitlement"],
        "functional_requirements": ["birth_gender", "day_night_context"],
        "strength_rules": ["luminary parity", "lagna parity"],
        "house_field_mapping": {"1": "identity"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["background fortune signature"],
        "interaction_rules": ["strengthens Raj and Pushkala patterns"],
        "report_usage": ["life_direction", "opportunity"],
    },
    "Adhi Yoga": {
        "yoga_name": "Adhi Yoga",
        "base_condition": "Benefics in 6th, 7th, 8th from Moon",
        "involved_planets": ["Jupiter", "Venus", "Mercury"],
        "involved_houses": [6, 7, 8],
        "domain": "support_strategy",
        "positive_signals": ["mental composure", "social grace"],
        "risk_signals": ["reliance on external harmony"],
        "functional_requirements": ["moon_reference"],
        "strength_rules": ["benefic count around Moon"],
        "house_field_mapping": {"7": "social support"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["most visible in relational contexts"],
        "interaction_rules": ["aligns with Gaj Kesari"],
        "report_usage": ["emotional", "relationship"],
    },
    "Ruchaka Yoga": {
        "yoga_name": "Ruchaka Yoga",
        "base_condition": "Mars in own or exalted sign in kendra",
        "involved_planets": ["Mars"],
        "involved_houses": [1, 4, 7, 10],
        "domain": "discipline",
        "positive_signals": ["decisive action", "courage", "tactical force"],
        "risk_signals": ["reactivity", "conflict intensity"],
        "functional_requirements": ["mars_sign", "mars_house"],
        "strength_rules": ["own/exalted dignity", "kendra placement"],
        "house_field_mapping": {"1": "identity", "10": "career"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["stronger in pressure periods"],
        "interaction_rules": ["can amplify Chandra Mangala"],
        "report_usage": ["discipline", "career", "risk"],
    },
    "Bhadra Yoga": {
        "yoga_name": "Bhadra Yoga",
        "base_condition": "Mercury in own or exalted sign in kendra",
        "involved_planets": ["Mercury"],
        "involved_houses": [1, 4, 7, 10],
        "domain": "communication",
        "positive_signals": ["clarity", "adaptation", "intellectual control"],
        "risk_signals": ["nervous over-processing"],
        "functional_requirements": ["mercury_sign", "mercury_house"],
        "strength_rules": ["own/exalted dignity", "kendra placement"],
        "house_field_mapping": {"1": "identity", "10": "career"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["highly visible in work and learning"],
        "interaction_rules": ["aligns with Budha-Aditya"],
        "report_usage": ["communication", "career", "visibility"],
    },
    "Hamsa Yoga": {
        "yoga_name": "Hamsa Yoga",
        "base_condition": "Jupiter in own or exalted sign in kendra",
        "involved_planets": ["Jupiter"],
        "involved_houses": [1, 4, 7, 10],
        "domain": "guidance",
        "positive_signals": ["wisdom", "ethical support", "teaching force"],
        "risk_signals": ["moral certainty"],
        "functional_requirements": ["jupiter_sign", "jupiter_house"],
        "strength_rules": ["own/exalted dignity", "kendra placement"],
        "house_field_mapping": {"1": "identity", "4": "inner guidance", "10": "public stature"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["strong in guidance and family decisions"],
        "interaction_rules": ["supports Gaj Kesari"],
        "report_usage": ["spiritual", "career", "support_strategy"],
    },
    "Malavya Yoga": {
        "yoga_name": "Malavya Yoga",
        "base_condition": "Venus in own or exalted sign in kendra",
        "involved_planets": ["Venus"],
        "involved_houses": [1, 4, 7, 10],
        "domain": "relationship",
        "positive_signals": ["relational grace", "comfort", "aesthetic intelligence"],
        "risk_signals": ["comfort dependence"],
        "functional_requirements": ["venus_sign", "venus_house"],
        "strength_rules": ["own/exalted dignity", "kendra placement"],
        "house_field_mapping": {"4": "comfort", "7": "partnership", "10": "public charm"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["strong in relationship and visibility cycles"],
        "interaction_rules": ["supports Dhan and Laxmi patterns"],
        "report_usage": ["relationship", "wealth", "visibility"],
    },
    "Shasha Yoga": {
        "yoga_name": "Shasha Yoga",
        "base_condition": "Saturn in own or exalted sign in kendra",
        "involved_planets": ["Saturn"],
        "involved_houses": [1, 4, 7, 10],
        "domain": "discipline",
        "positive_signals": ["endurance", "strategy", "institutional strength"],
        "risk_signals": ["hardness", "delay pressure"],
        "functional_requirements": ["saturn_sign", "saturn_house"],
        "strength_rules": ["own/exalted dignity", "kendra placement"],
        "house_field_mapping": {"10": "authority", "4": "stability"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["strong in long pressure cycles"],
        "interaction_rules": ["can intensify responsibility-heavy yogas"],
        "report_usage": ["discipline", "authority", "career"],
    },
    "Sunapha Yoga": {
        "yoga_name": "Sunapha Yoga",
        "base_condition": "Planet in 2nd from Moon excluding Sun",
        "involved_planets": [],
        "involved_houses": [2],
        "domain": "resourcefulness",
        "positive_signals": ["self-generated support", "mental resourcefulness"],
        "risk_signals": ["over-self-reliance"],
        "functional_requirements": ["moon_reference"],
        "strength_rules": ["planet quality in 2nd from Moon"],
        "house_field_mapping": {"2": "resources"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["visible in self-sustaining phases"],
        "interaction_rules": ["can combine with Dhurdhara"],
        "report_usage": ["emotional", "wealth"],
    },
    "Anapha Yoga": {
        "yoga_name": "Anapha Yoga",
        "base_condition": "Planet in 12th from Moon excluding Sun",
        "involved_planets": [],
        "involved_houses": [12],
        "domain": "interiority",
        "positive_signals": ["self-possession", "inner buffer"],
        "risk_signals": ["withdrawal"],
        "functional_requirements": ["moon_reference"],
        "strength_rules": ["planet quality in 12th from Moon"],
        "house_field_mapping": {"12": "withdrawal"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["strong in reflective cycles"],
        "interaction_rules": ["can combine with Dhurdhara"],
        "report_usage": ["emotional", "spiritual"],
    },
    "Dhurdhara Yoga": {
        "yoga_name": "Dhurdhara Yoga",
        "base_condition": "Planets on both sides of Moon excluding Sun",
        "involved_planets": [],
        "involved_houses": [2, 12],
        "domain": "resourcefulness",
        "positive_signals": ["mental support on both sides", "buffered adaptability"],
        "risk_signals": ["too many moving inputs"],
        "functional_requirements": ["moon_reference"],
        "strength_rules": ["quality of 2nd and 12th from Moon"],
        "house_field_mapping": {"2": "resources", "12": "release"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["strong in adaptive phases"],
        "interaction_rules": ["contains Sunapha and Anapha logic"],
        "report_usage": ["emotional", "support_strategy"],
    },
    "Kemadruma Yoga": {
        "yoga_name": "Kemadruma Yoga",
        "base_condition": "No planets in 2nd or 12th from Moon",
        "involved_planets": ["Moon"],
        "involved_houses": [2, 12],
        "domain": "emotional",
        "positive_signals": ["independence through inner development"],
        "risk_signals": ["emotional isolation", "support gaps"],
        "functional_requirements": ["moon_reference"],
        "strength_rules": ["absence around Moon", "Moon condition"],
        "house_field_mapping": {"2": "containment", "12": "release"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["felt strongly in low-support periods"],
        "interaction_rules": ["cancelled by Sunapha/Anapha/Dhurdhara"],
        "report_usage": ["emotional", "risk"],
    },
    "Chandra Mangala Yoga": {
        "yoga_name": "Chandra Mangala Yoga",
        "base_condition": "Moon and Mars conjunction or same sign",
        "involved_planets": ["Moon", "Mars"],
        "involved_houses": [],
        "domain": "action",
        "positive_signals": ["initiative under feeling", "earning drive"],
        "risk_signals": ["emotional reactivity", "conflict heat"],
        "functional_requirements": ["moon_sign", "mars_sign"],
        "strength_rules": ["same sign", "house relevance"],
        "house_field_mapping": {"2": "money drive", "10": "action drive"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["strong during pressure and money cycles"],
        "interaction_rules": ["amplifies Ruchaka"],
        "report_usage": ["career", "risk", "wealth"],
    },
    "Budha-Aditya Yoga": {
        "yoga_name": "Budha-Aditya Yoga",
        "base_condition": "Sun and Mercury conjunction or same sign",
        "involved_planets": ["Sun", "Mercury"],
        "involved_houses": [],
        "domain": "communication",
        "positive_signals": ["intellect plus visibility", "clear self-expression"],
        "risk_signals": ["ego-mind fusion", "over-analysis"],
        "functional_requirements": ["sun_sign", "mercury_sign"],
        "strength_rules": ["same sign", "house support"],
        "house_field_mapping": {"1": "identity voice", "10": "public speech"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["strong in study and career phases"],
        "interaction_rules": ["aligns with Bhadra"],
        "report_usage": ["communication", "career", "visibility"],
    },
    "Amala Yoga": {
        "yoga_name": "Amala Yoga",
        "base_condition": "Benefic in 10th from Lagna or Moon",
        "involved_planets": ["Jupiter", "Venus", "Mercury"],
        "involved_houses": [10],
        "domain": "visibility",
        "positive_signals": ["clean reputation", "steady public merit"],
        "risk_signals": ["reputation sensitivity"],
        "functional_requirements": ["lagna_reference", "moon_reference"],
        "strength_rules": ["benefic quality in 10th"],
        "house_field_mapping": {"10": "public reputation"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["visible in career and public life"],
        "interaction_rules": ["supports Raj and career yogas"],
        "report_usage": ["career", "visibility", "authority"],
    },
    "Parvata Yoga": {
        "yoga_name": "Parvata Yoga",
        "base_condition": "Strength of kendras and benefic support around Lagna",
        "involved_planets": [],
        "involved_houses": [1, 4, 7, 10],
        "domain": "stability",
        "positive_signals": ["supportive social standing", "stability"],
        "risk_signals": ["comfort rigidity"],
        "functional_requirements": ["house_lord_data", "benefic_malefic_balance"],
        "strength_rules": ["kendra cleanliness", "benefic support"],
        "house_field_mapping": {"1": "identity", "10": "reputation"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["background support pattern"],
        "interaction_rules": ["supports Amala and Raj patterns"],
        "report_usage": ["career", "support_strategy"],
    },
    "Vesi Yoga": {
        "yoga_name": "Vesi Yoga",
        "base_condition": "Planet in 2nd from Sun excluding Moon",
        "involved_planets": [],
        "involved_houses": [2],
        "domain": "visibility",
        "positive_signals": ["self-curated expression", "controlled output"],
        "risk_signals": ["self-conscious presentation"],
        "functional_requirements": ["sun_reference"],
        "strength_rules": ["planet quality in 2nd from Sun"],
        "house_field_mapping": {"2": "speech and output"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["visible in reputation cycles"],
        "interaction_rules": ["can combine with Obhayachari"],
        "report_usage": ["communication", "visibility"],
    },
    "Vasi Yoga": {
        "yoga_name": "Vasi Yoga",
        "base_condition": "Planet in 12th from Sun excluding Moon",
        "involved_planets": [],
        "involved_houses": [12],
        "domain": "interiority",
        "positive_signals": ["preparatory intelligence", "strategic restraint"],
        "risk_signals": ["hidden pressure"],
        "functional_requirements": ["sun_reference"],
        "strength_rules": ["planet quality in 12th from Sun"],
        "house_field_mapping": {"12": "withdrawal and preparation"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["visible before public moves"],
        "interaction_rules": ["can combine with Obhayachari"],
        "report_usage": ["communication", "support_strategy"],
    },
    "Obhayachari Yoga": {
        "yoga_name": "Obhayachari Yoga",
        "base_condition": "Planets on both sides of Sun excluding Moon",
        "involved_planets": [],
        "involved_houses": [2, 12],
        "domain": "visibility",
        "positive_signals": ["rounded self-expression", "adaptive presence"],
        "risk_signals": ["split attention"],
        "functional_requirements": ["sun_reference"],
        "strength_rules": ["quality of both flanks around Sun"],
        "house_field_mapping": {"2": "output", "12": "processing"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": False,
        "activation_rules": ["visible in role-heavy phases"],
        "interaction_rules": ["contains Vesi and Vasi logic"],
        "report_usage": ["communication", "visibility", "career"],
    },
    "Sakata Yoga": {
        "yoga_name": "Sakata Yoga",
        "base_condition": "Jupiter in 6th, 8th, or 12th from Moon",
        "involved_planets": ["Moon", "Jupiter"],
        "involved_houses": [6, 8, 12],
        "domain": "risk",
        "positive_signals": ["resilience through fluctuation"],
        "risk_signals": ["instability cycles", "faith under strain"],
        "functional_requirements": ["moon_sign", "jupiter_sign"],
        "strength_rules": ["distance from Moon", "Jupiter support"],
        "house_field_mapping": {"6": "friction", "8": "upheaval", "12": "loss-release"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["felt in fluctuating life phases"],
        "interaction_rules": ["countered by strong Gaj Kesari support"],
        "report_usage": ["risk", "emotional"],
    },
    "Kahala Yoga": {
        "yoga_name": "Kahala Yoga",
        "base_condition": "Strong 4th lord and Jupiter support for courage/leadership",
        "involved_planets": ["Jupiter"],
        "involved_houses": [4, 10],
        "domain": "authority",
        "positive_signals": ["administrative courage", "standing power"],
        "risk_signals": ["rigid control"],
        "functional_requirements": ["house_lord_data"],
        "strength_rules": ["4th lord strength", "Jupiter support"],
        "house_field_mapping": {"4": "base", "10": "authority"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["visible in leadership challenges"],
        "interaction_rules": ["aligns with Raj patterns"],
        "report_usage": ["authority", "career"],
    },
    "Pushkala Yoga": {
        "yoga_name": "Pushkala Yoga",
        "base_condition": "Strong Lagna and Moon support with benefic reinforcement",
        "involved_planets": ["Moon", "Jupiter", "Venus"],
        "involved_houses": [1, 7],
        "domain": "fortune",
        "positive_signals": ["supportive dignity", "social esteem"],
        "risk_signals": ["complacency"],
        "functional_requirements": ["house_lord_data", "benefic_support"],
        "strength_rules": ["Lagna support", "Moon support", "benefic reinforcement"],
        "house_field_mapping": {"1": "identity", "7": "social reflection"},
        "nakshatra_modifier_enabled": True,
        "navamsa_modifier_enabled": True,
        "activation_rules": ["background fortune signal"],
        "interaction_rules": ["supports Raj and Mahabhagya patterns"],
        "report_usage": ["opportunity", "visibility"],
    },
}

OWN_SIGNS = {
    "Mars": {0, 7},
    "Mercury": {2, 5},
    "Jupiter": {8, 11},
    "Venus": {1, 6},
    "Saturn": {9, 10},
}
EXALTATION_SIGNS = {
    "Mars": 9,
    "Mercury": 5,
    "Jupiter": 3,
    "Venus": 11,
    "Saturn": 6,
}
BENEFICS = {"Jupiter", "Venus", "Mercury"}
EXCLUDED_NODES = {"Rahu", "Ketu"}
COMPLEX_YOGAS = {
    "Raj Yoga", "Laxmi Yoga", "Vipareeta Raj Yoga", "Dhan Yoga", "Neechabhanga Raj Yoga",
    "Mahabhagya Yoga", "Adhi Yoga", "Parvata Yoga", "Kahala Yoga", "Pushkala Yoga",
}


def detect_yogas(natal_data, navamsa_data=None, chart_relationships=None):
    natal_data = natal_data or {}
    navamsa_data = navamsa_data or {}
    planets = _planet_map(natal_data.get("planets") or [])
    chart_relationships = _ensure_chart_relationships(natal_data, navamsa_data, chart_relationships)
    detected = []
    not_evaluated = []
    confidence_notes = []

    detectors = (
        _detect_ruchaka,
        _detect_bhadra,
        _detect_hamsa,
        _detect_malavya,
        _detect_shasha,
        _detect_gaj_kesari,
        _detect_chandra_mangala,
        _detect_budha_aditya,
        _detect_kemadruma_family,
        _detect_vesi_family,
        _detect_amala,
        _detect_sakata,
        _detect_raj_yoga,
        _detect_laxmi_yoga,
        _detect_vipareeta_raj_yoga,
        _detect_dhan_yoga,
        _detect_neechabhanga_raj_yoga,
        _detect_adhi_yoga,
        _detect_parvata_yoga,
        _detect_kahala_yoga,
        _detect_pushkala_yoga,
    )
    for detector in detectors:
        detected.extend(detector(planets, natal_data, navamsa_data, confidence_notes, chart_relationships))

    detected_names = {item["yoga_name"] for item in detected}
    for yoga_name in COMPLEX_YOGAS:
        if yoga_name in detected_names:
            continue
        meta = deepcopy(YOGA_CATALOG[yoga_name])
        meta["confidence"] = "not_evaluated"
        if yoga_name == "Mahabhagya Yoga":
            meta["reason"] = "Mahabhagya Yoga requires gender plus day/night context and remains unevaluated without them."
        else:
            meta["reason"] = "Exact conditional support for this yoga is still incomplete in the current chart relationship payload."
        not_evaluated.append(meta)

    return {
        "source": "yoga_signal_engine",
        "detected_yogas": _dedupe_yoga_rows(detected),
        "not_evaluated_yogas": not_evaluated,
        "confidence_notes": confidence_notes,
    }


def score_yoga_strength(yoga, natal_data, navamsa_data=None, chart_relationships=None):
    score = 2
    planets = _planet_map((natal_data or {}).get("planets") or [])
    chart_relationships = _ensure_chart_relationships(natal_data, navamsa_data, chart_relationships)
    dignities = chart_relationships.get("planet_dignities") or {}
    functional_nature = chart_relationships.get("functional_nature") or {}
    afflictions = chart_relationships.get("afflictions") or {}
    navamsa_strength = chart_relationships.get("navamsa_strength") or {}
    aspects = chart_relationships.get("vedic_aspects") or []
    involved = list(dict.fromkeys((yoga.get("matched_planets") or []) + (yoga.get("involved_planets") or [])))
    for name in involved:
        planet = planets.get(name)
        if not planet:
            continue
        dignity = dignities.get(name)
        if dignity in {"own", "exalted"}:
            score += 2
        elif dignity == "neutral":
            score += 1
        elif dignity == "debilitated":
            score -= 2
        nature = functional_nature.get(name)
        if nature == "yogakaraka":
            score += 2
        elif nature == "benefic":
            score += 1
        elif nature == "malefic":
            score -= 1
        if int(chart_relationships.get("planet_houses", {}).get(name) or 0) in {1, 4, 5, 7, 9, 10}:
            score += 1
        if yoga.get("nakshatra_modifier_enabled") and planet.get("nakshatra") in {"Pushya", "Rohini", "Anuradha", "Shravana"}:
            score += 1
        navamsa_dignity = navamsa_strength.get(name)
        if yoga.get("navamsa_modifier_enabled") and navamsa_dignity in {"own", "exalted"}:
            score += 1
        elif yoga.get("navamsa_modifier_enabled") and navamsa_dignity == "debilitated":
            score -= 1
        if (afflictions.get(name) or {}).get("is_afflicted"):
            score -= 1
    involved_set = set(involved)
    if any(
        aspect.get("from_planet") in BENEFICS and aspect.get("to_planet") in involved_set
        for aspect in aspects
    ):
        score += 1
    if any(
        aspect.get("from_planet") in {"Saturn", "Mars", "Rahu", "Ketu"} and aspect.get("to_planet") in involved_set
        for aspect in aspects
    ):
        score -= 1
    return _score_to_strength(score)


def build_yoga_signal_bundle(natal_data, navamsa_data=None, report_type="birth_chart_karma", chart_relationships=None):
    chart_relationships = _ensure_chart_relationships(natal_data, navamsa_data, chart_relationships)
    detected_context = detect_yogas(natal_data, navamsa_data=navamsa_data, chart_relationships=chart_relationships)
    detected = detected_context["detected_yogas"]
    signal_items = [
        _yoga_to_signal_item(yoga, natal_data, navamsa_data=navamsa_data, chart_relationships=chart_relationships)
        for yoga in detected
    ]
    filtered = _filter_signals_for_report_type(signal_items, report_type)
    return {
        "source": "yoga_signal_engine",
        "detected_yogas": detected,
        "not_evaluated_yogas": detected_context["not_evaluated_yogas"],
        "dominant_signals": filtered,
        "risk_signals": [signal for signal in filtered if any(tag in {"risk", "discipline", "pressure"} for tag in signal.get("report_usage", []))],
        "opportunity_signals": [signal for signal in filtered if any(tag in {"career", "wealth", "visibility", "opportunity", "relationship"} for tag in signal.get("report_usage", []))],
        "report_type_signals": filtered,
        "confidence_notes": detected_context["confidence_notes"],
        "signals": filtered,
        "chart_relationships": chart_relationships,
    }


def _planet_map(planets):
    return {str(item.get("name") or "").strip(): item for item in planets if str(item.get("name") or "").strip()}


def _ensure_chart_relationships(natal_data, navamsa_data, chart_relationships):
    if chart_relationships:
        return chart_relationships
    if build_chart_relationships is None:
        return {}
    try:
        return build_chart_relationships(natal_data or {}, navamsa_data or {})
    except Exception:  # pragma: no cover - defensive guard
        return {}


def _is_own_or_exalted(planet_name, planet):
    sign_idx = planet.get("sign_idx")
    if sign_idx is None:
        return False
    return int(sign_idx) in OWN_SIGNS.get(planet_name, set()) or int(sign_idx) == EXALTATION_SIGNS.get(planet_name)


def _same_sign(planet_a, planet_b):
    if not planet_a or not planet_b:
        return False
    return planet_a.get("sign_idx") == planet_b.get("sign_idx")


def _house_from_reference(reference_sign, planet_sign):
    return ((int(planet_sign) - int(reference_sign)) % 12) + 1


def _detected_yoga(name, matched_planets, explanation):
    yoga = deepcopy(YOGA_CATALOG[name])
    yoga["matched_planets"] = matched_planets
    yoga["explanation"] = explanation
    yoga["confidence"] = "high"
    return yoga


def _detect_ruchaka(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    mars = planets.get("Mars")
    mars_house = int((chart_relationships.get("planet_houses") or {}).get("Mars") or (mars or {}).get("house") or 0)
    if mars and _is_own_or_exalted("Mars", mars) and mars_house in {1, 4, 7, 10}:
        return [_detected_yoga("Ruchaka Yoga", ["Mars"], "Mars is in own or exalted sign in a kendra house.")]
    return []


def _detect_bhadra(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    mercury = planets.get("Mercury")
    mercury_house = int((chart_relationships.get("planet_houses") or {}).get("Mercury") or (mercury or {}).get("house") or 0)
    if mercury and _is_own_or_exalted("Mercury", mercury) and mercury_house in {1, 4, 7, 10}:
        return [_detected_yoga("Bhadra Yoga", ["Mercury"], "Mercury is in own or exalted sign in a kendra house.")]
    return []


def _detect_hamsa(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    jupiter = planets.get("Jupiter")
    jupiter_house = int((chart_relationships.get("planet_houses") or {}).get("Jupiter") or (jupiter or {}).get("house") or 0)
    if jupiter and _is_own_or_exalted("Jupiter", jupiter) and jupiter_house in {1, 4, 7, 10}:
        return [_detected_yoga("Hamsa Yoga", ["Jupiter"], "Jupiter is in own or exalted sign in a kendra house.")]
    return []


def _detect_malavya(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    venus = planets.get("Venus")
    venus_house = int((chart_relationships.get("planet_houses") or {}).get("Venus") or (venus or {}).get("house") or 0)
    if venus and _is_own_or_exalted("Venus", venus) and venus_house in {1, 4, 7, 10}:
        return [_detected_yoga("Malavya Yoga", ["Venus"], "Venus is in own or exalted sign in a kendra house.")]
    return []


def _detect_shasha(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    saturn = planets.get("Saturn")
    saturn_house = int((chart_relationships.get("planet_houses") or {}).get("Saturn") or (saturn or {}).get("house") or 0)
    if saturn and _is_own_or_exalted("Saturn", saturn) and saturn_house in {1, 4, 7, 10}:
        return [_detected_yoga("Shasha Yoga", ["Saturn"], "Saturn is in own or exalted sign in a kendra house.")]
    return []


def _detect_gaj_kesari(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    relation = (chart_relationships.get("moon_relative_houses") or {}).get("Jupiter")
    if relation in {1, 4, 7, 10}:
        return [_detected_yoga("Gaj Kesari Yoga", ["Moon", "Jupiter"], "Jupiter falls in a kendra from Moon.")]
    return []


def _detect_chandra_mangala(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    moon = planets.get("Moon")
    mars = planets.get("Mars")
    if _same_sign(moon, mars):
        return [_detected_yoga("Chandra Mangala Yoga", ["Moon", "Mars"], "Moon and Mars share the same sign.")]
    return []


def _detect_budha_aditya(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    sun = planets.get("Sun")
    mercury = planets.get("Mercury")
    if _same_sign(sun, mercury):
        return [_detected_yoga("Budha-Aditya Yoga", ["Sun", "Mercury"], "Sun and Mercury share the same sign.")]
    return []


def _detect_kemadruma_family(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    moon = planets.get("Moon")
    if not moon:
        confidence_notes.append("Moon is missing; Moon-based yogas could not be evaluated.")
        return []
    second = []
    twelfth = []
    moon_relative_houses = chart_relationships.get("moon_relative_houses") or {}
    for name, planet in planets.items():
        if name in EXCLUDED_NODES or name == "Moon":
            continue
        relation = moon_relative_houses.get(name)
        if relation == 2 and name != "Sun":
            second.append(name)
        if relation == 12 and name != "Sun":
            twelfth.append(name)
    detected = []
    if second:
        detected.append(_detected_yoga("Sunapha Yoga", second, "Planets occupy the 2nd from Moon, excluding Sun."))
    if twelfth:
        detected.append(_detected_yoga("Anapha Yoga", twelfth, "Planets occupy the 12th from Moon, excluding Sun."))
    if second and twelfth:
        detected.append(_detected_yoga("Dhurdhara Yoga", sorted(set(second + twelfth)), "Planets flank Moon on both sides."))
    if not second and not twelfth:
        detected.append(_detected_yoga("Kemadruma Yoga", ["Moon"], "No qualifying planets occupy the 2nd or 12th from Moon."))
    return detected


def _detect_vesi_family(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    sun = planets.get("Sun")
    if not sun:
        confidence_notes.append("Sun is missing; Sun-based yogas could not be evaluated.")
        return []
    second = []
    twelfth = []
    sun_relative_houses = chart_relationships.get("sun_relative_houses") or {}
    for name, planet in planets.items():
        if name in EXCLUDED_NODES or name in {"Sun", "Moon"}:
            continue
        relation = sun_relative_houses.get(name)
        if relation == 2:
            second.append(name)
        if relation == 12:
            twelfth.append(name)
    detected = []
    if second:
        detected.append(_detected_yoga("Vesi Yoga", second, "Planets occupy the 2nd from Sun, excluding Moon."))
    if twelfth:
        detected.append(_detected_yoga("Vasi Yoga", twelfth, "Planets occupy the 12th from Sun, excluding Moon."))
    if second and twelfth:
        detected.append(_detected_yoga("Obhayachari Yoga", sorted(set(second + twelfth)), "Planets flank Sun on both sides."))
    return detected


def _detect_amala(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    moon_relative_houses = chart_relationships.get("moon_relative_houses") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    detected = []
    for name in BENEFICS:
        planet = planets.get(name)
        if not planet:
            continue
        if planet_houses.get(name) == 10:
            detected.append(_detected_yoga("Amala Yoga", [name], f"{name} is 10th from Lagna."))
            break
        if moon_relative_houses.get(name) == 10:
            detected.append(_detected_yoga("Amala Yoga", [name], f"{name} is 10th from Moon."))
            break
    return detected


def _detect_sakata(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    relation = (chart_relationships.get("moon_relative_houses") or {}).get("Jupiter")
    if relation in {6, 8, 12}:
        return [_detected_yoga("Sakata Yoga", ["Moon", "Jupiter"], "Jupiter falls in the 6th, 8th, or 12th from Moon.")]
    return []


def _detect_raj_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    aspects = chart_relationships.get("vedic_aspects") or []
    dignities = chart_relationships.get("planet_dignities") or {}
    kendra_lords = {house_lords.get(str(h)) for h in (4, 7, 10)} - {None}
    trikona_lords = {house_lords.get(str(h)) for h in (5, 9)} - {None}
    matches = []
    for kendra in sorted(kendra_lords):
        for trikona in sorted(trikona_lords):
            if kendra == trikona and dignities.get(kendra) in {"own", "exalted", "neutral"}:
                matches.append(kendra)
                continue
            if _planets_related(kendra, trikona, planets, aspects):
                matches.extend([kendra, trikona])
    matches = sorted(set(matches))
    if matches:
        return [_detected_yoga("Raj Yoga", matches, "Kendra and trikona lords are structurally linked through conjunction, aspect, or shared lordship.")]
    return []


def _detect_laxmi_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    aspects = chart_relationships.get("vedic_aspects") or []
    dignities = chart_relationships.get("planet_dignities") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    ninth_lord = house_lords.get("9")
    if not ninth_lord or ninth_lord not in planets:
        confidence_notes.append("Laxmi Yoga could not be evaluated because the 9th lord is unavailable.")
        return []
    if dignities.get(ninth_lord) not in {"own", "exalted"} and planet_houses.get(ninth_lord) not in {1, 5, 9, 10}:
        return []
    supporters = []
    for name in ("Venus", "Jupiter"):
        if name in planets and (
            dignities.get(name) in {"own", "exalted"}
            or _planets_related(ninth_lord, name, planets, aspects)
            or planet_houses.get(name) in {1, 5, 9, 10, 11}
        ):
            supporters.append(name)
    if supporters:
        return [_detected_yoga("Laxmi Yoga", [ninth_lord] + supporters, "A strong 9th lord is reinforced by Venus or Jupiter support.")]
    return []


def _detect_vipareeta_raj_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    placements = []
    for dusthana in (6, 8, 12):
        lord = house_lords.get(str(dusthana))
        if lord and planet_houses.get(lord) in {6, 8, 12}:
            placements.append(lord)
    placements = sorted(set(placements))
    if len(placements) >= 2:
        return [_detected_yoga("Vipareeta Raj Yoga", placements, "Multiple dusthana lords are placed in dusthanas, indicating reversal-through-adversity potential.")]
    return []


def _detect_dhan_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    aspects = chart_relationships.get("vedic_aspects") or []
    wealth_lords = {house_lords.get(str(h)) for h in (2, 5, 9, 11)} - {None}
    matches = []
    for left in sorted(wealth_lords):
        for right in sorted(wealth_lords):
            if left >= right:
                continue
            if _planets_related(left, right, planets, aspects):
                matches.extend([left, right])
    matches = sorted(set(matches))
    if len(matches) >= 2:
        return [_detected_yoga("Dhan Yoga", matches, "Wealth-supporting house lords are linked through conjunction or aspect.")]
    return []


def _detect_neechabhanga_raj_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    dignities = chart_relationships.get("planet_dignities") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    moon_relative_houses = chart_relationships.get("moon_relative_houses") or {}
    navamsa_strength = chart_relationships.get("navamsa_strength") or {}
    matches = []
    for name, dignity in dignities.items():
        if dignity != "debilitated":
            continue
        planet = planets.get(name) or {}
        sign_idx = planet.get("sign_idx")
        if sign_idx is None:
            continue
        sign_lord = SIGN_RULERS.get(int(sign_idx))
        if sign_lord and (planet_houses.get(sign_lord) in {1, 4, 7, 10} or moon_relative_houses.get(sign_lord) in {1, 4, 7, 10}):
            matches.extend([name, sign_lord])
            continue
        if navamsa_strength.get(name) in {"own", "exalted"}:
            matches.append(name)
    matches = sorted(set(matches))
    if matches:
        return [_detected_yoga("Neechabhanga Raj Yoga", matches, "A debilitated planet shows cancellation support through dispositor placement or strong navamsa reinforcement.")]
    return []


def _detect_adhi_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    moon_relative_houses = chart_relationships.get("moon_relative_houses") or {}
    afflictions = chart_relationships.get("afflictions") or {}
    benefics = []
    for name in ("Jupiter", "Venus", "Mercury"):
        if moon_relative_houses.get(name) in {6, 7, 8} and not (afflictions.get(name) or {}).get("is_afflicted"):
            benefics.append(name)
    if len(benefics) >= 2:
        return [_detected_yoga("Adhi Yoga", benefics, "At least two benefics hold the 6th, 7th, or 8th from Moon without strong affliction.")]
    return []


def _detect_parvata_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    dignities = chart_relationships.get("planet_dignities") or {}
    functional_nature = chart_relationships.get("functional_nature") or {}
    afflictions = chart_relationships.get("afflictions") or {}
    kendra_lords = [house_lords.get(str(h)) for h in (1, 4, 7, 10) if house_lords.get(str(h))]
    if not kendra_lords:
        return []
    if any(dignities.get(lord) == "debilitated" for lord in kendra_lords):
        return []
    strong_benefics = [
        name for name in ("Jupiter", "Venus", "Mercury")
        if planet_houses.get(name) in {1, 4, 7, 10} and not (afflictions.get(name) or {}).get("is_afflicted")
    ]
    if len(strong_benefics) >= 2 and any(functional_nature.get(lord) in {"benefic", "yogakaraka"} for lord in kendra_lords):
        return [_detected_yoga("Parvata Yoga", sorted(set(kendra_lords + strong_benefics)), "Kendra strength is reinforced by benefic support without obvious debility in the kendra lords.")]
    return []


def _detect_kahala_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    dignities = chart_relationships.get("planet_dignities") or {}
    aspects = chart_relationships.get("vedic_aspects") or []
    fourth_lord = house_lords.get("4")
    if not fourth_lord or fourth_lord not in planets:
        return []
    if dignities.get(fourth_lord) == "debilitated":
        return []
    if planet_houses.get(fourth_lord) not in {1, 4, 5, 7, 9, 10}:
        return []
    if "Jupiter" in planets and _planets_related(fourth_lord, "Jupiter", planets, aspects):
        return [_detected_yoga("Kahala Yoga", [fourth_lord, "Jupiter"], "A strengthened 4th lord is supported by Jupiter through conjunction or aspect.")]
    return []


def _detect_pushkala_yoga(planets, natal_data, navamsa_data, confidence_notes, chart_relationships):
    house_lords = chart_relationships.get("house_lords") or {}
    planet_houses = chart_relationships.get("planet_houses") or {}
    dignities = chart_relationships.get("planet_dignities") or {}
    aspects = chart_relationships.get("vedic_aspects") or []
    lagna_lord = house_lords.get("1")
    if not lagna_lord or lagna_lord not in planets or "Moon" not in planets:
        return []
    if dignities.get(lagna_lord) == "debilitated" or dignities.get("Moon") == "debilitated":
        return []
    if planet_houses.get(lagna_lord) not in {1, 4, 5, 7, 9, 10}:
        return []
    supporters = [
        name for name in ("Jupiter", "Venus")
        if name in planets and (
            _planets_related(lagna_lord, name, planets, aspects)
            or _planets_related("Moon", name, planets, aspects)
            or planet_houses.get(name) in {1, 5, 7, 9, 10}
        )
    ]
    if supporters:
        return [_detected_yoga("Pushkala Yoga", [lagna_lord, "Moon"] + supporters, "A supported Lagna lord and Moon receive benefic reinforcement.")]
    return []


def _score_to_strength(score):
    if score >= 7:
        return "very_high"
    if score >= 5:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def _yoga_to_signal_item(yoga, natal_data, navamsa_data=None, chart_relationships=None):
    strength = score_yoga_strength(yoga, natal_data, navamsa_data=navamsa_data, chart_relationships=chart_relationships)
    confidence = "high" if len(yoga.get("matched_planets") or []) >= 1 else "medium"
    explanation = (
        f"{yoga['yoga_name']} reflects a {yoga['domain']} potential pattern. "
        f"Detected because {yoga.get('explanation', yoga.get('base_condition', 'the base condition is present'))}"
    )
    return {
        "key": yoga["yoga_name"].lower().replace(" ", "_"),
        "label": yoga["yoga_name"],
        "domain": yoga["domain"],
        "strength": strength,
        "confidence": confidence,
        "source": f"yoga:{yoga['yoga_name'].lower().replace(' ', '_')}",
        "explanation": explanation,
        "report_usage": list(yoga.get("report_usage") or []),
    }


def _filter_signals_for_report_type(signals, report_type):
    normalized = str(report_type or "birth_chart_karma").strip().lower()
    allowed = {
        "birth_chart_karma": {"authority", "wealth", "guidance", "discipline", "emotional", "spiritual", "life_direction", "relationship", "risk", "visibility"},
        "annual_transit": {"risk", "visibility", "emotional", "support_strategy"},
        "career": {"authority", "wealth", "communication", "discipline", "visibility", "career"},
        "parent_child": {"emotional", "support_strategy", "relationship", "discipline"},
    }.get(normalized, {"authority", "wealth", "guidance", "discipline", "emotional", "spiritual", "life_direction", "relationship", "risk", "visibility"})
    filtered = []
    for signal in signals:
        if signal.get("domain") in allowed or any(tag in allowed for tag in signal.get("report_usage") or []):
            filtered.append(signal)
    return filtered


def _dedupe_yoga_rows(rows):
    seen = {}
    for row in rows:
        seen[row["yoga_name"]] = row
    return list(seen.values())


def _planets_related(left, right, planets, aspects):
    if left == right:
        return True
    if not planets.get(left) or not planets.get(right):
        return False
    if _same_sign(planets[left], planets[right]):
        return True
    for aspect in aspects or []:
        if {aspect.get("from_planet"), aspect.get("to_planet")} == {left, right}:
            return True
    return False
