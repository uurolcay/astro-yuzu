from __future__ import annotations

from copy import deepcopy

from engines.engines_natal import get_nakshatra_name


PLANET_SIGNAL_RULES = {
    "Moon": {
        "domain": "emotional_pattern",
        "focus": "emotional pattern, mental rhythm, and security need",
        "label": "Moon nakshatra emotional signal",
        "base_strength": "very_high",
        "base_confidence": "high",
    },
    "Lagna": {
        "domain": "identity_style",
        "focus": "identity style, body-life approach, and first response to life",
        "label": "Lagna nakshatra identity signal",
        "base_strength": "very_high",
        "base_confidence": "high",
    },
    "Sun": {
        "domain": "authority_pattern",
        "focus": "purpose expression, visibility, and authority tone",
        "label": "Sun nakshatra authority signal",
        "base_strength": "high",
        "base_confidence": "medium",
    },
    "Mercury": {
        "domain": "communication_pattern",
        "focus": "communication style and learning style",
        "label": "Mercury nakshatra communication signal",
        "base_strength": "high",
        "base_confidence": "high",
    },
    "Venus": {
        "domain": "relationship_pattern",
        "focus": "affection style and relationship comfort pattern",
        "label": "Venus nakshatra relationship signal",
        "base_strength": "high",
        "base_confidence": "medium",
    },
    "Mars": {
        "domain": "action_pattern",
        "focus": "action style and conflict style",
        "label": "Mars nakshatra action signal",
        "base_strength": "high",
        "base_confidence": "medium",
    },
    "Jupiter": {
        "domain": "guidance_pattern",
        "focus": "belief style and guidance-wisdom style",
        "label": "Jupiter nakshatra guidance signal",
        "base_strength": "high",
        "base_confidence": "medium",
    },
    "Saturn": {
        "domain": "discipline_pattern",
        "focus": "discipline style and pressure response",
        "label": "Saturn nakshatra discipline signal",
        "base_strength": "high",
        "base_confidence": "high",
    },
    "Rahu": {
        "domain": "desire_pattern",
        "focus": "desire pattern, obsession vector, and unconventional growth",
        "label": "Rahu nakshatra desire signal",
        "base_strength": "high",
        "base_confidence": "medium",
    },
    "Ketu": {
        "domain": "release_pattern",
        "focus": "detachment pattern, karmic release, and spiritualization",
        "label": "Ketu nakshatra release signal",
        "base_strength": "high",
        "base_confidence": "medium",
    },
}

REPORT_PLANET_FILTERS = {
    "birth_chart_karma": {"Moon", "Lagna", "Rahu", "Ketu", "Saturn", "Jupiter"},
    "annual_transit": {"Moon", "Saturn", "Jupiter", "Rahu", "Ketu"},
    "career": {"Lagna", "Sun", "Mercury", "Mars", "Jupiter", "Saturn", "Venus"},
    "parent_child": {"Moon", "Lagna", "Mercury", "Saturn", "Venus", "Jupiter"},
}


def _nakshatra_entry(
    nakshatra,
    core_action,
    dependency,
    output,
    psychological_pattern,
    behavioral_pattern,
    relationship_pattern,
    risk_pattern,
    conflict_trigger,
    guna_profile,
    tempo,
    interaction_style,
    evolution_path,
    report_usage,
):
    return {
        "nakshatra": nakshatra,
        "core_action": core_action,
        "dependency": dependency,
        "output": output,
        "psychological_pattern": psychological_pattern,
        "behavioral_pattern": behavioral_pattern,
        "relationship_pattern": relationship_pattern,
        "risk_pattern": risk_pattern,
        "conflict_trigger": conflict_trigger,
        "guna_profile": guna_profile,
        "tempo": tempo,
        "interaction_style": interaction_style,
        "evolution_path": evolution_path,
        "report_usage": report_usage,
    }


NAKSHATRA_MATRIX = {
    "Ashwini": _nakshatra_entry("Ashwini", "initiates quickly", "needs room for immediate movement", "opens cycles fast", "thinks through action", "acts before momentum fades", "prefers space and freshness", "impatience with drag", "being slowed or over-explained", "rajas-tamas", "fast", "direct and catalytic", "learn pacing without losing courage", ["identity", "career", "life_direction"]),
    "Bharani": _nakshatra_entry("Bharani", "contains intensity", "needs emotional and moral clarity", "carries weight through pressure", "internalizes strong feeling", "holds until a threshold breaks", "tests loyalty and capacity", "suppression then reaction", "control, shame, or moral pressure", "rajas", "slow-build", "intense and private", "transform burden into conscious stewardship", ["emotional", "discipline", "karmic"]),
    "Krittika": _nakshatra_entry("Krittika", "cuts and clarifies", "needs a clean standard", "separates what is useful", "thinks through discernment", "moves by selection and correction", "shows care through truth and refinement", "criticism can harden bonds", "mess, vagueness, or incompetence", "rajas", "sharp", "precise and corrective", "temper truth with warmth", ["communication", "authority", "career"]),
    "Rohini": _nakshatra_entry("Rohini", "cultivates growth", "needs consistency and nourishment", "stabilizes and attracts", "settles through beauty and reassurance", "builds patiently around what feels fertile", "seeks comfort, affection, and continuity", "attachment to comfort", "disruption of security or value", "rajas", "steady", "warm and inviting", "choose growth over attachment", ["relationship", "wealth", "emotional"]),
    "Mrigashirsha": _nakshatra_entry("Mrigashirsha", "searches and samples", "needs curiosity to stay alive", "finds paths through exploration", "rests in questions more than answers", "moves in loops of testing", "connects through interest and discovery", "restlessness and scattered hunger", "feeling trapped or over-defined", "tamas-rajas", "variable", "curious and light-footed", "turn searching into directed inquiry", ["learning_style", "communication", "life_direction"]),
    "Ardra": _nakshatra_entry("Ardra", "breaks through weather", "needs honest confrontation", "extracts truth from turbulence", "processes through intensity and catharsis", "moves after emotional storms", "bonds through raw honesty", "destructive speech or overwhelm", "denial, glossing over pain", "tamas", "storm-fast", "penetrating and uncompromising", "channel force into conscious repair", ["emotional", "risk", "karmic"]),
    "Punarvasu": _nakshatra_entry("Punarvasu", "returns and restores", "needs hope and room to reset", "rebuilds from disruption", "finds safety in renewed coherence", "tries again with a softer arc", "brings forgiveness and re-opening", "repeating cycles without closure", "being denied a second beginning", "sattva", "elastic", "reassuring and spacious", "pair optimism with structure", ["emotional", "support_strategy", "spiritual"]),
    "Pushya": _nakshatra_entry("Pushya", "nourishes and protects", "needs meaningful responsibility", "creates support structures", "feels safest while caring and being useful", "moves through dependable routines", "shows love through holding and guidance", "over-functioning or subtle control", "ingratitude or instability", "sattva", "measured", "protective and steady", "offer care without over-carrying", ["emotional_needs", "support_strategy", "parent_child"]),
    "Ashlesha": _nakshatra_entry("Ashlesha", "binds and penetrates", "needs psychological attunement", "reads hidden motives quickly", "tracks threat and subtext", "acts through coiling, waiting, and precise moves", "seeks deep fusion and psychic contact", "entanglement, suspicion, or manipulation", "betrayal, mixed signals, loss of trust", "tamas", "coiled", "magnetic and strategic", "replace enmeshment with conscious intimacy", ["emotional", "relationship", "risk"]),
    "Magha": _nakshatra_entry("Magha", "inherits and enthrones", "needs dignity and rightful place", "acts from lineage and status memory", "orients around legacy and recognition", "moves strongly when role is clear", "offers protection through rank and duty", "ego rigidity or entitlement", "disrespect, invisibility, or status threat", "tamas", "formal", "commanding and ceremonial", "lead from service rather than entitlement", ["authority", "karmic", "career"]),
    "Purva Phalguni": _nakshatra_entry("Purva Phalguni", "magnetizes and relaxes", "needs pleasure and relational ease", "draws support through charm and warmth", "opens when life feels enjoyable", "moves through attraction and invitation", "bonds through affection and enjoyment", "avoidance of effort or excess indulgence", "coldness, rejection, joylessness", "rajas", "languid", "playful and magnetic", "balance enjoyment with staying power", ["relationship", "visibility", "comfort"]),
    "Uttara Phalguni": _nakshatra_entry("Uttara Phalguni", "stabilizes agreements", "needs reliable reciprocity", "turns warmth into durable bonds", "trusts what can be named and sustained", "acts through follow-through and duty", "prefers committed mutual support", "over-duty or resentment in bonds", "uneven effort or broken agreements", "rajas-sattva", "steady", "supportive and contractual", "build warmth into sustainable commitments", ["relationship", "discipline", "career"]),
    "Hasta": _nakshatra_entry("Hasta", "shapes with the hands", "needs responsiveness and control of tools", "translates ideas into skilled execution", "thinks through dexterity and adjustment", "acts through repetition and technique", "connects through usefulness and tact", "over-control or nervous over-management", "chaos, inefficiency, or clumsy timing", "rajas", "nimble", "clever and adaptive", "use skill without micromanaging life", ["communication", "learning_style", "career"]),
    "Chitra": _nakshatra_entry("Chitra", "designs and highlights", "needs beauty with precision", "produces standout form", "builds identity through crafting excellence", "acts when something can be shaped visibly", "bonds through admiration and distinctiveness", "image fixation or perfectionism", "being overlooked or aesthetically compromised", "tamas-rajas", "intentional", "polished and vivid", "let substance hold the shine", ["visibility", "identity", "career"]),
    "Swati": _nakshatra_entry("Swati", "disperses and self-organizes", "needs freedom to calibrate", "finds strength through independent adjustment", "learns by trial, exchange, and movement", "moves with changing currents", "prefers light-touch connection", "diffusion, inconsistency, or avoidance of roots", "pressure, enmeshment, or forced commitment", "rajas", "wind-like", "independent and negotiable", "turn flexibility into mastery", ["communication", "life_direction", "career"]),
    "Vishakha": _nakshatra_entry("Vishakha", "targets and intensifies", "needs a compelling aim", "concentrates energy toward attainment", "organizes around desire and outcome", "acts through sustained pursuit", "forms bonds around shared ambition or devotion", "obsession, comparison, or moral splitting", "blocked ambition or divided loyalty", "rajas", "driven", "focused and intense", "choose worthy goals and simplify pursuit", ["career", "relationship", "desire"]),
    "Anuradha": _nakshatra_entry("Anuradha", "devotes and organizes friendship", "needs trust and rhythm", "creates loyal relational systems", "feels stable through sincere bonds", "acts through disciplined cooperation", "connects through devotion and reliability", "hurt through exclusion or hidden resentment", "disloyalty, coldness, or broken rhythm", "sattva", "steady", "loyal and relational", "keep devotion paired with boundaries", ["relationship", "support_strategy", "parent_child"]),
    "Jyeshtha": _nakshatra_entry("Jyeshtha", "protects from the top", "needs meaningful control and competence", "takes charge under pressure", "expects to anticipate complexity", "acts through strategic response and seniority", "relates through guarded responsibility", "control, stress dominance, or isolation", "humiliation, incompetence, or instability", "tamas", "compressed", "authoritative and vigilant", "turn control into conscious guardianship", ["authority", "discipline", "risk"]),
    "Mula": _nakshatra_entry("Mula", "uproots to find truth", "needs access to root causes", "strips away false structures", "moves toward core reality beneath form", "acts through radical investigation or reset", "relates intensely but unsentimentally", "destructive severing or nihilism", "surface explanations or forced optimism", "tamas", "rootward", "probing and uncompromising", "destroy only what no longer serves truth", ["karmic", "spiritual", "life_direction"]),
    "Purva Ashadha": _nakshatra_entry("Purva Ashadha", "surges and declares", "needs conviction and emotional momentum", "wins through belief and wave-force", "trusts inspiration and persuasion", "acts in strong forward arcs", "bonds through shared inspiration", "dogmatism or emotional overreach", "dismissal, cynicism, or premature defeat", "rajas", "surging", "inspiring and forceful", "let conviction mature into wisdom", ["communication", "visibility", "belief"]),
    "Uttara Ashadha": _nakshatra_entry("Uttara Ashadha", "secures the long win", "needs ethical endurance", "builds irreversible progress", "organizes around lasting legitimacy", "acts through sustained responsibility", "prefers serious, dependable bonds", "rigidity or joyless duty", "shortcuts, moral compromise, weak follow-through", "sattva", "enduring", "serious and principled", "keep ambition aligned with ethics", ["authority", "discipline", "career"]),
    "Shravana": _nakshatra_entry("Shravana", "listens and maps", "needs signal, instruction, and pattern", "translates information into ordered movement", "processes by hearing and arranging", "acts after absorbing context", "connects through listening and meaningful exchange", "over-listening, anxiety, or second-hand living", "noise, contradiction, or disrespectful tone", "rajas", "measured", "observant and responsive", "move from listening to embodied knowing", ["learning_style", "communication", "parent_child"]),
    "Dhanishta": _nakshatra_entry("Dhanishta", "synchronizes and performs", "needs rhythm and coordinated momentum", "creates impact through timing and group flow", "tracks tempo and social timing", "acts decisively when the beat is clear", "bonds through shared movement and contribution", "status comparison or emotional skipping", "being slowed, excluded, or mistimed", "rajas", "rhythmic", "social and momentum-based", "align ambition with inner rhythm", ["career", "visibility", "group_dynamics"]),
    "Shatabhisha": _nakshatra_entry("Shatabhisha", "isolates to diagnose", "needs distance for clear pattern recognition", "finds remedy through detachment and systems", "understands through abstraction and repair", "acts by withdrawing, seeing, then correcting", "relates selectively and conceptually", "emotional coldness or over-detachment", "intrusion, chaos, or unclear boundaries", "tamas", "detached", "private and analytical", "pair detachment with humane contact", ["spiritual", "emotional", "risk"]),
    "Purva Bhadrapada": _nakshatra_entry("Purva Bhadrapada", "intensifies toward transformation", "needs seriousness and threshold work", "brings austerity and potent conviction", "moves between idealism and extremity", "acts through intensity and one-pointed drive", "bonds through shared depth or ordeal", "extremism, volatility, or self-consuming pressure", "betrayal of ideals or moral hypocrisy", "rajas-tamas", "intense", "austere and confrontational", "temper intensity with embodied balance", ["spiritual", "karmic", "risk"]),
    "Uttara Bhadrapada": _nakshatra_entry("Uttara Bhadrapada", "stabilizes depth", "needs inner composure and long-range steadiness", "holds complexity without panic", "processes deeply and slowly", "acts with delayed but durable force", "offers grounded emotional containment", "passivity, heaviness, or withdrawn endurance", "chaos, pressure, or emotional volatility", "sattva-tamas", "deep-slow", "calm and containing", "bring inner depth into practical support", ["emotional", "support_strategy", "discipline"]),
    "Revati": _nakshatra_entry("Revati", "guides toward completion", "needs gentleness and meaningful direction", "protects transitions and endings", "feels through subtle atmospheres and closure cycles", "acts through care, timing, and escorting", "connects through softness and guidance", "drift, rescuing, or porous boundaries", "harshness, abandonment, or disorientation", "sattva", "soft", "gentle and guiding", "keep compassion paired with boundaries", ["spiritual", "relationship", "parent_child"]),
}


def build_nakshatra_signal_profile(natal_data, report_type="birth_chart_karma"):
    natal_data = natal_data or {}
    planet_rows = list(natal_data.get("planets") or [])
    allowed_planets = REPORT_PLANET_FILTERS.get(str(report_type or "birth_chart_karma").strip().lower(), REPORT_PLANET_FILTERS["birth_chart_karma"])

    moon_signal = _build_planet_signal(_planet_row(planet_rows, "Moon"), "Moon") if "Moon" in allowed_planets else None
    lagna_signal = _build_lagna_signal(natal_data) if "Lagna" in allowed_planets else None

    planetary_signals = []
    for planet_name in ("Sun", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "Rahu", "Ketu"):
        if planet_name not in allowed_planets:
            continue
        signal = _build_planet_signal(_planet_row(planet_rows, planet_name), planet_name)
        if signal:
            planetary_signals.append(signal)

    all_signals = [signal for signal in [moon_signal, lagna_signal, *planetary_signals] if signal]
    dominant_signals = sorted(all_signals, key=lambda item: _strength_rank(item.get("strength")), reverse=True)
    risk_signals = [signal for signal in dominant_signals if any(tag in {"risk", "pressure", "conflict"} for tag in signal.get("report_usage", []))]
    opportunity_signals = [signal for signal in dominant_signals if any(tag in {"opportunity", "growth", "support"} for tag in signal.get("report_usage", []))]
    report_type_signals = [signal for signal in dominant_signals if signal.get("planet") in allowed_planets]
    confidence_notes = _confidence_notes(natal_data, moon_signal, lagna_signal)

    return {
        "source": "nakshatra_signal_engine",
        "moon_nakshatra": moon_signal or {},
        "lagna_nakshatra": lagna_signal or {},
        "planetary_nakshatra_signals": planetary_signals,
        "dominant_signals": dominant_signals,
        "risk_signals": risk_signals,
        "opportunity_signals": opportunity_signals,
        "report_type_signals": report_type_signals,
        "confidence_notes": confidence_notes,
    }


def _planet_row(planets, name):
    for row in planets or []:
        if str(row.get("name") or "").strip() == name:
            return row
    return None


def _build_lagna_signal(natal_data):
    ascendant = deepcopy(natal_data.get("ascendant") or {})
    if not ascendant:
        return None
    asc_name = str(ascendant.get("nakshatra") or "").strip()
    if not asc_name and ascendant.get("abs_longitude") is not None:
        try:
            asc_name = get_nakshatra_name(float(ascendant.get("abs_longitude")))
        except (TypeError, ValueError):
            asc_name = ""
    if not asc_name:
        return None
    ascendant["name"] = "Lagna"
    ascendant["nakshatra"] = asc_name
    return _build_planet_signal(ascendant, "Lagna")


def _build_planet_signal(planet_row, planet_name):
    if not planet_row:
        return None
    nakshatra = str(planet_row.get("nakshatra") or "").strip()
    if not nakshatra:
        return None
    profile = NAKSHATRA_MATRIX.get(nakshatra)
    if not profile:
        return None
    rule = PLANET_SIGNAL_RULES.get(planet_name)
    if not rule:
        return None
    explanation = (
        f"{planet_name} in {nakshatra} channels {rule['focus']}. "
        f"Core action: {profile['core_action']}; dependency: {profile['dependency']}; "
        f"output: {profile['output']}; risk: {profile['risk_pattern']}; evolution path: {profile['evolution_path']}."
    )
    return {
        "key": f"{planet_name.lower()}_{nakshatra.lower().replace(' ', '_')}",
        "label": f"{planet_name} - {nakshatra}",
        "planet": planet_name,
        "domain": rule["domain"],
        "strength": _planet_strength(planet_name, planet_row, profile, rule),
        "confidence": _planet_confidence(planet_name, planet_row, rule),
        "source": f"nakshatra:{planet_name.lower()}:{nakshatra.lower().replace(' ', '_')}",
        "explanation": explanation,
        "report_usage": list(dict.fromkeys(profile["report_usage"] + _planet_usage_tags(planet_name))),
        "nakshatra_profile": deepcopy(profile),
    }


def _planet_strength(planet_name, planet_row, profile, rule):
    house = int(planet_row.get("house") or 0)
    if planet_name in {"Moon", "Lagna"}:
        return "very_high"
    if planet_name in {"Saturn", "Rahu", "Ketu"} and any(tag in {"risk", "discipline", "karmic"} for tag in profile["report_usage"]):
        return "high"
    if house in {1, 4, 7, 10}:
        return "high"
    return rule["base_strength"]


def _planet_confidence(planet_name, planet_row, rule):
    if planet_name in {"Moon", "Lagna", "Mercury", "Saturn"}:
        return "high"
    if planet_row.get("is_retrograde") and planet_name in {"Mercury", "Mars", "Saturn", "Jupiter"}:
        return "medium"
    return rule["base_confidence"]


def _planet_usage_tags(planet_name):
    return {
        "Moon": ["emotional", "security", "parent_child"],
        "Lagna": ["identity", "life_direction", "career"],
        "Sun": ["authority", "visibility", "career"],
        "Mercury": ["communication", "learning_style", "career"],
        "Venus": ["relationship", "comfort", "support"],
        "Mars": ["action", "conflict", "risk"],
        "Jupiter": ["guidance", "belief", "opportunity"],
        "Saturn": ["discipline", "pressure", "risk"],
        "Rahu": ["desire", "risk", "growth"],
        "Ketu": ["karmic", "release", "spiritual"],
    }.get(planet_name, [])


def _confidence_notes(natal_data, moon_signal, lagna_signal):
    notes = []
    if not natal_data or not natal_data.get("planets"):
        notes.append("Natal planet rows are missing; nakshatra signal coverage is limited.")
    if not moon_signal:
        notes.append("Moon nakshatra is unavailable; emotional and security signals are reduced.")
    if not lagna_signal:
        notes.append("Lagna nakshatra is unavailable or could not be derived; identity signal confidence is reduced.")
    return notes


def _strength_rank(value):
    return {"low": 1, "medium": 2, "high": 3, "very_high": 4}.get(str(value or "").lower(), 0)
