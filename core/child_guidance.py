from __future__ import annotations


SIGN_ELEMENT_MAP = {
    0: "fire",
    1: "earth",
    2: "air",
    3: "water",
    4: "fire",
    5: "earth",
    6: "air",
    7: "water",
    8: "fire",
    9: "earth",
    10: "air",
    11: "water",
}


def _planet(child_bundle, name):
    for item in child_bundle.get("natal_data", {}).get("planets", []):
        if item.get("name") == name:
            return item
    return {}


def _child_anchor_titles(child_bundle):
    return [
        anchor.get("title")
        for anchor in ((child_bundle.get("interpretation_context") or {}).get("signal_layer", {}) or {}).get("top_anchors", [])[:3]
        if anchor.get("title")
    ]


def _elemental_temperament(sign_idx):
    element = SIGN_ELEMENT_MAP.get(sign_idx)
    if element == "fire":
        return "Expressive, fast-moving, and naturally motivated by visible momentum."
    if element == "earth":
        return "Steady, tactile, and more secure when life feels structured and predictable."
    if element == "air":
        return "Curious, verbal, and stimulated by questions, patterns, and exchange."
    if element == "water":
        return "Sensitive, imaginative, and emotionally responsive to the tone of the environment."
    return "Layered, responsive, and shaped strongly by emotional tone and rhythm."


def build_child_core_nature(child_bundle):
    moon = _planet(child_bundle, "Moon")
    jupiter = _planet(child_bundle, "Jupiter")
    saturn = _planet(child_bundle, "Saturn")
    anchor_titles = _child_anchor_titles(child_bundle)
    natural_strengths = anchor_titles[:2] or [
        "Shows clear inner patterning when given consistency.",
        "Responds well to supportive encouragement rather than force.",
    ]
    sensitivity_areas = []
    if moon.get("house") in {4, 8, 12}:
        sensitivity_areas.append("Needs more recovery time after emotional intensity or overstimulation.")
    if saturn.get("house") in {3, 4, 5, 10}:
        sensitivity_areas.append("Can internalize pressure quickly when expectations feel heavy or rigid.")
    if not sensitivity_areas:
        sensitivity_areas.append("May become guarded when emotional tone changes too quickly around them.")
    emotional_needs = (
        "Needs calm emotional mirroring, predictability, and room to process feelings before being pushed into action."
        if SIGN_ELEMENT_MAP.get(moon.get("sign_idx")) in {"water", "earth"}
        else "Needs responsive conversation, movement, and encouragement that keeps emotional expression active."
    )
    return {
        "temperament": _elemental_temperament(moon.get("sign_idx")),
        "emotional_needs": emotional_needs,
        "natural_strengths": natural_strengths,
        "sensitivity_areas": sensitivity_areas,
        "support_style": (
            "Grows best when guidance feels spacious and confidence-building."
            if jupiter.get("house") in {1, 5, 9, 10}
            else "Grows best when support is steady, simple, and emotionally consistent."
        ),
    }


def build_learning_school_pattern(child_bundle):
    mercury = _planet(child_bundle, "Mercury")
    mars = _planet(child_bundle, "Mars")
    saturn = _planet(child_bundle, "Saturn")
    learning_style = (
        "Learns quickly through conversation, questions, and repeating ideas out loud."
        if SIGN_ELEMENT_MAP.get(mercury.get("sign_idx")) in {"air", "fire"}
        else "Learns best through repetition, examples, and steady hands-on reinforcement."
    )
    attention_tendency = (
        "Attention rises when the task feels active and mentally alive."
        if mars.get("house") in {3, 5, 6}
        else "Attention improves when pace is slower and the environment is clearly organized."
    )
    expression_pattern = (
        "Confidence grows when the child is invited to speak before being corrected."
        if mercury.get("house") in {1, 2, 3, 5}
        else "Confidence grows when the child is given time to prepare before public expression."
    )
    school_challenges = []
    if saturn.get("house") in {3, 5, 10}:
        school_challenges.append("May become self-critical when school pressure feels constant.")
    if mars.get("house") in {3, 6}:
        school_challenges.append("Can push too hard, then lose patience when the pace feels repetitive.")
    if not school_challenges:
        school_challenges.append("May need adults to translate pressure into calm structure rather than urgency.")
    return {
        "learning_style": learning_style,
        "attention_tendency": attention_tendency,
        "expression_pattern": expression_pattern,
        "school_challenges": school_challenges,
    }


def build_child_growth_path(child_bundle):
    top_recommendations = ((child_bundle.get("interpretation_context") or {}).get("recommendation_layer") or {}).get("top_recommendations", [])
    return {
        "thrive_support": (
            "The child thrives when encouragement is consistent, emotionally safe, and tied to realistic pacing."
        ),
        "relationship_strengthening": (
            "The relationship strengthens when correction becomes guidance and timing is treated with patience."
        ),
        "next_growth_theme": top_recommendations[0].get("title") if top_recommendations else "Confidence grows through calm repetition and trust-building support.",
    }


def build_child_timing_notes(child_bundle):
    dasha_data = child_bundle.get("dasha_data") or []
    if not dasha_data:
        return []
    first = dasha_data[0]
    planet = first.get("planet") or "current dasha"
    return [
        {
            "title": f"{planet} period emphasis",
            "time_window": f"{first.get('start', '-') } - {first.get('end', '-')}",
            "note": f"The child's current timing is being colored by {planet}, so emotional support and pacing should be read in that context.",
        }
    ]
