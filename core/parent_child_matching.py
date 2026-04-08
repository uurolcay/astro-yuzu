from __future__ import annotations


def _planet(bundle, name):
    for item in bundle.get("natal_data", {}).get("planets", []):
        if item.get("name") == name:
            return item
    return {}


def build_parent_child_dynamics(parent_bundle, child_bundle):
    parent_moon = _planet(parent_bundle, "Moon")
    child_moon = _planet(child_bundle, "Moon")
    parent_mercury = _planet(parent_bundle, "Mercury")
    child_mercury = _planet(child_bundle, "Mercury")
    parent_mars = _planet(parent_bundle, "Mars")
    child_mars = _planet(child_bundle, "Mars")
    parent_jupiter = _planet(parent_bundle, "Jupiter")
    child_saturn = _planet(child_bundle, "Saturn")

    emotional_compatibility = (
        "Emotional flow is easier here because both charts process feelings with a similar tempo."
        if parent_moon.get("sign_idx") == child_moon.get("sign_idx")
        else "Emotional tone may differ: one chart processes quickly while the other needs more time and softness."
    )
    communication_style = (
        "Communication can feel naturally smooth when ideas are spoken through in real time."
        if parent_mercury.get("sign_idx") == child_mercury.get("sign_idx")
        else "Misunderstandings are more likely when the parent explains quickly but the child needs slower repetition or reassurance."
    )
    friction = []
    if parent_mars.get("house") in {1, 3, 6, 10} or child_mars.get("house") in {1, 3, 6, 10}:
        friction.append("Conflict can rise when urgency replaces regulation.")
    if child_saturn.get("house") in {4, 5, 10}:
        friction.append("The child may carry correction as pressure more strongly than adults expect.")
    if not friction:
        friction.append("Friction is more likely from pacing differences than from lack of care.")

    support_flow = []
    if parent_jupiter.get("house") in {1, 5, 9, 10}:
        support_flow.append("The parent chart naturally brings encouragement, perspective, and long-range support.")
    support_flow.append("The relationship flows best when emotional safety comes before instruction.")

    return {
        "easy_flow": support_flow,
        "friction_points": friction,
        "communication_style_difference": communication_style,
        "emotional_compatibility": emotional_compatibility,
    }


def build_parenting_guidance(parent_bundle, child_bundle):
    child_mercury = _planet(child_bundle, "Mercury")
    child_moon = _planet(child_bundle, "Moon")
    child_saturn = _planet(child_bundle, "Saturn")
    communication_support = (
        "Use short, calm, repeatable language and check for emotional readiness before teaching."
        if child_mercury.get("house") in {4, 8, 12}
        else "Invite questions and dialogue so the child can process by interacting, not only by listening."
    )
    avoid = []
    if child_saturn.get("house") in {3, 5, 10}:
        avoid.append("Avoid turning discipline into chronic pressure or constant evaluation.")
    if child_moon.get("house") in {8, 12}:
        avoid.append("Avoid demanding emotional expression on a timetable when the child is still internalizing.")
    if not avoid:
        avoid.append("Avoid over-correcting in the moment; timing matters as much as content.")
    return {
        "best_support": "The parent can help most by combining consistency, emotional safety, and a clear rhythm.",
        "communication_guidance": communication_support,
        "avoid_patterns": avoid,
    }


def build_parent_child_anchors(child_profile, dynamics, parenting_guidance):
    anchors = [
        {
            "rank": 1,
            "title": "Child core emotional pattern",
            "summary": child_profile["temperament"],
            "why_it_matters": child_profile["emotional_needs"],
            "opportunity": child_profile["support_style"],
            "risk": child_profile["sensitivity_areas"][0] if child_profile.get("sensitivity_areas") else None,
        },
        {
            "rank": 2,
            "title": "Parent-child relationship dynamic",
            "summary": dynamics["emotional_compatibility"],
            "why_it_matters": dynamics["communication_style_difference"],
            "opportunity": dynamics["easy_flow"][0] if dynamics.get("easy_flow") else None,
            "risk": dynamics["friction_points"][0] if dynamics.get("friction_points") else None,
        },
        {
            "rank": 3,
            "title": "Parenting guidance that lands best",
            "summary": parenting_guidance["best_support"],
            "why_it_matters": parenting_guidance["communication_guidance"],
            "opportunity": "The relationship strengthens when support stays calm, specific, and repeatable.",
            "risk": parenting_guidance["avoid_patterns"][0] if parenting_guidance.get("avoid_patterns") else None,
        },
    ]
    return anchors[:3]


def build_parent_child_recommendations(child_profile, school_guidance, parenting_guidance, timing_notes):
    recommendations = [
        {
            "title": "Lead with calm, specific communication",
            "type": "focus",
            "priority": "high",
            "time_window": "Current phase",
            "reasoning": parenting_guidance["communication_guidance"],
            "linked_anchors": [{"title": "Parent-child relationship dynamic"}],
        },
        {
            "title": "Support learning through the child's natural pace",
            "type": "guidance",
            "priority": "medium",
            "time_window": "School and routine cycles",
            "reasoning": school_guidance["learning_style"],
            "linked_anchors": [{"title": "Child core emotional pattern"}],
        },
        {
            "title": "Reduce pressure before correcting behavior",
            "type": "avoidance",
            "priority": "high",
            "time_window": "Moments of stress",
            "reasoning": parenting_guidance["avoid_patterns"][0],
            "linked_anchors": [{"title": "Parenting guidance that lands best"}],
        },
    ]
    return {
        "top_recommendations": recommendations[:3],
        "opportunity_windows": [
            {
                "title": "Growth-supportive rhythm",
                "time_window": timing_notes[0]["time_window"] if timing_notes else "Current season",
            }
        ],
        "risk_windows": [
            {
                "title": "Pressure-sensitive periods",
                "time_window": timing_notes[0]["time_window"] if timing_notes else "When routines feel overloaded",
            }
        ],
    }
