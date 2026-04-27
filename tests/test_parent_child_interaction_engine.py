import unittest

from services import parent_child_interaction_engine as interaction_engine


def _context(*, dominant=None, risk=None, nakshatra=None, atmakaraka=None):
    return {
        "dominant_signals": dominant or [],
        "risk_signals": risk or [],
        "nakshatra_signals": {"signals": nakshatra or []},
        "atmakaraka_signals": atmakaraka or {},
    }


class ParentChildInteractionEngineTests(unittest.TestCase):
    def test_pressure_vs_sensitivity_creates_withdrawal_loop(self):
        parent = _context(dominant=[{"key": "mars_drive", "label": "Mars drive", "planet": "Mars", "categories": ["discipline"], "strength": 4.2, "tone": "risk"}])
        child = _context(dominant=[{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional"], "strength": 4.0, "tone": "opportunity"}])
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child)
        self.assertTrue(bundle["risk_loops"])
        self.assertIn("pressure -> withdrawal -> more pressure", bundle["risk_loops"][0]["loop"])

    def test_control_vs_rebellion_creates_conflict_loop(self):
        parent = _context(dominant=[{"key": "saturn_control", "label": "Saturn control", "planet": "Saturn", "categories": ["discipline", "authority"], "strength": 4.4, "tone": "risk"}])
        child = _context(dominant=[{"key": "mars_rebel", "label": "Mars rebellion", "planet": "Mars", "categories": ["discipline_response", "parent_child_friction"], "strength": 4.1, "tone": "risk"}])
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child)
        self.assertTrue(any("conflict" in item["outcome"] for item in bundle["risk_loops"]))

    def test_emotional_alignment_creates_support_pattern(self):
        parent = _context(dominant=[{"key": "venus_harmony", "label": "Venus harmony", "planet": "Venus", "categories": ["relationship", "support_strategy"], "strength": 3.8, "tone": "opportunity"}])
        child = _context(dominant=[{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional_needs"], "strength": 3.9, "tone": "opportunity"}])
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child)
        self.assertTrue(bundle["support_patterns"])
        self.assertIn("trust", bundle["support_patterns"][0]["outcome"])

    def test_karmic_conflict_is_detected(self):
        parent = _context(
            dominant=[{"key": "saturn_control", "label": "Saturn control", "planet": "Saturn", "categories": ["discipline"], "strength": 4.1, "tone": "risk"}],
            atmakaraka={
                "atmakaraka_planet": "Saturn",
                "affliction_level": "high",
                "karmic_intensity": "very_high",
                "signals": [{"key": "atmakaraka:saturn", "label": "Atmakaraka - Saturn", "source": "atmakaraka", "planet": "Saturn", "categories": ["discipline", "karmic"], "tone": "risk"}],
            },
        )
        child = _context(dominant=[{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional"], "strength": 4.0, "tone": "opportunity"}])
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child)
        self.assertTrue(bundle["karmic_dynamics"])
        self.assertEqual(bundle["karmic_dynamics"][0]["intensity"], "high")

    def test_recommended_actions_are_generated(self):
        parent = _context(dominant=[{"key": "mars_drive", "label": "Mars drive", "planet": "Mars", "categories": ["discipline"], "strength": 4.2, "tone": "risk"}])
        child = _context(dominant=[{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional"], "strength": 4.0, "tone": "opportunity"}])
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child)
        self.assertTrue(bundle["recommended_parent_actions"])
        self.assertIn("slow communication", bundle["recommended_parent_actions"][0]["do"])

    def test_missing_signals_do_not_crash(self):
        bundle = interaction_engine.build_parent_child_interaction_bundle({}, {})
        self.assertEqual(bundle["interaction_patterns"], [])
        self.assertTrue(bundle["confidence_notes"])

    def test_no_hallucinated_interactions(self):
        parent = _context(dominant=[{"key": "random", "label": "Random", "categories": ["visibility"], "strength": 2.0, "tone": "opportunity"}])
        child = _context(dominant=[{"key": "other", "label": "Other", "categories": ["wealth"], "strength": 2.0, "tone": "opportunity"}])
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child)
        self.assertEqual(bundle["interaction_patterns"], [])
        self.assertTrue(any("no stable parent-child interaction pattern" in note.lower() for note in bundle["confidence_notes"]))

    def test_turkish_localization_removes_parent_child_english_leaks(self):
        parent = _context(dominant=[{"key": "mars_drive", "label": "Mars drive", "planet": "Mars", "categories": ["discipline"], "strength": 4.2, "tone": "risk"}])
        child = _context(
            dominant=[{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional"], "strength": 4.0, "tone": "opportunity"}],
            atmakaraka={
                "atmakaraka_planet": "Moon",
                "affliction_level": "medium",
                "karmic_intensity": "high",
                "signals": [{"key": "atmakaraka:moon", "label": "Atmakaraka - Moon", "source": "atmakaraka", "planet": "Moon", "categories": ["emotional", "karmic"], "tone": "risk"}],
            },
        )
        bundle = interaction_engine.build_parent_child_interaction_bundle(parent, child, language="tr")
        serialized = str(bundle)
        self.assertNotIn("direct pressure", serialized)
        self.assertNotIn("slow communication", serialized)
        self.assertNotIn("No stable parent-child interaction pattern", serialized)
        self.assertIn("doğrudan baskı", serialized)
        self.assertIn("iletişimi yavaşlatın", serialized)


if __name__ == "__main__":
    unittest.main()
