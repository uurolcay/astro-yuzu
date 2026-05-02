import unittest

from services import report_structure_v3


def _signal_context():
    return {
        "nakshatra_signals": {
            "moon_nakshatra": {"label": "Pushya", "summary": "Emotional steadiness"},
            "lagna_nakshatra": {"label": "Rohini", "summary": "Visible grounding"},
        },
        "yoga_signals": {
            "detected_yogas": [
                {"yoga_name": "Gaj Kesari Yoga", "explanation": "Protective expansion pattern", "strength": "high"},
            ]
        },
        "atmakaraka_signals": {
            "atmakaraka_planet": "Saturn",
            "soul_lesson": "Build disciplined trust slowly.",
            "evolution_path": "Mature through steadiness.",
            "signals": [{"label": "Atmakaraka - Saturn", "explanation": "Build disciplined trust slowly."}],
        },
        "dominant_signals": [
            {"key": "career_signal", "label": "Career signal", "explanation": "Career structure is active.", "categories": ["career", "visibility"], "tone": "opportunity", "strength": 4.3},
            {"key": "discipline_signal", "label": "Discipline signal", "explanation": "Pressure and pacing matter.", "categories": ["discipline", "timing"], "tone": "risk", "strength": 4.0},
        ],
        "risk_signals": [
            {"key": "discipline_signal", "label": "Discipline signal", "explanation": "Pressure and pacing matter.", "categories": ["discipline", "timing"], "tone": "risk", "strength": 4.0},
        ],
        "opportunity_signals": [
            {"key": "career_signal", "label": "Career signal", "explanation": "Career structure is active.", "categories": ["career", "visibility"], "tone": "opportunity", "strength": 4.3},
        ],
        "dasha_signal_bundle": {
            "dasha_lord": "Saturn",
            "active_nakshatra_patterns": [{"nakshatra": "Pushya", "label": "Pushya pattern"}],
        },
        "transit_trigger_signals": {
            "delivery_events": [
                {"planet": "Jupiter", "effect": "activate", "duration": "next 3 months", "explanation": "A supported opening is being delivered."}
            ]
        },
        "prediction_fusion": {
            "available": True,
            "timing_summary": [{"label": "Dasha aktivasyonu", "value": "Saturn"}],
            "active_dasha_signals": [{"title": "Saturn", "dasha_driver": "Saturn", "domain": "career"}],
            "active_transit_signals": [{"title": "Jupiter", "transit_trigger": "Jupiter", "domain": "career"}],
            "fusion_signals": [
                {
                    "title": "Career signal",
                    "theme": "Career signal",
                    "domain": "career",
                    "dasha_driver": "Saturn",
                    "transit_trigger": "Jupiter",
                    "strength": "high",
                    "timing_type": "opportunity",
                    "interpretation_hint": "Career timing may activate.",
                    "safe_language_note": "Use soft timing language.",
                }
            ],
            "risk_windows": [],
            "opportunity_windows": [{"title": "Career signal", "domain": "career", "timing_type": "opportunity", "note": "Career timing may activate."}],
            "confidence_notes": ["Soft timing context available."],
            "report_type_focus": {"primary_domain": "career"},
        },
        "confidence_notes": ["Signals are consistent."],
    }


class ReportStructureV3Tests(unittest.TestCase):
    def test_structure_builds_with_all_sections(self):
        structure = report_structure_v3.build_report_structure_v3(_signal_context(), "birth_chart_karma", "en")
        for key in (
            "identity_layer",
            "core_drivers",
            "dominant_signals",
            "interaction_layer",
            "risk_opportunity_map",
            "timing_engine",
            "action_engine",
            "strategic_summary",
        ):
            self.assertIn(key, structure)

    def test_parent_child_includes_interaction_layer(self):
        context = _signal_context()
        context["child_profile_signals"] = _signal_context()
        context["parent_profile_signals"] = _signal_context()
        context["parent_child_interaction_signals"] = {
            "interaction_patterns": [{"trigger_pair": "pressure -> sensitivity", "trigger": "direct pressure", "loop": "pressure -> withdrawal", "outcome": "trust drops"}],
            "recommended_parent_actions": [{"do": "slow communication", "avoid": "sudden pressure", "reason": "child overload"}],
            "risk_loops": [{"loop": "pressure -> withdrawal"}],
            "support_patterns": [{"pattern": "guidance -> trust"}],
        }
        structure = report_structure_v3.build_report_structure_v3(context, "parent_child", "tr")
        self.assertTrue(structure["interaction_layer"])
        self.assertIn("child_nature", structure)
        self.assertIn("parent_influence", structure)

    def test_career_includes_risk_and_opportunity_focus(self):
        structure = report_structure_v3.build_report_structure_v3(_signal_context(), "career", "en")
        self.assertTrue(structure["risk_opportunity_map"]["risks"])
        self.assertTrue(structure["risk_opportunity_map"]["opportunities"])
        self.assertTrue(all(item.get("domain") == "career" for item in structure["action_engine"]))

    def test_annual_transit_emphasizes_timing(self):
        structure = report_structure_v3.build_report_structure_v3(_signal_context(), "annual_transit", "en")
        self.assertTrue(structure["timing_engine"]["active_period"])
        self.assertTrue(structure["timing_engine"]["transit_triggers"])
        self.assertTrue(structure["identity_layer"]["short_reference_only"])
        self.assertTrue(structure["timing_engine"]["fusion_available"])
        self.assertTrue(structure["timing_engine"]["fusion_signals"])

    def test_missing_signals_does_not_crash(self):
        structure = report_structure_v3.build_report_structure_v3({}, "career", "tr")
        self.assertEqual(structure, {})

    def test_fusion_unavailable_preserves_prior_timing_behavior(self):
        context = _signal_context()
        context["prediction_fusion"] = {
            "available": False,
            "timing_summary": [],
            "active_dasha_signals": [],
            "active_transit_signals": [],
            "fusion_signals": [],
            "risk_windows": [],
            "opportunity_windows": [],
            "confidence_notes": [],
            "report_type_focus": {},
        }
        structure = report_structure_v3.build_report_structure_v3(context, "career", "en")
        self.assertNotIn("fusion_available", structure["timing_engine"])
        self.assertEqual(structure["timing_engine"]["active_period"], "Saturn")


if __name__ == "__main__":
    unittest.main()
