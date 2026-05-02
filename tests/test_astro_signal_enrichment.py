import copy
import unittest

from services import astro_signal_enrichment as enrichment


SAMPLE_NATAL_DATA = {
    "planets": [
        {"name": "Sun", "house": 10, "nakshatra": "Magha", "abs_longitude": 129.1},
        {"name": "Moon", "house": 4, "nakshatra": "Pushya", "abs_longitude": 93.2},
        {"name": "Mercury", "house": 10, "nakshatra": "Hasta", "abs_longitude": 173.0},
        {"name": "Jupiter", "house": 1, "nakshatra": "Rohini", "abs_longitude": 44.3},
        {"name": "Saturn", "house": 10, "nakshatra": "Shravana", "abs_longitude": 281.9},
        {"name": "Mars", "house": 6, "nakshatra": "Jyeshtha", "abs_longitude": 236.5},
    ],
    "ascendant": {"sign_idx": 1, "degree": 3.2, "abs_longitude": 33.2},
}

SAMPLE_NAVAMSA_DATA = {
    "planets": [
        {"name": "Sun", "house": 9},
        {"name": "Mercury", "house": 10},
        {"name": "Jupiter", "house": 1},
        {"name": "Moon", "house": 4},
    ]
}

SAMPLE_DASHA_DATA = [
    {"planet": "Jupiter", "start": "2025-01-01", "end": "2027-12-31"},
    {"planet": "Saturn", "start": "2028-01-01", "end": "2030-12-31"},
]

SAMPLE_TRANSIT_CONTEXT = {
    "major_peak_windows": [{"title": "Career acceleration", "start": "2026-05-01", "end": "2026-06-20"}],
    "trigger_periods": [{"title": "Decision week", "start": "2026-05-12", "end": "2026-05-19"}],
    "opportunity_windows": [{"title": "Visibility opening"}],
    "pressure_windows": [{"title": "Authority pressure"}],
}

JUPITER_TRIAD_NATAL_DATA = {
    "planets": [
        {"name": "Moon", "house": 1, "nakshatra": "Vishakha", "abs_longitude": 201.2},
        {"name": "Jupiter", "house": 10, "nakshatra": "Punarvasu", "abs_longitude": 88.0, "sign_idx": 3},
        {"name": "Venus", "house": 5, "nakshatra": "Purva Bhadrapada", "abs_longitude": 334.0},
    ],
    "ascendant": {"sign_idx": 3, "degree": 4.2, "abs_longitude": 94.2},
}


class AstroSignalEnrichmentTests(unittest.TestCase):
    def test_builds_signal_context_from_existing_natal_data(self):
        context = enrichment.build_astro_signal_context(
            SAMPLE_NATAL_DATA,
            navamsa_data=SAMPLE_NAVAMSA_DATA,
            dasha_data=SAMPLE_DASHA_DATA,
            transit_context=SAMPLE_TRANSIT_CONTEXT,
            report_type="birth_chart_karma",
        )
        self.assertIn("nakshatra_signals", context)
        self.assertIn("yoga_signals", context)
        self.assertIn("atmakaraka_signals", context)
        self.assertIn("dasha_activation_signals", context)
        self.assertIn("transit_trigger_signals", context)
        self.assertIn("chart_relationships", context)
        self.assertIn("prediction_fusion", context)
        self.assertTrue(context["dominant_signals"])
        self.assertTrue(any(signal["source"] == "nakshatra" for signal in context["dominant_signals"]))

    def test_missing_navamsa_and_dasha_data_does_not_crash(self):
        context = enrichment.build_astro_signal_context(
            SAMPLE_NATAL_DATA,
            navamsa_data=None,
            dasha_data=None,
            transit_context={},
            report_type="career",
        )
        self.assertIsInstance(context["dominant_signals"], list)
        self.assertEqual(context["dasha_activation_signals"]["signals"], [])
        self.assertTrue(context["confidence_notes"])

    def test_report_type_filtering_works(self):
        base_context = enrichment.build_astro_signal_context(
            SAMPLE_NATAL_DATA,
            navamsa_data=SAMPLE_NAVAMSA_DATA,
            dasha_data=SAMPLE_DASHA_DATA,
            transit_context=SAMPLE_TRANSIT_CONTEXT,
            report_type="birth_chart_karma",
        )
        filtered = enrichment.filter_signal_context_by_report_type(base_context, "career")
        self.assertEqual(filtered["filtered_report_type"], "career")
        self.assertTrue(filtered["dominant_signals"])
        for signal in filtered["dominant_signals"]:
            self.assertTrue(set(signal.get("categories") or []) & enrichment.REPORT_TYPE_FOCUS["career"])
        self.assertFalse(filtered["report_type_signals"].get("identity"))

    def test_merge_signal_strength_adds_cleanly(self):
        self.assertEqual(enrichment.merge_signal_strength(1.5, 2.0), 3.5)
        self.assertEqual(enrichment.merge_signal_strength(None, "2.3"), 2.3)

    def test_existing_timing_outputs_are_not_changed(self):
        transit_context = copy.deepcopy(SAMPLE_TRANSIT_CONTEXT)
        before = copy.deepcopy(transit_context)
        enrichment.build_astro_signal_context(
            SAMPLE_NATAL_DATA,
            navamsa_data=SAMPLE_NAVAMSA_DATA,
            dasha_data=SAMPLE_DASHA_DATA,
            transit_context=transit_context,
            report_type="annual_transit",
        )
        self.assertEqual(transit_context, before)

    def test_dasha_activates_correct_nakshatra_triad(self):
        context = enrichment.build_astro_signal_context(
            JUPITER_TRIAD_NATAL_DATA,
            navamsa_data=None,
            dasha_data=SAMPLE_DASHA_DATA,
            transit_context={},
            report_type="birth_chart_karma",
        )
        bundle = context["dasha_signal_bundle"]
        self.assertEqual(bundle["dasha_lord"], "Jupiter")
        self.assertEqual(bundle["active_triads"], ["Punarvasu", "Vishakha", "Purva Bhadrapada"])
        self.assertTrue(bundle["active_nakshatra_patterns"])
        active_names = {item["nakshatra"] for item in bundle["active_nakshatra_patterns"]}
        self.assertTrue(active_names <= {"Punarvasu", "Vishakha", "Purva Bhadrapada"})

    def test_dasha_activation_increases_matching_signal_strength(self):
        base_context = enrichment.build_astro_signal_context(
            JUPITER_TRIAD_NATAL_DATA,
            navamsa_data=None,
            dasha_data=[],
            transit_context={},
            report_type="birth_chart_karma",
        )
        dasha_context = enrichment.build_astro_signal_context(
            JUPITER_TRIAD_NATAL_DATA,
            navamsa_data=None,
            dasha_data=SAMPLE_DASHA_DATA,
            transit_context=SAMPLE_TRANSIT_CONTEXT,
            report_type="birth_chart_karma",
        )
        base_signal = next(signal for signal in base_context["dominant_signals"] if signal["key"] == "moon_vishakha")
        dasha_signal = next(signal for signal in dasha_context["dominant_signals"] if signal["key"] == "moon_vishakha")
        self.assertGreater(float(dasha_signal["strength"]), float(base_signal["strength"]))

    def test_dasha_does_not_hallucinate_nakshatra_activation_when_no_match_exists(self):
        no_match_dasha = [{"planet": "Venus", "start": "2025-01-01", "end": "2027-12-31"}]
        context = enrichment.build_astro_signal_context(
            SAMPLE_NATAL_DATA,
            navamsa_data=SAMPLE_NAVAMSA_DATA,
            dasha_data=no_match_dasha,
            transit_context={},
            report_type="birth_chart_karma",
        )
        bundle = context["dasha_signal_bundle"]
        self.assertEqual(bundle["active_triads"], ["Bharani", "Purva Phalguni", "Purva Ashadha"])
        self.assertEqual(bundle["active_nakshatra_patterns"], [])
        self.assertTrue(any("no matching nakshatra signals" in note.lower() for note in context["confidence_notes"]))

    def test_parent_child_interaction_signals_attach_when_dual_context_is_provided(self):
        parent_context = {
            "dominant_signals": [{"key": "saturn_control", "label": "Saturn control", "planet": "Saturn", "categories": ["discipline"], "strength": 4.0, "tone": "risk"}],
            "risk_signals": [],
            "nakshatra_signals": {"signals": []},
            "atmakaraka_signals": {"signals": []},
        }
        child_context = {
            "dominant_signals": [{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional"], "strength": 4.0, "tone": "opportunity"}],
            "risk_signals": [],
            "nakshatra_signals": {"signals": []},
            "atmakaraka_signals": {"signals": []},
        }
        context = enrichment.build_astro_signal_context(
            SAMPLE_NATAL_DATA,
            navamsa_data=SAMPLE_NAVAMSA_DATA,
            dasha_data=SAMPLE_DASHA_DATA,
            transit_context={"parent_profile_signals": parent_context, "child_profile_signals": child_context},
            report_type="parent_child",
        )
        self.assertIn("parent_child_interaction_signals", context)
        self.assertTrue(context["parent_child_interaction_signals"]["interaction_patterns"])

    def test_parent_child_public_wrapper_matches_private_helper(self):
        container = {
            "language": "en",
            "parent_profile_signals": {
                "dominant_signals": [{"key": "saturn_control", "label": "Saturn control", "planet": "Saturn", "categories": ["discipline"], "strength": 4.0, "tone": "risk"}],
                "risk_signals": [],
                "nakshatra_signals": {"signals": []},
                "atmakaraka_signals": {"signals": []},
            },
            "child_profile_signals": {
                "dominant_signals": [{"key": "moon_sensitive", "label": "Moon sensitivity", "planet": "Moon", "categories": ["emotional"], "strength": 4.0, "tone": "opportunity"}],
                "risk_signals": [],
                "nakshatra_signals": {"signals": []},
                "atmakaraka_signals": {"signals": []},
            },
        }
        public = enrichment.build_parent_child_interaction_signals(container, report_type="parent_child")
        private = enrichment._build_parent_child_interaction_signals(container, report_type="parent_child")
        self.assertEqual(public, private)

        empty_public = enrichment.build_parent_child_interaction_signals({}, report_type="parent_child")
        empty_private = enrichment._build_parent_child_interaction_signals({}, report_type="parent_child")
        self.assertEqual(empty_public, empty_private)


if __name__ == "__main__":
    unittest.main()
