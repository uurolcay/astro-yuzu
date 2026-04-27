import unittest

from services import astro_signal_enrichment as enrichment
from services import nakshatra_signal_engine as nakshatra_engine


SAMPLE_NATAL_DATA = {
    "planets": [
        {"name": "Moon", "sign_idx": 3, "degree": 3.2, "nakshatra": "Pushya", "abs_longitude": 93.2},
        {"name": "Mercury", "sign_idx": 5, "degree": 23.0, "nakshatra": "Hasta", "abs_longitude": 173.0, "is_retrograde": False},
        {"name": "Saturn", "sign_idx": 9, "degree": 11.9, "nakshatra": "Shravana", "abs_longitude": 281.9, "is_retrograde": False},
        {"name": "Jupiter", "sign_idx": 1, "degree": 14.3, "nakshatra": "Rohini", "abs_longitude": 44.3},
        {"name": "Venus", "sign_idx": 1, "degree": 17.6, "nakshatra": "Rohini", "abs_longitude": 47.6},
        {"name": "Mars", "sign_idx": 7, "degree": 26.5, "nakshatra": "Jyeshtha", "abs_longitude": 236.5},
        {"name": "Rahu", "sign_idx": 0, "degree": 1.0, "nakshatra": "Ashwini", "abs_longitude": 1.0},
        {"name": "Ketu", "sign_idx": 6, "degree": 1.0, "nakshatra": "Swati", "abs_longitude": 181.0},
        {"name": "Sun", "sign_idx": 4, "degree": 9.1, "nakshatra": "Magha", "abs_longitude": 129.1},
    ],
    "ascendant": {"nakshatra": "Rohini", "abs_longitude": 44.1, "degree": 14.1, "sign_idx": 1},
}


class NakshatraSignalEngineTests(unittest.TestCase):
    def test_moon_nakshatra_creates_emotional_signal(self):
        profile = nakshatra_engine.build_nakshatra_signal_profile(SAMPLE_NATAL_DATA)
        self.assertEqual(profile["moon_nakshatra"]["domain"], "emotional_pattern")
        self.assertIn("security need", profile["moon_nakshatra"]["explanation"])

    def test_lagna_nakshatra_uses_present_nakshatra(self):
        profile = nakshatra_engine.build_nakshatra_signal_profile(SAMPLE_NATAL_DATA)
        self.assertEqual(profile["lagna_nakshatra"]["label"], "Lagna - Rohini")
        self.assertEqual(profile["lagna_nakshatra"]["domain"], "identity_style")

    def test_lagna_nakshatra_can_be_derived_from_abs_longitude(self):
        natal_data = {
            **SAMPLE_NATAL_DATA,
            "ascendant": {"abs_longitude": 94.0, "degree": 4.0, "sign_idx": 3},
        }
        profile = nakshatra_engine.build_nakshatra_signal_profile(natal_data)
        self.assertEqual(profile["lagna_nakshatra"]["label"], "Lagna - Pushya")

    def test_mercury_nakshatra_creates_communication_signal(self):
        profile = nakshatra_engine.build_nakshatra_signal_profile(SAMPLE_NATAL_DATA, report_type="career")
        mercury_signal = next(item for item in profile["planetary_nakshatra_signals"] if item["planet"] == "Mercury")
        self.assertEqual(mercury_signal["domain"], "communication_pattern")
        self.assertIn("learning style", mercury_signal["explanation"])

    def test_saturn_nakshatra_creates_discipline_signal(self):
        profile = nakshatra_engine.build_nakshatra_signal_profile(SAMPLE_NATAL_DATA)
        saturn_signal = next(item for item in profile["planetary_nakshatra_signals"] if item["planet"] == "Saturn")
        self.assertEqual(saturn_signal["domain"], "discipline_pattern")
        self.assertEqual(saturn_signal["confidence"], "high")

    def test_report_type_filtering_works_for_career(self):
        profile = nakshatra_engine.build_nakshatra_signal_profile(SAMPLE_NATAL_DATA, report_type="career")
        planets = {item["planet"] for item in profile["report_type_signals"]}
        self.assertIn("Mercury", planets)
        self.assertIn("Sun", planets)
        self.assertNotIn("Rahu", planets)
        self.assertNotIn("Ketu", planets)

    def test_missing_nakshatra_does_not_crash(self):
        natal_data = {
            "planets": [{"name": "Moon", "abs_longitude": 93.2}],
            "ascendant": {"abs_longitude": 44.1},
        }
        profile = nakshatra_engine.build_nakshatra_signal_profile(natal_data)
        self.assertEqual(profile["moon_nakshatra"], {})
        self.assertTrue(profile["lagna_nakshatra"])
        self.assertTrue(profile["confidence_notes"])

    def test_astro_signal_enrichment_includes_nakshatra_signals(self):
        context = enrichment.build_astro_signal_context(SAMPLE_NATAL_DATA, report_type="birth_chart_karma")
        self.assertEqual(context["nakshatra_signals"]["source"], "nakshatra_signal_engine")
        self.assertTrue(context["nakshatra_signals"]["signals"])
        self.assertTrue(any(signal["source"] == "nakshatra" for signal in context["dominant_signals"]))


if __name__ == "__main__":
    unittest.main()
