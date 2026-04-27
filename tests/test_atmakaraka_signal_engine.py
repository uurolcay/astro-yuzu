import unittest

from services import atmakaraka_signal_engine as atmakaraka
from services import chart_relationships
from services import astro_signal_enrichment as enrichment


BASE_NATAL = {
    "planets": [
        {"name": "Sun", "degree": 12.0, "sign_idx": 0},
        {"name": "Moon", "degree": 18.5, "sign_idx": 3},
        {"name": "Mars", "degree": 27.4, "sign_idx": 6},
        {"name": "Mercury", "degree": 14.0, "sign_idx": 1},
        {"name": "Jupiter", "degree": 9.0, "sign_idx": 8},
        {"name": "Venus", "degree": 23.0, "sign_idx": 11},
        {"name": "Saturn", "degree": 5.0, "sign_idx": 9},
    ],
    "ascendant": {"sign_idx": 9},
}


class AtmakarakaSignalEngineTests(unittest.TestCase):
    def test_correct_planet_detection(self):
        detected = atmakaraka.detect_atmakaraka(BASE_NATAL)
        self.assertEqual(detected["planet"], "Mars")

    def test_rahu_correction_works(self):
        natal = {
            "planets": [
                {"name": "Mars", "degree": 27.0, "sign_idx": 6},
                {"name": "Rahu", "degree": 1.0, "sign_idx": 2},
            ],
            "ascendant": {"sign_idx": 0},
        }
        detected = atmakaraka.detect_atmakaraka(natal)
        self.assertEqual(detected["planet"], "Rahu")
        self.assertTrue(detected["rahu_adjusted"])

    def test_house_mapping_works(self):
        relationships = chart_relationships.build_chart_relationships(BASE_NATAL)
        signal = atmakaraka.build_atmakaraka_signal(BASE_NATAL, chart_relationships=relationships)
        self.assertEqual(signal["atmakaraka_planet"], "Mars")
        self.assertEqual(signal["house_domain"], "career, duty, visibility, and public consequence")

    def test_affliction_increases_intensity(self):
        natal = {
            "planets": [
                {"name": "Mars", "degree": 29.0, "sign_idx": 6},
                {"name": "Rahu", "degree": 10.0, "sign_idx": 6},
                {"name": "Saturn", "degree": 12.0, "sign_idx": 9},
            ],
            "ascendant": {"sign_idx": 9},
        }
        relationships = chart_relationships.build_chart_relationships(natal)
        signal = atmakaraka.build_atmakaraka_signal(natal, chart_relationships=relationships)
        self.assertEqual(signal["atmakaraka_planet"], "Mars")
        self.assertEqual(signal["affliction_level"], "high")
        self.assertEqual(signal["karmic_intensity"], "very_high")

    def test_enrichment_includes_atmakaraka_signals(self):
        context = enrichment.build_astro_signal_context(BASE_NATAL, report_type="birth_chart_karma")
        self.assertIn("atmakaraka_signals", context)
        self.assertEqual(context["atmakaraka_signals"]["atmakaraka_planet"], "Mars")
        self.assertTrue(any(signal["source"] == "atmakaraka" for signal in context["dominant_signals"]))


if __name__ == "__main__":
    unittest.main()
