import unittest

from services import astro_signal_enrichment as enrichment
from services import chart_relationships


SAMPLE_NATAL_DATA = {
    "planets": [
        {"name": "Sun", "sign_idx": 0, "degree": 10.0, "nakshatra": "Ashwini"},
        {"name": "Moon", "sign_idx": 3, "degree": 12.0, "nakshatra": "Pushya"},
        {"name": "Mars", "sign_idx": 8, "degree": 5.0, "nakshatra": "Mula"},
        {"name": "Mercury", "sign_idx": 5, "degree": 7.0, "nakshatra": "Hasta"},
        {"name": "Jupiter", "sign_idx": 3, "degree": 14.0, "nakshatra": "Pushya"},
        {"name": "Venus", "sign_idx": 11, "degree": 21.0, "nakshatra": "Revati"},
        {"name": "Saturn", "sign_idx": 6, "degree": 2.0, "nakshatra": "Swati"},
        {"name": "Rahu", "sign_idx": 3, "degree": 18.0, "nakshatra": "Pushya"},
        {"name": "Ketu", "sign_idx": 7, "degree": 18.0, "nakshatra": "Vishakha"},
    ],
    "ascendant": {"sign_idx": 1, "degree": 4.0, "abs_longitude": 34.0},
}

SAMPLE_NAVAMSA = {
    "planets": [
        {"name": "Sun", "sign_idx": 4},
        {"name": "Jupiter", "sign_idx": 3},
        {"name": "Venus", "sign_idx": 11},
        {"name": "Saturn", "sign_idx": 0},
    ]
}


class ChartRelationshipsTests(unittest.TestCase):
    def test_planet_houses_calculated_correctly(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["planet_houses"]["Sun"], 12)
        self.assertEqual(context["planet_houses"]["Moon"], 3)
        self.assertEqual(context["planet_houses"]["Mars"], 8)

    def test_house_lords_calculated_correctly_for_sample_ascendant(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["house_lords"]["1"], "Venus")
        self.assertEqual(context["house_lords"]["7"], "Mars")
        self.assertEqual(context["lorded_houses_by_planet"]["Venus"], [1, 6])

    def test_planet_dignity_detects_exalted_own_debilitated(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["planet_dignities"]["Mars"], "neutral")
        self.assertEqual(context["planet_dignities"]["Mercury"], "exalted")
        self.assertEqual(context["planet_dignities"]["Sun"], "exalted")
        self.assertEqual(context["planet_dignities"]["Saturn"], "exalted")

    def test_functional_nature_returns_yogakaraka_where_applicable(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["functional_nature"]["Saturn"], "yogakaraka")
        self.assertEqual(context["functional_nature"]["Mercury"], "benefic")

    def test_vedic_aspects_include_jupiter_and_saturn_special_aspects(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        aspect_types = {item["aspect_type"] for item in context["vedic_aspects"]}
        self.assertIn("jupiter_5th", aspect_types)
        self.assertIn("jupiter_9th", aspect_types)
        self.assertIn("saturn_3rd", aspect_types)
        self.assertIn("saturn_10th", aspect_types)

    def test_moon_relative_houses_work(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["moon_relative_houses"]["Jupiter"], 1)
        self.assertEqual(context["moon_relative_houses"]["Mars"], 6)

    def test_sun_relative_houses_work(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["sun_relative_houses"]["Mercury"], 6)
        self.assertEqual(context["sun_relative_houses"]["Moon"], 4)

    def test_affliction_detects_nodes_or_malefic_influence_conservatively(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertTrue(context["afflictions"]["Moon"]["is_afflicted"])
        self.assertIn("conjunct_with_malefic", context["afflictions"]["Moon"]["reasons"])
        self.assertIn("with_rahu_or_ketu", context["afflictions"]["Moon"]["reasons"])

    def test_navamsa_strength_works_when_navamsa_provided(self):
        context = chart_relationships.build_chart_relationships(SAMPLE_NATAL_DATA, SAMPLE_NAVAMSA)
        self.assertEqual(context["navamsa_strength"]["Sun"], "own")
        self.assertEqual(context["navamsa_strength"]["Jupiter"], "exalted")
        self.assertEqual(context["navamsa_strength"]["Venus"], "exalted")

    def test_missing_optional_data_does_not_crash(self):
        context = chart_relationships.build_chart_relationships({"planets": [{"name": "Moon", "sign_idx": 3}]}, None)
        self.assertIsInstance(context["planet_houses"], dict)
        self.assertIsInstance(context["confidence_notes"], list)

    def test_astro_signal_enrichment_includes_chart_relationships(self):
        context = enrichment.build_astro_signal_context(SAMPLE_NATAL_DATA, navamsa_data=SAMPLE_NAVAMSA, report_type="career")
        self.assertIn("chart_relationships", context)
        self.assertIn("planet_houses", context["chart_relationships"])


if __name__ == "__main__":
    unittest.main()
