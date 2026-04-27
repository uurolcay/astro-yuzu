import unittest

from services import astro_signal_enrichment as enrichment
from services import yoga_signal_engine as yoga_engine


BASE_NATAL_DATA = {
    "planets": [
        {"name": "Moon", "sign_idx": 0, "house": 1, "nakshatra": "Ashwini"},
        {"name": "Jupiter", "sign_idx": 3, "house": 4, "nakshatra": "Pushya"},
        {"name": "Venus", "sign_idx": 11, "house": 10, "nakshatra": "Revati"},
        {"name": "Mercury", "sign_idx": 0, "house": 1, "nakshatra": "Ashwini"},
        {"name": "Sun", "sign_idx": 0, "house": 1, "nakshatra": "Ashwini"},
        {"name": "Mars", "sign_idx": 0, "house": 1, "nakshatra": "Ashwini"},
        {"name": "Saturn", "sign_idx": 10, "house": 11, "nakshatra": "Shatabhisha"},
    ],
    "ascendant": {"sign_idx": 0},
}

BASE_NAVAMSA = {
    "planets": [
        {"name": "Jupiter", "house": 1},
        {"name": "Venus", "house": 9},
        {"name": "Mercury", "house": 10},
    ]
}


class YogaSignalEngineTests(unittest.TestCase):
    def test_detects_hamsa_yoga(self):
        detected = yoga_engine.detect_yogas(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Hamsa Yoga", names)

    def test_detects_malavya_yoga(self):
        natal_data = {
            "planets": [
                {"name": "Venus", "sign_idx": 11, "nakshatra": "Revati"},
                {"name": "Moon", "sign_idx": 3, "nakshatra": "Pushya"},
                {"name": "Jupiter", "sign_idx": 5, "nakshatra": "Hasta"},
            ],
            "ascendant": {"sign_idx": 2},
        }
        detected = yoga_engine.detect_yogas(natal_data, navamsa_data=BASE_NAVAMSA)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Malavya Yoga", names)

    def test_detects_gaj_kesari_yoga(self):
        detected = yoga_engine.detect_yogas(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Gaj Kesari Yoga", names)

    def test_detects_chandra_mangala_yoga(self):
        detected = yoga_engine.detect_yogas(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Chandra Mangala Yoga", names)

    def test_detects_budha_aditya_yoga(self):
        detected = yoga_engine.detect_yogas(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Budha-Aditya Yoga", names)

    def test_detects_kemadruma_or_flanking_family(self):
        natal_data = {
            "planets": [
                {"name": "Moon", "sign_idx": 5, "house": 1},
                {"name": "Jupiter", "sign_idx": 6, "house": 2},
                {"name": "Saturn", "sign_idx": 4, "house": 12},
                {"name": "Sun", "sign_idx": 2, "house": 10},
            ],
            "ascendant": {"sign_idx": 5},
        }
        detected = yoga_engine.detect_yogas(natal_data)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertTrue({"Sunapha Yoga", "Anapha Yoga", "Dhurdhara Yoga"} & names)

    def test_raj_yoga_detected_from_kendra_trikona_lord_relationship(self):
        natal_data = {
            "planets": [
                {"name": "Saturn", "sign_idx": 10, "nakshatra": "Shatabhisha"},
                {"name": "Moon", "sign_idx": 3, "nakshatra": "Pushya"},
                {"name": "Jupiter", "sign_idx": 5, "nakshatra": "Hasta"},
            ],
            "ascendant": {"sign_idx": 1},
        }
        detected = yoga_engine.detect_yogas(natal_data)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Raj Yoga", names)

    def test_dhan_yoga_detected_from_wealth_lord_relationship(self):
        natal_data = {
            "planets": [
                {"name": "Mercury", "sign_idx": 2, "nakshatra": "Mrigashira"},
                {"name": "Jupiter", "sign_idx": 2, "nakshatra": "Ardra"},
                {"name": "Moon", "sign_idx": 4, "nakshatra": "Magha"},
            ],
            "ascendant": {"sign_idx": 1},
        }
        detected = yoga_engine.detect_yogas(natal_data)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Dhan Yoga", names)

    def test_vipareeta_raj_yoga_detected_from_dusthana_lords_in_dusthanas(self):
        natal_data = {
            "planets": [
                {"name": "Venus", "sign_idx": 8, "nakshatra": "Mula"},
                {"name": "Mars", "sign_idx": 6, "nakshatra": "Swati"},
                {"name": "Moon", "sign_idx": 3, "nakshatra": "Pushya"},
            ],
            "ascendant": {"sign_idx": 1},
        }
        detected = yoga_engine.detect_yogas(natal_data)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Vipareeta Raj Yoga", names)

    def test_neechabhanga_detected_when_cancellation_data_exists(self):
        natal_data = {
            "planets": [
                {"name": "Sun", "sign_idx": 6, "nakshatra": "Swati"},
                {"name": "Venus", "sign_idx": 3, "nakshatra": "Pushya"},
                {"name": "Moon", "sign_idx": 9, "nakshatra": "Shravana"},
            ],
            "ascendant": {"sign_idx": 0},
        }
        detected = yoga_engine.detect_yogas(natal_data)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertIn("Neechabhanga Raj Yoga", names)

    def test_mahabhagya_remains_not_evaluated_without_gender_day_night(self):
        detected = yoga_engine.detect_yogas(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA)
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertNotIn("Mahabhagya Yoga", names)
        not_eval = {item["yoga_name"] for item in detected["not_evaluated_yogas"]}
        self.assertIn("Mahabhagya Yoga", not_eval)

    def test_no_false_positives_on_missing_data(self):
        detected = yoga_engine.detect_yogas({"planets": [{"name": "Moon"}], "ascendant": {}}, navamsa_data={})
        names = {item["yoga_name"] for item in detected["detected_yogas"]}
        self.assertNotIn("Raj Yoga", names)
        self.assertNotIn("Dhan Yoga", names)
        self.assertNotIn("Vipareeta Raj Yoga", names)
        not_eval = {item["yoga_name"] for item in detected["not_evaluated_yogas"]}
        self.assertIn("Mahabhagya Yoga", not_eval)

    def test_report_type_filtering_works_for_career(self):
        bundle = yoga_engine.build_yoga_signal_bundle(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA, report_type="career")
        self.assertTrue(bundle["report_type_signals"])
        self.assertTrue(all(any(tag in {"career", "wealth", "visibility", "authority", "communication"} for tag in item["report_usage"]) for item in bundle["report_type_signals"]))

    def test_astro_signal_enrichment_includes_yoga_signals(self):
        context = enrichment.build_astro_signal_context(BASE_NATAL_DATA, navamsa_data=BASE_NAVAMSA, report_type="career")
        self.assertEqual(context["yoga_signals"]["source"], "yoga_signal_engine")
        self.assertTrue(context["yoga_signals"]["signals"])
        self.assertTrue(any(signal["source"] == "yoga" for signal in context["dominant_signals"]))

    def test_missing_partial_natal_data_does_not_crash(self):
        bundle = yoga_engine.build_yoga_signal_bundle({"planets": [{"name": "Moon"}]}, report_type="birth_chart_karma")
        self.assertIsInstance(bundle["detected_yogas"], list)
        self.assertIsInstance(bundle["not_evaluated_yogas"], list)


if __name__ == "__main__":
    unittest.main()
