import unittest

from services.chart_knowledge_mapper import build_knowledge_queries_from_signal_context


class ChartKnowledgeMapperTests(unittest.TestCase):
    def test_mapper_creates_nakshatra_pada_house_dasha_and_yoga_queries(self):
        queries = build_knowledge_queries_from_signal_context(
            {
                "natal_data": {
                    "planets": [
                        {"name": "Moon", "house": 10, "sign": "Libra", "nakshatra": "Swati", "pada": 2},
                    ],
                    "active_dasha": {"planet": "Rahu"},
                    "ascendant": {"nakshatra": "Ashwini", "pada": 1},
                },
                "yoga_signals": {"signals": [{"key": "gaja_kesari_yoga", "domain": "career"}]},
                "dominant_signals": [{"planet": "Saturn", "domain": "authority"}],
            },
            "career",
            "tr",
        )
        keys = {(row["entity_type"], row["entity"], row["sub_entity"]) for row in queries}
        self.assertIn(("nakshatra", "Swati", "pada_2"), keys)
        self.assertIn(("nakshatra", "Ashwini", "pada_1"), keys)
        self.assertTrue(any(row["entity_type"] == "house" and "10" in row["entity"] for row in queries))
        self.assertTrue(any(row["entity_type"] == "dasha" and "Rahu" in row["entity"] for row in queries))
        self.assertTrue(any(row["entity_type"] == "yoga" and "gaja" in row["entity"] for row in queries))

    def test_report_type_changes_domain_priorities(self):
        career_queries = build_knowledge_queries_from_signal_context(
            {"dominant_signals": [{"nakshatra": "Swati"}]},
            "career",
            "en",
        )
        transit_queries = build_knowledge_queries_from_signal_context(
            {"dominant_signals": [{"nakshatra": "Swati"}]},
            "annual_transit",
            "en",
        )
        self.assertEqual(career_queries[0]["domain"], "career")
        self.assertEqual(transit_queries[0]["domain"], "timing")
        self.assertIn("career", career_queries[0]["query_text"])
        self.assertIn("timing", transit_queries[0]["query_text"])


if __name__ == "__main__":
    unittest.main()
