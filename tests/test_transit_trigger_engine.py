import unittest

from services import transit_trigger_engine as trigger_engine


TRANSIT_CONTEXT = {
    "major_peak_windows": [{"title": "Jupiter opportunity window", "planet": "Jupiter", "start": "2026-05-01", "end": "2026-06-20"}],
    "pressure_windows": [{"title": "Saturn pressure week", "planet": "Saturn", "start": "2026-05-12", "end": "2026-05-19"}],
}

BASE_DOMINANT_SIGNALS = [
    {
        "key": "moon_pushya",
        "label": "Moon - Pushya",
        "planet": "Moon",
        "categories": ["emotional", "emotional_needs", "support_strategy"],
        "tone": "opportunity",
        "report_usage": ["emotional", "support"],
        "strength": 4.0,
    },
    {
        "key": "saturn_shravana",
        "label": "Saturn - Shravana",
        "planet": "Saturn",
        "categories": ["discipline", "risk", "career"],
        "tone": "risk",
        "report_usage": ["discipline", "pressure"],
        "strength": 3.4,
    },
]


class TransitTriggerEngineTests(unittest.TestCase):
    def test_transit_without_dasha_support_does_not_create_signal(self):
        bundle = trigger_engine.build_transit_trigger_bundle(
            TRANSIT_CONTEXT,
            dasha_signal_bundle={"active_nakshatra_patterns": [], "amplified_signals": []},
            astro_signal_context={"dominant_signals": BASE_DOMINANT_SIGNALS},
            chart_relationships={"moon_relative_houses": {"Jupiter": 4, "Saturn": 8}},
        )
        self.assertEqual(bundle["transit_triggers"], [])
        self.assertEqual(bundle["signals"], [])
        self.assertTrue(bundle["blocked_events"])
        self.assertTrue(bundle["unconfirmed_transit_observations"])
        for observation in bundle["unconfirmed_transit_observations"]:
            self.assertEqual(
                sorted(observation.keys()),
                ["confidence", "effect", "event_id", "note", "planet"],
            )
            self.assertEqual(observation["confidence"], "low")

    def test_jupiter_matching_dasha_creates_opportunity_trigger(self):
        bundle = trigger_engine.build_transit_trigger_bundle(
            TRANSIT_CONTEXT,
            dasha_signal_bundle={
                "active_nakshatra_patterns": [{"planet": "Moon"}],
                "amplified_signals": [{"key": "moon_pushya"}],
            },
            astro_signal_context={"dominant_signals": BASE_DOMINANT_SIGNALS},
            chart_relationships={"moon_relative_houses": {"Jupiter": 4, "Saturn": 8}},
        )
        trigger = next(item for item in bundle["transit_triggers"] if item["planet"] == "Jupiter")
        self.assertEqual(trigger["target_signal_key"], "moon_pushya")
        self.assertEqual(trigger["effect"], "activate")

    def test_saturn_matching_signal_creates_pressure_trigger(self):
        bundle = trigger_engine.build_transit_trigger_bundle(
            TRANSIT_CONTEXT,
            dasha_signal_bundle={
                "active_nakshatra_patterns": [{"planet": "Saturn"}],
                "amplified_signals": [{"key": "saturn_shravana"}],
            },
            astro_signal_context={"dominant_signals": BASE_DOMINANT_SIGNALS},
            chart_relationships={"moon_relative_houses": {"Jupiter": 4, "Saturn": 8}},
        )
        trigger = next(item for item in bundle["transit_triggers"] if item["planet"] == "Saturn")
        self.assertEqual(trigger["target_signal_key"], "saturn_shravana")
        self.assertEqual(trigger["effect"], "delay")

    def test_moon_relative_activation_is_prioritized(self):
        bundle = trigger_engine.build_transit_trigger_bundle(
            {"major_peak_windows": [{"title": "Jupiter emotional trigger", "planet": "Jupiter"}]},
            dasha_signal_bundle={
                "active_nakshatra_patterns": [{"planet": "Moon"}],
                "amplified_signals": [{"key": "moon_pushya"}],
            },
            astro_signal_context={"dominant_signals": BASE_DOMINANT_SIGNALS},
            chart_relationships={"moon_relative_houses": {"Jupiter": 4}},
        )
        self.assertEqual(bundle["transit_triggers"][0]["target_signal_key"], "moon_pushya")

    def test_no_hallucinated_triggers_when_planet_unmapped(self):
        bundle = trigger_engine.build_transit_trigger_bundle(
            {"trigger_periods": [{"title": "Unknown activation", "planet": "Pluto"}]},
            dasha_signal_bundle={"active_nakshatra_patterns": [{"planet": "Moon"}], "amplified_signals": [{"key": "moon_pushya"}]},
            astro_signal_context={"dominant_signals": BASE_DOMINANT_SIGNALS},
            chart_relationships={"moon_relative_houses": {"Jupiter": 4}},
        )
        self.assertEqual(bundle["transit_triggers"], [])
        self.assertTrue(bundle["blocked_events"])


if __name__ == "__main__":
    unittest.main()
