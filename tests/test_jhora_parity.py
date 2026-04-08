import json
import unittest
from datetime import datetime
from types import SimpleNamespace

import pytz

import app
from config.astro_config import ASTRO_CONFIG, ASTRO_ENGINE_VERSION
from core.calculation_context import CalculationContext
from core.metadata import build_calculation_metadata_snapshot
from core.validation import assert_parity, serialize_natal_for_validation, validate_against_expected
from engines import engines_natal


def _fixture_meta(fixture_id, case_input, notes):
    return {
        "fixture_id": fixture_id,
        "source": "JHora",
        "jhora_version": "unknown",
        "exported_by": "manual",
        "ayanamsa": case_input["ayanamsa"],
        "node_mode": case_input["node_mode"],
        "house_system": ASTRO_CONFIG["house_system"],
        "latitude": case_input["lat"],
        "longitude": case_input["lon"],
        "timezone": case_input["timezone"],
        "notes": notes,
    }


PARITY_FIXTURES = [
    {
        "meta": _fixture_meta(
            "jhora_case_001",
            {"datetime": "2025-04-23 08:37", "lat": 41.043, "lon": 29.009, "timezone": "Europe/Istanbul", "ayanamsa": "lahiri", "node_mode": "true"},
            "Besiktas-style Istanbul parity baseline",
        ),
        "input": {
            "datetime": "2025-04-23 08:37",
            "lat": 41.043,
            "lon": 29.009,
            "timezone": "Europe/Istanbul",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Taurus", 23.1761),
            "planets": {
                "Sun": ("Aries", 9.1104, 12),
                "Moon": ("Aquarius", 6.0816, 10),
                "Mars": ("Cancer", 7.9223, 3),
                "Mercury": ("Pisces", 11.8866, 11),
                "Jupiter": ("Taurus", 25.6381, 1),
                "Venus": ("Pisces", 2.3332, 11),
                "Saturn": ("Pisces", 2.8255, 11),
                "Rahu": ("Pisces", 2.6832, 11),
                "Ketu": ("Virgo", 2.6832, 5),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_002",
            {"datetime": "1992-11-03 14:20", "lat": 28.6139, "lon": 77.2090, "timezone": "Asia/Kolkata", "ayanamsa": "lahiri", "node_mode": "true"},
            "Delhi daytime reference",
        ),
        "input": {
            "datetime": "1992-11-03 14:20",
            "lat": 28.6139,
            "lon": 77.2090,
            "timezone": "Asia/Kolkata",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Aquarius", 11.7812),
            "planets": {
                "Sun": ("Libra", 17.4322, 9),
                "Moon": ("Capricorn", 28.1490, 12),
                "Mars": ("Gemini", 29.8811, 5),
                "Mercury": ("Scorpio", 10.7775, 10),
                "Jupiter": ("Virgo", 11.0864, 8),
                "Venus": ("Scorpio", 23.9799, 10),
                "Saturn": ("Capricorn", 18.3417, 12),
                "Rahu": ("Scorpio", 28.6275, 10),
                "Ketu": ("Taurus", 28.6275, 4),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_003",
            {"datetime": "1988-06-01 05:15", "lat": 51.5074, "lon": -0.1278, "timezone": "Europe/London", "ayanamsa": "lahiri", "node_mode": "true"},
            "London early morning case",
        ),
        "input": {
            "datetime": "1988-06-01 05:15",
            "lat": 51.5074,
            "lon": -0.1278,
            "timezone": "Europe/London",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Taurus", 22.7978),
            "planets": {
                "Sun": ("Taurus", 17.1952, 1),
                "Moon": ("Scorpio", 26.5266, 7),
                "Mars": ("Aquarius", 12.5251, 10),
                "Mercury": ("Gemini", 3.0856, 2),
                "Jupiter": ("Aries", 25.8639, 12),
                "Venus": ("Gemini", 4.9016, 2),
                "Saturn": ("Sagittarius", 6.9348, 8),
                "Rahu": ("Aquarius", 25.6724, 10),
                "Ketu": ("Leo", 25.6724, 4),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_004",
            {"datetime": "2001-01-15 22:45", "lat": 40.7128, "lon": -74.0060, "timezone": "America/New_York", "ayanamsa": "lahiri", "node_mode": "true"},
            "New York evening case",
        ),
        "input": {
            "datetime": "2001-01-15 22:45",
            "lat": 40.7128,
            "lon": -74.0060,
            "timezone": "America/New_York",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Virgo", 12.3970),
            "planets": {
                "Sun": ("Capricorn", 2.2074, 5),
                "Moon": ("Virgo", 27.6837, 1),
                "Mars": ("Libra", 19.8280, 2),
                "Mercury": ("Capricorn", 15.4188, 5),
                "Jupiter": ("Taurus", 7.4663, 9),
                "Venus": ("Aquarius", 19.2959, 6),
                "Saturn": ("Taurus", 0.2663, 9),
                "Rahu": ("Gemini", 21.5938, 10),
                "Ketu": ("Sagittarius", 21.5938, 4),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_005",
            {"datetime": "1979-09-09 09:09", "lat": 39.9334, "lon": 32.8597, "timezone": "Europe/Istanbul", "ayanamsa": "lahiri", "node_mode": "true"},
            "Ankara reference case",
        ),
        "input": {
            "datetime": "1979-09-09 09:09",
            "lat": 39.9334,
            "lon": 32.8597,
            "timezone": "Europe/Istanbul",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Virgo", 24.6348),
            "planets": {
                "Sun": ("Leo", 22.4053, 12),
                "Moon": ("Aries", 1.5709, 8),
                "Mars": ("Gemini", 26.8701, 10),
                "Mercury": ("Leo", 18.7235, 12),
                "Jupiter": ("Leo", 2.2915, 12),
                "Venus": ("Leo", 26.4233, 12),
                "Saturn": ("Leo", 23.5159, 12),
                "Rahu": ("Leo", 14.8906, 12),
                "Ketu": ("Aquarius", 14.8906, 6),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_006",
            {"datetime": "1999-12-31 23:59", "lat": 48.8566, "lon": 2.3522, "timezone": "Europe/Paris", "ayanamsa": "lahiri", "node_mode": "true"},
            "Paris millennium edge case",
        ),
        "input": {
            "datetime": "1999-12-31 23:59",
            "lat": 48.8566,
            "lon": 2.3522,
            "timezone": "Europe/Paris",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Virgo", 3.9794),
            "planets": {
                "Sun": ("Sagittarius", 15.9628, 4),
                "Moon": ("Libra", 12.9273, 2),
                "Mars": ("Aquarius", 3.6894, 6),
                "Mercury": ("Sagittarius", 7.1928, 4),
                "Jupiter": ("Aries", 1.3782, 8),
                "Venus": ("Scorpio", 7.0570, 3),
                "Saturn": ("Aries", 16.5535, 8),
                "Rahu": ("Cancer", 10.1277, 11),
                "Ketu": ("Capricorn", 10.1277, 5),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_007",
            {"datetime": "1985-03-20 11:05", "lat": 35.6895, "lon": 139.6917, "timezone": "Asia/Tokyo", "ayanamsa": "lahiri", "node_mode": "true"},
            "Tokyo midday case",
        ),
        "input": {
            "datetime": "1985-03-20 11:05",
            "lat": 35.6895,
            "lon": 139.6917,
            "timezone": "Asia/Tokyo",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Gemini", 12.3413),
            "planets": {
                "Sun": ("Pisces", 5.7673, 10),
                "Moon": ("Aquarius", 20.1918, 9),
                "Mars": ("Aries", 9.8882, 11),
                "Mercury": ("Pisces", 23.5520, 10),
                "Jupiter": ("Capricorn", 15.1579, 8),
                "Venus": ("Pisces", 27.8283, 10),
                "Saturn": ("Scorpio", 4.3466, 6),
                "Rahu": ("Aries", 25.6838, 11),
                "Ketu": ("Libra", 25.6838, 5),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_008",
            {"datetime": "1995-07-17 06:40", "lat": -33.8688, "lon": 151.2093, "timezone": "Australia/Sydney", "ayanamsa": "lahiri", "node_mode": "true"},
            "Sydney southern hemisphere case",
        ),
        "input": {
            "datetime": "1995-07-17 06:40",
            "lat": -33.8688,
            "lon": 151.2093,
            "timezone": "Australia/Sydney",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Gemini", 24.2058),
            "planets": {
                "Sun": ("Cancer", 0.0459, 2),
                "Moon": ("Aquarius", 29.1883, 9),
                "Mars": ("Virgo", 3.5532, 4),
                "Mercury": ("Gemini", 17.3687, 1),
                "Jupiter": ("Scorpio", 12.1655, 6),
                "Venus": ("Gemini", 20.3883, 1),
                "Saturn": ("Pisces", 0.8627, 10),
                "Rahu": ("Libra", 7.8764, 5),
                "Ketu": ("Aries", 7.8764, 11),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_009",
            {"datetime": "1970-02-28 18:30", "lat": 55.7558, "lon": 37.6173, "timezone": "Europe/Moscow", "ayanamsa": "lahiri", "node_mode": "true"},
            "Moscow winter case",
        ),
        "input": {
            "datetime": "1970-02-28 18:30",
            "lat": 55.7558,
            "lon": 37.6173,
            "timezone": "Europe/Moscow",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Leo", 22.0300),
            "planets": {
                "Sun": ("Aquarius", 16.1898, 7),
                "Moon": ("Scorpio", 10.4027, 4),
                "Mars": ("Aries", 1.9485, 9),
                "Mercury": ("Capricorn", 28.1610, 6),
                "Jupiter": ("Libra", 12.4084, 3),
                "Venus": ("Aquarius", 24.6111, 7),
                "Saturn": ("Aries", 11.3332, 9),
                "Rahu": ("Aquarius", 18.3852, 7),
                "Ketu": ("Leo", 18.3852, 1),
            },
        },
    },
    {
        "meta": _fixture_meta(
            "jhora_case_010",
            {"datetime": "2010-10-10 10:10", "lat": 41.0082, "lon": 28.9784, "timezone": "Europe/Istanbul", "ayanamsa": "lahiri", "node_mode": "true"},
            "Istanbul daytime parity case",
        ),
        "input": {
            "datetime": "2010-10-10 10:10",
            "lat": 41.0082,
            "lon": 28.9784,
            "timezone": "Europe/Istanbul",
            "ayanamsa": "lahiri",
            "node_mode": "true",
        },
        "expected": {
            "asc": ("Libra", 27.2082),
            "planets": {
                "Sun": ("Virgo", 22.8704, 12),
                "Moon": ("Libra", 27.2586, 1),
                "Mars": ("Libra", 23.2820, 1),
                "Mercury": ("Virgo", 17.8679, 12),
                "Jupiter": ("Pisces", 1.9750, 6),
                "Venus": ("Libra", 19.1419, 1),
                "Saturn": ("Virgo", 14.8739, 12),
                "Rahu": ("Sagittarius", 12.1311, 3),
                "Ketu": ("Gemini", 12.1311, 9),
            },
        },
    },
]


def _build_context(case_input):
    timezone = pytz.timezone(case_input["timezone"])
    local_dt = timezone.localize(datetime.strptime(case_input["datetime"], "%Y-%m-%d %H:%M"))
    utc_dt = local_dt.astimezone(pytz.UTC)
    return CalculationContext(
        datetime_local=local_dt,
        datetime_utc=utc_dt,
        latitude=case_input["lat"],
        longitude=case_input["lon"],
        timezone=case_input["timezone"],
        ayanamsa=case_input["ayanamsa"],
        node_mode=case_input["node_mode"],
        house_system=ASTRO_CONFIG["house_system"],
    )


class FakeDb:
    def __init__(self):
        self.added = []

    def add(self, item):
        self.added.append(item)


class JHoraParityTests(unittest.TestCase):
    def test_fixed_jhora_parity_cases(self):
        for fixture in PARITY_FIXTURES:
            with self.subTest(case=fixture["meta"]["fixture_id"]):
                context = _build_context(fixture["input"])
                calculated = serialize_natal_for_validation(engines_natal.calculate_natal_data(context))
                validation = assert_parity(calculated, fixture)
                self.assertEqual(validation["status"], "pass")

    def test_every_fixture_has_traceable_metadata(self):
        required = {
            "fixture_id",
            "source",
            "jhora_version",
            "exported_by",
            "ayanamsa",
            "node_mode",
            "house_system",
            "latitude",
            "longitude",
            "timezone",
            "notes",
        }
        for fixture in PARITY_FIXTURES:
            with self.subTest(case=fixture["meta"]["fixture_id"]):
                self.assertTrue(required.issubset(set(fixture["meta"].keys())))

    def test_node_mode_is_explicitly_controlled(self):
        base_input = PARITY_FIXTURES[0]["input"]
        true_context = _build_context(base_input)
        mean_context = CalculationContext(
            datetime_local=true_context.datetime_local,
            datetime_utc=true_context.datetime_utc,
            latitude=true_context.latitude,
            longitude=true_context.longitude,
            timezone=true_context.timezone,
            ayanamsa=true_context.ayanamsa,
            node_mode="mean",
            house_system=true_context.house_system,
        )
        true_nodes = serialize_natal_for_validation(engines_natal.calculate_natal_data(true_context))["planets"]
        mean_nodes = serialize_natal_for_validation(engines_natal.calculate_natal_data(mean_context))["planets"]
        self.assertNotEqual(true_nodes["Rahu"]["abs_longitude"], mean_nodes["Rahu"]["abs_longitude"])
        self.assertNotEqual(true_nodes["Ketu"]["abs_longitude"], mean_nodes["Ketu"]["abs_longitude"])

    def test_house_system_is_locked_to_whole_sign(self):
        base_input = PARITY_FIXTURES[0]["input"]
        context = _build_context(base_input)
        bad_context = CalculationContext(
            datetime_local=context.datetime_local,
            datetime_utc=context.datetime_utc,
            latitude=context.latitude,
            longitude=context.longitude,
            timezone=context.timezone,
            ayanamsa=context.ayanamsa,
            node_mode=context.node_mode,
            house_system="placidus",
        )
        with self.assertRaisesRegex(ValueError, "Unsupported house system"):
            engines_natal.calculate_natal_data(bad_context)

    def test_true_lahiri_does_not_silently_fall_back_to_lahiri(self):
        fixture = PARITY_FIXTURES[0]
        context = _build_context({**fixture["input"], "ayanamsa": "true_lahiri"})
        with self.assertRaisesRegex(NotImplementedError, "true_lahiri"):
            engines_natal.calculate_natal_data(context)

    def test_unsupported_true_lahiri_produces_structured_validation_failure(self):
        fixture = PARITY_FIXTURES[0]
        calculated = {
            "ascendant": {},
            "planets": {},
            "config_error": "Ayanamsa 'true_lahiri' is configured but not implemented with the current ephemeris integration.",
            "calculation_config": {
                "engine_version": ASTRO_ENGINE_VERSION,
                "ayanamsa": "true_lahiri",
                "ayanamsa_requested": "true_lahiri",
                "ayanamsa_applied": None,
                "ayanamsa_supported": False,
                "node_mode": "true",
                "house_system": "whole_sign",
                "warning": "true_lahiri_not_implemented",
            },
        }
        validation = validate_against_expected(calculated, fixture)
        self.assertEqual(validation["status"], "fail")
        self.assertEqual(validation["differences"][0]["field"], "unsupported_config")
        self.assertEqual(validation["differences"][0]["severity"], "critical")

    def test_validation_diff_includes_delta_tolerance_and_severity(self):
        fixture = PARITY_FIXTURES[0]
        context = _build_context(fixture["input"])
        calculated = serialize_natal_for_validation(engines_natal.calculate_natal_data(context))
        calculated["planets"]["Jupiter"]["degree"] += 0.25
        validation = validate_against_expected(calculated, fixture)
        self.assertEqual(validation["status"], "fail")
        jupiter_diff = next(diff for diff in validation["differences"] if diff["entity"] == "Jupiter" and diff["field"] == "longitude")
        self.assertIsNotNone(jupiter_diff["delta"])
        self.assertIsNotNone(jupiter_diff["tolerance"])
        self.assertTrue(jupiter_diff["severity"])

    def test_strict_parity_fails_on_unsupported_config(self):
        fixture = PARITY_FIXTURES[0]
        bad_result = {
            "ascendant": {},
            "planets": {},
            "config_error": "Unsupported true_lahiri",
            "calculation_config": {
                "ayanamsa_requested": "true_lahiri",
                "ayanamsa_applied": None,
                "ayanamsa_supported": False,
                "node_mode": "true",
                "house_system": "whole_sign",
            },
        }
        with self.assertRaisesRegex(AssertionError, "unsupported_config"):
            assert_parity(bad_result, fixture)

    def test_generated_report_metadata_includes_calculation_snapshot_and_engine_version(self):
        fixture = PARITY_FIXTURES[0]
        context = _build_context(fixture["input"])
        birth_context = {
            "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
            "geocode_provider": "nominatim",
            "location_source": "provider:nominatim",
            "geocode_cache_hit": False,
        }
        snapshot = build_calculation_metadata_snapshot(calculation_context=context, birth_context=birth_context)
        self.assertEqual(snapshot["engine_version"], ASTRO_ENGINE_VERSION)
        self.assertEqual(snapshot["ayanamsa"], "lahiri")
        self.assertEqual(snapshot["normalized_birth_place"], birth_context["normalized_birth_place"])

        fake_db = FakeDb()
        fake_user = SimpleNamespace(id=99)
        payload = {
            "full_name": "Test User",
            "birth_date": "2025-04-23",
            "birth_time": "08:37",
            "birth_city": "Besiktas, Istanbul",
            "birth_country": "Turkey",
            "raw_birth_place_input": "Besiktas, Istanbul, Turkey",
            "normalized_birth_place": birth_context["normalized_birth_place"],
            "latitude": fixture["input"]["lat"],
            "longitude": fixture["input"]["lon"],
            "timezone": fixture["input"]["timezone"],
            "geocode_provider": "nominatim",
            "geocode_confidence": 0.9,
        }
        report = app._save_generated_report(
            fake_db,
            fake_user,
            None,
            "premium",
            payload,
            {"primary_focus": "Timing"},
            snapshot,
        )
        self.assertEqual(len(fake_db.added), 1)
        saved_metadata = json.loads(report.calculation_metadata_json)
        self.assertEqual(saved_metadata["engine_version"], ASTRO_ENGINE_VERSION)
        self.assertEqual(saved_metadata["ayanamsa"], "lahiri")


if __name__ == "__main__":
    unittest.main()
