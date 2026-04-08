"""Validation helpers for parity testing."""

from __future__ import annotations

from config.astro_config import STRICT_ASTRO_MODE

SIGN_NAMES = [
    "Aries",
    "Taurus",
    "Gemini",
    "Cancer",
    "Leo",
    "Virgo",
    "Libra",
    "Scorpio",
    "Sagittarius",
    "Capricorn",
    "Aquarius",
    "Pisces",
]


def _degree_diff(actual, expected):
    diff = abs(float(actual) - float(expected))
    return min(diff, 360.0 - diff)


def _append_difference(differences, *, entity, field, expected, actual, delta=None, tolerance=None, severity="error", context=None):
    differences.append(
        {
            "entity": entity,
            "field": field,
            "expected": expected,
            "actual": actual,
            "delta": delta,
            "tolerance": tolerance,
            "severity": severity,
            "context": context or {},
        }
    )


def serialize_natal_for_validation(natal_data):
    ascendant = natal_data.get("ascendant", {})
    planets = {}
    for planet in natal_data.get("planets", []):
        planets[planet["name"]] = {
            "sign": SIGN_NAMES[planet["sign_idx"]],
            "degree": round(float(planet.get("degree", 0.0)), 4),
            "house": planet.get("house"),
            "abs_longitude": round(float(planet.get("abs_longitude", 0.0)), 4),
        }
    return {
        "ascendant": {
            "sign": SIGN_NAMES[ascendant.get("sign_idx", 0)],
            "degree": round(float(ascendant.get("degree", 0.0)), 4),
            "abs_longitude": round(float(ascendant.get("abs_longitude", 0.0)), 4),
        },
        "planets": planets,
        "calculation_config": natal_data.get("calculation_config", {}),
        "config_error": natal_data.get("config_error"),
    }


def validate_against_expected(calculated, expected):
    differences = []

    expected_body = expected.get("expected", expected)
    expected_meta = expected.get("meta", {})
    calculated_config = calculated.get("calculation_config", {})

    if calculated.get("config_error"):
        _append_difference(
            differences,
            entity="config",
            field="unsupported_config",
            expected="supported configuration",
            actual=calculated.get("config_error"),
            severity="critical",
            context={"calculation_config": calculated_config},
        )

    if calculated_config.get("ayanamsa_supported") is False:
        _append_difference(
            differences,
            entity="config",
            field="ayanamsa",
            expected=expected_meta.get("ayanamsa"),
            actual=calculated_config.get("ayanamsa_requested"),
            severity="critical",
            context={"warning": calculated_config.get("warning")},
        )

    for field in ("ayanamsa", "node_mode", "house_system"):
        if expected_meta.get(field) and calculated_config.get(field) != expected_meta.get(field):
            _append_difference(
                differences,
                entity="config",
                field=field,
                expected=expected_meta.get(field),
                actual=calculated_config.get(field),
                severity="critical" if field == "ayanamsa" else "error",
            )

    expected_asc = expected_body.get("asc")
    if expected_asc:
        actual_asc = calculated.get("ascendant", {})
        if actual_asc.get("sign") != expected_asc[0]:
            _append_difference(
                differences,
                entity="Ascendant",
                field="sign",
                expected=expected_asc[0],
                actual=actual_asc.get("sign"),
                severity="error",
            )
        asc_delta = _degree_diff(actual_asc.get("degree", 0.0), expected_asc[1])
        if asc_delta > 0.1:
            _append_difference(
                differences,
                entity="Ascendant",
                field="asc_degree",
                expected=expected_asc[1],
                actual=actual_asc.get("degree", 0.0),
                delta=round(asc_delta, 6),
                tolerance=0.1,
                severity="error",
            )

    for planet_name, expected_value in expected_body.get("planets", {}).items():
        actual_value = calculated.get("planets", {}).get(planet_name)
        if not actual_value:
            _append_difference(
                differences,
                entity=planet_name,
                field="missing_planet",
                expected=expected_value,
                actual=None,
                severity="critical",
            )
            continue
        tolerance = 0.05 if planet_name in {"Rahu", "Ketu"} else 0.1
        if actual_value.get("sign") != expected_value[0]:
            _append_difference(
                differences,
                entity=planet_name,
                field="sign",
                expected=expected_value[0],
                actual=actual_value.get("sign"),
                severity="error",
            )
        longitude_delta = _degree_diff(actual_value.get("degree", 0.0), expected_value[1])
        if longitude_delta > tolerance:
            _append_difference(
                differences,
                entity=planet_name,
                field="longitude",
                expected=expected_value[1],
                actual=actual_value.get("degree", 0.0),
                delta=round(longitude_delta, 6),
                tolerance=tolerance,
                severity="critical" if planet_name in {"Rahu", "Ketu"} else "error",
            )
        if len(expected_value) > 2 and actual_value.get("house") != expected_value[2]:
            _append_difference(
                differences,
                entity=planet_name,
                field="house",
                expected=expected_value[2],
                actual=actual_value.get("house"),
                severity="error",
            )

    return {"status": "pass" if not differences else "fail", "differences": differences}


def assert_parity(calculated, expected):
    validation = validate_against_expected(calculated, expected)
    if STRICT_ASTRO_MODE and validation["status"] != "pass":
        first_difference = validation["differences"][0]
        raise AssertionError(
            f"Parity validation failed [{first_difference['field']}] "
            f"expected={first_difference['expected']} actual={first_difference['actual']}"
        )
    return validation
