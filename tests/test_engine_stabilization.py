from datetime import datetime

import pytest

from core.calculation_context import CalculationContext
from core.karaka_utils import select_atmakaraka
from engines import engines_natal
from engines.engines_lunations import MODERN_CO_RULER_MAP, RULER_MAP
from engines.engines_transits import score_current_impact
from services.atmakaraka_signal_engine import detect_atmakaraka


def _context():
    moment = datetime(2026, 4, 25, 12, 0, 0)
    return CalculationContext(
        datetime_local=moment,
        datetime_utc=moment,
        latitude=41.0,
        longitude=29.0,
        timezone="UTC",
        ayanamsa="lahiri",
        node_mode="true",
        house_system="whole_sign",
    )


def _patch_natal_engine(monkeypatch, longitude_by_pid, *, rahu_degree, ketu_degree, lagna_lon=15.0):
    monkeypatch.setattr(engines_natal, "configure_sidereal_mode", lambda context: None)
    monkeypatch.setattr(engines_natal.swe, "julday", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        engines_natal.swe,
        "calc_ut",
        lambda jd, pid, flags: ([longitude_by_pid[pid], 0.0, 0.0, 1.0],),
    )
    monkeypatch.setattr(
        engines_natal.swe,
        "houses_ex",
        lambda jd, lat, lon, hs, flags: ([], [lagna_lon]),
    )
    monkeypatch.setattr(
        engines_natal,
        "get_nodes",
        lambda context: {"rahu": rahu_degree, "ketu": ketu_degree, "speed": -0.1},
    )


def test_atmakaraka_consistency_default_mode_uses_same_logic_as_signal_engine(monkeypatch):
    longitude_by_pid = {
        0: 10.0,   # Sun 10
        1: 75.0,   # Moon 15
        4: 89.0,   # Mars 29
        2: 103.0,  # Mercury 13
        5: 217.0,  # Jupiter 7
        3: 198.0,  # Venus 18
        6: 256.0,  # Saturn 16
    }
    _patch_natal_engine(monkeypatch, longitude_by_pid, rahu_degree=250.0, ketu_degree=70.0)
    natal = engines_natal.calculate_natal_data(_context())

    detected = detect_atmakaraka(natal)
    assert natal["karakas"]["atmakaraka"] == detected["planet"] == "Mars"
    assert natal["karakas"]["karaka_mode"] == "7"
    assert natal["karakas"]["calculation_basis"] == "degree_within_sign"


def test_atmakaraka_consistency_eight_karaka_mode_supports_rahu(monkeypatch):
    longitude_by_pid = {
        0: 10.0,   # Sun 10
        1: 72.0,   # Moon 12
        4: 108.0,  # Mars 18
        2: 103.0,  # Mercury 13
        5: 217.0,  # Jupiter 7
        3: 198.0,  # Venus 18
        6: 256.0,  # Saturn 16
    }
    _patch_natal_engine(monkeypatch, longitude_by_pid, rahu_degree=70.0, ketu_degree=250.0)
    natal = engines_natal.calculate_natal_data(_context(), karaka_mode="8")

    detected = detect_atmakaraka(natal)
    assert natal["karakas"]["atmakaraka"] == detected["planet"] == "Rahu"
    assert detected["degree_value"] == pytest.approx(20.0)
    assert natal["karakas"]["karaka_mode"] == "8"


def test_select_atmakaraka_never_returns_ketu_and_rahu_uses_reverse_degree():
    planets = [
        {"name": "Sun", "degree": 19.0},
        {"name": "Mars", "degree": 18.5},
        {"name": "Rahu", "degree": 10.0},
        {"name": "Ketu", "degree": 29.9},
    ]
    seven_mode = select_atmakaraka(planets, mode="7")
    eight_mode = select_atmakaraka(planets, mode="8")

    assert seven_mode["planet"] == "Sun"
    assert eight_mode["planet"] == "Rahu"
    assert eight_mode["degree_value"] == pytest.approx(20.0)
    assert seven_mode["planet"] != "Ketu"
    assert eight_mode["planet"] != "Ketu"


def test_lunation_ruler_map_defaults_to_vedic_single_rulers():
    assert RULER_MAP["Scorpio"] == ["Mars"]
    assert RULER_MAP["Aquarius"] == ["Saturn"]
    assert RULER_MAP["Pisces"] == ["Jupiter"]
    assert MODERN_CO_RULER_MAP["Scorpio"] == ["Mars", "Pluto"]


def test_score_current_impact_marks_legacy_scoring_model():
    natal_data = {
        "ascendant": {"sign_idx": 0},
        "planets": [{"name": "Moon", "sign_idx": 0, "degree": 12.0}],
    }
    transits = [{"name": "Sun", "sign_idx": 0, "degree": 13.5}]
    scored = score_current_impact(natal_data, transits)
    assert scored
    assert all(item["scoring_model"] == "legacy_conjunction_only" for item in scored)
