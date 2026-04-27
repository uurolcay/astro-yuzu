from services.prediction_fusion_engine import NOT_FATED_NOTE, build_prediction_fusion


def _signal(key, label, categories, *, planet="", tone="opportunity", strength=3.0):
    return {
        "key": key,
        "label": label,
        "categories": list(categories),
        "planet": planet,
        "tone": tone,
        "strength": strength,
        "report_usage": [],
    }


def _base_context():
    dominant = [
        _signal("career:path", "Career Path", ["career", "visibility"], planet="Jupiter", strength=3.6),
        _signal("bond:care", "Bond Care", ["relationship", "emotional_needs"], planet="Moon", strength=2.8),
        _signal("child:loop", "Child Loop", ["parent_child", "communication"], planet="Mercury", strength=3.1, tone="risk"),
    ]
    return {
        "dominant_signals": dominant,
        "risk_signals": [dominant[2]],
        "opportunity_signals": [dominant[0], dominant[1]],
        "yoga_signals": {"signals": []},
        "atmakaraka_signals": {"signals": []},
    }


def _dasha_bundle(*, lord="Jupiter", amplified=None):
    return {
        "dasha_lord": lord,
        "active_period": {"start": "2026-05-01", "end": "2026-08-31"},
        "active_nakshatra_patterns": [{"planet": lord, "label": f"{lord} pattern"}],
        "amplified_signals": list(amplified or []),
    }


def _transit_bundle(*, planet="Saturn", target_signal_key="career:path", confidence="high"):
    return {
        "transit_triggers": [
            {
                "trigger_id": f"t:{planet.lower()}",
                "planet": planet,
                "target_signal_key": target_signal_key,
                "effect": "activate",
                "duration": "2026-06",
                "confidence": confidence,
            }
        ],
        "blocked_events": [],
    }


def test_dasha_and_transit_with_matching_signal_creates_prediction_window():
    result = build_prediction_fusion(
        astro_signal_context=_base_context(),
        dasha_signal_bundle=_dasha_bundle(amplified=[{"key": "career:path"}]),
        transit_trigger_bundle=_transit_bundle(),
        report_type="career",
        language="en",
    )
    assert len(result["prediction_windows"]) == 1
    item = result["prediction_windows"][0]
    assert item["prediction_id"] == "career:career:path:Jupiter"
    assert item["activation_source"] == "Jupiter"
    assert item["delivery_source"] == "Saturn"
    assert item["confidence"] in {"medium", "high"}


def test_dasha_present_transit_absent_creates_active_theme_only():
    result = build_prediction_fusion(
        astro_signal_context=_base_context(),
        dasha_signal_bundle=_dasha_bundle(amplified=[{"key": "career:path"}]),
        transit_trigger_bundle={"transit_triggers": [], "blocked_events": []},
        report_type="career",
        language="en",
    )
    assert result["prediction_windows"] == []
    assert len(result["active_themes"]) == 1
    assert result["active_themes"][0]["theme"] == "Career Path"


def test_transit_present_dasha_absent_creates_blocked_prediction_only():
    result = build_prediction_fusion(
        astro_signal_context=_base_context(),
        dasha_signal_bundle={},
        transit_trigger_bundle=_transit_bundle(),
        report_type="career",
        language="en",
    )
    assert result["prediction_windows"] == []
    assert result["active_themes"] == []
    assert len(result["blocked_predictions"]) >= 1


def test_yoga_boosted_true_when_yoga_signal_matches_domain():
    context = _base_context()
    context["yoga_signals"]["signals"] = [_signal("yoga:career", "Career Yoga", ["career"], strength=2.0)]
    result = build_prediction_fusion(
        astro_signal_context=context,
        dasha_signal_bundle=_dasha_bundle(amplified=[{"key": "career:path"}]),
        transit_trigger_bundle=_transit_bundle(),
        report_type="career",
        language="en",
    )
    item = result["prediction_windows"][0]
    assert item["yoga_boosted"] is True
    assert item["opportunity_level"] in {"high", "very_high"}


def test_yoga_alone_does_not_create_window_or_raise_confidence():
    context = _base_context()
    context["yoga_signals"]["signals"] = [_signal("yoga:career", "Career Yoga", ["career"], strength=2.0)]
    result = build_prediction_fusion(
        astro_signal_context=context,
        dasha_signal_bundle={},
        transit_trigger_bundle={"transit_triggers": [], "blocked_events": []},
        report_type="career",
        language="en",
    )
    assert result["prediction_windows"] == []
    assert result["active_themes"] == []
    assert result["blocked_predictions"] == []


def test_atmakaraka_boosted_true_when_domain_matches():
    context = _base_context()
    context["atmakaraka_signals"]["signals"] = [_signal("atmakaraka:jupiter", "Atmakaraka", ["career"], strength=3.0)]
    result = build_prediction_fusion(
        astro_signal_context=context,
        dasha_signal_bundle=_dasha_bundle(amplified=[{"key": "career:path"}]),
        transit_trigger_bundle=_transit_bundle(),
        report_type="career",
        language="en",
    )
    item = result["prediction_windows"][0]
    assert item["atmakaraka_boosted"] is True
    assert any("Atmakaraka emphasis" in note for note in result["confidence_notes"])


def test_not_fated_note_is_present_and_exact_in_every_prediction_item():
    result = build_prediction_fusion(
        astro_signal_context=_base_context(),
        dasha_signal_bundle=_dasha_bundle(amplified=[{"key": "career:path"}]),
        transit_trigger_bundle=_transit_bundle(),
        report_type="career",
        language="en",
    )
    assert result["prediction_windows"]
    assert all(item["not_fated_note"] == NOT_FATED_NOTE for item in result["prediction_windows"])


def test_all_inputs_none_or_empty_returns_empty_schema_without_exception():
    result = build_prediction_fusion(
        astro_signal_context=None,
        dasha_signal_bundle=None,
        transit_trigger_bundle=None,
        chart_relationships=None,
        report_type="birth_chart_karma",
        language="tr",
    )
    assert result["source"] == "prediction_fusion_engine"
    assert result["prediction_windows"] == []
    assert result["active_themes"] == []
    assert result["blocked_predictions"] == []
    assert result["unconfirmed_observations"] == []
    assert result["confidence_notes"]


def test_parent_child_domain_mapping_works_end_to_end():
    result = build_prediction_fusion(
        astro_signal_context=_base_context(),
        dasha_signal_bundle=_dasha_bundle(lord="Mercury", amplified=[{"key": "child:loop"}]),
        transit_trigger_bundle=_transit_bundle(planet="Mercury", target_signal_key="child:loop"),
        report_type="parent_child",
        language="en",
    )
    assert len(result["prediction_windows"]) == 1
    assert result["prediction_windows"][0]["domain"] == "parent_child"


def test_prediction_id_is_deterministic_and_follows_schema():
    result = build_prediction_fusion(
        astro_signal_context=_base_context(),
        dasha_signal_bundle=_dasha_bundle(amplified=[{"key": "career:path"}]),
        transit_trigger_bundle=_transit_bundle(),
        report_type="career",
        language="en",
    )
    item = result["prediction_windows"][0]
    assert item["prediction_id"] == "career:career:path:Jupiter"
