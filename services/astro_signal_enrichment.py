from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from typing import Any

try:
    from services.nakshatra_signal_engine import build_nakshatra_signal_profile
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_nakshatra_signal_profile = None
try:
    from services.yoga_signal_engine import build_yoga_signal_bundle
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_yoga_signal_bundle = None
try:
    from services.chart_relationships import build_chart_relationships
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_chart_relationships = None
try:
    from services.atmakaraka_signal_engine import build_atmakaraka_signal
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_atmakaraka_signal = None
try:
    from services.transit_trigger_engine import build_transit_trigger_bundle
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_transit_trigger_bundle = None
try:
    from services.parent_child_interaction_engine import build_parent_child_interaction_bundle
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_parent_child_interaction_bundle = None
try:
    from services.prediction_fusion_engine import build_prediction_fusion_context
except Exception:  # pragma: no cover - import failure is handled at runtime
    build_prediction_fusion_context = None

REPORT_TYPE_FOCUS = {
    "birth_chart_karma": {"identity", "emotional", "karmic", "spiritual", "relationship", "life_direction"},
    "annual_transit": {"dasha_activation", "timing", "risk", "opportunity"},
    "career": {"career", "wealth", "authority", "communication", "discipline", "visibility"},
    "parent_child": {"emotional_needs", "learning_style", "discipline_response", "parent_child_friction", "support_strategy"},
}

DASHA_PLANET_SIGNAL_MAP = {
    "Sun": {"categories": ["authority", "visibility", "life_direction"], "tone": "opportunity", "keywords": ["leadership", "recognition"]},
    "Moon": {"categories": ["emotional", "emotional_needs"], "tone": "opportunity", "keywords": ["regulation", "belonging"]},
    "Mars": {"categories": ["discipline", "risk", "parent_child_friction"], "tone": "risk", "keywords": ["reactivity", "assertion"]},
    "Mercury": {"categories": ["communication", "learning_style"], "tone": "opportunity", "keywords": ["speech", "adaptation"]},
    "Jupiter": {"categories": ["career", "wealth", "opportunity"], "tone": "opportunity", "keywords": ["growth", "guidance"]},
    "Venus": {"categories": ["relationship", "support_strategy", "wealth"], "tone": "opportunity", "keywords": ["harmony", "attraction"]},
    "Saturn": {"categories": ["discipline", "career", "risk"], "tone": "risk", "keywords": ["responsibility", "delay"]},
    "Rahu": {"categories": ["life_direction", "risk", "visibility"], "tone": "risk", "keywords": ["amplification", "instability"]},
    "Ketu": {"categories": ["karmic", "spiritual", "timing"], "tone": "opportunity", "keywords": ["release", "detachment"]},
}

DASHA_NAKSHATRA_TRIADS = {
    "Ketu": ("Ashwini", "Magha", "Mula"),
    "Venus": ("Bharani", "Purva Phalguni", "Purva Ashadha"),
    "Sun": ("Krittika", "Uttara Phalguni", "Uttara Ashadha"),
    "Moon": ("Rohini", "Hasta", "Shravana"),
    "Mars": ("Mrigashirsha", "Chitra", "Dhanishta"),
    "Rahu": ("Ardra", "Swati", "Shatabhisha"),
    "Jupiter": ("Punarvasu", "Vishakha", "Purva Bhadrapada"),
    "Saturn": ("Pushya", "Anuradha", "Uttara Bhadrapada"),
    "Mercury": ("Ashlesha", "Jyeshtha", "Revati"),
}


def merge_signal_strength(existing_strength, added_strength):
    try:
        base = float(existing_strength or 0.0)
    except (TypeError, ValueError):
        base = 0.0
    try:
        extra = float(added_strength or 0.0)
    except (TypeError, ValueError):
        extra = 0.0
    return round(base + extra, 2)


def build_astro_signal_context(
    natal_data,
    navamsa_data=None,
    dasha_data=None,
    transit_context=None,
    report_type="birth_chart_karma",
):
    natal_data = natal_data or {}
    navamsa_data = navamsa_data or {}
    dasha_data = dasha_data or []
    transit_context = transit_context or {}

    chart_relationships = _build_chart_relationships(natal_data, navamsa_data)
    nakshatra_signals = _build_nakshatra_signals(natal_data, report_type=report_type)
    yoga_signals = _build_yoga_signals(
        natal_data,
        navamsa_data,
        report_type=report_type,
        chart_relationships=chart_relationships,
    )
    atmakaraka_signals = _build_atmakaraka_signals(
        natal_data,
        chart_relationships=chart_relationships,
    )
    dasha_activation_signals = _build_dasha_activation_signals(
        dasha_data,
        transit_context,
        nakshatra_signals=nakshatra_signals,
        yoga_signals=yoga_signals,
    )
    transit_trigger_signals = _build_transit_trigger_signals(
        transit_context,
        dasha_signal_bundle=dasha_activation_signals.get("dasha_signal_bundle") or {},
        astro_signal_context={
            "dominant_signals": list((nakshatra_signals.get("signals") or []))
            + list((yoga_signals.get("signals") or []))
            + list((atmakaraka_signals.get("signals") or []))
            + list((dasha_activation_signals.get("signals") or [])),
        },
        chart_relationships=chart_relationships,
    )
    parent_child_interaction_signals = _build_parent_child_interaction_signals(
        transit_context,
        report_type=report_type,
    )

    aggregate: dict[str, dict[str, Any]] = {}
    for group_name, group in (
        ("nakshatra", nakshatra_signals),
        ("yoga", yoga_signals),
        ("atmakaraka", atmakaraka_signals),
        ("dasha_activation", dasha_activation_signals),
        ("transit_trigger", transit_trigger_signals),
    ):
        for signal in group.get("signals", []):
            _merge_dominant_signal(aggregate, signal, source=group_name)

    dominant_signals = sorted(aggregate.values(), key=lambda item: (-float(item.get("strength", 0.0)), item.get("key", "")))
    risk_signals = [item for item in dominant_signals if item.get("tone") == "risk"]
    opportunity_signals = [item for item in dominant_signals if item.get("tone") == "opportunity"]
    report_type_signals = _build_report_type_signal_map(dominant_signals)
    confidence_notes = _build_confidence_notes(natal_data, navamsa_data, dasha_data, transit_context)
    confidence_notes.extend(nakshatra_signals.get("confidence_notes") or [])
    confidence_notes.extend(yoga_signals.get("confidence_notes") or [])
    confidence_notes.extend(atmakaraka_signals.get("confidence_notes") or [])
    confidence_notes.extend(dasha_activation_signals.get("confidence_notes") or [])
    confidence_notes.extend(transit_trigger_signals.get("confidence_notes") or [])
    confidence_notes.extend(parent_child_interaction_signals.get("confidence_notes") or [])
    confidence_notes.extend(chart_relationships.get("confidence_notes") or [])

    base_signal_context = {
        "nakshatra_signals": nakshatra_signals,
        "yoga_signals": yoga_signals,
        "atmakaraka_signals": atmakaraka_signals,
        "dasha_activation_signals": dasha_activation_signals,
        "dasha_signal_bundle": dasha_activation_signals.get("dasha_signal_bundle") or {},
        "transit_trigger_signals": transit_trigger_signals,
        "parent_child_interaction_signals": parent_child_interaction_signals,
        "chart_relationships": chart_relationships,
        "dominant_signals": dominant_signals,
        "risk_signals": risk_signals,
        "opportunity_signals": opportunity_signals,
        "report_type_signals": report_type_signals,
        "confidence_notes": confidence_notes,
    }
    prediction_fusion_context = _build_prediction_fusion_context_safe(
        natal_data=natal_data,
        dasha_data=dasha_data,
        transit_context=transit_context,
        signal_context=base_signal_context,
        report_type=report_type,
    )
    confidence_notes.extend(prediction_fusion_context.get("confidence_notes") or [])

    signal_context = {
        **base_signal_context,
        "prediction_fusion": prediction_fusion_context,
    }
    _merge_prediction_fusion_into_signal_context(signal_context, prediction_fusion_context)
    return filter_signal_context_by_report_type(signal_context, report_type)


def filter_signal_context_by_report_type(signal_context, report_type):
    context = deepcopy(signal_context or {})
    normalized_report_type = str(report_type or "birth_chart_karma").strip().lower()
    allowed_categories = REPORT_TYPE_FOCUS.get(normalized_report_type, REPORT_TYPE_FOCUS["birth_chart_karma"])

    def _allowed(signal):
        categories = set(signal.get("categories") or [])
        return bool(categories & allowed_categories)

    context["dominant_signals"] = [signal for signal in context.get("dominant_signals", []) if _allowed(signal)]
    context["risk_signals"] = [signal for signal in context.get("risk_signals", []) if _allowed(signal)]
    context["opportunity_signals"] = [signal for signal in context.get("opportunity_signals", []) if _allowed(signal)]
    context["report_type_signals"] = {
        category: [signal for signal in signals if _allowed(signal)]
        for category, signals in (context.get("report_type_signals") or {}).items()
        if category in allowed_categories
    }
    context["filtered_report_type"] = normalized_report_type
    context["allowed_categories"] = sorted(allowed_categories)
    return context


def _build_nakshatra_signals(natal_data, *, report_type="birth_chart_karma"):
    if build_nakshatra_signal_profile is None:
        return {
            "signals": [],
            "source_count": 0,
            "confidence_notes": ["Dedicated nakshatra signal engine is unavailable; enrichment falls back without nakshatra profiling."],
        }
    try:
        profile = build_nakshatra_signal_profile(natal_data, report_type=report_type)
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "signals": [],
            "source_count": 0,
            "confidence_notes": [f"Nakshatra signal engine failed safely: {exc}"],
        }
    signals = []
    for item in profile.get("planetary_nakshatra_signals") or []:
        signals.append(_normalize_nakshatra_signal(item))
    for singular in ("moon_nakshatra", "lagna_nakshatra"):
        item = profile.get(singular) or {}
        if item:
            signals.append(_normalize_nakshatra_signal(item))
    return {
        **profile,
        "signals": _dedupe_signal_rows(signals),
        "source_count": len(signals),
    }


def _build_prediction_fusion_context_safe(
    *,
    natal_data,
    dasha_data,
    transit_context,
    signal_context,
    report_type,
):
    if build_prediction_fusion_context is None:
        return _empty_prediction_fusion_context()
    try:
        return build_prediction_fusion_context(
            natal_data=natal_data,
            dasha_data=dasha_data,
            transit_data=transit_context,
            signal_context=signal_context,
            report_type=report_type,
            language="tr",
        )
    except Exception:  # pragma: no cover - defensive guard
        return _empty_prediction_fusion_context()


def _empty_prediction_fusion_context():
    return {
        "available": False,
        "timing_summary": [],
        "active_dasha_signals": [],
        "active_transit_signals": [],
        "fusion_signals": [],
        "risk_windows": [],
        "opportunity_windows": [],
        "confidence_notes": [],
        "report_type_focus": {},
    }


def _merge_prediction_fusion_into_signal_context(signal_context, prediction_fusion_context):
    if not isinstance(signal_context, dict):
        return
    fusion = prediction_fusion_context if isinstance(prediction_fusion_context, dict) else _empty_prediction_fusion_context()
    signal_context["prediction_fusion"] = fusion
    if not fusion.get("available"):
        return

    risk_rows = signal_context.setdefault("risk_signals", [])
    opportunity_rows = signal_context.setdefault("opportunity_signals", [])
    confidence_notes = signal_context.setdefault("confidence_notes", [])
    dasha_activation = signal_context.setdefault("dasha_activation_signals", {})
    dasha_activation.setdefault("signals", [])

    for item in fusion.get("fusion_signals") or []:
        normalized = _normalize_prediction_fusion_signal(item)
        if item.get("timing_type") in {"pressure", "review"}:
            risk_rows.append(normalized)
        if item.get("timing_type") in {"activation", "opportunity"}:
            opportunity_rows.append(normalized)
    for item in fusion.get("active_dasha_signals") or []:
        dasha_activation["signals"].append(_normalize_prediction_activation_signal(item))
    confidence_notes.extend(fusion.get("confidence_notes") or [])


def _normalize_prediction_fusion_signal(item):
    return {
        "key": f"prediction_fusion:{str(item.get('domain') or 'general')}:{str(item.get('title') or item.get('theme') or '').strip().lower().replace(' ', '_')}",
        "label": item.get("title") or item.get("theme") or "Timing context",
        "explanation": item.get("interpretation_hint") or "",
        "summary": item.get("interpretation_hint") or "",
        "strength": {"low": 1.8, "medium": 2.8, "high": 3.8}.get(str(item.get("strength") or "medium").lower(), 2.8),
        "categories": [item.get("domain") or "general", "timing"],
        "tone": "risk" if item.get("timing_type") in {"pressure", "review"} else "opportunity",
        "source": "prediction_fusion",
        "safe_language_note": item.get("safe_language_note") or "",
    }


def _normalize_prediction_activation_signal(item):
    domain = item.get("domain") or "general"
    title = item.get("title") or item.get("theme") or "Timing activation"
    return {
        "key": f"prediction_activation:{domain}:{str(title).strip().lower().replace(' ', '_')}",
        "label": title,
        "explanation": item.get("interpretation_hint") or "",
        "summary": item.get("interpretation_hint") or "",
        "strength": {"low": 1.6, "medium": 2.6, "high": 3.6}.get(str(item.get("strength") or "medium").lower(), 2.6),
        "categories": [domain, "timing"],
        "tone": "opportunity",
        "source": "prediction_fusion",
    }


def _build_yoga_signals(natal_data, navamsa_data, *, report_type="birth_chart_karma", chart_relationships=None):
    if build_yoga_signal_bundle is None:
        return {
            "signals": [],
            "source_count": 0,
            "confidence_notes": ["Dedicated yoga signal engine is unavailable; enrichment falls back without yoga profiling."],
        }
    try:
        bundle = build_yoga_signal_bundle(
            natal_data,
            navamsa_data=navamsa_data,
            report_type=report_type,
            chart_relationships=chart_relationships,
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "signals": [],
            "source_count": 0,
            "confidence_notes": [f"Yoga signal engine failed safely: {exc}"],
        }
    signals = [_normalize_yoga_signal(item) for item in (bundle.get("signals") or bundle.get("dominant_signals") or [])]
    return {
        **bundle,
        "signals": signals,
        "source_count": len(signals),
    }


def _build_atmakaraka_signals(natal_data, *, chart_relationships=None):
    if build_atmakaraka_signal is None:
        return {
            "signals": [],
            "confidence_notes": ["Atmakaraka signal engine is unavailable; soul-direction enrichment is skipped."],
        }
    try:
        return build_atmakaraka_signal(natal_data, chart_relationships=chart_relationships)
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "signals": [],
            "confidence_notes": [f"Atmakaraka signal engine failed safely: {exc}"],
        }


def _build_transit_trigger_signals(transit_context, *, dasha_signal_bundle=None, astro_signal_context=None, chart_relationships=None):
    if build_transit_trigger_bundle is None:
        return {
            "signals": [],
            "confidence_notes": ["Transit trigger engine is unavailable; delivery triggers are skipped."],
        }
    try:
        bundle = build_transit_trigger_bundle(
            transit_context,
            dasha_signal_bundle=dasha_signal_bundle,
            astro_signal_context=astro_signal_context,
            chart_relationships=chart_relationships,
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "signals": [],
            "confidence_notes": [f"Transit trigger engine failed safely: {exc}"],
        }
    bundle["signals"] = [signal for signal in (bundle.get("signals") or []) if signal.get("target_signal_key")]
    return bundle


def build_parent_child_interaction_signals(container, *, report_type):
    return _build_parent_child_interaction_signals(container, report_type=report_type)


def _build_parent_child_interaction_signals(transit_context, *, report_type="birth_chart_karma"):
    if str(report_type or "").strip().lower() != "parent_child":
        return {"source": "parent_child_interaction_engine", "interaction_patterns": [], "confidence_notes": []}
    if build_parent_child_interaction_bundle is None:
        return {
            "source": "parent_child_interaction_engine",
            "interaction_patterns": [],
            "confidence_notes": ["Parent-child interaction engine is unavailable; dynamic relationship modeling is skipped."],
        }
    container = transit_context or {}
    parent_context = container.get("parent_astro_signal_context") or container.get("parent_profile_signals") or {}
    child_context = container.get("child_astro_signal_context") or container.get("child_profile_signals") or {}
    if not parent_context or not child_context:
        return {
            "source": "parent_child_interaction_engine",
            "interaction_patterns": [],
            "confidence_notes": [],
        }
    language = str(
        container.get("language")
        or (parent_context.get("language") if isinstance(parent_context, dict) else "")
        or (child_context.get("language") if isinstance(child_context, dict) else "")
        or "en"
    ).strip().lower()
    try:
        return build_parent_child_interaction_bundle(parent_context, child_context, report_type=report_type, language=language)
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "source": "parent_child_interaction_engine",
            "interaction_patterns": [],
            "confidence_notes": [f"Parent-child interaction engine failed safely: {exc}"],
        }


def _build_dasha_activation_signals(dasha_data, transit_context, *, nakshatra_signals=None, yoga_signals=None):
    active_period = _pick_active_dasha_period(dasha_data)
    if not active_period:
        return {
            "signals": [],
            "active_period": None,
            "source_count": 0,
            "dasha_signal_bundle": {
                "active_nakshatra_patterns": [],
                "amplified_signals": [],
                "suppressed_signals": [],
                "karmic_direction": "",
            },
        }

    planet = str(active_period.get("planet") or "").strip()
    active_triads = list(DASHA_NAKSHATRA_TRIADS.get(planet, ()))
    base_signals = list((nakshatra_signals or {}).get("signals") or [])
    active_nakshatra_patterns = []
    amplified_signals = []
    active_domains = set()
    confidence_notes = []

    for signal in base_signals:
        nakshatra_name = str(((signal.get("nakshatra_profile") or {}).get("nakshatra") or "")).strip()
        if nakshatra_name not in active_triads:
            continue
        active_nakshatra_patterns.append(
            {
                "planet": signal.get("planet"),
                "label": signal.get("label"),
                "nakshatra": nakshatra_name,
                "core_action": (signal.get("nakshatra_profile") or {}).get("core_action"),
                "dependency": (signal.get("nakshatra_profile") or {}).get("dependency"),
                "output": (signal.get("nakshatra_profile") or {}).get("output"),
            }
        )
        active_domains.update(signal.get("categories") or [])
        amplified = dict(signal)
        amplified["source"] = "dasha_nakshatra_activation"
        amplified["strength"] = _amplified_strength(signal, planet, transit_context, yoga_signals)
        amplified["keywords"] = sorted(set((signal.get("keywords") or []) + [planet.lower(), "dasha_nakshatra_activation"]))
        amplified["time_window"] = {
            "start": active_period.get("start"),
            "end": active_period.get("end"),
        }
        amplified["explanation"] = (
            f"{planet} dasha activates the {nakshatra_name} triad, so this nakshatra pattern becomes the primary behavioral lens. "
            f"Core action is amplified through {((signal.get('nakshatra_profile') or {}).get('core_action') or 'the active pattern')}."
        )
        amplified_signals.append(amplified)

    suppressed_signals = []
    if active_domains:
        for signal in base_signals:
            nakshatra_name = str(((signal.get("nakshatra_profile") or {}).get("nakshatra") or "")).strip()
            if nakshatra_name in active_triads:
                continue
            if set(signal.get("categories") or []) & active_domains:
                suppressed_signals.append(
                    {
                        "key": signal.get("key"),
                        "label": signal.get("label"),
                        "planet": signal.get("planet"),
                        "nakshatra": nakshatra_name,
                        "reason": f"{planet} dasha prioritizes the active nakshatra triad over parallel patterns in the same domain.",
                    }
                )

    if not active_triads:
        confidence_notes.append(f"No nakshatra triad mapping is defined for {planet} dasha.")
    elif not active_nakshatra_patterns:
        confidence_notes.append(
            f"{planet} dasha triad {', '.join(active_triads)} is known, but no matching nakshatra signals were available in the current natal payload."
        )

    descriptor = DASHA_PLANET_SIGNAL_MAP.get(
        planet,
        {"categories": ["timing", "life_direction"], "tone": "opportunity", "keywords": [planet.lower() or "dasha"]},
    )
    karmic_direction = _build_karmic_direction(planet, active_nakshatra_patterns)
    signal = {
        "key": f"dasha_nakshatra:{planet.lower() or 'active'}",
        "label": f"{planet} dasha nakshatra activation" if planet else "Dasha nakshatra activation",
        "source": "dasha_nakshatra_activation",
        "categories": sorted(set(list(descriptor["categories"]) + ["dasha_activation", "timing"] + list(active_domains))),
        "tone": descriptor["tone"],
        "keywords": list(dict.fromkeys(list(descriptor["keywords"]) + [item["nakshatra"] for item in active_nakshatra_patterns])),
        "strength": 2.0 if not active_nakshatra_patterns else 3.2,
        "time_window": {
            "start": active_period.get("start"),
            "end": active_period.get("end"),
        },
        "explanation": (
            f"{planet} dasha should be read through its nakshatra triad activation, not generic planetary meaning. "
            f"Active patterns: {', '.join(item['nakshatra'] for item in active_nakshatra_patterns) or 'no direct natal triad matches available'}."
        ),
    }
    return {
        "signals": [signal] + amplified_signals,
        "active_period": active_period,
        "source_count": 1 + len(amplified_signals),
        "confidence_notes": confidence_notes,
        "dasha_signal_bundle": {
            "active_nakshatra_patterns": active_nakshatra_patterns,
            "amplified_signals": [
                {"key": item.get("key"), "label": item.get("label"), "strength": item.get("strength"), "source": item.get("source")}
                for item in amplified_signals
            ],
            "suppressed_signals": suppressed_signals,
            "karmic_direction": karmic_direction,
            "active_triads": active_triads,
            "dasha_lord": planet,
        },
    }


def _build_chart_relationships(natal_data, navamsa_data):
    if build_chart_relationships is None:
        return {"confidence_notes": ["Chart relationship layer is unavailable."], "source": "chart_relationships"}
    try:
        context = build_chart_relationships(natal_data, navamsa_data)
    except Exception as exc:  # pragma: no cover - defensive guard
        return {"confidence_notes": [f"Chart relationship layer failed safely: {exc}"], "source": "chart_relationships"}
    return {
        "source": "chart_relationships",
        **context,
    }


def _merge_dominant_signal(aggregate, signal, source):
    key = str(signal.get("key") or "").strip()
    if not key:
        return
    existing = aggregate.get(key)
    if existing is None:
        aggregate[key] = {
            **signal,
            "strength": float(signal.get("strength") or 0.0),
            "sources": [source],
        }
        return
    existing["strength"] = merge_signal_strength(existing.get("strength"), signal.get("strength"))
    existing["sources"] = sorted(set(existing.get("sources") or []) | {source})
    existing["categories"] = sorted(set(existing.get("categories") or []) | set(signal.get("categories") or []))
    existing["keywords"] = sorted(set(existing.get("keywords") or []) | set(signal.get("keywords") or []))
    if signal.get("tone") == "risk":
        existing["tone"] = "risk"


def _build_report_type_signal_map(dominant_signals):
    report_map = {}
    for category_set in REPORT_TYPE_FOCUS.values():
        for category in category_set:
            report_map.setdefault(category, [])
    for signal in dominant_signals:
        for category in signal.get("categories") or []:
            if category in report_map:
                report_map[category].append(signal)
    return report_map


def _build_confidence_notes(natal_data, navamsa_data, dasha_data, transit_context):
    notes = []
    if not natal_data or not natal_data.get("planets"):
        notes.append("Natal signal layer is limited because natal_data is missing planet rows.")
    if not navamsa_data:
        notes.append("Navamsa-based refinement is unavailable; relationship/spiritual enrichment stays conservative.")
    if not dasha_data:
        notes.append("Dasha activation enrichment is unavailable; timing confidence relies on the existing transit context only.")
    if transit_context and not any(transit_context.get(key) for key in ("major_peak_windows", "trigger_periods", "opportunity_windows", "pressure_windows")):
        notes.append("Transit context is present but does not expose major timing windows for enrichment.")
    return notes


def _amplified_strength(signal, dasha_planet, transit_context, yoga_signals):
    base_strength = float(signal.get("strength") or 0.0)
    strength = merge_signal_strength(base_strength, 1.2)
    if transit_context.get("major_peak_windows") or transit_context.get("trigger_periods"):
        strength = merge_signal_strength(strength, 0.6)
    if _signal_overlaps_yoga(signal, yoga_signals):
        strength = merge_signal_strength(strength, 0.7)
    if str(signal.get("planet") or "").strip() == dasha_planet:
        strength = merge_signal_strength(strength, 0.5)
    return strength


def _signal_overlaps_yoga(signal, yoga_signals):
    signal_categories = set(signal.get("categories") or [])
    signal_usage = set(signal.get("report_usage") or [])
    for yoga_signal in (yoga_signals or {}).get("signals") or []:
        if signal_categories & set(yoga_signal.get("categories") or []):
            return True
        if signal_usage & set(yoga_signal.get("report_usage") or []):
            return True
    return False


def _build_karmic_direction(dasha_planet, active_nakshatra_patterns):
    if not active_nakshatra_patterns:
        return f"{dasha_planet} dasha is active, but no natal nakshatra triad pattern could be amplified safely from the current payload."
    actions = [item.get("core_action") for item in active_nakshatra_patterns if item.get("core_action")]
    outputs = [item.get("output") for item in active_nakshatra_patterns if item.get("output")]
    return (
        f"{dasha_planet} dasha amplifies nakshatra-level action patterns first: "
        f"{'; '.join(actions[:3])}. The karmic direction leans toward {', '.join(outputs[:2]) or 'the active nakshatra outputs'}."
    )


def _normalize_nakshatra_signal(item):
    usage = list(item.get("report_usage") or [])
    categories = []
    for value in usage:
        normalized = str(value or "").strip().lower().replace(" ", "_")
        if normalized in REPORT_TYPE_FOCUS["birth_chart_karma"] | REPORT_TYPE_FOCUS["annual_transit"] | REPORT_TYPE_FOCUS["career"] | REPORT_TYPE_FOCUS["parent_child"]:
            categories.append(normalized)
    if item.get("domain") == "emotional_pattern":
        categories.extend(["emotional", "emotional_needs"])
    elif item.get("domain") == "identity_style":
        categories.extend(["identity", "life_direction"])
    elif item.get("domain") == "communication_pattern":
        categories.extend(["communication", "learning_style"])
    elif item.get("domain") == "discipline_pattern":
        categories.extend(["discipline", "discipline_response", "risk"])
    elif item.get("domain") == "relationship_pattern":
        categories.extend(["relationship", "support_strategy"])
    elif item.get("domain") == "guidance_pattern":
        categories.extend(["spiritual", "career", "support_strategy"])
    elif item.get("domain") == "desire_pattern":
        categories.extend(["risk", "life_direction", "visibility"])
    elif item.get("domain") == "release_pattern":
        categories.extend(["karmic", "spiritual", "timing"])
    elif item.get("domain") == "authority_pattern":
        categories.extend(["authority", "visibility", "career"])
    elif item.get("domain") == "action_pattern":
        categories.extend(["risk", "discipline", "parent_child_friction"])
    tone = "risk" if any(tag in {"risk", "pressure", "conflict"} for tag in usage) else "opportunity"
    strength = {"low": 1.0, "medium": 2.0, "high": 3.0, "very_high": 4.0}.get(str(item.get("strength") or "").lower(), 2.0)
    return {
        "key": item.get("key"),
        "label": item.get("label"),
        "source": "nakshatra",
        "planet": item.get("planet"),
        "categories": sorted(set(categories)),
        "tone": tone,
        "keywords": [item.get("domain"), item.get("planet"), item.get("nakshatra_profile", {}).get("nakshatra")],
        "strength": strength,
        "explanation": item.get("explanation"),
        "report_usage": usage,
        "nakshatra_profile": deepcopy(item.get("nakshatra_profile") or {}),
    }


def _normalize_yoga_signal(item):
    usage = list(item.get("report_usage") or [])
    categories = []
    for value in usage:
        normalized = str(value or "").strip().lower().replace(" ", "_")
        if normalized in REPORT_TYPE_FOCUS["birth_chart_karma"] | REPORT_TYPE_FOCUS["annual_transit"] | REPORT_TYPE_FOCUS["career"] | REPORT_TYPE_FOCUS["parent_child"]:
            categories.append(normalized)
    domain = str(item.get("domain") or "").strip().lower().replace(" ", "_")
    if domain == "guidance":
        categories.extend(["spiritual", "support_strategy", "career"])
    elif domain == "discipline":
        categories.extend(["discipline", "risk", "discipline_response"])
    elif domain == "communication":
        categories.extend(["communication", "visibility", "career"])
    elif domain == "relationship":
        categories.extend(["relationship", "support_strategy"])
    elif domain == "visibility":
        categories.extend(["visibility", "career", "authority"])
    elif domain == "wealth":
        categories.extend(["wealth", "career", "opportunity"])
    elif domain == "emotional":
        categories.extend(["emotional", "risk", "emotional_needs"])
    elif domain == "authority":
        categories.extend(["authority", "career", "visibility"])
    elif domain == "interiority":
        categories.extend(["spiritual", "timing"])
    elif domain == "resourcefulness":
        categories.extend(["wealth", "support_strategy"])
    elif domain == "risk":
        categories.extend(["risk", "timing"])
    tone = "risk" if any(tag in {"risk", "discipline"} for tag in usage) or domain == "risk" else "opportunity"
    strength = {"low": 1.0, "medium": 2.0, "high": 3.0, "very_high": 4.0}.get(str(item.get("strength") or "").lower(), 2.0)
    return {
        "key": item.get("key"),
        "label": item.get("label"),
        "source": "yoga",
        "categories": sorted(set(categories)),
        "tone": tone,
        "keywords": [item.get("domain"), item.get("label")],
        "strength": strength,
        "explanation": item.get("explanation"),
        "report_usage": usage,
    }


def _pick_active_dasha_period(dasha_data):
    today = date.today()
    active = None
    for row in dasha_data or []:
        start_date = _parse_date(row.get("start"))
        end_date = _parse_date(row.get("end"))
        if start_date and end_date and start_date <= today <= end_date:
            active = row
            break
    return active or (dasha_data[0] if dasha_data else None)


def _parse_date(value):
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _dedupe_signal_rows(signals):
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for signal in signals:
        row_key = (str(signal.get("key") or ""), str(signal.get("planet") or ""))
        existing = deduped.get(row_key)
        if existing is None:
            deduped[row_key] = dict(signal)
            continue
        existing["strength"] = merge_signal_strength(existing.get("strength"), signal.get("strength"))
        existing["keywords"] = sorted(set(existing.get("keywords") or []) | set(signal.get("keywords") or []))
    return list(deduped.values())
