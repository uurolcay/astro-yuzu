from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime


NOT_FATED_NOTE = "This indicates an active tendency, not a guaranteed outcome."

DOMAIN_CATEGORY_MAP = {
    "career": {"career", "authority", "visibility", "discipline", "wealth"},
    "relationship": {"relationship", "emotional_needs", "support_strategy"},
    "identity": {"identity", "karmic", "life_direction", "spiritual"},
    "timing": {"timing", "risk", "opportunity"},
    "parent_child": {"parent_child", "discipline_response", "communication"},
}

REPORT_TYPE_PRIMARY_DOMAINS = {
    "career": ("career", "timing"),
    "annual_transit": ("timing", "career", "relationship"),
    "birth_chart_karma": ("identity", "relationship"),
    "parent_child": ("parent_child", "relationship"),
}

RISK_SCALE = ("low", "medium", "high", "very_high")
OPPORTUNITY_SCALE = ("low", "medium", "high", "very_high")

FUSION_DOMAIN_MAP = {
    "career": "career",
    "wealth": "money",
    "money": "money",
    "relationship": "relationship",
    "emotional_needs": "family",
    "parent_child": "family",
    "family": "family",
    "health": "health_safe",
    "health_safe": "health_safe",
    "spiritual": "spiritual",
    "karmic": "spiritual",
    "timing": "general",
    "risk": "general",
    "opportunity": "general",
}


def build_prediction_fusion(
    astro_signal_context=None,
    dasha_signal_bundle=None,
    transit_trigger_bundle=None,
    chart_relationships=None,
    report_type="birth_chart_karma",
    language="en",
):
    try:
        return _build_prediction_fusion(
            astro_signal_context=astro_signal_context,
            dasha_signal_bundle=dasha_signal_bundle,
            transit_trigger_bundle=transit_trigger_bundle,
            chart_relationships=chart_relationships,
            report_type=report_type,
            language=language,
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "source": "prediction_fusion_engine",
            "prediction_windows": [],
            "active_themes": [],
            "blocked_predictions": [],
            "unconfirmed_observations": [],
            "confidence_notes": [f"Prediction fusion failed safely: {exc}"],
        }


def _build_prediction_fusion(
    astro_signal_context=None,
    dasha_signal_bundle=None,
    transit_trigger_bundle=None,
    chart_relationships=None,
    report_type="birth_chart_karma",
    language="en",
):
    astro_signal_context = deepcopy(astro_signal_context or {})
    dasha_signal_bundle = deepcopy(dasha_signal_bundle or {})
    transit_trigger_bundle = deepcopy(transit_trigger_bundle or {})
    chart_relationships = deepcopy(chart_relationships or {})
    report_type = _normalize_report_type(report_type)
    language = "tr" if str(language or "").strip().lower() == "tr" else "en"

    empty = {
        "source": "prediction_fusion_engine",
        "prediction_windows": [],
        "active_themes": [],
        "blocked_predictions": [],
        "unconfirmed_observations": [],
        "confidence_notes": [],
    }

    dominant_signals = list(astro_signal_context.get("dominant_signals") or [])
    risk_signals = list(astro_signal_context.get("risk_signals") or [])
    opportunity_signals = list(astro_signal_context.get("opportunity_signals") or [])
    yoga_signals = list((astro_signal_context.get("yoga_signals") or {}).get("signals") or [])
    atmakaraka_signals = list((astro_signal_context.get("atmakaraka_signals") or {}).get("signals") or [])
    dasha_active_planet = str(dasha_signal_bundle.get("dasha_lord") or "").strip()
    dasha_amplified = list(dasha_signal_bundle.get("amplified_signals") or [])
    transit_triggers = list(transit_trigger_bundle.get("transit_triggers") or [])
    blocked_events = list(transit_trigger_bundle.get("blocked_events") or [])

    if not any([dominant_signals, dasha_active_planet, dasha_amplified, transit_triggers, blocked_events]):
        empty["confidence_notes"].append(_message("missing_all", language))
        return empty

    dasha_domains = _dasha_supported_domains(dasha_signal_bundle, dominant_signals)
    transit_domains = _transit_supported_domains(transit_triggers, dominant_signals)
    yoga_domains = _signal_domains(yoga_signals)
    atmakaraka_domains = _signal_domains(atmakaraka_signals)
    primary_domains = REPORT_TYPE_PRIMARY_DOMAINS.get(report_type, REPORT_TYPE_PRIMARY_DOMAINS["birth_chart_karma"])

    prediction_windows = []
    active_themes = []
    blocked_predictions = []
    unconfirmed_observations = []
    confidence_notes = []

    for signal in dominant_signals:
        signal_key = str(signal.get("key") or "").strip()
        if not signal_key:
            continue
        signal_domains = _domains_for_signal(signal)
        matched_primary = [domain for domain in primary_domains if domain in signal_domains]
        if not matched_primary:
            continue
        domain = matched_primary[0]
        dasha_supported = _is_dasha_supported(signal, domain, dasha_signal_bundle, dasha_domains)
        transit_support = _matching_transit_support(signal, domain, transit_triggers, dominant_signals)

        if dasha_supported and transit_support:
            prediction_windows.append(
                _build_prediction_item(
                    signal=signal,
                    domain=domain,
                    dasha_signal_bundle=dasha_signal_bundle,
                    transit_support=transit_support,
                    yoga_domains=yoga_domains,
                    atmakaraka_domains=atmakaraka_domains,
                    risk_signals=risk_signals,
                    opportunity_signals=opportunity_signals,
                    chart_relationships=chart_relationships,
                )
            )
            continue

        if dasha_supported and not transit_support:
            active_themes.append(
                _build_active_theme_item(
                    signal=signal,
                    domain=domain,
                    dasha_signal_bundle=dasha_signal_bundle,
                )
            )
            continue

        if transit_support and not dasha_supported:
            blocked_predictions.append(
                {
                    "theme": signal.get("label") or signal_key,
                    "domain": domain,
                    "delivery_source": transit_support.get("planet"),
                    "target_signal_key": signal_key,
                    "reason": _message("blocked_prediction", language),
                    "confidence": "low",
                }
            )
            continue

        unconfirmed_observations.append(
            {
                "theme": signal.get("label") or signal_key,
                "domain": domain,
                "supporting_signals": [signal_key],
                "confidence": "low",
                "note": _message("unconfirmed_signal", language),
            }
        )

    if blocked_events and not blocked_predictions:
        for event in blocked_events:
            blocked_predictions.append(
                {
                    "theme": event.get("planet") or "transit",
                    "domain": _domain_for_blocked_event(event),
                    "delivery_source": event.get("planet"),
                    "target_signal_key": "",
                    "reason": _message("blocked_prediction", language),
                    "confidence": "low",
                }
            )

    if atmakaraka_domains:
        for domain in sorted(atmakaraka_domains & set(primary_domains)):
            confidence_notes.append(_message("atmakaraka_boost", language, domain=domain))

    if not prediction_windows and not active_themes and not blocked_predictions and not unconfirmed_observations:
        confidence_notes.append(_message("missing_required_links", language))

    return {
        "source": "prediction_fusion_engine",
        "prediction_windows": prediction_windows,
        "active_themes": active_themes,
        "blocked_predictions": blocked_predictions,
        "unconfirmed_observations": unconfirmed_observations,
        "confidence_notes": confidence_notes,
    }


def _build_prediction_item(
    *,
    signal,
    domain,
    dasha_signal_bundle,
    transit_support,
    yoga_domains,
    atmakaraka_domains,
    risk_signals,
    opportunity_signals,
    chart_relationships,
):
    signal_key = str(signal.get("key") or "").strip()
    dasha_planet = str(dasha_signal_bundle.get("dasha_lord") or "").strip() or "active"
    yoga_boosted = domain in yoga_domains
    atmakaraka_boosted = domain in atmakaraka_domains
    risk_level = _base_level_for_signal(signal, risk_signals, scale=RISK_SCALE)
    opportunity_level = _base_level_for_signal(signal, opportunity_signals, scale=OPPORTUNITY_SCALE)
    if yoga_boosted:
        opportunity_level = _raise_level(opportunity_level, OPPORTUNITY_SCALE)
    confidence = _confidence_for_prediction(signal, transit_support)
    timing_window = _format_timing_window(dasha_signal_bundle, transit_support)
    theme = signal.get("label") or signal_key
    supporting_signals = sorted(
        {
            signal_key,
            *(key for key in _dasha_signal_keys(dasha_signal_bundle) if key),
            *(key for key in [transit_support.get("target_signal_key")] if key),
        }
    )
    return {
        "prediction_id": f"{domain}:{signal_key}:{dasha_planet}",
        "theme": theme,
        "domain": domain,
        "timing_window": timing_window,
        "activation_source": dasha_planet,
        "delivery_source": transit_support.get("planet") or "",
        "supporting_signals": supporting_signals,
        "yoga_boosted": yoga_boosted,
        "atmakaraka_boosted": atmakaraka_boosted,
        "risk_level": risk_level,
        "opportunity_level": opportunity_level,
        "confidence": confidence,
        "why_now": _why_now_text(signal, dasha_planet, transit_support),
        "recommended_action": _recommended_action(domain, signal, chart_relationships),
        "avoid_action": _avoid_action(domain, signal),
        "not_fated_note": NOT_FATED_NOTE,
    }


def _build_active_theme_item(*, signal, domain, dasha_signal_bundle):
    signal_key = str(signal.get("key") or "").strip()
    dasha_planet = str(dasha_signal_bundle.get("dasha_lord") or "").strip() or "active"
    return {
        "theme": signal.get("label") or signal_key,
        "domain": domain,
        "activation_source": dasha_planet,
        "supporting_signals": sorted({signal_key, *[key for key in _dasha_signal_keys(dasha_signal_bundle) if key]}),
        "confidence": _active_theme_confidence(signal),
        "why_now": (
            f"{dasha_planet} dasha is activating this theme, but no transit delivery trigger is confirming a tighter action window yet."
        ),
        "not_fated_note": NOT_FATED_NOTE,
    }


def _normalize_report_type(value):
    normalized = str(value or "birth_chart_karma").strip().lower().replace("-", "_")
    return normalized if normalized in REPORT_TYPE_PRIMARY_DOMAINS else "birth_chart_karma"


def _domains_for_signal(signal):
    categories = set(signal.get("categories") or [])
    domains = {domain for domain, mapped in DOMAIN_CATEGORY_MAP.items() if categories & mapped}
    if not domains and "parent_child" in categories:
        domains.add("parent_child")
    return domains


def _signal_domains(signals):
    domains = set()
    for signal in signals or []:
        domains.update(_domains_for_signal(signal))
    return domains


def _dasha_supported_domains(dasha_signal_bundle, dominant_signals):
    domains = set()
    amplified_keys = {item.get("key") for item in (dasha_signal_bundle.get("amplified_signals") or []) if item.get("key")}
    active_planet = str(dasha_signal_bundle.get("dasha_lord") or "").strip()
    active_patterns = list(dasha_signal_bundle.get("active_nakshatra_patterns") or [])
    for signal in dominant_signals or []:
        if signal.get("key") in amplified_keys:
            domains.update(_domains_for_signal(signal))
        if active_planet and str(signal.get("planet") or "").strip() == active_planet:
            domains.update(_domains_for_signal(signal))
    for pattern in active_patterns:
        planet = str(pattern.get("planet") or "").strip()
        for signal in dominant_signals or []:
            if planet and str(signal.get("planet") or "").strip() == planet:
                domains.update(_domains_for_signal(signal))
    return domains


def _transit_supported_domains(transit_triggers, dominant_signals):
    domains = set()
    lookup = {str(signal.get("key") or "").strip(): signal for signal in dominant_signals or []}
    for trigger in transit_triggers or []:
        target = lookup.get(str(trigger.get("target_signal_key") or "").strip())
        if target:
            domains.update(_domains_for_signal(target))
            continue
        planet = str(trigger.get("planet") or "").strip()
        for signal in dominant_signals or []:
            if planet and str(signal.get("planet") or "").strip() == planet:
                domains.update(_domains_for_signal(signal))
    return domains


def _is_dasha_supported(signal, domain, dasha_signal_bundle, dasha_domains):
    if domain not in dasha_domains:
        return False
    signal_key = str(signal.get("key") or "").strip()
    if signal_key and signal_key in _dasha_signal_keys(dasha_signal_bundle):
        return True
    dasha_planet = str(dasha_signal_bundle.get("dasha_lord") or "").strip()
    if dasha_planet and str(signal.get("planet") or "").strip() == dasha_planet:
        return True
    return domain in dasha_domains


def _matching_transit_support(signal, domain, transit_triggers, dominant_signals):
    signal_key = str(signal.get("key") or "").strip()
    for trigger in transit_triggers or []:
        if str(trigger.get("target_signal_key") or "").strip() == signal_key:
            return trigger
    for trigger in transit_triggers or []:
        target_domain = _domain_for_trigger(trigger, dominant_signals)
        if target_domain == domain:
            return trigger
    return None


def _domain_for_trigger(trigger, dominant_signals):
    target_key = str(trigger.get("target_signal_key") or "").strip()
    for signal in dominant_signals or []:
        if str(signal.get("key") or "").strip() == target_key:
            domains = _domains_for_signal(signal)
            if domains:
                return sorted(domains)[0]
    categories = set()
    planet = str(trigger.get("planet") or "").strip()
    for signal in dominant_signals or []:
        if planet and str(signal.get("planet") or "").strip() == planet:
            categories.update(signal.get("categories") or [])
    for domain, mapped in DOMAIN_CATEGORY_MAP.items():
        if categories & mapped:
            return domain
    return "timing"


def _domain_for_blocked_event(event):
    planet = str(event.get("planet") or "").strip()
    if planet in {"Jupiter", "Saturn", "Rahu", "Ketu"}:
        return "timing"
    if planet in {"Venus", "Moon"}:
        return "relationship"
    if planet in {"Mercury"}:
        return "parent_child"
    return "career"


def _dasha_signal_keys(dasha_signal_bundle):
    return {item.get("key") for item in (dasha_signal_bundle.get("amplified_signals") or []) if item.get("key")}


def _base_level_for_signal(signal, compared_signals, *, scale):
    strength = float(signal.get("strength") or 0.0)
    related_keys = {str(item.get("key") or "").strip() for item in compared_signals or []}
    if str(signal.get("key") or "").strip() in related_keys:
        strength += 0.8
    if strength >= 4.5:
        return scale[3]
    if strength >= 3.2:
        return scale[2]
    if strength >= 1.6:
        return scale[1]
    return scale[0]


def _raise_level(level, scale):
    try:
        index = scale.index(level)
    except ValueError:
        return scale[0]
    return scale[min(index + 1, len(scale) - 1)]


def _confidence_for_prediction(signal, transit_support):
    strength = float(signal.get("strength") or 0.0)
    if transit_support.get("confidence") == "high" and strength >= 3.0:
        return "high"
    return "medium"


def _active_theme_confidence(signal):
    return "medium" if float(signal.get("strength") or 0.0) >= 2.5 else "low"


def _format_timing_window(dasha_signal_bundle, transit_support):
    start = _parse_date((dasha_signal_bundle.get("active_period") or {}).get("start"))
    end = _parse_date((dasha_signal_bundle.get("active_period") or {}).get("end"))
    if start and end:
        return f"{start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}"
    duration = str(transit_support.get("duration") or "").strip()
    return duration or "active window"


def _parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _why_now_text(signal, dasha_planet, transit_support):
    return (
        f"{dasha_planet} dasha is activating {signal.get('label') or signal.get('key')}, "
        f"and {transit_support.get('planet') or 'the current transit'} is providing the delivery trigger now."
    )


def _recommended_action(domain, signal, chart_relationships):
    if domain == "career":
        return "Prioritize concrete planning, visible execution, and measured professional follow-through."
    if domain == "relationship":
        return "Use clear emotional communication and make room for deliberate relational repair or deepening."
    if domain == "identity":
        return "Choose actions that align with long-range values rather than short-term emotional relief."
    if domain == "parent_child":
        moon_houses = chart_relationships.get("moon_relative_houses") or {}
        if moon_houses:
            return "Slow the pace, clarify expectations, and support regulation before correction."
        return "Use steady guidance, simple communication, and repeatable routines."
    return "Act on the theme in small, observable steps while timing conditions are active."


def _avoid_action(domain, signal):
    if domain == "career":
        return "Avoid reactive commitments, status-driven decisions, or forcing outcomes without structure."
    if domain == "relationship":
        return "Avoid emotional overreaction, mixed signals, or avoidant silence."
    if domain == "identity":
        return "Avoid making final conclusions from temporary pressure states."
    if domain == "parent_child":
        return "Avoid escalating pressure before understanding the response pattern."
    return "Avoid treating background pressure as certainty."


def _message(key, language, **kwargs):
    messages = {
        "missing_all": {
            "en": "Prediction fusion stayed empty because no dominant signals, dasha activation, or transit triggers were available.",
            "tr": "Baskın sinyal, dasha aktivasyonu veya transit tetikleyici bulunmadığı için tahmin füzyonu boş kaldı.",
        },
        "blocked_prediction": {
            "en": "Transit support is visible, but dasha activation is missing, so this cannot be treated as a confirmed delivery window.",
            "tr": "Transit desteği görünür durumda; ancak dasha aktivasyonu olmadığı için bu durum doğrulanmış bir teslim penceresi sayılamaz.",
        },
        "unconfirmed_signal": {
            "en": "A signal exists, but neither dasha activation nor transit delivery is supporting a prediction window yet.",
            "tr": "Bir sinyal mevcut; ancak henüz ne dasha aktivasyonu ne de transit teslimi bir öngörü penceresini destekliyor.",
        },
        "atmakaraka_boost": {
            "en": "Atmakaraka emphasis also touches the {domain} domain, so the soul-direction layer reinforces its relevance.",
            "tr": "Atmakaraka vurgusu {domain} alanına da temas ediyor; bu nedenle ruhsal yön katmanı bu alanın önemini güçlendiriyor.",
        },
        "missing_required_links": {
            "en": "Signals were present, but the required natal-dasha-transit links were not strong enough to build a fused prediction.",
            "tr": "Sinyaller mevcut olsa da birleşik bir öngörü kurmak için gereken natal-dasha-transit bağlantıları yeterince güçlü değildi.",
        },
    }
    return messages[key][language].format(**kwargs)


def build_prediction_fusion_context(
    natal_data=None,
    dasha_data=None,
    transit_data=None,
    signal_context=None,
    report_type=None,
    language="tr",
):
    try:
        return _build_prediction_fusion_context(
            natal_data=natal_data,
            dasha_data=dasha_data,
            transit_data=transit_data,
            signal_context=signal_context,
            report_type=report_type,
            language=language,
        )
    except Exception:  # pragma: no cover - defensive guard
        return _empty_prediction_fusion_context(language=language)


def _build_prediction_fusion_context(
    *,
    natal_data=None,
    dasha_data=None,
    transit_data=None,
    signal_context=None,
    report_type=None,
    language="tr",
):
    language = "tr" if str(language or "").strip().lower() == "tr" else "en"
    context = deepcopy(signal_context or {})
    natal_data = deepcopy(natal_data or {})
    dasha_data = deepcopy(dasha_data or {})
    transit_data = deepcopy(transit_data or {})
    normalized_report_type = _normalize_report_type(report_type)
    empty = _empty_prediction_fusion_context(language=language)

    dasha_bundle = context.get("dasha_signal_bundle") or _coerce_dasha_bundle(dasha_data)
    transit_bundle = context.get("transit_trigger_signals") or _coerce_transit_bundle(transit_data)
    dominant_signals = list(context.get("dominant_signals") or [])
    risk_signals = list(context.get("risk_signals") or [])
    opportunity_signals = list(context.get("opportunity_signals") or [])

    active_dasha_signals = _build_active_dasha_signal_rows(
        dasha_bundle=dasha_bundle,
        dominant_signals=dominant_signals,
        language=language,
    )
    active_transit_signals = _build_active_transit_signal_rows(
        transit_bundle=transit_bundle,
        dominant_signals=dominant_signals,
        language=language,
    )
    fusion_signals = _build_fusion_signal_rows(
        active_dasha_signals=active_dasha_signals,
        active_transit_signals=active_transit_signals,
        risk_signals=risk_signals,
        opportunity_signals=opportunity_signals,
        report_type=normalized_report_type,
        natal_data=natal_data,
        language=language,
    )
    risk_windows = [
        _window_row(signal, language=language)
        for signal in fusion_signals
        if signal.get("timing_type") in {"pressure", "review"}
    ]
    opportunity_windows = [
        _window_row(signal, language=language)
        for signal in fusion_signals
        if signal.get("timing_type") in {"activation", "opportunity"}
    ]
    timing_summary = _build_timing_summary(
        active_dasha_signals=active_dasha_signals,
        active_transit_signals=active_transit_signals,
        fusion_signals=fusion_signals,
        language=language,
    )
    confidence_notes = _build_fusion_confidence_notes(
        dasha_bundle=dasha_bundle,
        transit_bundle=transit_bundle,
        fusion_signals=fusion_signals,
        language=language,
    )
    available = bool(
        active_dasha_signals
        or active_transit_signals
        or fusion_signals
        or risk_windows
        or opportunity_windows
    )
    report_type_focus = _build_report_type_focus(
        report_type=normalized_report_type,
        fusion_signals=fusion_signals,
        language=language,
    )

    return {
        "available": available,
        "timing_summary": timing_summary,
        "active_dasha_signals": active_dasha_signals,
        "active_transit_signals": active_transit_signals,
        "fusion_signals": fusion_signals,
        "risk_windows": risk_windows,
        "opportunity_windows": opportunity_windows,
        "confidence_notes": confidence_notes,
        "report_type_focus": report_type_focus,
    }


def _empty_prediction_fusion_context(language="tr"):
    language = "tr" if str(language or "").strip().lower() == "tr" else "en"
    return {
        "available": False,
        "timing_summary": [],
        "active_dasha_signals": [],
        "active_transit_signals": [],
        "fusion_signals": [],
        "risk_windows": [],
        "opportunity_windows": [],
        "confidence_notes": [
            (
                "Zamanlama verisi sinirli; bu nedenle tahmin katmani aktiflestirilemedi."
                if language == "tr"
                else "Timing data is limited, so the prediction layer could not be activated."
            )
        ],
        "report_type_focus": {},
    }


def _coerce_dasha_bundle(dasha_data):
    if isinstance(dasha_data, dict):
        return {
            "dasha_lord": dasha_data.get("dasha_lord")
            or dasha_data.get("planet")
            or ((dasha_data.get("active_period") or {}).get("planet")),
            "active_period": dasha_data.get("active_period") or {},
            "amplified_signals": list(dasha_data.get("amplified_signals") or []),
            "active_nakshatra_patterns": list(dasha_data.get("active_nakshatra_patterns") or []),
        }
    if isinstance(dasha_data, list) and dasha_data:
        first = dasha_data[0] or {}
        return {
            "dasha_lord": first.get("planet"),
            "active_period": {
                "planet": first.get("planet"),
                "start": first.get("start"),
                "end": first.get("end"),
            },
            "amplified_signals": [],
            "active_nakshatra_patterns": [],
        }
    return {}


def _coerce_transit_bundle(transit_data):
    if not isinstance(transit_data, dict):
        return {"transit_triggers": [], "blocked_events": []}
    triggers = list(transit_data.get("transit_triggers") or [])
    for period in list(transit_data.get("trigger_periods") or []):
        triggers.append(
            {
                "planet": period.get("planet") or period.get("title") or "Transit",
                "effect": period.get("effect") or "activation",
                "duration": _format_window(period.get("start"), period.get("end")),
                "target_signal_key": period.get("target_signal_key"),
                "title": period.get("title"),
            }
        )
    for period in list(transit_data.get("opportunity_windows") or []):
        triggers.append(
            {
                "planet": period.get("planet") or period.get("title") or "Transit",
                "effect": "opportunity",
                "duration": period.get("duration") or "",
                "title": period.get("title"),
                "target_signal_key": period.get("target_signal_key"),
            }
        )
    blocked_events = list(transit_data.get("blocked_events") or [])
    for period in list(transit_data.get("pressure_windows") or []):
        blocked_events.append(
            {
                "planet": period.get("planet") or period.get("title") or "Transit",
                "reason": period.get("title") or "pressure",
            }
        )
    return {"transit_triggers": triggers, "blocked_events": blocked_events}


def _build_active_dasha_signal_rows(*, dasha_bundle, dominant_signals, language):
    lord = str(dasha_bundle.get("dasha_lord") or "").strip()
    if not lord:
        return []
    amplified_keys = _dasha_signal_keys(dasha_bundle)
    rows = []
    for signal in dominant_signals or []:
        categories = list(signal.get("categories") or [])
        if str(signal.get("planet") or "").strip() != lord and str(signal.get("key") or "").strip() not in amplified_keys:
            continue
        rows.append(
            {
                "title": signal.get("label") or lord,
                "theme": signal.get("label") or lord,
                "domain": _fusion_domain_for_categories(categories),
                "dasha_driver": lord,
                "categories": categories,
                "strength": _strength_label(signal.get("strength")),
                "interpretation_hint": (
                    f"{lord} donemi bu temayi one cikarabilir."
                    if language == "tr"
                    else f"The {lord} period may bring this theme forward."
                ),
            }
        )
    if rows:
        return rows[:5]
    return [
        {
            "title": lord,
            "theme": lord,
            "domain": "general",
            "dasha_driver": lord,
            "categories": [],
            "strength": "medium",
            "interpretation_hint": (
                f"{lord} donemi belirli temalari aktiflestirebilir."
                if language == "tr"
                else f"The {lord} period may activate specific themes."
            ),
        }
    ]


def _build_active_transit_signal_rows(*, transit_bundle, dominant_signals, language):
    rows = []
    lookup = {str(signal.get("key") or "").strip(): signal for signal in dominant_signals or []}
    for trigger in list(transit_bundle.get("transit_triggers") or []):
        linked = lookup.get(str(trigger.get("target_signal_key") or "").strip()) or {}
        categories = list(linked.get("categories") or [])
        title = trigger.get("title") or linked.get("label") or trigger.get("planet") or "Transit"
        rows.append(
            {
                "title": title,
                "theme": linked.get("label") or title,
                "domain": _fusion_domain_for_categories(categories),
                "transit_trigger": trigger.get("planet") or title,
                "categories": categories,
                "effect": trigger.get("effect") or "activation",
                "duration": trigger.get("duration") or "",
                "interpretation_hint": (
                    f"{title} temasi bu donemde hareketlenebilir."
                    if language == "tr"
                    else f"The {title} theme may become more active in this period."
                ),
            }
        )
    return rows[:6]


def _build_fusion_signal_rows(
    *,
    active_dasha_signals,
    active_transit_signals,
    risk_signals,
    opportunity_signals,
    report_type,
    natal_data,
    language,
):
    if not active_dasha_signals or not active_transit_signals:
        return []
    fusion = []
    opportunity_keys = {str(item.get("key") or "").strip() for item in opportunity_signals or []}
    risk_keys = {str(item.get("key") or "").strip() for item in risk_signals or []}
    natal_anchor = _pick_natal_anchor(natal_data)
    for dasha_row in active_dasha_signals:
        d_categories = set(dasha_row.get("categories") or [])
        for transit_row in active_transit_signals:
            t_categories = set(transit_row.get("categories") or [])
            shared = sorted(d_categories & t_categories)
            if not shared and dasha_row.get("domain") != transit_row.get("domain"):
                continue
            domain = _resolve_fusion_domain(
                dasha_row.get("domain"),
                transit_row.get("domain"),
                report_type,
            )
            theme = dasha_row.get("theme") or transit_row.get("theme") or domain
            timing_type = _timing_type_for_rows(dasha_row, transit_row, theme, risk_keys, opportunity_keys)
            fusion.append(
                {
                    "title": theme,
                    "theme": theme,
                    "domain": domain,
                    "dasha_driver": dasha_row.get("dasha_driver"),
                    "transit_trigger": transit_row.get("transit_trigger"),
                    "natal_anchor": natal_anchor,
                    "strength": _combine_strength_labels(dasha_row.get("strength"), transit_row.get("effect")),
                    "timing_type": timing_type,
                    "interpretation_hint": _fusion_hint(
                        theme=theme,
                        domain=domain,
                        timing_type=timing_type,
                        language=language,
                    ),
                    "safe_language_note": (
                        "Bunu kesin sonuc olarak degil, aktiflesebilecek bir tema olarak okuyun."
                        if language == "tr"
                        else "Read this as an activating theme, not a guaranteed outcome."
                    ),
                }
            )
    return fusion[:6]


def _window_row(signal, *, language):
    return {
        "title": signal.get("title") or signal.get("theme") or "",
        "domain": signal.get("domain") or "general",
        "timing_type": signal.get("timing_type") or "review",
        "note": signal.get("interpretation_hint")
        or (
            "Bu tema bu donemde daha dikkatli okunmali."
            if language == "tr"
            else "This theme should be read with more care in this period."
        ),
    }


def _build_timing_summary(*, active_dasha_signals, active_transit_signals, fusion_signals, language):
    summary = []
    if active_dasha_signals:
        summary.append(
            {
                "label": "Dasha activation" if language != "tr" else "Dasha aktivasyonu",
                "value": active_dasha_signals[0].get("dasha_driver") or active_dasha_signals[0].get("theme") or "",
            }
        )
    if active_transit_signals:
        summary.append(
            {
                "label": "Transit trigger" if language != "tr" else "Transit tetigi",
                "value": active_transit_signals[0].get("transit_trigger") or active_transit_signals[0].get("theme") or "",
            }
        )
    if fusion_signals:
        summary.append(
            {
                "label": "Fusion theme" if language != "tr" else "Bilesik tema",
                "value": fusion_signals[0].get("theme") or "",
            }
        )
    return summary


def _build_fusion_confidence_notes(*, dasha_bundle, transit_bundle, fusion_signals, language):
    notes = []
    if not dasha_bundle.get("dasha_lord"):
        notes.append(
            "Dasha verisi sinirli oldugu icin zamanlama dili yumusak tutulmali."
            if language == "tr"
            else "Timing language should stay soft because dasha data is limited."
        )
    if not (transit_bundle.get("transit_triggers") or []):
        notes.append(
            "Transit destegi sinirli; bu nedenle pencere yorumu genel tutulmali."
            if language == "tr"
            else "Transit support is limited, so the timing window should stay general."
        )
    if fusion_signals:
        notes.append(
            "Bu katman yalnizca zamanlama baglami saglar; kaderci yorum icin kullanilmamali."
            if language == "tr"
            else "This layer provides timing context only and should not be used for fatalistic statements."
        )
    if not notes:
        notes.append(
            "Zamanlama sinyalleri mevcut, ancak dikkatli ve kosullu okunmali."
            if language == "tr"
            else "Timing signals are present, but they should be read carefully and conditionally."
        )
    return notes


def _build_report_type_focus(*, report_type, fusion_signals, language):
    if not fusion_signals:
        return {}
    domain_counts = {}
    for item in fusion_signals:
        domain = item.get("domain") or "general"
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
    top_domain = sorted(domain_counts.items(), key=lambda pair: (-pair[1], pair[0]))[0][0]
    if language == "tr":
        description = f"Zamanlama vurgusu en cok {top_domain} alaninda yogunlasiyor."
    else:
        description = f"Timing emphasis is concentrating most strongly in the {top_domain} domain."
    return {
        "report_type": report_type,
        "primary_domain": top_domain,
        "description": description,
    }


def _fusion_domain_for_categories(categories):
    for category in categories or []:
        mapped = FUSION_DOMAIN_MAP.get(str(category or "").strip().lower())
        if mapped:
            return mapped
    return "general"


def _resolve_fusion_domain(dasha_domain, transit_domain, report_type):
    for value in (dasha_domain, transit_domain):
        if value and value != "general":
            return value
    if report_type == "career":
        return "career"
    if report_type == "parent_child":
        return "family"
    if report_type == "annual_transit":
        return "general"
    return "general"


def _timing_type_for_rows(dasha_row, transit_row, theme, risk_keys, opportunity_keys):
    effect = str(transit_row.get("effect") or "").strip().lower()
    theme_key = str(theme or "").strip().lower()
    if effect in {"pressure", "delay"} or theme_key in risk_keys:
        return "pressure"
    if effect in {"review", "revision"}:
        return "review"
    if effect in {"opportunity", "support"} or theme_key in opportunity_keys:
        return "opportunity"
    return "activation"


def _fusion_hint(*, theme, domain, timing_type, language):
    if language == "tr":
        mapping = {
            "pressure": f"{theme} temasinda dikkat alani artabilir; tempoyu ve beklentiyi iyi yonetmek faydali olur.",
            "opportunity": f"{theme} temasinda firsat penceresi acilabilir; gorunen acilimi bilincli kullanin.",
            "review": f"{theme} temasinda gozden gecirme ihtiyaci belirginlesebilir; acele karar yerine ayar yapmak daha iyi olur.",
            "activation": f"{theme} temasinin {domain} alaninda aktiflesmesi mumkun; sinyalleri takip ederek ilerlemek daha saglikli olur.",
        }
        return mapping.get(timing_type, mapping["activation"])
    mapping = {
        "pressure": f"The {theme} theme may become a pressure point; slower pacing and cleaner expectations will help.",
        "opportunity": f"The {theme} theme may open an opportunity window; use visible openings deliberately.",
        "review": f"The {theme} theme may call for review and recalibration rather than rushed action.",
        "activation": f"The {theme} theme may activate in the {domain} domain; respond to it as a live context, not a certainty.",
    }
    return mapping.get(timing_type, mapping["activation"])


def _pick_natal_anchor(natal_data):
    planets = list((natal_data or {}).get("planets") or [])
    for item in planets:
        name = str(item.get("name") or "").strip()
        nakshatra = str(item.get("nakshatra") or "").strip()
        if name and nakshatra:
            return f"{name} - {nakshatra}"
        if name:
            return name
    return None


def _strength_label(value):
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        numeric = 0.0
    if numeric >= 4.0:
        return "high"
    if numeric >= 2.2:
        return "medium"
    return "low"


def _combine_strength_labels(dasha_strength, transit_effect):
    if dasha_strength == "high" or str(transit_effect or "").strip().lower() in {"opportunity", "support"}:
        return "high"
    if dasha_strength == "medium":
        return "medium"
    return "low"


def _format_window(start, end):
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if start_date and end_date:
        return f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    return ""
