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
