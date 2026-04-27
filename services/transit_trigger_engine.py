from __future__ import annotations

from copy import deepcopy


TRANSIT_PLANET_ROLES = {
    "Jupiter": {"effect": "activate", "categories": {"opportunity", "career", "wealth", "guidance", "life_direction"}},
    "Saturn": {"effect": "delay", "categories": {"risk", "discipline", "career", "karmic"}},
    "Rahu": {"effect": "intensify", "categories": {"risk", "visibility", "life_direction"}},
    "Ketu": {"effect": "release", "categories": {"karmic", "spiritual", "timing"}},
    "Mars": {"effect": "intensify", "categories": {"risk", "discipline", "parent_child_friction"}},
    "Venus": {"effect": "activate", "categories": {"relationship", "support_strategy", "wealth", "opportunity"}},
    "Mercury": {"effect": "activate", "categories": {"communication", "learning_style", "visibility"}},
    "Sun": {"effect": "activate", "categories": {"authority", "identity", "visibility", "life_direction"}},
    "Moon": {"effect": "activate", "categories": {"emotional", "emotional_needs", "support_strategy"}},
}

TRANSIT_CONTEXT_KEYS = (
    "major_peak_windows",
    "trigger_periods",
    "opportunity_windows",
    "pressure_windows",
)


def build_transit_trigger_bundle(transit_context, dasha_signal_bundle=None, astro_signal_context=None, chart_relationships=None):
    transit_context = deepcopy(transit_context or {})
    dasha_signal_bundle = deepcopy(dasha_signal_bundle or {})
    astro_signal_context = deepcopy(astro_signal_context or {})
    chart_relationships = deepcopy(chart_relationships or {})

    events = _collect_transit_events(transit_context)
    dominant_signals = list(astro_signal_context.get("dominant_signals") or [])
    if not events:
        return {
            "source": "transit_trigger_engine",
            "transit_triggers": [],
            "delivery_events": [],
            "blocked_events": [],
            "unconfirmed_transit_observations": [],
            "confidence_notes": [],
            "signals": [],
        }

    active_patterns = list(dasha_signal_bundle.get("active_nakshatra_patterns") or [])
    amplified_keys = {item.get("key") for item in (dasha_signal_bundle.get("amplified_signals") or []) if item.get("key")}
    if not active_patterns and not amplified_keys:
        blocked = [
            {
                "event_id": event["event_id"],
                "planet": event["planet"],
                "reason": "Transit present but no dasha-supported outcome available.",
            }
            for event in events
        ]
        return {
            "source": "transit_trigger_engine",
            "transit_triggers": [],
            "delivery_events": [],
            "blocked_events": blocked,
            "unconfirmed_transit_observations": [_unconfirmed_observation(event) for event in events],
            "confidence_notes": ["Transit present but no dasha-supported outcome available."],
            "signals": [],
        }

    active_planets = {item.get("planet") for item in active_patterns if item.get("planet")}
    active_domains = _active_domains_from_dasha_bundle(dasha_signal_bundle, dominant_signals)
    moon_relative_houses = chart_relationships.get("moon_relative_houses") or {}
    transit_triggers = []
    delivery_events = []
    blocked_events = []
    unconfirmed_transit_observations = []
    confidence_notes = []

    for event in events:
        planet = event["planet"]
        role = TRANSIT_PLANET_ROLES.get(planet)
        if not role:
            blocked_events.append(
                {"event_id": event["event_id"], "planet": planet, "reason": "Transit planet role is not defined for trigger delivery."}
            )
            continue
        candidates = _candidate_signals_for_event(
            dominant_signals,
            planet,
            role["categories"],
            moon_relative_houses,
        )
        target_signal = _pick_supported_target(candidates, active_domains, active_planets, amplified_keys)
        if not target_signal:
            blocked_events.append(
                {
                    "event_id": event["event_id"],
                    "planet": planet,
                    "reason": "Transit present but no dasha-supported outcome available.",
                }
            )
            unconfirmed_transit_observations.append(_unconfirmed_observation(event))
            continue
        trigger = _build_trigger(event, target_signal, role, moon_relative_houses, active_planets, amplified_keys)
        transit_triggers.append(trigger)
        delivery_events.append(
            {
                "trigger_id": trigger["trigger_id"],
                "planet": planet,
                "target_signal_key": target_signal["key"],
                "effect": trigger["effect"],
                "delivery_reason": "Transit is acting as a delivery mechanism for an already-supported natal/dasha signal.",
            }
        )

    if blocked_events and not transit_triggers:
        confidence_notes.append("Transit present but no dasha-supported outcome available.")

    signals = [_trigger_to_signal_item(trigger, dominant_signals) for trigger in transit_triggers if trigger.get("target_signal_key")]
    return {
        "source": "transit_trigger_engine",
        "transit_triggers": transit_triggers,
        "delivery_events": delivery_events,
        "blocked_events": blocked_events,
        "unconfirmed_transit_observations": unconfirmed_transit_observations,
        "confidence_notes": confidence_notes,
        "signals": signals,
    }


def _collect_transit_events(transit_context):
    events = []
    for key in TRANSIT_CONTEXT_KEYS:
        for index, item in enumerate(transit_context.get(key) or []):
            planet = _infer_transit_planet(item)
            if not planet:
                continue
            events.append(
                {
                    "event_id": f"{key}:{index}:{planet.lower()}",
                    "planet": planet,
                    "source_bucket": key,
                    "title": item.get("title") or "",
                    "start": item.get("start") or item.get("date") or item.get("time_window"),
                    "end": item.get("end") or item.get("time_window"),
                    "raw": item,
                }
            )
    return events


def _infer_transit_planet(item):
    raw_planet = str((item or {}).get("planet") or "").strip()
    if raw_planet:
        return raw_planet
    haystack = " ".join(str((item or {}).get(field) or "") for field in ("title", "summary", "label")).lower()
    for planet in TRANSIT_PLANET_ROLES:
        if planet.lower() in haystack:
            return planet
    return ""


def _active_domains_from_dasha_bundle(dasha_signal_bundle, dominant_signals):
    amplified_keys = {item.get("key") for item in (dasha_signal_bundle.get("amplified_signals") or []) if item.get("key")}
    domains = set()
    for signal in dominant_signals or []:
        if signal.get("key") in amplified_keys:
            domains.update(signal.get("categories") or [])
    return domains


def _candidate_signals_for_event(dominant_signals, planet, role_categories, moon_relative_houses):
    emotional = []
    general = []
    for signal in dominant_signals or []:
        categories = set(signal.get("categories") or [])
        if not categories:
            continue
        if signal.get("planet") == "Moon" or categories & {"emotional", "emotional_needs"}:
            if moon_relative_houses.get(planet):
                emotional.append(signal)
                continue
        if categories & role_categories or signal.get("planet") == planet:
            general.append(signal)
    return emotional + general


def _pick_supported_target(candidates, active_domains, active_planets, amplified_keys):
    for signal in candidates:
        if signal.get("key") in amplified_keys:
            return signal
        if signal.get("planet") in active_planets:
            return signal
        if set(signal.get("categories") or []) & active_domains:
            return signal
    return None


def _build_trigger(event, target_signal, role, moon_relative_houses, active_planets, amplified_keys):
    effect = role["effect"]
    strength = "medium"
    if target_signal.get("key") in amplified_keys or target_signal.get("planet") in active_planets:
        strength = "high"
    elif target_signal.get("planet") == "Moon" or set(target_signal.get("categories") or []) & {"emotional", "emotional_needs"}:
        strength = "high"
    confidence = "high" if target_signal.get("key") in amplified_keys else "medium"
    relation_note = ""
    if moon_relative_houses.get(event["planet"]):
        relation_note = f" Moon-relative house activation is {moon_relative_houses.get(event['planet'])}."
    return {
        "trigger_id": f"{event['event_id']}->{target_signal['key']}",
        "planet": event["planet"],
        "target_signal_key": target_signal["key"],
        "effect": effect,
        "strength": strength,
        "duration": event.get("end") or event.get("start") or event.get("source_bucket"),
        "based_on_dasha": True,
        "explanation": (
            f"{event['planet']} transit can deliver the already-supported signal {target_signal['label']} because dasha support exists."
            f"{relation_note}"
        ),
        "confidence": confidence,
    }


def _trigger_to_signal_item(trigger, dominant_signals):
    target = next((signal for signal in dominant_signals or [] if signal.get("key") == trigger.get("target_signal_key")), None)
    categories = list(target.get("categories") or []) if target else []
    tone = "risk" if trigger.get("effect") in {"delay", "intensify"} and (target or {}).get("tone") == "risk" else (target or {}).get("tone", "opportunity")
    strength = {"low": 0.8, "medium": 1.3, "high": 1.8}.get(trigger.get("strength"), 1.0)
    return {
        "key": trigger["target_signal_key"],
        "label": (target or {}).get("label"),
        "source": "transit_trigger",
        "categories": categories + ["timing"],
        "tone": tone,
        "keywords": [trigger.get("planet"), trigger.get("effect"), "transit_delivery"],
        "strength": strength,
        "explanation": trigger.get("explanation"),
        "report_usage": list((target or {}).get("report_usage") or []),
        "target_signal_key": trigger["target_signal_key"],
    }


def _unconfirmed_observation(event):
    role = TRANSIT_PLANET_ROLES.get(event.get("planet")) or {}
    return {
        "planet": event.get("planet"),
        "event_id": event.get("event_id"),
        "effect": role.get("effect"),
        "note": "Transit present but not dasha-confirmed. Treat as background indicator only.",
        "confidence": "low",
    }
