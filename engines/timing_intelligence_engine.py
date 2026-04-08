from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any


IMPORTANT_EVENT_THRESHOLD = 60
OPPORTUNITY_THEMES = {
    "career_growth",
    "relationship_opportunity",
    "new_beginning",
    "opportunity_window",
    "stability_building",
    "expansion_period",
    "growth_opportunity",
}
PRESSURE_THEMES = {
    "career_pressure",
    "financial_pressure",
    "relationship_testing",
    "responsibility_phase",
    "power_struggle",
    "pressure_test_phase",
    "responsibility_cycle",
}
TRANSFORMATION_THEMES = {
    "identity_transformation",
    "closure_cycle",
    "life_direction_shift",
    "inner_transformation",
    "identity_reinvention",
    "release_and_closure",
}
BACKGROUND_PLANET_WEIGHTS = {
    "Jupiter": 16,
    "Saturn": 22,
    "Uranus": 18,
    "Neptune": 18,
    "Pluto": 26,
}
TRIGGER_PLANET_WEIGHTS = {
    "Sun": 8,
    "Moon": 10,
    "Mercury": 10,
    "Venus": 10,
    "Mars": 14,
}
HOUSE_TO_LIFE_AREA = {
    1: "personal_growth",
    2: "finances",
    4: "home",
    5: "creativity",
    6: "health",
    7: "relationships",
    8: "personal_growth",
    9: "spirituality",
    10: "career",
    11: "social_network",
    12: "personal_growth",
}


def build_timing_intelligence(
    scored_events: list[dict[str, Any]],
    narrative_package: dict[str, Any],
) -> dict[str, Any]:
    important_events = [
        _normalize_event(event)
        for event in scored_events
        if float(event.get("importance_score", event.get("score", 0))) >= IMPORTANT_EVENT_THRESHOLD
    ]
    important_events.sort(key=lambda event: event["date_obj"])

    narratives = []
    for bucket in ("primary_narratives", "secondary_narratives", "emerging_narratives"):
        narratives.extend(narrative_package.get(bucket, []))

    global_clusters = _detect_clusters(important_events)
    cluster_lookup = _cluster_lookup(global_clusters)

    narrative_models = [
        _build_narrative_timing_model(narrative, important_events, cluster_lookup)
        for narrative in narratives
    ]
    narrative_models = [model for model in narrative_models if model is not None]
    narrative_models.sort(key=lambda model: (-model["timing_intensity"], model["activation_start"]))

    major_peak_windows = _top_major_peak_windows(narrative_models, limit=5)
    trigger_periods = _top_trigger_periods(narrative_models, limit=5)
    stability_periods = _detect_stability_windows(important_events)
    opportunity_windows = _classify_windows(global_clusters, opportunity=True)
    pressure_windows = _classify_windows(global_clusters, pressure=True)
    transformation_windows = _classify_windows(global_clusters, transformation=True)

    overall_confidence = _overall_timing_confidence(narrative_models, global_clusters)
    strategy = _interpretation_strategy(global_clusters, narrative_models)

    return {
        "narrative_timing_models": narrative_models,
        "major_peak_windows": major_peak_windows,
        "trigger_periods": trigger_periods,
        "stability_periods": stability_periods,
        "opportunity_windows": opportunity_windows,
        "pressure_windows": pressure_windows,
        "transformation_windows": transformation_windows,
        "timing_confidence": overall_confidence,
        "interpretation_timing_strategy": strategy,
    }


def _build_narrative_timing_model(
    narrative: dict[str, Any],
    important_events: list[dict[str, Any]],
    cluster_lookup: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    matched_events = _match_narrative_events(narrative, important_events)
    if not matched_events:
        return None

    matched_events.sort(key=lambda event: event["date_obj"])
    narrative_clusters = _clusters_for_events(matched_events, cluster_lookup)

    activation_event = matched_events[0]
    last_major_event = max(matched_events, key=lambda event: (event["date_obj"], event["importance_score"]))
    peak_source = _pick_peak_source(matched_events, narrative_clusters)

    build_start = activation_event["date_obj"]
    peak_start, peak_end, peak_label = _peak_window(peak_source)
    integration_start = peak_end + timedelta(days=1)
    resolution_end = max(
        last_major_event["date_obj"] + timedelta(days=last_major_event["duration_days"]),
        integration_start + timedelta(days=14),
    )
    build_end = max(build_start, peak_start - timedelta(days=1))
    integration_end = max(integration_start, resolution_end - timedelta(days=7))

    supporting_events = len(matched_events)
    strongest_cluster_score = max((cluster["cluster_score"] for cluster in narrative_clusters), default=0)
    outer_planet_weight = max((_outer_planet_weight(event) for event in matched_events), default=0)
    cluster_density = max((cluster["event_count"] * 12 for cluster in narrative_clusters), default=0)
    anchor_importance = peak_source.get("importance_score", peak_source.get("cluster_score", 0))
    timing_intensity = min(
        100,
        round(anchor_importance + cluster_density + outer_planet_weight),
    )

    trigger_windows = _trigger_windows(matched_events, peak_start, resolution_end)
    acceleration = _narrative_acceleration(matched_events, narrative_clusters)
    timing_focus = "cluster_driven" if narrative_clusters else "peak_driven"
    timing_confidence = _narrative_confidence(
        supporting_events=supporting_events,
        strongest_cluster_score=strongest_cluster_score,
        narrative_score=float(narrative.get("narrative_score", 0)),
    )

    return {
        "narrative_type": narrative.get("narrative_type"),
        "narrative_score": narrative.get("narrative_score"),
        "primary_life_area": narrative.get("primary_life_area"),
        "dominant_themes": narrative.get("dominant_themes", []),
        "activation_start": activation_event["date"],
        "activation_build_phase": _window_object(build_start, build_end),
        "peak_window": {
            **_window_object(peak_start, peak_end),
            "focus": peak_label,
        },
        "integration_phase": _window_object(integration_start, integration_end),
        "resolution_window": _window_object(max(integration_end, last_major_event["date_obj"]), resolution_end),
        "timing_intensity": timing_intensity,
        "timing_model": {
            "activation_start": activation_event["date"],
            "build_phase": _window_object(build_start, build_end),
            "peak_window": _window_object(peak_start, peak_end),
            "integration": _window_object(integration_start, integration_end),
            "resolution": _window_object(max(integration_end, last_major_event["date_obj"]), resolution_end),
        },
        "cluster": _serialize_cluster(narrative_clusters[0]) if narrative_clusters else None,
        "trigger_windows": trigger_windows,
        "narrative_acceleration": acceleration,
        "stability_windows": _narrative_stability_windows(matched_events),
        "timing_confidence": timing_confidence,
        "interpretation_timing_focus": timing_focus,
        "human_timing_labels": {
            "activation": _timing_label(build_start, build_end),
            "peak": _timing_label(peak_start, peak_end),
            "resolution": _timing_label(max(integration_end, last_major_event["date_obj"]), resolution_end),
        },
        "supporting_events": [event["event_id"] for event in matched_events[:8]],
    }


def _match_narrative_events(
    narrative: dict[str, Any],
    important_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    key_events = set(narrative.get("key_events", []))
    dominant_themes = set(narrative.get("dominant_themes", []))
    life_areas = {narrative.get("primary_life_area")}
    life_areas.update(narrative.get("supporting_life_areas", []))

    start = _parse_date(narrative.get("start"))
    resolution = _parse_date(narrative.get("resolution"))
    matched: list[dict[str, Any]] = []

    for event in important_events:
        if event["event_id"] in key_events:
            matched.append(event)
            continue
        if event["life_area"] in life_areas and _in_window(event["date_obj"], start, resolution, 45):
            matched.append(event)
            continue
        if dominant_themes & set(event["theme_tags"]) and _in_window(event["date_obj"], start, resolution, 45):
            matched.append(event)

    unique: dict[str, dict[str, Any]] = {}
    for event in matched:
        unique[event["event_id"]] = event
    return list(unique.values())


def _detect_clusters(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(events) < 3:
        return []

    candidates: list[dict[str, Any]] = []
    candidates.extend(_window_clusters(events, days=21, minimum=3, density_label="activation_cluster"))
    candidates.extend(_window_clusters(events, days=14, minimum=4, density_label="major_cluster"))
    candidates.extend(_window_clusters(events, days=10, minimum=5, density_label="critical_cluster"))
    return _merge_cluster_candidates(candidates)


def _window_clusters(
    events: list[dict[str, Any]],
    days: int,
    minimum: int,
    density_label: str,
) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    left = 0

    for right, event in enumerate(events):
        while (event["date_obj"] - events[left]["date_obj"]).days > days:
            left += 1

        count = right - left + 1
        if count < minimum:
            continue

        members = events[left : right + 1]
        importance_avg = sum(item["importance_score"] for item in members) / count
        outer_weight = max((_outer_planet_weight(item) for item in members), default=0)
        cluster_score = min(100, round(importance_avg + count * 4 + outer_weight))
        dominant_theme = _dominant_value(item["theme_tags"] for item in members)
        dominant_life_area = _dominant_value([[item["life_area"]] for item in members if item["life_area"]])
        trigger_event = max(members, key=lambda item: item["importance_score"])

        clusters.append(
            {
                "cluster_id": f"cluster_{members[0]['date']}_{members[-1]['date']}_{count}",
                "cluster_score": cluster_score,
                "event_count": count,
                "date_range": {
                    "start": members[0]["date_obj"],
                    "end": members[-1]["date_obj"],
                    "label": _window_label(members[0]["date_obj"], members[-1]["date_obj"]),
                },
                "dominant_theme": dominant_theme or "mixed",
                "dominant_life_area": dominant_life_area or "general",
                "trigger_event": trigger_event["event_id"],
                "event_ids": [member["event_id"] for member in members],
                "average_importance": round(importance_avg, 2),
                "density_label": density_label,
            }
        )

    return clusters


def _merge_cluster_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not candidates:
        return []

    candidates.sort(
        key=lambda cluster: (
            cluster["date_range"]["start"],
            cluster["date_range"]["end"],
            cluster["cluster_score"],
        )
    )

    merged: list[dict[str, Any]] = []
    for cluster in candidates:
        if not merged:
            merged.append(cluster)
            continue

        previous = merged[-1]
        if cluster["date_range"]["start"] <= previous["date_range"]["end"]:
            previous["cluster_score"] = max(previous["cluster_score"], cluster["cluster_score"])
            previous["event_count"] = max(previous["event_count"], cluster["event_count"])
            previous["date_range"]["end"] = max(previous["date_range"]["end"], cluster["date_range"]["end"])
            previous["event_ids"] = sorted(set(previous["event_ids"] + cluster["event_ids"]))
            if cluster["cluster_score"] >= previous["cluster_score"]:
                previous["dominant_theme"] = cluster["dominant_theme"]
                previous["dominant_life_area"] = cluster["dominant_life_area"]
                previous["trigger_event"] = cluster["trigger_event"]
                previous["density_label"] = cluster["density_label"]
            continue

        merged.append(cluster)

    return merged


def _cluster_lookup(clusters: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for cluster in clusters:
        for event_id in cluster["event_ids"]:
            lookup.setdefault(event_id, []).append(cluster)
    return lookup


def _clusters_for_events(
    events: list[dict[str, Any]],
    cluster_lookup: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    collected: dict[str, dict[str, Any]] = {}
    for event in events:
        for cluster in cluster_lookup.get(event["event_id"], []):
            collected[cluster["cluster_id"]] = cluster
    clusters = list(collected.values())
    clusters.sort(key=lambda cluster: cluster["cluster_score"], reverse=True)
    return clusters


def _pick_peak_source(
    matched_events: list[dict[str, Any]],
    narrative_clusters: list[dict[str, Any]],
) -> dict[str, Any]:
    top_event = max(matched_events, key=lambda event: event["importance_score"])
    top_cluster = max(narrative_clusters, key=lambda cluster: cluster["cluster_score"], default=None)

    if top_cluster and top_cluster["cluster_score"] >= 75 and top_cluster["cluster_score"] >= top_event["importance_score"]:
        return top_cluster
    return top_event


def _peak_window(peak_source: dict[str, Any]) -> tuple[datetime, datetime, str]:
    if "cluster_id" in peak_source:
        start = peak_source["date_range"]["start"]
        end = peak_source["date_range"]["end"]
        return start, end, peak_source.get("dominant_life_area", "cluster activation")

    center = peak_source["date_obj"]
    if peak_source["primary_planet"] in {"Saturn", "Pluto"}:
        span = 60
    elif peak_source["primary_planet"] in {"Jupiter", "Uranus", "Neptune"}:
        span = 30
    else:
        span = 10
    return center - timedelta(days=span), center + timedelta(days=span), peak_source["primary_planet"] or "event peak"


def _trigger_windows(
    events: list[dict[str, Any]],
    peak_start: datetime,
    resolution_end: datetime,
) -> list[dict[str, Any]]:
    windows = []
    for event in events:
        if event["primary_planet"] not in TRIGGER_PLANET_WEIGHTS and event["event_type"] not in {"FULL_MOON", "NEW_MOON"}:
            continue
        if event["date_obj"] < peak_start - timedelta(days=30) or event["date_obj"] > resolution_end:
            continue
        windows.append(
            {
                "date": event["date"],
                "intensity": min(100, round(event["importance_score"] + TRIGGER_PLANET_WEIGHTS.get(event["primary_planet"], 8))),
                "trigger_type": event["event_type"].lower(),
            }
        )
    windows.sort(key=lambda window: (-window["intensity"], window["date"]))
    return windows[:5]


def _narrative_acceleration(
    events: list[dict[str, Any]],
    narrative_clusters: list[dict[str, Any]],
) -> str:
    max_events = max((cluster["event_count"] for cluster in narrative_clusters), default=len(events))
    if max_events >= 7:
        return "critical"
    if max_events >= 5:
        return "high"
    if max_events >= 3:
        return "moderate"
    return "low"


def _narrative_stability_windows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _detect_stability_windows(events)


def _detect_stability_windows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(events) < 2:
        return []

    windows: list[dict[str, Any]] = []
    for previous, current in zip(events, events[1:]):
        gap = (current["date_obj"] - previous["date_obj"]).days
        if gap < 30:
            continue
        start = previous["date_obj"] + timedelta(days=1)
        end = current["date_obj"] - timedelta(days=1)
        windows.append(
            {
                **_window_object(start, end),
                "purpose": "consolidation",
            }
        )
    return windows[:5]


def _classify_windows(
    clusters: list[dict[str, Any]],
    opportunity: bool = False,
    pressure: bool = False,
    transformation: bool = False,
) -> list[dict[str, Any]]:
    selected = []
    for cluster in clusters:
        themes = {cluster.get("dominant_theme")}
        label = cluster.get("dominant_life_area", "general")
        if opportunity and cluster["average_importance"] >= 70 and themes & OPPORTUNITY_THEMES:
            selected.append({**_window_object(cluster["date_range"]["start"], cluster["date_range"]["end"]), "label": label, "cluster_score": cluster["cluster_score"]})
        elif pressure and cluster["average_importance"] >= 70 and (
            themes & PRESSURE_THEMES or cluster["dominant_theme"] == "responsibility_phase"
        ):
            selected.append({**_window_object(cluster["date_range"]["start"], cluster["date_range"]["end"]), "label": label, "cluster_score": cluster["cluster_score"]})
        elif transformation and (
            themes & TRANSFORMATION_THEMES or cluster["dominant_theme"] in {"identity_transformation", "closure_cycle"}
        ):
            selected.append({**_window_object(cluster["date_range"]["start"], cluster["date_range"]["end"]), "label": label, "cluster_score": cluster["cluster_score"]})

    selected.sort(key=lambda item: item["cluster_score"], reverse=True)
    return selected[:5]


def _top_major_peak_windows(narrative_models: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    peaks = []
    for model in narrative_models:
        peaks.append(
            {
                **model["peak_window"],
                "narrative_type": model["narrative_type"],
                "life_area": model["primary_life_area"],
                "timing_intensity": model["timing_intensity"],
            }
        )
    peaks.sort(key=lambda item: item["timing_intensity"], reverse=True)
    return peaks[:limit]


def _top_trigger_periods(narrative_models: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    trigger_periods = []
    for model in narrative_models:
        for trigger in model["trigger_windows"]:
            trigger_periods.append(
                {
                    **trigger,
                    "narrative_type": model["narrative_type"],
                }
            )
    trigger_periods.sort(key=lambda item: (-item["intensity"], item["date"]))
    return trigger_periods[:limit]


def _narrative_confidence(
    supporting_events: int,
    strongest_cluster_score: float,
    narrative_score: float,
) -> str:
    raw = supporting_events * 8 + strongest_cluster_score * 0.35 + narrative_score * 0.35
    if raw >= 85:
        return "very_high"
    if raw >= 65:
        return "high"
    if raw >= 45:
        return "moderate"
    return "low"


def _overall_timing_confidence(
    narrative_models: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
) -> str:
    supporting_events = sum(len(model.get("supporting_events", [])) for model in narrative_models)
    strongest_cluster_score = max((cluster["cluster_score"] for cluster in clusters), default=0)
    strongest_narrative = max((float(model.get("narrative_score", 0)) for model in narrative_models), default=0)
    return _narrative_confidence(supporting_events, strongest_cluster_score, strongest_narrative)


def _interpretation_strategy(
    clusters: list[dict[str, Any]],
    narrative_models: list[dict[str, Any]],
) -> str:
    if clusters:
        return "cluster_driven"
    if any(model["trigger_windows"] for model in narrative_models):
        return "mixed"
    if narrative_models:
        return "peak_driven"
    return "window_driven"


def _normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    date_obj = _parse_date(event.get("date"))
    primary_planet = _event_planet(event)
    importance_score = round(float(event.get("importance_score", event.get("score", 0))))
    duration_days = int(event.get("duration_days") or event.get("duration") or _default_duration(event))
    house = _event_house(event)

    normalized = dict(event)
    normalized.update(
        {
            "event_id": str(event.get("event_id", f"event_{date_obj.strftime('%Y%m%d')}")),
            "date_obj": date_obj,
            "date": date_obj.strftime("%Y-%m-%d"),
            "importance_score": importance_score,
            "duration_days": duration_days,
            "primary_planet": primary_planet,
            "life_area": event.get("life_area") or HOUSE_TO_LIFE_AREA.get(house, "general"),
            "theme_tags": _event_theme_tags(event),
            "event_type": str(event.get("event_type", "UNKNOWN")),
        }
    )
    return normalized


def _event_theme_tags(event: dict[str, Any]) -> list[str]:
    tags = []
    for key in ("dominant_theme", "dominant_activation_type", "event_type", "event_id"):
        value = event.get(key)
        if value:
            tags.append(str(value).lower())
    return tags


def _event_planet(event: dict[str, Any]) -> str:
    if event.get("transit_planet"):
        return str(event["transit_planet"])
    if event.get("planet"):
        return str(event["planet"])
    transit_planets = event.get("transit_planets") or []
    if transit_planets:
        return str(transit_planets[0])
    return ""


def _event_house(event: dict[str, Any]) -> int | None:
    for key in ("house", "moon_house", "new_house", "sun_house"):
        value = event.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _default_duration(event: dict[str, Any]) -> int:
    planet = _event_planet(event)
    if planet in {"Saturn", "Pluto"}:
        return 90
    if planet in {"Jupiter", "Uranus", "Neptune"}:
        return 45
    if event.get("event_type") in {"FULL_MOON", "NEW_MOON"}:
        return 3
    return 14


def _outer_planet_weight(event: dict[str, Any]) -> int:
    return BACKGROUND_PLANET_WEIGHTS.get(event.get("primary_planet", ""), 0)


def _window_object(start: datetime, end: datetime) -> dict[str, Any]:
    if end < start:
        end = start
    return {
        "start": start.strftime("%Y-%m-%d"),
        "end": end.strftime("%Y-%m-%d"),
        "label": _window_label(start, end),
    }


def _serialize_cluster(cluster: dict[str, Any]) -> dict[str, Any]:
    return {
        **cluster,
        "date_range": _window_object(cluster["date_range"]["start"], cluster["date_range"]["end"]),
    }


def _window_label(start: datetime, end: datetime) -> str:
    if start.year == end.year and start.month == end.month:
        return start.strftime("%B %Y")
    if start.year == end.year:
        return f"{start.strftime('%b')} - {end.strftime('%b %Y')}"
    return f"{start.strftime('%b %Y')} - {end.strftime('%b %Y')}"


def _timing_label(start: datetime, end: datetime) -> str:
    days = max((end - start).days, 0)
    if days <= 30:
        return "immediate"
    if days <= 90:
        return "short_term"
    if days <= 270:
        return "mid_term"
    return "long_term"


def _dominant_value(groups: Any) -> str | None:
    counter: Counter[str] = Counter()
    for group in groups:
        for value in group:
            if value:
                counter[str(value)] += 1
    return counter.most_common(1)[0][0] if counter else None


def _in_window(value: datetime, start: datetime, end: datetime, padding_days: int) -> bool:
    lower = start - timedelta(days=padding_days)
    upper = end + timedelta(days=padding_days)
    return lower <= value <= upper


def _parse_date(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(value, "%Y-%m-%d")
    return datetime.max
