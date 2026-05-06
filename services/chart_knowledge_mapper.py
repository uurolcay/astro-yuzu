from __future__ import annotations

import re
from typing import Any


REPORT_TYPE_DOMAINS = {
    "birth_chart_karma": ["identity", "karma", "life pattern", "spiritual growth"],
    "annual_transit": ["timing", "dasha", "transit", "opportunity", "risk"],
    "career": ["career", "authority", "money", "work style", "decision-making"],
    "parent_child": ["family", "emotional pattern", "compatibility", "parenting"],
    "personal_consultation": ["question-specific", "guidance", "timing"],
}

ENTITY_FIELD_MAP = {
    "planet": "planet",
    "sign": "sign",
    "house": "house",
    "nakshatra": "nakshatra",
    "dasha": "dasha",
    "yoga": "yoga",
    "karaka": "karaka",
}


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("_", " ").strip())


def _normalize_key(value: Any) -> str:
    return _clean(value).lower().replace(" ", "_")


def _domain_for_report(report_type: str) -> str:
    domains = REPORT_TYPE_DOMAINS.get(str(report_type or "").strip().lower(), REPORT_TYPE_DOMAINS["birth_chart_karma"])
    return domains[0]


def _priority(entity_type: str, domain: str, base: int = 70) -> int:
    boost = {
        "nakshatra": 18,
        "dasha": 16,
        "yoga": 14,
        "house": 10,
        "planet": 8,
        "karaka": 8,
        "sign": 4,
    }.get(entity_type, 0)
    if domain in {"career", "timing", "risk", "opportunity"}:
        boost += 5
    return min(100, base + boost)


def _query(entity_type: str, entity: str, *, sub_entity: str = "", domain: str = "", report_type: str = "", priority: int | None = None) -> dict:
    entity = _clean(entity)
    sub_entity = _normalize_key(sub_entity) if sub_entity else ""
    if entity_type == "nakshatra" and sub_entity and sub_entity.isdigit():
        sub_entity = f"pada_{sub_entity}"
    domain = _clean(domain) or _domain_for_report(report_type)
    parts = [entity, sub_entity.replace("_", " "), domain, str(report_type or "").replace("_", " ")]
    return {
        "entity_type": entity_type,
        "entity": entity,
        "sub_entity": sub_entity,
        "domain": domain,
        "report_type": report_type,
        "priority": priority if priority is not None else _priority(entity_type, domain),
        "query_text": " ".join(part for part in parts if part).strip(),
    }


def _append_unique(queries: list[dict], query: dict) -> None:
    if not query.get("entity"):
        return
    key = (
        _normalize_key(query.get("entity_type")),
        _normalize_key(query.get("entity")),
        _normalize_key(query.get("sub_entity")),
        _normalize_key(query.get("domain")),
        _normalize_key(query.get("report_type")),
    )
    for existing in queries:
        existing_key = (
            _normalize_key(existing.get("entity_type")),
            _normalize_key(existing.get("entity")),
            _normalize_key(existing.get("sub_entity")),
            _normalize_key(existing.get("domain")),
            _normalize_key(existing.get("report_type")),
        )
        if existing_key == key:
            if int(query.get("priority") or 0) > int(existing.get("priority") or 0):
                existing.update(query)
            return
    queries.append(query)


def _iter_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_dicts(child)


def _append_from_signal(queries: list[dict], signal: dict, *, report_type: str, fallback_domain: str) -> None:
    if not isinstance(signal, dict):
        return
    domain = _clean(signal.get("domain") or signal.get("category") or fallback_domain)
    for field, entity_type in ENTITY_FIELD_MAP.items():
        value = signal.get(field) or signal.get(f"{field}_name")
        if not value and field == "dasha":
            value = signal.get("dasha_planet") or signal.get("mahadasha") or signal.get("antardasha")
        if not value and field == "yoga":
            value = signal.get("key") if "yoga" in _normalize_key(signal.get("key")) else ""
        if not value:
            continue
        sub_entity = signal.get("pada") or signal.get("sub_entity") or signal.get("subtopic")
        if field == "nakshatra":
            _append_unique(
                queries,
                _query(entity_type, value, sub_entity=sub_entity, domain=domain, report_type=report_type),
            )
        elif field == "dasha":
            _append_unique(
                queries,
                _query(entity_type, f"{value} dasha", domain=domain or "dasha", report_type=report_type),
            )
        else:
            _append_unique(queries, _query(entity_type, value, domain=domain, report_type=report_type))


def _append_from_natal_data(queries: list[dict], natal_data: dict, *, report_type: str, fallback_domain: str) -> None:
    if not isinstance(natal_data, dict):
        return
    for planet in natal_data.get("planets") or []:
        if not isinstance(planet, dict):
            continue
        planet_name = planet.get("name") or planet.get("planet")
        if planet_name:
            _append_unique(queries, _query("planet", planet_name, domain=fallback_domain, report_type=report_type, priority=76))
        if planet.get("house"):
            _append_unique(queries, _query("house", f"{planet.get('house')}th house", domain=fallback_domain, report_type=report_type, priority=80))
        if planet.get("sign"):
            _append_unique(queries, _query("sign", planet.get("sign"), domain=fallback_domain, report_type=report_type, priority=72))
        if planet.get("nakshatra"):
            _append_unique(
                queries,
                _query("nakshatra", planet.get("nakshatra"), sub_entity=planet.get("pada"), domain=fallback_domain, report_type=report_type, priority=92),
            )
    asc = natal_data.get("ascendant") or {}
    if isinstance(asc, dict):
        if asc.get("nakshatra"):
            _append_unique(queries, _query("nakshatra", asc.get("nakshatra"), sub_entity=asc.get("pada"), domain="identity", report_type=report_type, priority=95))
        if asc.get("sign"):
            _append_unique(queries, _query("sign", asc.get("sign"), domain="identity", report_type=report_type, priority=78))
    active_dasha = natal_data.get("active_dasha") or {}
    dasha_planet = natal_data.get("dasha_planet") or (active_dasha.get("planet") if isinstance(active_dasha, dict) else None)
    if dasha_planet:
        _append_unique(queries, _query("dasha", f"{dasha_planet} dasha", domain="timing", report_type=report_type, priority=93))
    karakas = natal_data.get("karakas") or {}
    if isinstance(karakas, dict):
        for key in ("atmakaraka", "amatyakaraka"):
            if karakas.get(key):
                _append_unique(queries, _query("karaka", f"{key} {karakas.get(key)}", domain="identity", report_type=report_type, priority=84))


def build_knowledge_queries_from_signal_context(signal_context, report_type, language="tr") -> list[dict]:
    report_type = str(report_type or "birth_chart_karma").strip().lower() or "birth_chart_karma"
    fallback_domain = _domain_for_report(report_type)
    queries: list[dict] = []
    context = signal_context or {}
    if isinstance(context, dict):
        _append_from_natal_data(queries, context.get("natal_data") or {}, report_type=report_type, fallback_domain=fallback_domain)
        for signal in context.get("dominant_signals") or []:
            _append_from_signal(queries, signal, report_type=report_type, fallback_domain=fallback_domain)
        for signal in context.get("risk_signals") or []:
            _append_from_signal(queries, signal, report_type=report_type, fallback_domain="risk")
        for signal in context.get("opportunity_signals") or []:
            _append_from_signal(queries, signal, report_type=report_type, fallback_domain="opportunity")
        for nested in ("nakshatra_signals", "yoga_signals", "dasha_activation_signals", "atmakaraka_signals", "prediction_fusion"):
            for row in _iter_dicts(context.get(nested) or {}):
                _append_from_signal(queries, row, report_type=report_type, fallback_domain=fallback_domain)
        for category, rows in (context.get("report_type_signals") or {}).items():
            for row in rows or []:
                _append_from_signal(queries, row, report_type=report_type, fallback_domain=category)
    queries.sort(key=lambda item: (-int(item.get("priority") or 0), item.get("entity_type", ""), item.get("entity", "")))
    return queries
