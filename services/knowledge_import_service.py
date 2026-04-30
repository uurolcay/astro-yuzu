import json
from datetime import datetime

import database as db_mod
from services import knowledge_service
from services.knowledge_schema import (
    DEEP_KNOWLEDGE_REQUIRED_FIELDS,
    KNOWLEDGE_CATEGORIES,
    KNOWLEDGE_CONFIDENCE_LEVELS,
    KNOWLEDGE_ENTITY_TYPES,
    KNOWLEDGE_INTERPRETATION_MODES,
    KNOWLEDGE_REPORT_TYPES,
    KNOWLEDGE_SENSITIVITY_LEVELS,
    KNOWLEDGE_SOURCE_TYPES,
)


DEEP_KNOWLEDGE_FIELDS = (
    "title",
    "category",
    "entity_type",
    "primary_entity",
    "secondary_entity",
    "coverage_entities",
    "source_type",
    "source_title",
    "source_author",
    "source_reference",
    "classical_view",
    "modern_synthesis",
    "interpretation_logic",
    "condition_context",
    "strong_condition",
    "weak_condition",
    "contradiction_notes",
    "risk_pattern",
    "opportunity_pattern",
    "dasha_activation",
    "transit_activation",
    "report_type_usage",
    "safe_language_notes",
    "what_not_to_say",
    "premium_synthesis_sentence",
    "tags",
    "confidence_level",
    "sensitivity_level",
)


def _loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def _normalize_string(value):
    if value is None:
        return ""
    return str(value).strip()


def _normalize_list(values):
    normalized = []
    if values is None:
        return normalized
    if isinstance(values, str):
        values = [part.strip() for part in values.split(",")]
    for value in values:
        text = _normalize_string(value)
        if not text:
            continue
        if text not in normalized:
            normalized.append(text)
    return normalized


def _build_body_text(payload):
    sections = [
        ("Classical view", payload.get("classical_view")),
        ("Modern synthesis", payload.get("modern_synthesis")),
        ("Interpretation logic", payload.get("interpretation_logic")),
        ("Condition context", payload.get("condition_context")),
        ("Strong condition", payload.get("strong_condition")),
        ("Weak condition", payload.get("weak_condition")),
        ("Contradiction notes", payload.get("contradiction_notes")),
        ("Risk pattern", payload.get("risk_pattern")),
        ("Opportunity pattern", payload.get("opportunity_pattern")),
        ("Dasha activation", payload.get("dasha_activation")),
        ("Transit activation", payload.get("transit_activation")),
        ("Safe language notes", payload.get("safe_language_notes")),
        ("What not to say", payload.get("what_not_to_say")),
        ("Premium synthesis sentence", payload.get("premium_synthesis_sentence")),
    ]
    blocks = []
    for label, value in sections:
        text = _normalize_string(value)
        if text:
            blocks.append(f"{label}: {text}")
    return "\n\n".join(blocks)


def validate_deep_knowledge_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("Each knowledge payload must be an object.")
    for field in DEEP_KNOWLEDGE_REQUIRED_FIELDS:
        value = payload.get(field)
        if field in {"coverage_entities", "report_type_usage"}:
            if not _normalize_list(value):
                raise ValueError(f"Missing required field: {field}")
        elif not _normalize_string(value):
            raise ValueError(f"Missing required field: {field}")

    category = _normalize_string(payload.get("category"))
    if category not in KNOWLEDGE_CATEGORIES:
        raise ValueError("Invalid category.")

    entity_type = _normalize_string(payload.get("entity_type"))
    if entity_type not in KNOWLEDGE_ENTITY_TYPES:
        raise ValueError("Invalid entity_type.")

    source_type = _normalize_string(payload.get("source_type"))
    if source_type not in KNOWLEDGE_SOURCE_TYPES:
        raise ValueError("Invalid source_type.")

    confidence_level = _normalize_string(payload.get("confidence_level"))
    if confidence_level not in KNOWLEDGE_CONFIDENCE_LEVELS:
        raise ValueError("Invalid confidence_level.")

    sensitivity_level = _normalize_string(payload.get("sensitivity_level"))
    if sensitivity_level not in KNOWLEDGE_SENSITIVITY_LEVELS:
        raise ValueError("Invalid sensitivity_level.")

    for mode in _normalize_list(payload.get("interpretation_modes")):
        if mode not in KNOWLEDGE_INTERPRETATION_MODES:
            raise ValueError("Invalid interpretation mode.")

    for report_type in _normalize_list(payload.get("report_type_usage")):
        if report_type not in KNOWLEDGE_REPORT_TYPES:
            raise ValueError("Invalid report_type_usage.")

    return True


def normalize_deep_knowledge_payload(payload):
    validate_deep_knowledge_payload(payload)
    normalized = {}
    for field in DEEP_KNOWLEDGE_FIELDS:
        if field in {"coverage_entities", "tags", "report_type_usage"}:
            normalized[field] = _normalize_list(payload.get(field))
        else:
            normalized[field] = _normalize_string(payload.get(field))
    normalized["interpretation_modes"] = _normalize_list(payload.get("interpretation_modes"))
    normalized["secondary_entity"] = normalized.get("secondary_entity") or ""
    normalized["source_author"] = normalized.get("source_author") or ""
    normalized["source_reference"] = normalized.get("source_reference") or ""
    normalized["modern_synthesis"] = normalized.get("modern_synthesis") or ""
    normalized["condition_context"] = normalized.get("condition_context") or ""
    normalized["strong_condition"] = normalized.get("strong_condition") or ""
    normalized["weak_condition"] = normalized.get("weak_condition") or ""
    normalized["contradiction_notes"] = normalized.get("contradiction_notes") or ""
    normalized["risk_pattern"] = normalized.get("risk_pattern") or ""
    normalized["opportunity_pattern"] = normalized.get("opportunity_pattern") or ""
    normalized["dasha_activation"] = normalized.get("dasha_activation") or ""
    normalized["transit_activation"] = normalized.get("transit_activation") or ""
    normalized["safe_language_notes"] = normalized.get("safe_language_notes") or ""
    normalized["what_not_to_say"] = normalized.get("what_not_to_say") or ""
    normalized["premium_synthesis_sentence"] = normalized.get("premium_synthesis_sentence") or ""
    return normalized


def import_deep_knowledge_items(db, payloads, admin_user=None):
    if not isinstance(payloads, list):
        raise ValueError("payloads must be a list.")
    imported = []
    for payload in payloads:
        normalized = normalize_deep_knowledge_payload(payload)
        source_document = None
        source_title = normalized["source_title"]
        source_reference = normalized.get("source_reference") or None
        source_author = normalized.get("source_author") or None
        existing_source = (
            db.query(db_mod.SourceDocument)
            .filter(
                db_mod.SourceDocument.title == source_title,
                db_mod.SourceDocument.document_type == normalized["source_type"],
            )
            .first()
        )
        if existing_source:
            source_document = existing_source
        else:
            source_document = db_mod.SourceDocument(
                title=source_title,
                document_type=normalized["source_type"],
                source_label=source_author,
                source_uri=source_reference,
                language="tr",
                content_text=normalized["classical_view"],
                metadata_json=json.dumps(
                    {
                        "source_author": source_author,
                        "source_reference": source_reference,
                        "imported_at": datetime.utcnow().isoformat(),
                    },
                    ensure_ascii=False,
                ),
                created_by_user_id=getattr(admin_user, "id", None),
            )
            db.add(source_document)
            db.flush()

        metadata = dict(normalized)
        metadata["interpretation_modes"] = normalized.get("interpretation_modes") or []
        body_text = _build_body_text(normalized)
        item = knowledge_service.create_knowledge_item(
            db,
            title=normalized["title"],
            body_text=body_text,
            language="tr",
            item_type=normalized["category"],
            summary_text=normalized.get("premium_synthesis_sentence") or normalized.get("modern_synthesis") or normalized["classical_view"],
            entities=normalized["coverage_entities"],
            source_document=source_document,
            metadata=metadata,
            created_by_user_id=getattr(admin_user, "id", None),
            status="active",
        )
        imported.append(item)
    db.flush()
    return imported


def export_deep_knowledge_items(db, category=None, entity=None, report_type=None):
    query = db.query(db_mod.KnowledgeItem).order_by(db_mod.KnowledgeItem.updated_at.desc())
    if category:
        query = query.filter(db_mod.KnowledgeItem.item_type == str(category).strip())
    items = query.all()
    exported = []
    target_entity = _normalize_string(entity).lower()
    target_report_type = _normalize_string(report_type)
    for item in items:
        metadata = _loads(item.metadata_json, {})
        if not isinstance(metadata, dict):
            continue
        if target_entity and target_entity not in [value.lower() for value in _normalize_list(metadata.get("coverage_entities"))]:
            continue
        if target_report_type and target_report_type not in _normalize_list(metadata.get("report_type_usage")):
            continue
        payload = {}
        for field in DEEP_KNOWLEDGE_FIELDS:
            if field in {"coverage_entities", "tags", "report_type_usage"}:
                payload[field] = _normalize_list(metadata.get(field))
            else:
                payload[field] = _normalize_string(metadata.get(field))
        payload["interpretation_modes"] = _normalize_list(metadata.get("interpretation_modes"))
        payload["knowledge_item_id"] = item.id
        payload["chunk_count"] = len(item.chunks or [])
        exported.append(payload)
    return {
        "items": exported,
        "count": len(exported),
    }
