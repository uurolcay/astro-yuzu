import json
from datetime import datetime

import database as db_mod
from services import embedding_service


def _normalize_entities(entities):
    normalized = []
    for item in entities or []:
        value = str(item or "").strip().lower()
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def _dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def _loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def create_knowledge_item(
    db,
    *,
    title,
    body_text,
    language="tr",
    item_type="reference",
    summary_text=None,
    entities=None,
    source_document=None,
    metadata=None,
    created_by_user_id=None,
    status="active",
):
    item = db_mod.KnowledgeItem(
        title=str(title or "").strip() or "Untitled knowledge item",
        body_text=str(body_text or "").strip(),
        language="en" if str(language or "tr").lower() == "en" else "tr",
        item_type=str(item_type or "reference").strip() or "reference",
        summary_text=str(summary_text or "").strip() or None,
        entities_json=_dumps(_normalize_entities(entities)),
        coverage_entities_json=_dumps(_normalize_entities(entities)),
        metadata_json=_dumps(metadata or {}),
        status=str(status or "active").strip() or "active",
        created_by_user_id=created_by_user_id,
    )
    if source_document is not None:
        item.source_document = source_document
    db.add(item)
    db.flush()
    _rebuild_chunks(db, item)
    return item


def update_knowledge_item(db, item, **changes):
    for field in ("title", "body_text", "summary_text", "item_type", "status"):
        if field in changes:
            value = changes.get(field)
            if value is not None:
                setattr(item, field, str(value).strip() or getattr(item, field))
    if "language" in changes:
        item.language = "en" if str(changes.get("language") or "tr").lower() == "en" else "tr"
    if "metadata" in changes:
        item.metadata_json = _dumps(changes.get("metadata") or {})
    if "entities" in changes:
        normalized_entities = _normalize_entities(changes.get("entities"))
        item.entities_json = _dumps(normalized_entities)
        item.coverage_entities_json = _dumps(normalized_entities)
    item.updated_at = datetime.utcnow()
    db.flush()
    _rebuild_chunks(db, item)
    return item


def get_knowledge_by_entities(db, entities, *, language=None, item_type=None, limit=30):
    entity_set = set(_normalize_entities(entities))
    query = db.query(db_mod.KnowledgeItem)
    if language:
        query = query.filter(db_mod.KnowledgeItem.language == ("en" if str(language).lower() == "en" else "tr"))
    if item_type:
        query = query.filter(db_mod.KnowledgeItem.item_type == str(item_type))
    rows = query.order_by(db_mod.KnowledgeItem.updated_at.desc()).all()
    scored = []
    for row in rows:
        row_entities = set(_loads(row.entities_json, []))
        overlap = len(entity_set & row_entities)
        if entity_set and overlap == 0:
            continue
        scored.append((overlap, row))
    scored.sort(key=lambda pair: (pair[0], pair[1].updated_at), reverse=True)
    return [row for _, row in scored[:limit]]


def link_entities_to_knowledge(db, item, entities):
    merged = _normalize_entities((_loads(item.entities_json, []) or []) + _normalize_entities(entities))
    item.entities_json = _dumps(merged)
    item.coverage_entities_json = _dumps(merged)
    item.updated_at = datetime.utcnow()
    db.flush()
    _rebuild_chunks(db, item)
    return item


def _rebuild_chunks(db, item):
    for chunk in list(item.chunks or []):
        db.delete(chunk)
    db.flush()
    entities = _normalize_entities(_loads(item.entities_json, []))
    item.coverage_entities_json = _dumps(entities)
    text_chunks = embedding_service.chunk_text(item.body_text)
    if not text_chunks:
        return
    for index, chunk_text in enumerate(text_chunks):
        vector = embedding_service.generate_embedding(chunk_text)
        db.add(
            db_mod.KnowledgeChunk(
                knowledge_item=item,
                chunk_index=index,
                chunk_text=chunk_text,
                embedding_json=embedding_service.serialize_embedding(vector),
                entities_json=_dumps(entities),
                coverage_entities_json=_dumps(entities),
                token_count=len(chunk_text.split()),
            )
        )
    db.flush()
