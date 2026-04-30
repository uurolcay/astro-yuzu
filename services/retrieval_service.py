import json

import database as db_mod
from services import embedding_service


def _normalize_entities(entities):
    normalized = []
    for item in entities or []:
        value = str(item or "").strip().lower()
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def _loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def _is_retrieval_visible(knowledge_item):
    if knowledge_item is None:
        return False
    status = str(getattr(knowledge_item, "status", "") or "").strip().lower()
    if status not in {"active", "published"}:
        return False
    metadata = _loads(getattr(knowledge_item, "metadata_json", None), {}) or {}
    if bool(metadata.get("review_required")):
        return False
    if str(metadata.get("status") or "").strip().lower() == "rejected":
        return False
    return True


def infer_entities(payload):
    payload = payload or {}
    entities = []
    report_type = str(payload.get("report_type") or payload.get("workspace_report_type") or "").strip().lower()
    if report_type:
        entities.append(report_type)
    natal_data = payload.get("natal_data") or {}
    for planet in natal_data.get("planets") or []:
        name = str((planet or {}).get("name") or (planet or {}).get("planet") or "").strip()
        if name:
            entities.append(name.lower())
    astro_signal_context = payload.get("astro_signal_context") or {}
    for signal in astro_signal_context.get("dominant_signals") or []:
        if not isinstance(signal, dict):
            continue
        for field in ("key", "domain", "planet", "label"):
            value = str(signal.get(field) or "").strip()
            if value:
                entities.append(value.lower())
    return _normalize_entities(entities)


def retrieve_relevant_chunks(db, *, query_text="", entities=None, top_k=5, language=None):
    entity_set = set(_normalize_entities(entities))
    query_vector = embedding_service.generate_embedding(query_text or " ".join(entity_set))
    chunk_query = db.query(db_mod.KnowledgeChunk).join(db_mod.KnowledgeItem)
    if language:
        chunk_query = chunk_query.filter(db_mod.KnowledgeItem.language == ("en" if str(language).lower() == "en" else "tr"))
    scored = []
    for chunk in chunk_query.order_by(db_mod.KnowledgeItem.updated_at.desc()).all():
        if not _is_retrieval_visible(chunk.knowledge_item):
            continue
        item_entities = set(_loads(getattr(chunk, "coverage_entities_json", None), []))
        if not item_entities:
            item_entities = set(_loads(chunk.entities_json, []))
        entity_overlap = len(entity_set & item_entities)
        similarity = embedding_service.cosine_similarity(
            query_vector,
            embedding_service.deserialize_embedding(chunk.embedding_json),
        )
        total_score = similarity + (entity_overlap * 0.14)
        if entity_set and similarity < 0.18 and entity_overlap == 0:
            continue
        scored.append(
            {
                "chunk_id": chunk.id,
                "knowledge_item_id": chunk.knowledge_item_id,
                "title": chunk.knowledge_item.title if chunk.knowledge_item else "Knowledge",
                "text": chunk.chunk_text,
                "similarity": round(similarity, 4),
                "entity_overlap": entity_overlap,
                "score": round(total_score, 4),
                "source_label": (chunk.knowledge_item.source_document.source_label if chunk.knowledge_item and chunk.knowledge_item.source_document else None),
                "entities": list(item_entities),
            }
        )
    scored.sort(key=lambda row: row["score"], reverse=True)
    return scored[: max(int(top_k or 5), 1)]


def build_prompt_knowledge_context(payload, *, top_k=5, db=None):
    own_session = db is None
    session = db or db_mod.SessionLocal()
    try:
        entities = infer_entities(payload)
        query_text = " ".join(
            filter(
                None,
                [
                    payload.get("report_type") or payload.get("workspace_report_type"),
                    payload.get("full_name"),
                    ((payload.get("interpretation_context") or {}).get("primary_focus") if isinstance(payload.get("interpretation_context"), dict) else ""),
                ],
            )
        )
        chunks = retrieve_relevant_chunks(
            session,
            query_text=query_text,
            entities=entities,
            top_k=top_k,
            language=payload.get("language"),
        )
        source_hints = []
        chunk_ids = []
        for chunk in chunks:
            chunk_ids.append(chunk.get("chunk_id"))
            source_hints.append(
                {
                    "knowledge_item_id": chunk["knowledge_item_id"],
                    "title": chunk["title"],
                    "source_label": chunk.get("source_label"),
                    "similarity": chunk["similarity"],
                }
            )
        return {
            "entities": entities,
            "chunks": chunks,
            "chunk_ids": chunk_ids,
            "source_hints": source_hints,
            "coverage_note": (
                "Use retrieved knowledge when it supports the existing chart signals. "
                "If retrieved knowledge is empty, do not invent missing doctrine or citations."
            ),
        }
    finally:
        if own_session:
            session.close()
