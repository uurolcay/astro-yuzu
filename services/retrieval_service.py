import json

import database as db_mod
from services import chart_knowledge_mapper
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


def _metadata_bool(metadata, key):
    return metadata.get(key) is True or str(metadata.get(key) or "").strip().lower() in {"1", "true", "yes", "on"}


def _noise_score(metadata):
    try:
        return float(metadata.get("noise_score") or 0)
    except (TypeError, ValueError):
        return 0.0


def _is_retrieval_visible(knowledge_item):
    if knowledge_item is None:
        return False
    status = str(getattr(knowledge_item, "status", "") or "").strip().lower()
    if status not in {"active", "published"}:
        return False
    metadata = _loads(getattr(knowledge_item, "metadata_json", None), {}) or {}
    if bool(metadata.get("review_required")):
        return False
    metadata_status = str(metadata.get("status") or "").strip().lower()
    if metadata_status in {"rejected", "deleted"}:
        return False
    if _metadata_bool(metadata, "is_toc") or _metadata_bool(metadata, "is_index"):
        return False
    if _metadata_bool(metadata, "deleted"):
        return False
    if metadata.get("auto_reject_reason"):
        return False
    if _noise_score(metadata) >= 0.7:
        return False
    return True


def _is_chunk_retrieval_visible(chunk):
    if chunk is None or not _is_retrieval_visible(getattr(chunk, "knowledge_item", None)):
        return False
    metadata = _loads(getattr(chunk, "metadata_json", None), {}) or {}
    if str(getattr(chunk, "status", "") or metadata.get("status") or "").strip().lower() in {"rejected", "deleted"}:
        return False
    if _metadata_bool(metadata, "deleted") or _metadata_bool(metadata, "is_toc") or _metadata_bool(metadata, "is_index"):
        return False
    if metadata.get("auto_reject_reason") or _noise_score(metadata) >= 0.7:
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
        if not _is_chunk_retrieval_visible(chunk):
            continue
        item_metadata = _loads(getattr(chunk.knowledge_item, "metadata_json", None), {}) or {}
        item_entities = set(_loads(getattr(chunk, "coverage_entities_json", None), []))
        if not item_entities:
            item_entities = set(_loads(chunk.entities_json, []))
        if not item_entities:
            item_entities = set(_loads(getattr(chunk.knowledge_item, "coverage_entities_json", None), []))
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
                "source_title": (chunk.knowledge_item.source_document.title if chunk.knowledge_item and chunk.knowledge_item.source_document else None),
                "source_document_id": (chunk.knowledge_item.source_document_id if chunk.knowledge_item else None),
                "category": item_metadata.get("category") or getattr(chunk.knowledge_item, "item_type", None),
                "primary_entity": item_metadata.get("primary_entity") or (next(iter(item_entities), None) if item_entities else None),
                "safety_notes": item_metadata.get("safe_language_notes") or item_metadata.get("safety_notes") or "",
                "entities": list(item_entities),
            }
        )
    scored.sort(key=lambda row: row["score"], reverse=True)
    return scored[: max(int(top_k or 5), 1)]


def _query_entities(query):
    return _normalize_entities(
        [
            query.get("entity"),
            query.get("sub_entity"),
            query.get("domain"),
            query.get("entity_type"),
            query.get("report_type"),
        ]
    )


def _context_row(chunk, query):
    entities = set(_normalize_entities(chunk.get("entities") or []))
    entity = str(query.get("entity") or "").strip().lower()
    category = str(chunk.get("category") or "").strip().lower()
    exact_boost = 0.35 if entity and entity in entities else 0.0
    category_boost = 0.18 if str(query.get("entity_type") or "").strip().lower() == category else 0.0
    return {
        "chunk_id": chunk.get("chunk_id"),
        "knowledge_item_id": chunk.get("knowledge_item_id"),
        "source_document_id": chunk.get("source_document_id"),
        "source_title": chunk.get("source_title") or chunk.get("source_label"),
        "category": chunk.get("category"),
        "primary_entity": chunk.get("primary_entity"),
        "matched_query": query,
        "relevance_score": round(float(chunk.get("score") or 0.0) + exact_boost + category_boost, 4),
        "excerpt": str(chunk.get("text") or "")[:700],
        "safety_notes": chunk.get("safety_notes") or "",
    }


def _create_gap_for_missing_query(db, query, *, language="tr", interpretation_id=None):
    try:
        from services import gap_detector

        return gap_detector.create_missing_source_gap(
            db,
            query,
            language=language,
            originating_interpretation_id=interpretation_id,
        )
    except Exception:
        return None


def build_published_knowledge_context(
    signal_context,
    report_type,
    language="tr",
    max_chunks=10,
    *,
    db=None,
    create_gaps=True,
    interpretation_id=None,
):
    own_session = db is None
    session = db or db_mod.SessionLocal()
    try:
        max_rows = max(1, int(max_chunks or 10))
        queries = chart_knowledge_mapper.build_knowledge_queries_from_signal_context(signal_context or {}, report_type, language)
        selected = []
        seen_chunks = set()
        missing_queries = []
        source_documents = {}
        for query in queries:
            if len(selected) >= max_rows:
                break
            chunks = retrieve_relevant_chunks(
                session,
                query_text=query.get("query_text") or "",
                entities=_query_entities(query),
                top_k=3,
                language=language,
            )
            usable = []
            for chunk in chunks:
                chunk_id = chunk.get("chunk_id")
                if chunk_id in seen_chunks:
                    continue
                usable.append(chunk)
            if not usable:
                missing_queries.append(query)
                if create_gaps:
                    _create_gap_for_missing_query(session, query, language=language, interpretation_id=interpretation_id)
                continue
            for chunk in usable:
                row = _context_row(chunk, query)
                selected.append(row)
                seen_chunks.add(row["chunk_id"])
                if row.get("source_document_id"):
                    source_documents[row["source_document_id"]] = row.get("source_title")
                if len(selected) >= max_rows:
                    break
        selected.sort(key=lambda item: item["relevance_score"], reverse=True)
        selected = selected[:max_rows]
        chunk_ids = [row["chunk_id"] for row in selected if row.get("chunk_id") is not None]
        matched_entities = sorted(
            {
                str((row.get("matched_query") or {}).get("entity") or "").strip()
                for row in selected
                if str((row.get("matched_query") or {}).get("entity") or "").strip()
            }
        )
        missing_entities = sorted({str(query.get("entity") or "").strip() for query in missing_queries if str(query.get("entity") or "").strip()})
        coverage_total = len(matched_entities) + len(missing_entities)
        source_coverage_score = round(len(matched_entities) / coverage_total, 4) if coverage_total else (1.0 if selected else 0.0)
        if create_gaps and missing_queries:
            try:
                session.commit()
            except Exception:
                session.rollback()
        return {
            "queries": queries,
            "chunks": selected,
            "chunk_ids": chunk_ids,
            "matched_entities": matched_entities,
            "missing_entities": missing_entities,
            "missing_queries": missing_queries,
            "source_documents_used": [
                {"source_document_id": source_id, "source_title": title}
                for source_id, title in sorted(source_documents.items(), key=lambda item: str(item[1] or item[0]))
            ],
            "source_coverage_score": source_coverage_score,
            "no_source_available": not bool(selected),
        }
    finally:
        if own_session:
            session.close()


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
        signal_context = payload.get("astro_signal_context") or {}
        if isinstance(signal_context, dict) and payload.get("natal_data"):
            signal_context = {**signal_context, "natal_data": payload.get("natal_data") or {}}
        published_context = build_published_knowledge_context(
            signal_context,
            payload.get("report_type") or payload.get("workspace_report_type") or "birth_chart_karma",
            payload.get("language") or "tr",
            max_chunks=top_k,
            db=session,
        )
        chunks = published_context.get("chunks") or []
        source_hints = []
        for chunk in chunks:
            source_hints.append(
                {
                    "knowledge_item_id": chunk["knowledge_item_id"],
                    "source_document_id": chunk.get("source_document_id"),
                    "source_title": chunk.get("source_title"),
                    "relevance_score": chunk.get("relevance_score"),
                    "matched_query": chunk.get("matched_query"),
                }
            )
        return {
            "entities": entities,
            "chunks": chunks,
            "chunk_ids": published_context.get("chunk_ids") or [],
            "source_hints": source_hints,
            "retrieval_queries": published_context.get("queries") or [],
            "matched_entities": published_context.get("matched_entities") or [],
            "missing_entities": published_context.get("missing_entities") or [],
            "missing_queries": published_context.get("missing_queries") or [],
            "source_documents_used": published_context.get("source_documents_used") or [],
            "source_coverage_score": published_context.get("source_coverage_score"),
            "no_source_available": published_context.get("no_source_available"),
            "coverage_note": (
                "Use retrieved published knowledge only as supporting context. "
                "If retrieved knowledge is empty, do not invent missing doctrine or source-backed claims."
            ),
        }
    finally:
        if own_session:
            session.close()
