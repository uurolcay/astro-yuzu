import json

import database as db_mod
from services.evaluation_service import evaluate_interpretation


def _dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def _loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def _gap_key(query):
    return "|".join(
        str(query.get(key) or "").strip().lower()
        for key in ("entity_type", "entity", "sub_entity", "domain", "report_type")
    )


def create_missing_source_gap(db, query: dict, *, language="tr", originating_interpretation_id=None):
    query = dict(query or {})
    entity = str(query.get("entity") or "").strip()
    if not entity:
        return None
    key = _gap_key(query)
    open_gaps = db.query(db_mod.KnowledgeGap).filter(db_mod.KnowledgeGap.status == "open").limit(300).all()
    for gap in open_gaps:
        context = _loads(getattr(gap, "context_json", None), {}) or {}
        if context.get("dedupe_key") == key:
            return gap
    domain = str(query.get("domain") or "").strip()
    report_type = str(query.get("report_type") or "").strip() or None
    sub_entity = str(query.get("sub_entity") or "").strip()
    title_parts = [entity]
    if sub_entity:
        title_parts.append(sub_entity.replace("_", " "))
    if domain:
        title_parts.append(domain)
    gap_title = f"Missing source: {' '.join(title_parts)} interpretation"
    context = {
        "dedupe_key": key,
        "entity_type": query.get("entity_type"),
        "entity": entity,
        "sub_entity": sub_entity,
        "domain": domain,
        "report_type": report_type,
        "priority": query.get("priority"),
        "query_text": query.get("query_text"),
        "originating_interpretation_id": originating_interpretation_id,
        "suggested_training_task": f"Add reviewed source material for {entity} {domain}".strip(),
        "gap_title": gap_title,
        "reason": "no_source_available",
    }
    gap = db_mod.KnowledgeGap(
        report_type=report_type,
        language="en" if str(language or "tr").lower() == "en" else "tr",
        missing_entities_json=_dumps([entity]),
        missing_topics_json=_dumps([gap_title]),
        context_json=_dumps(context),
        status="open",
    )
    db.add(gap)
    db.flush()
    return gap


def detect_knowledge_gaps(
    db,
    *,
    chart_data,
    output,
    evaluation_result=None,
    report_type=None,
    language="tr",
):
    trace = chart_data.get("_knowledge_trace") if isinstance(chart_data, dict) else None
    if isinstance(trace, dict):
        gaps = []
        for query in trace.get("missing_queries") or []:
            gap = create_missing_source_gap(db, query, language=language)
            if gap is not None:
                gaps.append(gap)
        if gaps:
            return gaps
    result = evaluation_result or evaluate_interpretation(output, chart_data)
    missing_entities = list(result.get("missing_entities") or [])
    gaps = []
    if not missing_entities:
        return gaps
    grouped_topics = []
    for entity in missing_entities:
        grouped_topics.append(f"expand coverage for {entity}")
    gap = db_mod.KnowledgeGap(
        report_type=str(report_type or chart_data.get("report_type") or "").strip() or None,
        language="en" if str(language or "tr").lower() == "en" else "tr",
        missing_entities_json=_dumps(missing_entities),
        missing_topics_json=_dumps(grouped_topics),
        context_json=_dumps(
            {
                "evaluation": result.get("metadata") or {},
                "issues": result.get("detected_issues") or [],
            }
        ),
        status="open",
    )
    db.add(gap)
    db.flush()
    gaps.append(gap)
    return gaps
