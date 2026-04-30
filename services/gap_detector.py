import json

import database as db_mod
from services.evaluation_service import evaluate_interpretation


def _dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def detect_knowledge_gaps(
    db,
    *,
    chart_data,
    output,
    evaluation_result=None,
    report_type=None,
    language="tr",
):
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
