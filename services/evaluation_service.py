import json
import re


DETERMINISTIC_PATTERNS = (
    "will definitely",
    "guaranteed",
    "kaçınılmaz",
    "kesin olacak",
    "mutlaka olacak",
)
SAFETY_PATTERNS = (
    "medical diagnosis",
    "hukuki tavsiye",
    "legal advice",
    "you must divorce",
    "ölüm",
    "terminal",
)


def _extract_chart_entities(chart_data):
    chart_data = chart_data or {}
    entities = []
    for planet in (chart_data.get("natal_data") or {}).get("planets") or []:
        if isinstance(planet, dict):
            value = str(planet.get("name") or planet.get("planet") or "").strip()
            if value:
                entities.append(value.lower())
    signal_context = chart_data.get("astro_signal_context") or {}
    for signal in signal_context.get("dominant_signals") or []:
        if not isinstance(signal, dict):
            continue
        for field in ("key", "label", "domain", "planet"):
            value = str(signal.get(field) or "").strip()
            if value:
                entities.append(value.lower())
    return list(dict.fromkeys(entities))


def _knowledge_trace_metrics(chart_data):
    trace = {}
    if isinstance(chart_data, dict):
        trace = chart_data.get("_knowledge_trace") or chart_data.get("knowledge_trace") or chart_data.get("knowledge_context") or {}
    if not isinstance(trace, dict):
        trace = {}
    used_chunks = trace.get("used_chunk_ids") or trace.get("chunk_ids") or []
    if not used_chunks and isinstance(trace.get("chunks"), list):
        used_chunks = [row.get("chunk_id") for row in trace.get("chunks") if isinstance(row, dict)]
    missing_entities = trace.get("missing_entities") or []
    source_coverage = trace.get("source_coverage_score")
    try:
        source_coverage_score = float(source_coverage)
    except (TypeError, ValueError):
        total = len(used_chunks) + len(missing_entities)
        source_coverage_score = (len(used_chunks) / total) if total else 0.0
    knowledge_trace_available = bool(used_chunks or missing_entities or trace.get("retrieval_queries"))
    unsupported_claim_risk = max(0.0, min(1.0 - source_coverage_score, 1.0)) if knowledge_trace_available else 0.65
    return {
        "source_coverage_score": round(max(0.0, min(source_coverage_score, 1.0)), 4),
        "unsupported_claim_risk": round(unsupported_claim_risk, 4),
        "knowledge_trace_available": knowledge_trace_available,
        "used_chunk_count": len([chunk_id for chunk_id in used_chunks if chunk_id is not None]),
        "missing_source_count": len(missing_entities),
    }


def evaluate_interpretation(output, chart_data):
    text = str(output or "").strip()
    lowered = text.lower()
    entities = _extract_chart_entities(chart_data)
    covered = [entity for entity in entities if entity in lowered]
    coverage_ratio = (len(covered) / len(entities)) if entities else 0.5
    length_score = min(len(text.split()) / 280.0, 1.0)
    safety_hits = [pattern for pattern in SAFETY_PATTERNS if pattern in lowered]
    deterministic_hits = [pattern for pattern in DETERMINISTIC_PATTERNS if pattern in lowered]
    paragraph_count = len([part for part in re.split(r"\n\s*\n", text) if part.strip()])
    trace_metrics = _knowledge_trace_metrics(chart_data)
    depth_score = max(0.0, min((length_score * 0.55) + min(paragraph_count / 6.0, 0.45), 1.0))
    accuracy_score = max(0.0, min((coverage_ratio * 0.75) + (length_score * 0.25), 1.0))
    genericity_score = round(max(0.0, min(1.0 - depth_score + (0.18 if coverage_ratio < 0.35 else 0.0), 1.0)), 4)
    chart_signal_alignment_score = round(coverage_ratio, 4)
    safety_penalty = (0.22 * len(safety_hits)) + (0.08 * len(deterministic_hits))
    safety_score = max(0.0, min(1.0 - safety_penalty, 1.0))
    issues = []
    if coverage_ratio < 0.35:
        issues.append("low_chart_entity_coverage")
    if depth_score < 0.35:
        issues.append("shallow_output")
    if deterministic_hits:
        issues.append("deterministic_prediction")
    if safety_hits:
        issues.append("safety_overreach")
    if trace_metrics["knowledge_trace_available"] and trace_metrics["source_coverage_score"] < 0.35:
        issues.append("low_source_coverage")
    return {
        "accuracy_score": round(accuracy_score, 4),
        "depth_score": round(depth_score, 4),
        "safety_score": round(safety_score, 4),
        "source_coverage_score": trace_metrics["source_coverage_score"],
        "unsupported_claim_risk": trace_metrics["unsupported_claim_risk"],
        "genericity_score": genericity_score,
        "chart_signal_alignment_score": chart_signal_alignment_score,
        "knowledge_trace_available": trace_metrics["knowledge_trace_available"],
        "knowledge_grounding_summary": (
            f"Used {trace_metrics['used_chunk_count']} published knowledge chunks. "
            f"{trace_metrics['missing_source_count']} chart entities had no source coverage."
        ),
        "detected_issues": issues,
        "coverage_entities": covered,
        "missing_entities": [entity for entity in entities if entity not in covered],
        "metadata": {
            "entity_count": len(entities),
            "coverage_ratio": round(coverage_ratio, 4),
            "paragraph_count": paragraph_count,
            "deterministic_hits": deterministic_hits,
            "safety_hits": safety_hits,
            "used_published_knowledge_chunks": trace_metrics["used_chunk_count"],
            "missing_source_count": trace_metrics["missing_source_count"],
            "potentially_generic_section": "career guidance" if genericity_score >= 0.7 and "career" in lowered else None,
        },
    }


def serialize_evaluation_result(result):
    return json.dumps(result or {}, ensure_ascii=False, default=str)
