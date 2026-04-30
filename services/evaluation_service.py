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
    depth_score = max(0.0, min((length_score * 0.55) + min(paragraph_count / 6.0, 0.45), 1.0))
    accuracy_score = max(0.0, min((coverage_ratio * 0.75) + (length_score * 0.25), 1.0))
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
    return {
        "accuracy_score": round(accuracy_score, 4),
        "depth_score": round(depth_score, 4),
        "safety_score": round(safety_score, 4),
        "detected_issues": issues,
        "coverage_entities": covered,
        "missing_entities": [entity for entity in entities if entity not in covered],
        "metadata": {
            "entity_count": len(entities),
            "coverage_ratio": round(coverage_ratio, 4),
            "paragraph_count": paragraph_count,
            "deterministic_hits": deterministic_hits,
            "safety_hits": safety_hits,
        },
    }


def serialize_evaluation_result(result):
    return json.dumps(result or {}, ensure_ascii=False, default=str)
