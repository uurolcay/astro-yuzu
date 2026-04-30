import json
from collections import Counter, defaultdict

import database as db_mod
from report_quality_eval import evaluate_report_output


VALID_SAFETY_FLAGS = {
    "deterministic_prediction", "health_overreach", "legal_overreach",
    "fear_language", "fatalistic_tone", "cultural_issue",
}
VALID_STATUSES = {
    "draft", "reviewed", "approved", "rejected", "needs_regeneration",
}
VALID_SECTION_NAMES = {
    "opening", "identity", "signals", "dynamics", "timing",
    "actions", "summary", "risk_opportunity",
}
VALID_ISSUE_TYPES = {
    "too_generic", "repetitive", "vague", "deterministic",
    "weak_timing", "weak_advice", "tone_issue",
    "formatting_issue", "missing_synthesis",
}
VALID_INSIGHT_TYPES = {
    "recurring_failure", "top_weak_section",
    "best_performing", "worst_performing", "admin_note_pattern",
}
CHART_ENTITY_TYPES = {
    "planet": ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"],
    "house": [str(i) for i in range(1, 13)],
    "nakshatra_keyword": [
        "nakshatra", "dhanishta", "bharani", "ashwini", "rohini", "hasta", "chitra", "swati",
        "vishakha", "anuradha", "purva phalguni", "uttara phalguni", "ashlesha", "magha",
        "jyeshtha", "mula", "shravana", "revati",
    ],
    "dasha_keyword": ["dasha", "antardasha", "mahadasha"],
}
SECTION_ANCHOR_KEYWORDS = {
    "identity": ["lagna", "ascendant", "kimlik", "atmakaraka"],
    "signals": ["signal", "sinyal", "nakshatra", "yoga"],
    "dynamics": ["interaction", "etkileşim", "dynamic", "relationship"],
    "timing": ["dasha", "transit", "timing", "zamanlama", "pencere"],
    "actions": ["action", "aksiyon", "öneri", "recommend", "avoid"],
    "summary": ["summary", "özet", "strategic", "stratejik"],
    "opening": ["opening", "açılış", "focus", "odak"],
    "risk_opportunity": ["risk", "fırsat", "opportunity"],
}


def _safe_json_loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def _safe_json_dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def _parse_csv_values(raw_value):
    return [part.strip() for part in str(raw_value or "").split(",") if part.strip()]


def detect_missing_entities(chart_data: dict, output_text: str) -> list[str]:
    try:
        if not chart_data:
            return []
        text = str(output_text or "").lower()
        entities = []
        for planet in chart_data.get("planets", []) or []:
            if not isinstance(planet, dict):
                continue
            name = str(planet.get("name") or "").strip()
            if name:
                entities.append(name)
            nakshatra = str(planet.get("nakshatra") or "").strip()
            if nakshatra:
                entities.append(nakshatra)
        asc_nakshatra = str(((chart_data.get("ascendant") or {}).get("nakshatra")) or "").strip()
        if asc_nakshatra:
            entities.append(asc_nakshatra)
        dasha_planet = str(chart_data.get("dasha_planet") or ((chart_data.get("active_dasha") or {}).get("planet")) or "").strip()
        if dasha_planet:
            entities.append(dasha_planet)
        missing = []
        for entity in dict.fromkeys(entities):
            if entity and entity.lower() not in text:
                missing.append(entity)
        return missing
    except Exception:
        return []


def compute_section_coverage(output_text: str) -> dict:
    try:
        text = str(output_text or "").lower()
        covered = []
        missing = []
        for section, keywords in SECTION_ANCHOR_KEYWORDS.items():
            if any(str(keyword).lower() in text for keyword in keywords):
                covered.append(section)
            else:
                missing.append(section)
        total = len(SECTION_ANCHOR_KEYWORDS) or 1
        return {
            "covered_sections": covered,
            "missing_sections": missing,
            "coverage_score": round(len(covered) / total, 2),
        }
    except Exception:
        return {"covered_sections": [], "missing_sections": list(SECTION_ANCHOR_KEYWORDS.keys()), "coverage_score": 0.0}


def get_review_for_interpretation(db, interpretation_id: int):
    return db.query(db_mod.InterpretationReview).filter(db_mod.InterpretationReview.interpretation_id == interpretation_id).first()


def save_review(db, interpretation_id: int, form_data: dict, *, admin_user=None):
    interpretation = db.query(db_mod.InternalInterpretation).filter(db_mod.InternalInterpretation.id == interpretation_id).first()
    if not interpretation:
        raise ValueError("Interpretation not found.")

    rating_fields = (
        "rating_overall", "rating_clarity", "rating_depth",
        "rating_accuracy_feel", "rating_actionability", "rating_tone",
    )
    normalized = {}
    for field in rating_fields:
        raw = form_data.get(field)
        if str(raw or "").strip():
            try:
                value = int(raw)
            except Exception as exc:
                raise ValueError(f"{field} must be an integer.") from exc
            if value < 1 or value > 5:
                raise ValueError(f"{field} must be between 1 and 5.")
            normalized[field] = value
        else:
            normalized[field] = None

    status = str(form_data.get("status") or "draft").strip()
    if status not in VALID_STATUSES:
        raise ValueError("Invalid review status.")

    safety_flags = _parse_csv_values(form_data.get("safety_flags")) or [
        key for key in VALID_SAFETY_FLAGS if str(form_data.get(f"safety_flag_{key}") or "").strip()
    ]
    unknown_safety = [flag for flag in safety_flags if flag not in VALID_SAFETY_FLAGS]
    if unknown_safety:
        raise ValueError(f"Unknown safety flags: {', '.join(unknown_safety)}")

    strong_sections = _parse_csv_values(form_data.get("strong_sections")) or [
        key for key in VALID_SECTION_NAMES if str(form_data.get(f"strong_section_{key}") or "").strip()
    ]
    weak_sections = _parse_csv_values(form_data.get("weak_sections")) or [
        key for key in VALID_SECTION_NAMES if str(form_data.get(f"weak_section_{key}") or "").strip()
    ]
    invalid_sections = [section for section in strong_sections + weak_sections if section not in VALID_SECTION_NAMES]
    if invalid_sections:
        raise ValueError(f"Unknown section names: {', '.join(sorted(set(invalid_sections)))}")

    language = str(form_data.get("language") or "tr").strip().lower() or "tr"
    quality_eval = evaluate_report_output(interpretation.interpretation_text, language)
    chart_data = _safe_json_loads(interpretation.input_payload_json, {}).get("natal_data") or {}
    missing_entities = detect_missing_entities(chart_data, interpretation.interpretation_text)
    section_coverage = compute_section_coverage(interpretation.interpretation_text)

    existing = get_review_for_interpretation(db, interpretation_id)
    parent_version_id = None
    version_number = 1
    if existing:
        parent_version_id = existing.id
        version_number = int(existing.version_number or 1) + 1
        db.delete(existing)
        db.flush()

    review = db_mod.InterpretationReview(
        interpretation_id=interpretation_id,
        admin_feedback=str(form_data.get("admin_feedback") or "").strip() or None,
        strong_sections=_safe_json_dumps(strong_sections),
        weak_sections=_safe_json_dumps(weak_sections),
        improvement_notes=str(form_data.get("improvement_notes") or "").strip() or None,
        safety_flags=_safe_json_dumps(safety_flags),
        status=status,
        version_number=version_number,
        parent_version_id=parent_version_id,
        quality_eval_json=_safe_json_dumps(quality_eval),
        missing_entities_json=_safe_json_dumps(missing_entities),
        section_coverage_json=_safe_json_dumps(section_coverage),
        reviewed_by_user_id=getattr(admin_user, "id", None),
        **normalized,
    )
    db.add(review)
    db.flush()
    return review


def list_reviews(db, *, report_type: str | None = None, status: str | None = None, rating_min: int | None = None, limit: int = 50) -> list:
    query = db.query(db_mod.InterpretationReview).join(db_mod.InternalInterpretation)
    if report_type:
        query = query.filter(db_mod.InternalInterpretation.report_type == str(report_type))
    if status:
        query = query.filter(db_mod.InterpretationReview.status == str(status))
    if rating_min is not None:
        query = query.filter(db_mod.InterpretationReview.rating_overall >= int(rating_min))
    return query.order_by(db_mod.InterpretationReview.created_at.desc()).limit(limit).all()


def build_quality_dashboard(db) -> dict:
    empty = {
        "total_reviews": 0,
        "avg_rating_overall": None,
        "by_report_type": {},
        "top_weak_sections": [],
        "top_safety_flags": [],
        "top_missing_entities": [],
        "recent_approved": [],
        "recent_rejected": [],
    }
    try:
        reviews = db.query(db_mod.InterpretationReview).join(db_mod.InternalInterpretation).order_by(db_mod.InterpretationReview.created_at.desc()).all()
        if not reviews:
            return empty
        ratings = [review.rating_overall for review in reviews if review.rating_overall is not None]
        by_report_type = defaultdict(lambda: {"count": 0, "ratings": [], "approved_count": 0, "rejected_count": 0, "needs_regen_count": 0})
        weak_counter = Counter()
        safety_counter = Counter()
        missing_counter = Counter()
        recent_approved = []
        recent_rejected = []
        for review in reviews:
            report_type = str((review.interpretation.report_type if review.interpretation else None) or "unknown")
            bucket = by_report_type[report_type]
            bucket["count"] += 1
            if review.rating_overall is not None:
                bucket["ratings"].append(review.rating_overall)
            if review.status == "approved":
                bucket["approved_count"] += 1
            elif review.status == "rejected":
                bucket["rejected_count"] += 1
            elif review.status == "needs_regeneration":
                bucket["needs_regen_count"] += 1
            for section in _safe_json_loads(review.weak_sections, []):
                weak_counter[str(section)] += 1
            for flag in _safe_json_loads(review.safety_flags, []):
                safety_counter[str(flag)] += 1
            for entity in _safe_json_loads(review.missing_entities_json, []):
                missing_counter[str(entity)] += 1
            profile_name = None
            if review.interpretation and review.interpretation.profile:
                profile_name = review.interpretation.profile.full_name
            row = {
                "id": review.id,
                "profile_name": profile_name or "Internal profile",
                "report_type": report_type,
                "rating_overall": review.rating_overall,
            }
            if review.status == "approved" and len(recent_approved) < 5:
                recent_approved.append(row)
            if review.status == "rejected" and len(recent_rejected) < 5:
                recent_rejected.append(row)
        grouped = {}
        for report_type, values in by_report_type.items():
            report_ratings = values["ratings"]
            grouped[report_type] = {
                "count": values["count"],
                "avg_overall": round(sum(report_ratings) / len(report_ratings), 2) if report_ratings else None,
                "approved_count": values["approved_count"],
                "rejected_count": values["rejected_count"],
                "needs_regen_count": values["needs_regen_count"],
            }
        return {
            "total_reviews": len(reviews),
            "avg_rating_overall": round(sum(ratings) / len(ratings), 2) if ratings else None,
            "by_report_type": grouped,
            "top_weak_sections": [name for name, _ in weak_counter.most_common(5)],
            "top_safety_flags": [name for name, _ in safety_counter.most_common(5)],
            "top_missing_entities": [name for name, _ in missing_counter.most_common(5)],
            "recent_approved": recent_approved,
            "recent_rejected": recent_rejected,
        }
    except Exception:
        return empty


def save_prompt_insight(db, form_data: dict, *, admin_user=None):
    insight_type = str(form_data.get("insight_type") or "").strip()
    if insight_type not in VALID_INSIGHT_TYPES:
        raise ValueError("Invalid insight type.")
    source_review_ids = []
    for raw in _parse_csv_values(form_data.get("source_review_ids")):
        try:
            source_review_ids.append(int(raw))
        except Exception as exc:
            raise ValueError("source_review_ids must be comma-separated integers.") from exc
    insight = db_mod.PromptInsight(
        report_type=str(form_data.get("report_type") or "").strip() or None,
        insight_type=insight_type,
        title=str(form_data.get("title") or "").strip(),
        body=str(form_data.get("body") or "").strip(),
        source_review_ids=_safe_json_dumps(source_review_ids),
        prompt_version_ref=str(form_data.get("prompt_version_ref") or "").strip() or None,
        created_by_user_id=getattr(admin_user, "id", None),
    )
    db.add(insight)
    db.flush()
    return insight


def list_prompt_insights(db, *, report_type: str | None = None, limit: int = 30) -> list:
    query = db.query(db_mod.PromptInsight)
    if report_type:
        query = query.filter(db_mod.PromptInsight.report_type == str(report_type))
    return query.order_by(db_mod.PromptInsight.created_at.desc()).limit(limit).all()
