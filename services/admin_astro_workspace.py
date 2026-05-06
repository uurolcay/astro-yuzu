import json
from datetime import datetime
from pathlib import Path

import ai_interpreter as ai_logic
import database as db_mod
from sqlalchemy import inspect as sa_inspect
from services import ai_behavior_rules
from services import astro_signal_enrichment
from services.prediction_fusion_engine import build_prediction_fusion


PUBLIC_WORKSPACE_REPORT_TYPES = {
    "birth_chart_karma": "Birth chart / karma",
    "annual_transit": "Annual transit",
    "career": "Career",
    "parent_child": "Parent-child",
}

GENDER_OPTIONS = (
    {"value": "", "label_tr": "Belirtilmedi", "label_en": "Not specified"},
    {"value": "female", "label_tr": "Kadın", "label_en": "Female"},
    {"value": "male", "label_tr": "Erkek", "label_en": "Male"},
    {"value": "other", "label_tr": "Diğer / Belirtmek istemiyorum", "label_en": "Other / Prefer not to say"},
)

WORKSPACE_REPORT_LOCALIZATION = {
    "birth_chart_karma": {
        "tr": {
            "label": "Doğum Haritası / Karma",
            "title": "Doğum Haritası ve Karma Raporu",
            "subtitle": "Doğal eğilimleri, karmik tekrarları ve yaşam yönünü netleştiren içgörü odaklı analiz.",
            "task": "Generate a birth chart and karma reading. Prioritize natal structure, karmic patterns, repeating themes, strengths, vulnerabilities, and long-range life direction.",
        },
        "en": {
            "label": "Birth Chart / Karma",
            "title": "Birth Chart and Karma Report",
            "subtitle": "An insight-led reading of natal structure, karmic repetitions, natural strengths, and deeper life direction.",
            "task": "Generate a birth chart and karma reading. Prioritize natal structure, karmic patterns, repeating themes, strengths, vulnerabilities, and long-range life direction.",
        },
    },
    "annual_transit": {
        "tr": {
            "label": "Yıllık Transit",
            "title": "Yıllık Transit Raporu",
            "subtitle": "Önümüzdeki dönemin baskı, fırsat ve zamanlama pencerelerini stratejik şekilde okuyan yıllık görünüm.",
            "task": "Generate an annual transit reading. Prioritize timing, upcoming periods, pressure windows, opportunity windows, transit emphasis, and concrete timing-sensitive decision guidance.",
        },
        "en": {
            "label": "Annual Transit",
            "title": "Annual Transit Report",
            "subtitle": "A strategic annual timing reading focused on pressure cycles, openings, and the periods that matter most.",
            "task": "Generate an annual transit reading. Prioritize timing, upcoming periods, pressure windows, opportunity windows, transit emphasis, and concrete timing-sensitive decision guidance.",
        },
    },
    "career": {
        "tr": {
            "label": "Kariyer",
            "title": "Kariyer Yönü Raporu",
            "subtitle": "Mesleki yön, çalışma ritmi ve görünür büyüme alanlarını öne çıkaran odaklı kariyer analizi.",
            "task": "Generate a career reading. Prioritize vocation, professional strengths, work rhythm, long-term career direction, credibility, visibility, and decision quality in work matters.",
        },
        "en": {
            "label": "Career",
            "title": "Career Direction Report",
            "subtitle": "A focused professional reading on vocation, work rhythm, visibility, and longer-range career direction.",
            "task": "Generate a career reading. Prioritize vocation, professional strengths, work rhythm, long-term career direction, credibility, visibility, and decision quality in work matters.",
        },
    },
    "parent_child": {
        "tr": {
            "label": "Ebeveyn-Çocuk",
            "title": "Ebeveyn-Çocuk Rehberlik Raporu",
            "subtitle": "İlişki dinamiği, duygusal ihtiyaçlar ve destekleyici ebeveynlik yaklaşımını birlikte okuyan rehberlik raporu.",
            "task": "Use the existing parent-child interpretation path. Focus on the child profile, the parent-child dynamic, growth guidance, school pattern, supportive communication, and timing-sensitive parenting guidance.",
        },
        "en": {
            "label": "Parent-Child",
            "title": "Parent-Child Guidance Report",
            "subtitle": "A grounded family guidance reading focused on temperament, relationship dynamics, and supportive parenting.",
            "task": "Use the existing parent-child interpretation path. Focus on the child profile, the parent-child dynamic, growth guidance, school pattern, supportive communication, and timing-sensitive parenting guidance.",
        },
    },
}


def normalize_workspace_report_type(value):
    normalized = str(value or "").strip().lower().replace("-", "_")
    return normalized if normalized in PUBLIC_WORKSPACE_REPORT_TYPES else "birth_chart_karma"


def localized_workspace_report_meta(report_type, language="tr"):
    normalized = normalize_workspace_report_type(report_type)
    language = "en" if str(language or "tr").lower() == "en" else "tr"
    return dict(WORKSPACE_REPORT_LOCALIZATION.get(normalized, WORKSPACE_REPORT_LOCALIZATION["birth_chart_karma"])[language])


def normalize_profile_input(data):
    payload = {
        "full_name": str(data.get("full_name") or "").strip() or None,
        "gender": str(data.get("gender") or "").strip() or None,
        "birth_date": str(data.get("birth_date") or "").strip(),
        "birth_time": str(data.get("birth_time") or "").strip(),
        "birth_place_label": str(data.get("birth_place_label") or data.get("birth_city") or "").strip(),
        "birth_country": str(data.get("birth_country") or "").strip() or None,
        "birth_city": str(data.get("birth_city") or data.get("birth_place_label") or "").strip() or None,
        "resolved_birth_place": str(data.get("resolved_birth_place") or data.get("birth_place_label") or "").strip() or None,
        "resolved_latitude": str(data.get("resolved_latitude") or "").strip() or None,
        "resolved_longitude": str(data.get("resolved_longitude") or "").strip() or None,
        "resolved_timezone": str(data.get("resolved_timezone") or "").strip() or None,
        "resolved_geocode_provider": str(data.get("resolved_geocode_provider") or "").strip() or None,
        "resolved_geocode_confidence": str(data.get("resolved_geocode_confidence") or "").strip() or None,
        "notes": str(data.get("notes") or "").strip() or None,
        "is_favorite": bool(data.get("is_favorite")),
    }
    if not payload["birth_date"]:
        raise ValueError("Birth date is required.")
    if not payload["birth_time"]:
        raise ValueError("Birth time is required.")
    if not payload["birth_place_label"]:
        raise ValueError("Birth location is required.")
    return payload


def profile_to_form(profile):
    if not profile:
        return {}
    return {
        "profile_id": profile.id,
        "full_name": profile.full_name or "",
        "gender": profile.gender or "",
        "birth_date": profile.birth_date or "",
        "birth_time": profile.birth_time or "",
        "birth_place_label": profile.birth_place_label or profile.birth_city or "",
        "birth_country": profile.birth_country or "",
        "birth_city": profile.birth_city or profile.birth_place_label or "",
        "notes": profile.notes or "",
        "is_favorite": bool(profile.is_favorite),
    }


def gender_options(language="tr"):
    key = "label_en" if str(language or "tr").lower() == "en" else "label_tr"
    return [{"value": option["value"], "label": option[key]} for option in GENDER_OPTIONS]


def report_type_label(report_type):
    normalized = normalize_workspace_report_type(report_type)
    return PUBLIC_WORKSPACE_REPORT_TYPES.get(normalized, normalized.replace("_", " ").title())


def _safe_model_id(instance):
    if instance is None:
        return None
    try:
        identity = sa_inspect(instance).identity
        if identity:
            return identity[0]
    except Exception:
        pass
    try:
        return getattr(instance, "id", None)
    except Exception:
        return None


def create_or_update_internal_profile(db, data, *, admin_user=None, profile=None, location_payload=None):
    payload = normalize_profile_input(data)
    if profile is None:
        profile = db_mod.InternalProfile(created_by_user_id=_safe_model_id(admin_user))
        db.add(profile)
    profile.full_name = payload["full_name"]
    profile.gender = payload["gender"]
    profile.birth_date = payload["birth_date"]
    profile.birth_time = payload["birth_time"]
    profile.birth_place_label = payload["birth_place_label"]
    profile.birth_country = payload["birth_country"]
    profile.birth_city = payload["birth_city"]
    profile.notes = payload["notes"]
    profile.is_favorite = payload["is_favorite"]
    if location_payload:
        profile.birth_place_label = location_payload.get("normalized_birth_place") or profile.birth_place_label
        profile.birth_city = location_payload.get("normalized_birth_place") or profile.birth_city
        profile.birth_lat = location_payload.get("latitude")
        profile.birth_lng = location_payload.get("longitude")
        profile.birth_timezone = location_payload.get("timezone")
        profile.last_generated_at = datetime.utcnow()
    return profile


def build_workspace_signal_context(bundle, *, report_type, fallback_transit_context=None):
    bundle = bundle or {}
    try:
        return astro_signal_enrichment.build_astro_signal_context(
            bundle.get("natal_data") or {},
            navamsa_data=bundle.get("navamsa_data") or {},
            dasha_data=bundle.get("dasha_data") or [],
            transit_context=fallback_transit_context or bundle.get("timing_data") or bundle.get("interpretation_context") or {},
            report_type=report_type,
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        return {
            "nakshatra_signals": {"signals": [], "confidence_notes": []},
            "yoga_signals": {"signals": [], "detected_yogas": [], "confidence_notes": []},
            "dasha_activation_signals": {"signals": []},
            "chart_relationships": {"confidence_notes": [f"Signal enrichment failed safely: {exc}"]},
            "dominant_signals": [],
            "risk_signals": [],
            "opportunity_signals": [],
            "report_type_signals": {},
            "confidence_notes": [f"Signal enrichment failed safely: {exc}"],
        }


def build_ai_payload(*, report_type, primary_profile, primary_bundle, secondary_profile=None, secondary_bundle=None, interpretation_context=None, language="tr"):
    report_type = normalize_workspace_report_type(report_type)
    report_meta = localized_workspace_report_meta(report_type, language)
    primary_signal_context = build_workspace_signal_context(
        primary_bundle,
        report_type=report_type if report_type != "parent_child" else "birth_chart_karma",
        fallback_transit_context=interpretation_context,
    )
    payload = {
        "source": "admin_astro_workspace",
        "language": "en" if str(language or "tr").lower() == "en" else "tr",
        "debug_signals": bool((interpretation_context or {}).get("debug_signals") or primary_bundle.get("debug_signals")),
        "workspace_report_type": report_type,
        "report_order_type": report_type,
        "render_report_type": "parent_child" if report_type == "parent_child" else "premium",
        "report_type": report_type,
        "report_type_label": report_meta["label"],
        "report_title": report_meta["title"],
        "report_subtitle": report_meta["subtitle"],
        "full_name": primary_profile.get("full_name") or "Private Profile",
        "birth_date": primary_profile["birth_date"],
        "birth_time": primary_profile["birth_time"],
        "birth_city": primary_bundle["birth_context"].get("normalized_birth_place") or primary_profile["birth_place_label"],
        "birth_country": primary_profile.get("birth_country"),
        "raw_birth_place_input": primary_bundle["birth_context"].get("raw_birth_place_input"),
        "normalized_birth_place": primary_bundle["birth_context"].get("normalized_birth_place"),
        "latitude": primary_bundle["birth_context"].get("latitude"),
        "longitude": primary_bundle["birth_context"].get("longitude"),
        "timezone": primary_bundle["birth_context"].get("timezone"),
        "geocode_provider": primary_bundle["birth_context"].get("geocode_provider"),
        "geocode_confidence": primary_bundle["birth_context"].get("geocode_confidence"),
        "calculation_config": primary_bundle.get("calculation_config") or {},
        "natal_data": primary_bundle.get("natal_data") or {},
        "dasha_data": primary_bundle.get("dasha_data") or [],
        "navamsa_data": primary_bundle.get("navamsa_data") or {},
        "transit_data": primary_bundle.get("transit_data") or [],
        "eclipse_data": primary_bundle.get("eclipse_data") or [],
        "fullmoon_data": primary_bundle.get("fullmoon_data") or [],
        "interpretation_context": interpretation_context or primary_bundle.get("interpretation_context") or {},
        "astro_signal_context": primary_signal_context,
    }
    if report_type == "parent_child" and secondary_profile and secondary_bundle:
        secondary_signal_context = build_workspace_signal_context(
            secondary_bundle,
            report_type="parent_child",
            fallback_transit_context=interpretation_context,
        )
        payload.update(
            {
                "parent_profile": {
                    "full_name": primary_profile.get("full_name"),
                    "birth_summary": primary_bundle.get("birth_summary"),
                },
                "child_profile_meta": {
                    "full_name": secondary_profile.get("full_name"),
                    "birth_summary": secondary_bundle.get("birth_summary"),
                },
                "parent_natal_data": primary_bundle.get("natal_data") or {},
                "parent_dasha_data": primary_bundle.get("dasha_data") or [],
                "child_natal_data": secondary_bundle.get("natal_data") or {},
                "parent_astro_signal_context": primary_signal_context,
                "child_astro_signal_context": secondary_signal_context,
                "astro_signal_context": {
                    "parent_profile_signals": primary_signal_context,
                    "child_profile_signals": secondary_signal_context,
                    "confidence_notes": list(
                        dict.fromkeys(
                            list(primary_signal_context.get("confidence_notes") or [])
                            + list(secondary_signal_context.get("confidence_notes") or [])
                        )
                    ),
                },
            }
        )
        payload["astro_signal_context"]["parent_child_interaction_signals"] = astro_signal_enrichment.build_parent_child_interaction_signals(
            {
                "language": payload.get("language"),
                "parent_profile_signals": primary_signal_context,
                "child_profile_signals": secondary_signal_context,
            },
            report_type="parent_child",
        )
    try:
        signal_context = payload.get("astro_signal_context") or {}
        payload["prediction_fusion"] = build_prediction_fusion(
            astro_signal_context=signal_context,
            dasha_signal_bundle=payload.get("dasha_signal_bundle") or signal_context.get("dasha_signal_bundle"),
            transit_trigger_bundle=payload.get("transit_trigger_bundle") or signal_context.get("transit_trigger_signals"),
            chart_relationships=payload.get("chart_relationships") or signal_context.get("chart_relationships"),
            report_type=report_type,
            language=payload.get("language") or language,
        )
    except Exception:
        payload["prediction_fusion"] = {
            "source": "prediction_fusion_engine",
            "prediction_windows": [],
            "active_themes": [],
            "blocked_predictions": [],
            "unconfirmed_observations": [],
            "confidence_notes": ["prediction_fusion_engine unavailable."],
        }
    return payload


def build_workspace_prompt_payload(payload, *, behavior_rules=None, runtime_overrides=None, db=None):
    report_type = normalize_workspace_report_type(
        payload.get("workspace_report_type") or payload.get("report_order_type") or payload.get("report_type")
    )
    report_meta = localized_workspace_report_meta(report_type, payload.get("language"))
    prompt_payload = ai_behavior_rules.inject_rules_into_payload(
        payload,
        active_rules=behavior_rules,
        runtime_overrides=runtime_overrides,
        task_instruction=report_meta["task"],
    )
    augment = getattr(ai_logic, "_augment_payload_with_knowledge", None)
    if callable(augment):
        prompt_payload = augment(prompt_payload, db=db)
        chunk_ids = prompt_payload.get("_used_chunk_ids")
        if isinstance(chunk_ids, list):
            payload["_used_chunk_ids"] = [cid for cid in chunk_ids if cid is not None]
        for trace_key in ("knowledge_context", "_knowledge_trace"):
            if trace_key in prompt_payload:
                payload[trace_key] = prompt_payload.get(trace_key)
    return prompt_payload


def generate_workspace_interpretation(payload, *, generator=None, behavior_rules=None, runtime_overrides=None, db=None):
    report_type = normalize_workspace_report_type(
        payload.get("workspace_report_type") or payload.get("report_order_type") or payload.get("report_type")
    )
    if report_type == "parent_child":
        try:
            from core.dual_chart import build_parent_child_ai_summary
        except ImportError:
            return "[parent_child generation unavailable: core.dual_chart could not be loaded]"

        return build_parent_child_ai_summary(
            payload.get("interpretation_context") or {},
            language=payload.get("language") or "tr",
        )
    prompt_payload = build_workspace_prompt_payload(
        payload,
        behavior_rules=behavior_rules,
        runtime_overrides=runtime_overrides,
        db=db,
    )
    return (generator or ai_logic.generate_interpretation)(prompt_payload)


def save_internal_interpretation(db, *, profile=None, secondary_profile=None, report_type, payload, render_context=None, interpretation_text, admin_user=None, generation_mode="saved"):
    interpretation = db_mod.InternalInterpretation(
        report_type=normalize_workspace_report_type(report_type),
        input_payload_json=json.dumps(payload, ensure_ascii=False, default=str),
        render_context_json=json.dumps(render_context or {}, ensure_ascii=False, default=str) if render_context is not None else None,
        interpretation_text=interpretation_text,
        generation_mode=generation_mode,
        created_by_user_id=_safe_model_id(admin_user),
    )
    interpretation.profile = profile
    interpretation.secondary_profile = secondary_profile
    db.add(interpretation)
    if profile:
        profile.last_generated_at = datetime.utcnow()
    if secondary_profile:
        secondary_profile.last_generated_at = datetime.utcnow()
    chunk_ids = payload.get("_used_chunk_ids") if isinstance(payload, dict) else None
    if isinstance(chunk_ids, list) and chunk_ids:
        interpretation.used_chunk_ids_json = json.dumps(
            [cid for cid in chunk_ids if cid is not None],
            ensure_ascii=False,
        )
    return interpretation


def attach_pdf_path(interpretation, pdf_path):
    interpretation.pdf_path = str(pdf_path)
    return interpretation


def list_internal_profiles(db, search="", limit=50):
    query = db.query(db_mod.InternalProfile)
    term = str(search or "").strip()
    if term:
        query = query.filter(db_mod.InternalProfile.full_name.ilike(f"%{term}%"))
    return query.order_by(db_mod.InternalProfile.is_favorite.desc(), db_mod.InternalProfile.updated_at.desc()).limit(max(1, int(limit or 50))).all()


def internal_pdf_output_path(base_dir, interpretation_id):
    output_dir = Path(base_dir) / "data" / "internal_astro_pdfs"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"internal_interpretation_{interpretation_id}.pdf"


def interpretation_history_views(items):
    views = []
    for item in items or []:
        language = "tr"
        try:
            payload = json.loads(item.input_payload_json or "{}")
            language = str(payload.get("language") or "tr").lower()
        except Exception:
            payload = {}
        preview = " ".join(str(item.interpretation_text or "").strip().split())
        views.append(
            {
                "id": item.id,
                "report_type": item.report_type,
                "report_type_label": report_type_label(item.report_type),
                "language": language if language in {"tr", "en"} else "tr",
                "created_at": item.created_at,
                "pdf_path": item.pdf_path,
                "preview": preview[:140] + ("..." if len(preview) > 140 else ""),
            }
        )
    return views
