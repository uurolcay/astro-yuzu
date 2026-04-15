import json
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from translations import t as translate_text
from core.anchors import build_interpretation_anchors
from core.calibration import apply_calibration, build_personalization_summary, compute_calibration_adjustments
from core.domains import map_signals_to_domains
from core.feedback import load_feedback_history, save_interpretation_feedback
from core.interpretation import build_interpretation_layer
from core.recommendations import build_recommendations, derive_followup_time
from core.scoring import prioritize_signals
from core.signals import extract_signals


SAMPLE_NATAL_DATA = {
    "ascendant": {"sign_idx": 1, "degree": 23.1761, "abs_longitude": 53.1761},
    "planets": [
        {"name": "Sun", "sign_idx": 0, "degree": 9.1104, "abs_longitude": 9.1104, "house": 12},
        {"name": "Moon", "sign_idx": 10, "degree": 6.0816, "abs_longitude": 306.0816, "house": 10},
        {"name": "Mars", "sign_idx": 3, "degree": 7.9223, "abs_longitude": 97.9223, "house": 3},
        {"name": "Mercury", "sign_idx": 11, "degree": 11.8866, "abs_longitude": 341.8866, "house": 11},
        {"name": "Jupiter", "sign_idx": 1, "degree": 25.6381, "abs_longitude": 55.6381, "house": 1},
        {"name": "Venus", "sign_idx": 11, "degree": 2.3332, "abs_longitude": 332.3332, "house": 11},
        {"name": "Saturn", "sign_idx": 11, "degree": 2.8255, "abs_longitude": 332.8255, "house": 11},
        {"name": "Rahu", "sign_idx": 11, "degree": 2.6832, "abs_longitude": 332.6832, "house": 11},
        {"name": "Ketu", "sign_idx": 5, "degree": 2.6832, "abs_longitude": 152.6832, "house": 5},
    ],
}

SAMPLE_DASHA = [
    {"planet": "Jupiter", "start": "2025-01-01", "end": "2027-12-31"},
    {"planet": "Saturn", "start": "2028-01-01", "end": "2030-12-31"},
]

SAMPLE_TRANSITS = [
    {"event": "Jupiter trine Moon", "score": 8.1, "house": 10},
    {"event": "Saturn square Venus", "score": 6.4, "house": 11},
]


class InterpretationLayerTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()

    def tearDown(self):
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def test_signal_extraction_returns_structured_signals(self):
        signals = extract_signals(SAMPLE_NATAL_DATA, SAMPLE_DASHA)
        self.assertTrue(signals)
        self.assertIn("planet_in_house", {signal["type"] for signal in signals})
        self.assertIn("major_aspect", {signal["type"] for signal in signals})
        self.assertIn("node_placement", {signal["type"] for signal in signals})

    def test_scoring_prioritizes_small_high_impact_set(self):
        signals = extract_signals(SAMPLE_NATAL_DATA, SAMPLE_DASHA)
        prioritized = prioritize_signals(signals, natal_data=SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, limit=6)
        self.assertLessEqual(len(prioritized), 6)
        self.assertGreaterEqual(prioritized[0]["score"], prioritized[-1]["score"])
        self.assertIn("base_weight", prioritized[0])
        self.assertIn("timing_boost", prioritized[0])

    def test_domain_mapping_limits_signal_spread(self):
        signals = extract_signals(SAMPLE_NATAL_DATA, SAMPLE_DASHA)
        prioritized = prioritize_signals(signals, natal_data=SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, limit=6)
        mapping = map_signals_to_domains(prioritized)
        self.assertTrue(mapping["domain_scores"])
        self.assertTrue(any(domain in mapping["domain_scores"] for domain in {"career", "money", "growth"}))

    def test_interpretation_builder_returns_premium_structure(self):
        layer = build_interpretation_layer(
            SAMPLE_NATAL_DATA,
            dasha_data=SAMPLE_DASHA,
            transit_data=SAMPLE_TRANSITS,
            personalization={"dominant_patterns": ["career reinvention"]},
        )
        interpretation = layer["interpretation"]
        self.assertIn("summary", interpretation)
        self.assertIn("career", interpretation)
        self.assertIn("relationships", interpretation)
        self.assertIn("growth", interpretation)
        self.assertIn("key_advice", interpretation)
        self.assertIn("risk_areas", interpretation)
        self.assertLessEqual(len(layer["prioritized_signals"]), 6)

    def test_exactly_three_anchors_are_returned(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        anchors = layer["anchors"]["top_anchors"]
        self.assertEqual(len(anchors), 3)
        self.assertEqual([anchor["rank"] for anchor in anchors], [1, 2, 3])

    def test_overlapping_signals_are_merged_into_single_anchor(self):
        signals = extract_signals(SAMPLE_NATAL_DATA, SAMPLE_DASHA)
        prioritized = prioritize_signals(signals, natal_data=SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, limit=6)
        mapping = map_signals_to_domains(prioritized)
        for signal in prioritized:
            signal["domains"] = ["growth", "inner_state"]
        anchors = build_interpretation_anchors(prioritized, mapping["domain_scores"])
        titles = [anchor["title"] for anchor in anchors["top_anchors"]]
        self.assertEqual(len(set(titles)), 3)
        self.assertEqual(anchors["top_anchors"][0]["domains"][:2], ["growth", "inner_state"])

    def test_anchor_prompt_block_is_generated(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        prompt_block = layer["anchors"]["anchor_prompt_block"]
        self.assertIn("Primary chart anchors:", prompt_block)
        self.assertIn("1.", prompt_block)

    def test_confidence_notes_is_non_empty(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        self.assertTrue(layer["anchors"]["confidence_notes"])

    def test_anchors_are_derived_from_prioritized_signals(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        top_signal_planets = {signal["planet"] for signal in layer["prioritized_signals"] if signal.get("planet")}
        anchor_planets = {
            signal["planet"]
            for anchor in layer["anchors"]["top_anchors"]
            for signal in anchor["supporting_signals"]
            if signal.get("planet")
        }
        self.assertTrue(anchor_planets.issubset(top_signal_planets))

    def test_anchor_output_is_deterministic(self):
        layer_a = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        layer_b = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        self.assertEqual(layer_a["anchors"], layer_b["anchors"])

    def test_feedback_persistence_works(self):
        report, user = self._create_report_with_anchors()
        saved = save_interpretation_feedback(
            self.db,
            {
                "report_id": report.id,
                "anchor_rank": 1,
                "user_rating": 5,
                "feedback_label": "very_helpful",
                "free_text_comment": "This felt precise.",
            },
            report=report,
            user=user,
        )

        history = load_feedback_history(self.db, user_id=user.id)
        self.assertEqual(saved["report_id"], report.id)
        self.assertEqual(saved["anchor_rank"], 1)
        self.assertEqual(saved["anchor_title"], history[0]["anchor_title"])
        self.assertEqual(len(history), 1)

    def test_malformed_feedback_is_rejected(self):
        report, user = self._create_report_with_anchors()
        with self.assertRaises(ValueError):
            save_interpretation_feedback(
                self.db,
                {
                    "report_id": report.id,
                    "anchor_rank": 9,
                    "user_rating": 10,
                    "feedback_label": "bad_label",
                },
                report=report,
                user=user,
            )

    def test_calibration_adjustments_are_deterministic(self):
        feedback_history = [
            {"anchor_type": "growth_path", "domain": "growth", "user_rating": 5, "feedback_label": "very_helpful"},
            {"anchor_type": "growth_path", "domain": "growth", "user_rating": 4, "feedback_label": "accurate"},
            {"anchor_type": "core_life_theme", "domain": "money", "user_rating": 2, "feedback_label": "too_generic"},
        ]
        a = compute_calibration_adjustments(feedback_history)
        b = compute_calibration_adjustments(feedback_history)
        self.assertEqual(a, b)

    def test_no_feedback_keeps_calibration_neutral(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS, personalization={"user_feedback": []})
        self.assertEqual(layer["calibration_summary"]["anchor_type_boosts"], {})
        self.assertEqual(layer["calibration_summary"]["domain_penalties"], {})
        self.assertEqual(layer["anchor_calibration_notes"], [])

    def test_positive_anchor_feedback_creates_bounded_boosts(self):
        adjustments = compute_calibration_adjustments([
            {"anchor_type": "growth_path", "domain": "growth", "user_rating": 5, "feedback_label": "very_helpful"},
            {"anchor_type": "growth_path", "domain": "growth", "user_rating": 5, "feedback_label": "accurate"},
            {"anchor_type": "growth_path", "domain": "growth", "user_rating": 4, "feedback_label": "very_helpful"},
        ])
        self.assertGreater(adjustments["anchor_type_boosts"]["growth_path"], 0)
        self.assertLessEqual(adjustments["anchor_type_boosts"]["growth_path"], 0.15)

    def test_calibration_output_stays_structured(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        calibrated = apply_calibration(layer["prioritized_signals"], layer["anchors"]["top_anchors"], {"anchor_type_boosts": {}, "domain_penalties": {}, "narrative_flags": {}})
        self.assertIn("prioritized_signals", calibrated)
        self.assertIn("anchors", calibrated)
        self.assertIn("anchor_calibration_notes", calibrated)

    def test_interpretation_payload_includes_calibration_and_personalization_summary(self):
        feedback_history = [
            {"anchor_type": "growth_path", "domain": "growth", "user_rating": 5, "feedback_label": "very_helpful"},
            {"anchor_type": "relationship_pattern", "domain": "relationships", "user_rating": 2, "feedback_label": "too_generic"},
        ]
        layer = build_interpretation_layer(
            SAMPLE_NATAL_DATA,
            dasha_data=SAMPLE_DASHA,
            transit_data=SAMPLE_TRANSITS,
            personalization={"user_feedback": feedback_history},
        )
        self.assertTrue(layer["feedback_ready"])
        self.assertIn("anchor_type_boosts", layer["calibration_summary"])
        self.assertIn("feedback_volume", layer["personalization_summary"])
        self.assertIn("strongest_positive_anchor_types", layer["personalization_summary"])
        self.assertIn("anchor_calibration_notes", layer)
        self.assertIn("recommendation_layer", layer)

    def test_returns_max_five_recommendations(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        recommendations = layer["recommendation_layer"]["top_recommendations"]
        self.assertLessEqual(len(recommendations), 5)

    def test_each_recommendation_has_time_window(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        for item in layer["recommendation_layer"]["top_recommendations"]:
            self.assertTrue(item["time_window"])

    def test_recommendations_are_deterministic_for_same_state(self):
        layer_a = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        layer_b = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        self.assertEqual(layer_a["recommendation_layer"], layer_b["recommendation_layer"])

    def test_recommendations_are_anchored_to_signals_and_dasha(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        recommendation = layer["recommendation_layer"]["top_recommendations"][0]
        self.assertTrue(recommendation["linked_anchors"])
        self.assertTrue(recommendation["supporting_signals"])
        self.assertIn("dasha", recommendation["reasoning"].lower())

    def test_recommendations_avoid_generic_filler_wording(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        combined = " ".join(item["title"] + " " + item["reasoning"] for item in layer["recommendation_layer"]["top_recommendations"]).lower()
        self.assertNotIn("something may happen", combined)
        self.assertNotIn("just trust the universe", combined)

    def test_recommendation_calibration_effect_is_bounded(self):
        feedback_history = [{"anchor_type": "growth_path", "domain": "growth", "user_rating": 5, "feedback_label": "very_helpful"}] * 4
        neutral = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS, personalization={"user_feedback": []})
        boosted = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS, personalization={"user_feedback": feedback_history})
        delta = abs(boosted["recommendation_layer"]["top_recommendations"][0]["confidence"] - neutral["recommendation_layer"]["top_recommendations"][0]["confidence"])
        self.assertLessEqual(delta, 0.05)

    def test_recommendation_layer_neutral_without_feedback(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS, personalization={"user_feedback": []})
        notes = layer["recommendation_layer"]["recommendation_notes"]
        self.assertTrue(notes)

    def test_result_template_renders_recommendation_section_if_present(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("Recommended Focus Now", template)
        self.assertIn("recommendation_layer.top_recommendations", template)
        self.assertIn("Opportunity windows", template)
        self.assertIn("Risk windows", template)
        self.assertIn('data-recommendation-feedback-form', template)
        self.assertIn('data-recommendation-label="very_useful"', template)
        self.assertIn("Start here first.", template)

    def test_result_template_includes_next_product_recommendation_layer(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("next_product_probe", template)
        self.assertIn("Sizin için en uygun bir sonraki adım", template)
        self.assertIn("Doğum Haritası Karma’sı", template)
        self.assertIn("Yıllık Transit", template)
        self.assertIn("Kariyer", template)
        self.assertIn("Ebeveyn-Çocuk", template)
        self.assertIn("60 dk birebir vedik astroloji danışmanlığı", template)
        self.assertIn("Kişisel Danışmanlık Al", template)

    def test_result_template_recommends_career_report_from_career_context(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Career Reader",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "premium",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "primary_focus": "kariyer yönü",
                "secondary_focus": "profession decisions",
                "dominant_life_areas": ["career"],
                "dominant_narratives": ["work direction"],
                "signal_layer": {"top_anchors": []},
                "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                "top_timing_windows": {},
            },
            "payload_json": {},
            "report_access": {"is_preview": True, "show_unlock_cta": False, "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("Bu sonuca göre en güçlü devam yolu: Kariyer", html)
        self.assertIn("Continue with This Analysis", html)
        self.assertIn("Get Personal Consultation", html)

    def test_result_template_renders_interpretation_feedback_controls(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("Top Interpretation Themes", template)
        self.assertIn('data-feedback-form', template)
        self.assertIn('data-feedback-label="very_helpful"', template)
        self.assertIn('data-feedback-label="too_generic"', template)
        self.assertIn("This is the story of your current phase.", template)

    def test_result_template_renders_hero_section_when_anchors_or_recommendations_exist(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn('t("result.hero_personal_label"', template)
        self.assertIn('t("result.hero_parent_child_label"', template)
        self.assertIn('t("result.personal_decision_title"', template)
        self.assertIn('t("result.child_decision_title"', template)
        self.assertIn("primary_recommendation", template)
        self.assertIn("primary_anchor", template)
        self.assertIn('t("result.time_sensitive_guidance"', template)
        self.assertIn('t("result.unlock_full_reading"', template)
        self.assertIn('t("result.unlock_full_child_report"', template)
        self.assertEqual(translate_text("result.unlock_full_reading", "en"), "Unlock Your Full Reading")
        self.assertEqual(translate_text("result.unlock_full_child_report", "en"), "Unlock Your Full Child Report")

    def test_result_template_includes_value_section_and_multiple_ctas(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn('t("result.value_title_personal"', template)
        self.assertIn('t("result.value_title_child"', template)
        self.assertIn('t("result.value_personal_1_title"', template)
        self.assertIn('t("result.value_child_1_title"', template)
        self.assertIn('t("result.value_personal_2_title"', template)
        self.assertIn('t("result.value_personal_3_title"', template)
        self.assertIn('t("result.value_personal_4_title"', template)
        self.assertIn("result-conversion-card", template)
        self.assertGreaterEqual(template.count('data-unlock-report-btn'), 4)

    def test_result_template_keeps_locked_preview_cues_visible(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn('t("result.continue_title"', template)
        self.assertIn("Full continuation", template)
        self.assertIn('t("result.more_title"', template)
        self.assertIn('t("result.final_title_personal"', template)
        self.assertIn('t("result.parent_child_preview_lead"', template)
        self.assertIn('t("result.child_loss_title"', template)
        self.assertIn("locked-teaser-card", template)

    def test_result_template_uses_distinct_editorial_section_markers(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("result-hero-card", template)
        self.assertIn("result-section-card", template)
        self.assertIn("result-feedback-card", template)
        self.assertIn("result-section-card--support", template)
        self.assertIn("hero-shell--parent", template)
        self.assertIn("hero-shell--personal", template)

    def test_result_template_uses_consistent_primary_cta_language(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn('t("result.unlock_full_child_report"', template)
        self.assertIn('t("result.unlock_full_reading"', template)
        self.assertNotIn("Unlock Full Reading", template)
        self.assertNotIn("See Complete Insights", template)
        self.assertNotIn("Get Your Full Report", template)

    def test_result_template_removes_generic_paywall_wording(self):
        translations_file = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\translations.py").read_text(encoding="utf-8").lower()
        self.assertNotIn("more insights", translations_file)
        self.assertIn("one-time payment. no subscription.", translations_file)
        self.assertIn("secure checkout.", translations_file)
        self.assertIn("instant access after unlock.", translations_file)

    def test_recommendation_section_appears_before_anchor_section(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertLess(template.index("Recommended Focus Now"), template.index("Top Interpretation Themes"))

    def test_result_template_still_handles_missing_optional_data_patterns(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("{% elif primary_anchor %}", template)
        self.assertIn("{% else %}", template)
        self.assertIn("No concentrated risk window currently outweighs the main guidance above.", template)

    def test_result_template_refines_lower_half_into_editorial_layers(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("result-section-card--editorial", template)
        self.assertIn("ai-reading-surface", template)
        self.assertIn("ai-context-rail", template)
        self.assertIn('data-result-text="application_eyebrow"', template)
        self.assertIn('data-result-text="basis_eyebrow"', template)
        self.assertIn('data-result-text="technical_eyebrow"', template)
        self.assertIn('data-result-text="utility_eyebrow"', template)
        self.assertIn("technical-metric-grid", template)
        self.assertIn("result-action-card", template)

    def test_feedback_payload_hook_includes_report_id_and_anchor_rank(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn('data-report-id="{{ generated_report_id }}"', template)
        self.assertIn('data-anchor-rank="{{ anchor.rank }}"', template)
        self.assertIn('report_id: Number(form.dataset.reportId)', template)
        self.assertIn('anchor_rank: Number(form.dataset.anchorRank)', template)

    def test_frontend_submission_and_ui_states_exist(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn('fetch("/api/v1/interpretation-feedback"', template)
        self.assertIn('fetch("/api/v1/recommendation-feedback"', template)
        self.assertIn('Please choose one relevance option before saving.', template)
        self.assertIn('Please choose one usefulness option before saving.', template)
        self.assertIn('Thanks — this helps us improve the relevance of future interpretations.', template)
        self.assertIn('Thanks — this helps us improve your recommendations.', template)
        self.assertIn('We could not save your feedback. Please try again.', template)
        self.assertIn('if (isSubmitting || isSaved)', template)

    def test_feedback_wording_does_not_frame_future_outcome_validation(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("Did this feel relevant?", template)
        self.assertIn("Very helpful", template)
        self.assertIn("Was this helpful?", template)
        self.assertIn("Makes sense", template)
        self.assertNotIn("Did this happen?", template)
        self.assertNotIn("prediction came true", template.lower())

    def test_result_template_renders_without_recommendations_or_anchors(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Minimal Reader",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "premium",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "signal_layer": {"top_anchors": []},
                "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                "top_timing_windows": {},
            },
            "payload_json": {},
            "report_access": {"is_preview": True, "show_unlock_cta": False, "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("Your current focus", html)
        self.assertIn("What you&#39;ll unlock", html)
        self.assertNotIn("Related Insights", html)
        self.assertIn("You&#39;ve seen the beginning of your reading", html)

    def test_parent_child_result_template_renders_supportive_preview_copy(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Deniz",
            "birth_date": "2018-05-10",
            "birth_time": "09:15",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "parent_child",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "signal_layer": {"top_anchors": []},
                "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                "child_profile": {
                    "temperament": "Your child is emotionally porous and quickly affected by tone.",
                    "emotional_needs": "Warm reassurance and time to settle.",
                },
                "relationship_dynamics": {
                    "emotional_compatibility": "There is real closeness here, but pressure can be felt quickly.",
                    "communication_style_difference": "Your child listens best when guidance is calm and concrete.",
                },
                "parenting_guidance": {
                    "best_support": "Soft structure helps more than repeated correction.",
                    "communication_guidance": "Lead with reassurance before instruction.",
                },
                "watch_areas": [],
                "growth_guidance": {},
                "top_timing_windows": {},
            },
            "payload_json": {"generated_report_id": 42},
            "report_access": {"is_preview": True, "show_unlock_cta": True, "can_unlock_here": True, "checkout_mode": "payment", "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn(translate_text("result.hero_parent_child_title"), html)
        self.assertIn("Every child experiences the world differently.", html)
        self.assertIn("What you&#39;ll understand about your child", html)
        self.assertIn("This is only part of your child&#39;s profile", html)
        self.assertIn(translate_text("result.unlock_full_child_report"), html)
        self.assertIn("Designed to support - not label - your child.", html)
        self.assertIn(translate_text("result.child_decision_title"), html)
        self.assertIn(translate_text("result.child_decision_line_1").replace("'", "&#39;"), html)
        self.assertIn("Bu sonuca göre en güçlü devam yolu: Ebeveyn-Çocuk", html)
        self.assertIn("/reports/parent-child", html)
        self.assertIn(translate_text("result.child_decision_line_2"), html)

    def test_parent_child_result_template_handles_missing_sections_safely(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Child Reader",
            "birth_date": "2019-02-01",
            "birth_time": "13:20",
            "birth_city": "Ankara, Turkey",
            "normalized_birth_place": "Ankara, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "parent_child",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "signal_layer": {"top_anchors": []},
                "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                "child_profile": {},
                "relationship_dynamics": {},
                "parenting_guidance": {},
                "watch_areas": [],
                "growth_guidance": {},
                "top_timing_windows": {},
            },
            "payload_json": {"generated_report_id": 43},
            "report_access": {"is_preview": True, "show_unlock_cta": True, "can_unlock_here": True, "checkout_mode": "payment", "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("Parent-Child Guidance", html)
        self.assertIn("Recommended Approach", html)
        self.assertIn("Unlock Your Full Child Report", html)

    def test_personal_result_template_renders_decision_reassurance_block(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Reader",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "premium",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "signal_layer": {"top_anchors": []},
                "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                "top_timing_windows": {},
            },
            "payload_json": {"generated_report_id": 51},
            "report_access": {"is_preview": True, "show_unlock_cta": True, "can_unlock_here": True, "checkout_mode": "payment", "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("You&#39;re choosing the right reading", html)
        self.assertIn("This reading focuses on your own life patterns and direction.", html)
        self.assertIn("It helps you understand your current timing and next steps.", html)
        self.assertIn("Unlock Your Full Reading", html)

    def test_result_template_handles_missing_report_type_with_reassurance_fallback(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Fallback Reader",
            "birth_date": "1991-03-10",
            "birth_time": "07:15",
            "birth_city": "Ankara, Turkey",
            "normalized_birth_place": "Ankara, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "signal_layer": {"top_anchors": []},
                "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                "top_timing_windows": {},
            },
            "payload_json": {},
            "report_access": {"is_preview": True, "show_unlock_cta": False, "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("You&#39;re choosing the right reading", html)
        self.assertNotIn("A quick reminder before you continue", html)

    def test_recommendation_feedback_endpoint_accepts_valid_payload(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                "/api/v1/recommendation-feedback",
                json={
                    "report_id": report.id,
                    "recommendation_index": 1,
                    "user_feedback_label": "very_useful",
                    "user_rating": 5,
                    "acted_on": True,
                    "saved_for_later": False,
                    "free_text_comment": "This helped me delay a decision.",
                },
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["feedback"]["recommendation_index"], 1)
        self.assertEqual(payload["feedback"]["user_feedback_label"], "very_useful")

    def test_recommendation_feedback_invalid_label_rejected(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                "/api/v1/recommendation-feedback",
                json={
                    "report_id": report.id,
                    "recommendation_index": 1,
                    "user_feedback_label": "wrong_label",
                },
            )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_feedback_label")

    def test_recommendation_feedback_invalid_index_rejected(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                "/api/v1/recommendation-feedback",
                json={
                    "report_id": report.id,
                    "recommendation_index": 99,
                    "user_feedback_label": "very_useful",
                },
            )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_recommendation_index")

    def test_recommendation_feedback_stored_correctly_and_optional_fields_safe(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                "/api/v1/recommendation-feedback",
                json={
                    "report_id": report.id,
                    "recommendation_index": 1,
                    "user_feedback_label": "makes_sense",
                },
            )
        self.assertEqual(response.status_code, 200)
        stored = self.db.query(db_mod.RecommendationFeedback).one()
        self.assertEqual(stored.recommendation_index, 1)
        self.assertEqual(stored.user_feedback_label, "makes_sense")
        self.assertIsNone(stored.user_rating)

    def test_duplicate_recommendation_feedback_is_idempotently_updated(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            first = client.post(
                "/api/v1/recommendation-feedback",
                json={
                    "report_id": report.id,
                    "recommendation_index": 1,
                    "user_feedback_label": "makes_sense",
                    "user_rating": 3,
                },
            )
            second = client.post(
                "/api/v1/recommendation-feedback",
                json={
                    "report_id": report.id,
                    "recommendation_index": 1,
                    "user_feedback_label": "very_useful",
                    "user_rating": 5,
                    "acted_on": True,
                },
            )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(self.db.query(db_mod.RecommendationFeedback).count(), 1)
        stored = self.db.query(db_mod.RecommendationFeedback).one()
        self.assertEqual(stored.user_feedback_label, "very_useful")
        self.assertEqual(stored.user_rating, 5)
        self.assertTrue(stored.acted_on)

    def test_followup_time_is_derived_correctly(self):
        base_time = datetime(2026, 4, 7, 12, 0, 0)
        derived = derive_followup_time({"time_window": "next 2-3 months"}, base_time=base_time)
        self.assertEqual((derived - base_time).days, 60)

    def test_followup_created_when_report_is_saved(self):
        user = db_mod.AppUser(
            email="followup@example.com",
            password_hash="hash",
            name="Followup User",
            plan_code="premium",
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        report = app._save_generated_report(
            self.db,
            user,
            None,
            "premium",
            {
                "full_name": "Followup User",
                "birth_date": "1990-01-01",
                "birth_time": "08:30",
                "birth_city": "Besiktas, Istanbul, Turkey",
                "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
                "latitude": 41.0422,
                "longitude": 29.0083,
                "timezone": "Europe/Istanbul",
            },
            {"recommendation_layer": layer["recommendation_layer"]},
            {"engine_version": "2026.04-parity-2"},
        )
        self.db.commit()
        self.db.refresh(report)
        followups = self.db.query(db_mod.RecommendationFollowup).filter(db_mod.RecommendationFollowup.report_id == report.id).all()
        self.assertTrue(followups)

    def test_followup_retrieval_works(self):
        report, user = self._create_report_with_anchors()
        followup = db_mod.RecommendationFollowup(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=1,
            recommendation_title="Delay major financial commitments",
            scheduled_for=datetime.utcnow(),
            status="pending",
        )
        self.db.add(followup)
        self.db.commit()
        payload = app.get_pending_followups(self.db, user.id)
        self.assertEqual(len(payload), 1)
        self.assertIn("is_overdue", payload[0])

    def test_followup_completion_updates_state_and_feedback(self):
        report, user = self._create_report_with_anchors()
        followup = db_mod.RecommendationFollowup(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=1,
            recommendation_title="Delay major financial commitments",
            scheduled_for=datetime.utcnow(),
            status="pending",
        )
        self.db.add(followup)
        self.db.commit()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                f"/api/v1/recommendation-followups/{followup.id}/complete",
                json={
                    "feedback_label": "useful",
                    "acted_on": True,
                    "comment": "This helped me delay a decision.",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.db.refresh(followup)
        self.assertEqual(followup.status, "completed")
        stored = self.db.query(db_mod.RecommendationFeedback).one()
        self.assertEqual(stored.feedback_source, "followup")
        self.assertTrue(stored.acted_on)

    def test_recommendation_feedback_summary_returns_structured_output(self):
        report, user = self._create_report_with_anchors()
        self.db.add(db_mod.RecommendationFeedback(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=1,
            recommendation_title="Delay major financial commitments",
            recommendation_type="avoidance",
            domain="money",
            user_feedback_label="very_useful",
            user_rating=5,
            acted_on=True,
            saved_for_later=False,
        ))
        self.db.add(db_mod.RecommendationFeedback(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=2,
            recommendation_title="Prioritize deliberate career positioning",
            recommendation_type="action",
            domain="career",
            user_feedback_label="not_useful",
            user_rating=2,
            acted_on=False,
            saved_for_later=True,
        ))
        self.db.add(db_mod.RecommendationFeedback(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=3,
            recommendation_title="Use the current opening for targeted growth",
            recommendation_type="timing",
            domain="growth",
            user_feedback_label="helped_me",
            user_rating=5,
            acted_on=True,
            saved_for_later=False,
            feedback_source="followup",
        ))
        self.db.commit()
        summary = app.compute_recommendation_feedback_summary(app._load_recommendation_feedback_history(self.db, user_id=user.id))
        self.assertIn("preferred_recommendation_types", summary)
        self.assertIn("low_performing_domains", summary)
        self.assertIn("action_rate", summary)
        self.assertIn("average_usefulness", summary)
        self.assertIn("followup_usefulness_rate", summary)
        self.assertIn("followup_action_rate", summary)

    def test_existing_feedback_route_integration_still_works(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                "/api/v1/interpretation-feedback",
                json={
                    "report_id": report.id,
                    "anchor_rank": 1,
                    "user_rating": 5,
                    "feedback_label": "very_helpful",
                    "free_text_comment": "This part described me well.",
                },
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["feedback"]["anchor_rank"], 1)
        self.assertEqual(payload["feedback"]["feedback_label"], "very_helpful")

    def test_report_context_includes_anchor_recommendation_and_methodology_sections(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        context = app._prepare_report_context({
            "language": "en",
            "report_type": "premium",
            "full_name": "Test Client",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Besiktas, Istanbul, Turkey",
            "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
            "calculation_config": {
                "zodiac": "sidereal",
                "ayanamsa": "lahiri",
                "node_mode": "true",
                "house_system": "whole_sign",
                "engine_version": "2026.04-parity-2",
            },
            "interpretation_context": {
                "primary_focus": "career",
                "secondary_focus": "growth",
                "dominant_narratives": ["career_pressure_axis"],
                "dominant_life_areas": ["career"],
                "decision_posture": "prepare",
                "timing_strategy": "mixed",
                "confidence_level": "high",
                "top_timing_windows": {
                    "peak": {"label": "Peak window", "start": "2026-04-01", "end": "2026-05-15"},
                    "opportunity": {"label": "Opportunity window", "start": "2026-05-16", "end": "2026-06-15"},
                    "pressure": {"label": "Pressure window", "start": "2026-06-16", "end": "2026-07-15"},
                },
                "signal_layer": layer["anchors"],
                "recommendation_layer": layer["recommendation_layer"],
            },
            "ai_interpretation": "### Summary\nA premium report summary.",
            "natal_data": SAMPLE_NATAL_DATA,
            "dasha_data": SAMPLE_DASHA,
            "transit_data": SAMPLE_TRANSITS,
        })
        self.assertTrue(context["top_anchors"])
        self.assertTrue(context["top_recommendations"])
        self.assertTrue(context["methodology_notes"])
        self.assertEqual(context["methodology_notes"][0]["label"], "Zodiac")
        self.assertEqual(context["methodology_notes"][1]["value"], "Lahiri")

    def test_report_context_handles_missing_anchors_and_recommendations(self):
        context = app._prepare_report_context({
            "language": "en",
            "report_type": "premium",
            "full_name": "Test Client",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "ai_interpretation": "### Summary\nFallback summary.",
            "interpretation_context": {
                "primary_focus": "general",
                "secondary_focus": "growth",
                "dominant_narratives": [],
                "dominant_life_areas": [],
                "signal_layer": {},
                "recommendation_layer": {},
            },
        })
        self.assertEqual(context["top_anchors"], [])
        self.assertEqual(context["top_recommendations"], [])
        self.assertTrue(context["methodology_notes"])

    def test_report_context_localizes_methodology_notes_for_turkish(self):
        context = app._prepare_report_context({
            "language": "tr",
            "report_type": "premium",
            "full_name": "Test Client",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "ai_interpretation": "### Ozet\nTest ozeti.",
            "calculation_config": {
                "zodiac": "sidereal",
                "ayanamsa": "lahiri",
                "node_mode": "true",
                "house_system": "whole_sign",
                "engine_version": "2026.04-parity-2",
            },
            "interpretation_context": {
                "primary_focus": "general",
                "secondary_focus": "growth",
                "dominant_narratives": [],
                "dominant_life_areas": [],
                "signal_layer": {},
                "recommendation_layer": {},
            },
        })
        self.assertEqual(context["methodology_notes"][0]["label"], "Zodyak")
        self.assertEqual(context["methodology_notes"][0]["value"], "Sideral")
        self.assertEqual(context["methodology_notes"][2]["label"], "Ay düğümü modu")
        self.assertEqual(context["methodology_notes"][2]["value"], "Gerçek")
        self.assertEqual(context["methodology_notes"][3]["value"], "Bütün burç")

    def test_report_pdf_template_contains_premium_anchor_and_recommendation_headings(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\report_pdf.html").read_text(encoding="utf-8")
        self.assertIn("/static/focus-logo.png", template)
        self.assertNotIn("focus-logo-transparan%20(1).png", template)
        self.assertIn('t("pdf.meaning_title", language)', template)
        self.assertIn('t("pdf.promise_inside_label", language)', template)
        self.assertIn('t("pdf.core_themes", language)', template)
        self.assertIn('t("pdf.recommended_focus_now", language)', template)
        self.assertIn('t("pdf.opportunity_windows", language)', template)
        self.assertIn('t("pdf.risk_windows", language)', template)
        self.assertIn('t("pdf.how_to_use", language)', template)
        self.assertIn('t("pdf.return_when_title", language)', template)
        self.assertIn('t("pdf.calculation_notes", language)', template)
        self.assertIn("Focus Astrology", template)

    def test_report_pdf_template_renders_anchor_and_recommendation_sections(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        context = app._prepare_report_context({
            "language": "en",
            "report_type": "premium",
            "full_name": "Test Client",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Besiktas, Istanbul, Turkey",
            "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
            "calculation_config": {
                "zodiac": "sidereal",
                "ayanamsa": "lahiri",
                "node_mode": "true",
                "house_system": "whole_sign",
                "engine_version": "2026.04-parity-2",
            },
            "interpretation_context": {
                "primary_focus": "career",
                "secondary_focus": "growth",
                "dominant_narratives": ["career_pressure_axis"],
                "dominant_life_areas": ["career"],
                "decision_posture": "prepare",
                "timing_strategy": "mixed",
                "signal_layer": layer["anchors"],
                "recommendation_layer": layer["recommendation_layer"],
            },
            "ai_interpretation": "### Summary\nA premium report summary.",
        })
        html = app.templates.env.get_template("report_pdf.html").render(context)

        self.assertIn("Core Themes", html)
        self.assertIn(layer["anchors"]["top_anchors"][0]["title"], html)
        self.assertIn("Recommended Focus Now", html)
        self.assertIn(layer["recommendation_layer"]["top_recommendations"][0]["title"], html)
        self.assertIn("Inside this report", html)
        self.assertIn("Return to it when timing becomes real", html)
        self.assertIn("Calculation Notes", html)

    def test_report_pdf_template_handles_missing_optional_sections(self):
        context = app._prepare_report_context({
            "language": "en",
            "report_type": "premium",
            "full_name": "Fallback Client",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "interpretation_context": {
                "primary_focus": "general",
                "secondary_focus": "growth",
                "dominant_narratives": [],
                "dominant_life_areas": [],
                "signal_layer": {},
                "recommendation_layer": {},
            },
            "ai_interpretation": "### Summary\nFallback summary.",
        })
        html = app.templates.env.get_template("report_pdf.html").render(context)

        self.assertIn("What this means for you", html)
        self.assertIn("How to use this report", html)
        self.assertIn("Calculation Notes", html)
        self.assertNotIn("Traceback", html)
        self.assertNotIn("calibration_summary", html)

    def test_report_preview_context_remains_compatible_with_pdf_template(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        with patch.object(app.ai_logic, "generate_interpretation", return_value="### Summary\nPreview summary."):
            report_context = app._render_report_preview_context(
                request=None,
                payload_data={
                    "language": "en",
                    "report_type": "premium",
                    "full_name": "Preview Client",
                    "birth_date": "1990-01-01",
                    "birth_time": "08:30",
                    "birth_city": "Besiktas, Istanbul, Turkey",
                    "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
                    "interpretation_context": {
                        "primary_focus": "career",
                        "secondary_focus": "growth",
                        "dominant_narratives": ["career_pressure_axis"],
                        "dominant_life_areas": ["career"],
                        "signal_layer": layer["anchors"],
                        "recommendation_layer": layer["recommendation_layer"],
                    },
                    "calculation_config": {
                        "zodiac": "sidereal",
                        "ayanamsa": "lahiri",
                        "node_mode": "true",
                        "house_system": "whole_sign",
                    },
                },
            )
        self.assertIn("top_anchors", report_context)
        self.assertIn("top_recommendations", report_context)
        self.assertIn("methodology_notes", report_context)
        self.assertIn("payload_json", report_context)

    def test_followup_completion_accepts_missing_optional_fields(self):
        report, user = self._create_report_with_anchors()
        followup = db_mod.RecommendationFollowup(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=1,
            recommendation_title="Delay major financial commitments",
            scheduled_for=datetime.utcnow(),
            status="pending",
        )
        self.db.add(followup)
        self.db.commit()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.post(
                f"/api/v1/recommendation-followups/{followup.id}/complete",
                json={"feedback_label": "did_not_act"},
            )
        self.assertEqual(response.status_code, 200)
        stored = self.db.query(db_mod.RecommendationFeedback).one()
        self.assertEqual(stored.user_feedback_label, "did_not_act")

    def test_no_followups_keeps_summary_deterministic(self):
        layer_a = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS, personalization={"user_feedback": []})
        layer_b = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS, personalization={"user_feedback": []})
        self.assertEqual(layer_a["recommendation_layer"], layer_b["recommendation_layer"])

    def test_reports_route_renders(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("My Reports", response.text)
        self.assertIn(report.normalized_birth_place or report.birth_city, response.text)

    def test_reports_route_only_shows_current_users_reports(self):
        report, user = self._create_report_with_anchors()
        other_user = db_mod.AppUser(
            email="other@example.com",
            password_hash="hash",
            name="Other User",
            plan_code="premium",
            is_active=True,
        )
        self.db.add(other_user)
        self.db.commit()
        self.db.refresh(other_user)
        other_report = db_mod.GeneratedReport(
            user_id=other_user.id,
            report_type="premium",
            title="Other Secret Report",
            birth_city="Ankara, Turkey",
            normalized_birth_place="Ankara, Turkey",
            result_payload_json=json.dumps({"generated_report_id": 999}),
            interpretation_context_json=json.dumps({}),
        )
        self.db.add(other_report)
        self.db.commit()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn(report.normalized_birth_place, response.text)
        self.assertNotIn("Other Secret Report", response.text)

    def test_reports_template_renders_clean_card_actions(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\reports.html").read_text(encoding="utf-8")
        self.assertIn('t("reports.view_full_report")', template)
        self.assertIn('t("reports.view_preview")', template)
        self.assertIn('t("reports.download_pdf")', template)
        self.assertIn('t("reports.empty_cta")', template)
        self.assertIn(".reports-card-actions {", template)
        self.assertIn('href="/reports/order/birth_chart_karma">{{ t("common.cta_report_buy") }}', template)
        self.assertIn('href="/reports/order/annual_transit">{{ t("common.cta_report_buy") }}', template)
        self.assertIn('href="/reports/order/career">{{ t("common.cta_report_buy") }}', template)

    def test_reports_empty_state_works(self):
        user = db_mod.AppUser(
            email="empty@example.com",
            password_hash="hash",
            name="Empty User",
            plan_code="premium",
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("You do not have any saved readings yet.", response.text)

    def test_report_reopen_flow_works(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "_require_authenticated_user", return_value=user):
            response = client.get(f"/reports/{report.id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("From your previous reading", response.text)
        self.assertIn("View later", response.text)

    def test_feedback_state_does_not_break_revisit_rendering(self):
        report, user = self._create_report_with_anchors()
        report_id = report.id
        self.db.add(db_mod.RecommendationFeedback(
            user_id=user.id,
            report_id=report_id,
            recommendation_index=1,
            recommendation_title="Delay major financial commitments",
            recommendation_type="avoidance",
            domain="money",
            user_feedback_label="very_useful",
            feedback_source="initial",
        ))
        self.db.add(db_mod.InterpretationFeedback(
            user_id=user.id,
            report_id=report_id,
            anchor_rank=1,
            anchor_title="Inner expansion under invisible pressure",
            anchor_type="growth_path",
            domain="growth",
            user_rating=5,
            feedback_label="very_helpful",
        ))
        self.db.commit()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            response = client.get(f"/reports/{report_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("You already saved some guidance", response.text)
        self.assertIn("data-feedback-form", response.text)

    def test_followup_banner_appears_only_when_relevant(self):
        report, user = self._create_report_with_anchors()
        client = TestClient(app.app)
        with patch.object(app, "get_request_user", return_value=user):
            plain = client.get(f"/reports/{report.id}")
        self.assertEqual(plain.status_code, 200)
        self.assertNotIn("Some guidance from this report may be ready to revisit", plain.text)

        self.db.add(db_mod.RecommendationFollowup(
            user_id=user.id,
            report_id=report.id,
            recommendation_index=1,
            recommendation_title="Delay major financial commitments",
            scheduled_for=datetime.utcnow(),
            status="pending",
        ))
        self.db.commit()
        with patch.object(app, "get_request_user", return_value=user):
            revisit = client.get(f"/reports/{report.id}")
        self.assertEqual(revisit.status_code, 200)
        self.assertIn("Some guidance from this report may be ready to revisit", revisit.text)

    def _create_report_with_anchors(self):
        user = db_mod.AppUser(
            email="interp@example.com",
            password_hash="hash",
            name="Interp User",
            plan_code="premium",
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)

        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        report = db_mod.GeneratedReport(
            user_id=user.id,
            report_type="premium",
            title="Test Report",
            full_name="Interp User",
            birth_date="1990-01-01",
            birth_time="08:30",
            birth_city="Besiktas, Istanbul, Turkey",
            normalized_birth_place="Besiktas, Istanbul, Marmara Region, Turkey",
            timezone="Europe/Istanbul",
            interpretation_context_json=json.dumps({
                "signal_layer": layer["anchors"],
                "recommendation_layer": layer["recommendation_layer"],
            }),
            result_payload_json=json.dumps({
                "generated_report_id": 1,
                "full_name": "Interp User",
                "birth_date": "1990-01-01",
                "birth_time": "08:30",
                "birth_city": "Besiktas, Istanbul, Turkey",
                "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
                "timezone": "Europe/Istanbul",
                "report_type": "premium",
                "interpretation_context": {
                    "primary_focus": "career",
                    "secondary_focus": "growth",
                    "dominant_narratives": ["career_pressure_axis"],
                    "dominant_life_areas": ["career"],
                    "signal_layer": layer["anchors"],
                    "recommendation_layer": layer["recommendation_layer"],
                },
            }),
        )
        self.db.add(report)
        self.db.commit()
        self.db.refresh(report)
        payload = json.loads(report.result_payload_json)
        payload["generated_report_id"] = report.id
        report.result_payload_json = json.dumps(payload)
        self.db.commit()
        self.db.refresh(report)
        return report, user


if __name__ == "__main__":
    unittest.main()
