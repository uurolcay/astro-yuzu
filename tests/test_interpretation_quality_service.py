import json
import unittest

import database as db_mod
from services import interpretation_quality_service as quality_svc


class _BrokenChart:
    def get(self, *_args, **_kwargs):
        raise RuntimeError("boom")


class InterpretationQualityServiceTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.PromptInsight).delete()
        self.db.query(db_mod.InterpretationReview).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="quality-admin@example.com",
            password_hash="hash",
            name="Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.profile = db_mod.InternalProfile(
            full_name="Quality Profile",
            birth_date="1990-01-01",
            birth_time="08:30",
            birth_place_label="Istanbul",
        )
        self.db.add(self.admin)
        self.db.add(self.profile)
        self.db.flush()
        self.interpretation = db_mod.InternalInterpretation(
            profile=self.profile,
            report_type="career",
            input_payload_json=json.dumps(
                {
                    "natal_data": {
                        "planets": [
                            {"name": "Saturn", "nakshatra": "Rohini"},
                            {"name": "Moon", "nakshatra": "Hasta"},
                        ],
                        "ascendant": {"nakshatra": "Chitra"},
                        "active_dasha": {"planet": "Saturn"},
                    }
                },
                ensure_ascii=False,
            ),
            interpretation_text="This report mentions Moon, Rohini, timing, action, summary, and focus but omits the slow karmic planet.",
            created_by_user_id=self.admin.id,
        )
        self.db.add(self.interpretation)
        self.db.commit()

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.PromptInsight).delete()
        self.db.query(db_mod.InterpretationReview).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _valid_review_form(self, **overrides):
        payload = {
            "language": "en",
            "status": "reviewed",
            "rating_overall": "4",
            "rating_clarity": "4",
            "rating_depth": "3",
            "rating_accuracy_feel": "4",
            "rating_actionability": "4",
            "rating_tone": "5",
            "admin_feedback": "Solid but missing Saturn emphasis.",
            "improvement_notes": "Increase Saturn coverage.",
            "strong_sections": "summary",
            "weak_sections": "timing",
            "safety_flags": "deterministic_prediction",
        }
        payload.update(overrides)
        return payload

    def test_save_review_creates_interpretation_review_with_correct_fields(self):
        review = quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        self.assertEqual(review.interpretation_id, self.interpretation.id)
        self.assertEqual(review.rating_overall, 4)
        self.assertEqual(review.status, "reviewed")

    def test_save_review_auto_computes_quality_eval_json(self):
        review = quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        self.assertTrue(review.quality_eval_json)

    def test_save_review_auto_computes_missing_entities_json(self):
        review = quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        self.assertTrue(review.missing_entities_json)

    def test_save_review_auto_computes_section_coverage_json(self):
        review = quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        self.assertTrue(review.section_coverage_json)

    def test_save_review_rejects_rating_outside_range(self):
        with self.assertRaises(ValueError):
            quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(rating_overall="7"), admin_user=self.admin)

    def test_save_review_rejects_invalid_status(self):
        with self.assertRaises(ValueError):
            quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(status="bad"), admin_user=self.admin)

    def test_save_review_rejects_unknown_safety_flag(self):
        with self.assertRaises(ValueError):
            quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(safety_flags="unknown_flag"), admin_user=self.admin)

    def test_save_review_rejects_unknown_section_name(self):
        with self.assertRaises(ValueError):
            quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(weak_sections="not_a_section"), admin_user=self.admin)

    def test_save_review_versioning_sets_parent_version(self):
        first = quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        first_id = first.id
        self.db.commit()
        second = quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(rating_overall="5"), admin_user=self.admin)
        self.assertEqual(second.parent_version_id, first_id)
        self.assertEqual(second.version_number, 2)

    def test_get_review_for_interpretation_returns_none_when_absent(self):
        self.assertIsNone(quality_svc.get_review_for_interpretation(self.db, self.interpretation.id))

    def test_list_reviews_filters_by_status_correctly(self):
        quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(status="approved"), admin_user=self.admin)
        self.db.commit()
        rows = quality_svc.list_reviews(self.db, status="approved")
        self.assertEqual(len(rows), 1)

    def test_list_reviews_filters_by_rating_min_correctly(self):
        quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(rating_overall="4"), admin_user=self.admin)
        self.db.commit()
        rows = quality_svc.list_reviews(self.db, rating_min=5)
        self.assertEqual(len(rows), 0)

    def test_detect_missing_entities_returns_empty_for_empty_chart(self):
        self.assertEqual(quality_svc.detect_missing_entities({}, "text"), [])

    def test_detect_missing_entities_detects_missing_planet(self):
        chart = {"planets": [{"name": "Saturn"}]}
        self.assertEqual(quality_svc.detect_missing_entities(chart, "Moon and Venus only"), ["Saturn"])

    def test_detect_missing_entities_returns_empty_on_exception(self):
        self.assertEqual(quality_svc.detect_missing_entities(_BrokenChart(), "text"), [])

    def test_compute_section_coverage_returns_zero_for_empty_output(self):
        coverage = quality_svc.compute_section_coverage("")
        self.assertEqual(coverage["coverage_score"], 0.0)

    def test_compute_section_coverage_detects_covered_section(self):
        coverage = quality_svc.compute_section_coverage("This summary includes lagna and atmakaraka themes.")
        self.assertIn("identity", coverage["covered_sections"])

    def test_build_quality_dashboard_returns_correct_shape_with_empty_db(self):
        db_mod.init_db()
        empty_db = db_mod.SessionLocal()
        empty_db.query(db_mod.PromptInsight).delete()
        empty_db.query(db_mod.InterpretationReview).delete()
        empty_db.query(db_mod.InternalInterpretation).delete()
        empty_db.query(db_mod.InternalProfile).delete()
        empty_db.query(db_mod.ServiceOrder).delete()
        empty_db.query(db_mod.AppUser).delete()
        empty_db.commit()
        dashboard = quality_svc.build_quality_dashboard(empty_db)
        self.assertEqual(set(dashboard.keys()), {
            "total_reviews", "avg_rating_overall", "by_report_type",
            "top_weak_sections", "top_safety_flags", "top_missing_entities",
            "recent_approved", "recent_rejected",
        })
        empty_db.close()

    def test_build_quality_dashboard_computes_avg_rating_overall(self):
        quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(rating_overall="4"), admin_user=self.admin)
        self.db.commit()
        dashboard = quality_svc.build_quality_dashboard(self.db)
        self.assertEqual(dashboard["avg_rating_overall"], 4.0)

    def test_build_quality_dashboard_top_missing_entities_populated_correctly(self):
        quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        self.db.commit()
        dashboard = quality_svc.build_quality_dashboard(self.db)
        self.assertIn("Saturn", dashboard["top_missing_entities"])

    def test_save_prompt_insight_rejects_invalid_insight_type(self):
        with self.assertRaises(ValueError):
            quality_svc.save_prompt_insight(self.db, {"insight_type": "bad", "title": "X", "body": "Y"}, admin_user=self.admin)

    def test_list_prompt_insights_filters_by_report_type(self):
        quality_svc.save_prompt_insight(
            self.db,
            {"insight_type": "recurring_failure", "title": "A", "body": "B", "report_type": "career"},
            admin_user=self.admin,
        )
        self.db.commit()
        rows = quality_svc.list_prompt_insights(self.db, report_type="career")
        self.assertEqual(len(rows), 1)

    def test_no_service_order_is_created_by_quality_service_functions(self):
        before = self.db.query(db_mod.ServiceOrder).count()
        quality_svc.save_review(self.db, self.interpretation.id, self._valid_review_form(), admin_user=self.admin)
        quality_svc.save_prompt_insight(
            self.db,
            {"insight_type": "recurring_failure", "title": "A", "body": "B", "report_type": "career"},
            admin_user=self.admin,
        )
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before)
