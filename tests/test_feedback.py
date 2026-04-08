import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod


def sample_feedback_context():
    return {
        "primary_focus": "career",
        "secondary_focus": "growth",
        "dominant_narratives": ["career expansion"],
        "dominant_life_areas": ["career"],
        "decision_posture": "prepare",
        "timing_strategy": "mixed",
        "signal_layer": {
            "top_anchors": [
                {
                    "rank": 1,
                    "title": "Visible leadership stretch",
                    "summary": "You are being asked to take clearer ownership.",
                    "why_it_matters": "This sets the tone for your current cycle.",
                    "opportunity": "Step into visible responsibility.",
                    "risk": "Do not overextend too early.",
                }
            ],
        },
        "recommendation_layer": {
            "top_recommendations": [
                {
                    "title": "Prioritize the clearest career move",
                    "type": "focus",
                    "priority": "high",
                    "time_window": "Apr 1 - Apr 20",
                    "reasoning": "The chart currently rewards visible and deliberate career choices.",
                    "linked_anchors": [{"title": "Visible leadership stretch"}],
                }
            ],
        },
    }


class FeedbackFlowTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.FeedbackEntry).delete()
        self.db.query(db_mod.EmailCapture).delete()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.FeedbackEntry).delete()
        self.db.query(db_mod.EmailCapture).delete()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def test_feedback_endpoint_accepts_valid_data(self):
        report, _user = self._create_report()
        response = self.client.post(
            "/api/v1/feedback",
            json={
                "report_id": report.id,
                "report_type": "premium",
                "stage": "preview",
                "rating": "very_accurate",
                "comment": "The opening summary felt aligned.",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        saved = self.db.query(db_mod.FeedbackEntry).filter(db_mod.FeedbackEntry.report_id == report.id).one()
        self.assertEqual(saved.stage, "preview")
        self.assertEqual(saved.rating, "very_accurate")

    def test_invalid_rating_rejected(self):
        report, _user = self._create_report()
        response = self.client.post(
            "/api/v1/feedback",
            json={
                "report_id": report.id,
                "report_type": "premium",
                "stage": "preview",
                "rating": "wrong",
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_feedback_rating")

    def test_anonymous_submission_works(self):
        report, _user = self._create_report()
        response = self.client.post(
            "/api/v1/feedback",
            json={
                "report_id": report.id,
                "report_type": "premium",
                "stage": "preview",
                "rating": "somewhat",
            },
        )
        self.assertEqual(response.status_code, 200)
        entry = self.db.query(db_mod.FeedbackEntry).filter(db_mod.FeedbackEntry.report_id == report.id).one()
        self.assertIsNone(entry.user_id)

    def test_preview_feedback_stored(self):
        report, user = self._create_report()
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.post(
                "/api/v1/feedback",
                json={
                    "report_id": report.id,
                    "report_type": "premium",
                    "stage": "preview",
                    "rating": "not_really",
                    "comment": "The focus area was a little unclear.",
                },
            )
        self.assertEqual(response.status_code, 200)
        entry = self.db.query(db_mod.FeedbackEntry).filter(db_mod.FeedbackEntry.report_id == report.id).one()
        self.assertEqual(entry.stage, "preview")
        self.assertEqual(entry.comment, "The focus area was a little unclear.")
        self.assertEqual(entry.user_id, user.id)

    def test_full_feedback_stored(self):
        report, _user = self._create_report()
        response = self.client.post(
            "/api/v1/feedback",
            json={
                "report_id": report.id,
                "report_type": "premium",
                "stage": "full",
                "rating": "very_helpful",
                "comment": "The timing notes helped most.",
                "recommend_flag": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        entry = self.db.query(db_mod.FeedbackEntry).filter(db_mod.FeedbackEntry.report_id == report.id).one()
        self.assertEqual(entry.stage, "full")
        self.assertTrue(entry.recommend_flag)

    def test_result_page_renders_feedback_blocks(self):
        preview_report, user = self._create_report(email="preview-feedback@example.com")
        full_report, _ = self._create_report(user=user, email="preview-feedback@example.com")
        full_report = self.db.get(db_mod.GeneratedReport, full_report.id)
        app.mark_report_as_paid(full_report, payment_reference="pi_feedback")
        self.db.commit()

        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            preview_response = self.client.get(f"/reports/{preview_report.id}")
            full_response = self.client.get(f"/reports/{full_report.id}")

        self.assertEqual(preview_response.status_code, 200)
        self.assertIn("Did this feel accurate so far?", preview_response.text)
        self.assertIn('data-feedback-entry-form', preview_response.text)
        self.assertEqual(full_response.status_code, 200)
        self.assertIn("Was this reading helpful?", full_response.text)
        self.assertIn("I would recommend this", full_response.text)

    def test_template_is_safe_when_report_id_missing(self):
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None, lang="en")),
            "full_name": "No Report Id",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Besiktas, Istanbul, Turkey",
            "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
            "language": "en",
            "report_type": "premium",
            "report_type_value": "premium",
            "interpretation_context": sample_feedback_context(),
            "recommendation_layer": sample_feedback_context()["recommendation_layer"],
            "top_anchors": sample_feedback_context()["signal_layer"]["top_anchors"],
            "visible_top_recommendations": sample_feedback_context()["recommendation_layer"]["top_recommendations"],
            "visible_top_anchors": sample_feedback_context()["signal_layer"]["top_anchors"],
            "payload_json": {},
            "report_access": {
                "is_preview": True,
                "show_unlock_cta": True,
                "can_unlock_here": False,
                "can_view_full_report": False,
                "can_download_pdf": False,
                "show_login_hint": False,
                "unlock_success": False,
                "access_label": "Preview",
                "checkout_mode": "payment",
            },
            "related_articles": [],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("Focus Astrology | Result", html)
        self.assertNotIn('data-stage="preview"', html)
        self.assertNotIn('data-stage="full"', html)

    def _create_user(self, email):
        user = db_mod.AppUser(
            email=email,
            password_hash="hash",
            name="Reader",
            plan_code="premium",
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def _create_report(self, user=None, email="feedback@example.com"):
        if user is None:
            user = self._create_user(email)
        else:
            user = self._bound_user(user)
        interpretation_context = sample_feedback_context()
        payload = {
            "generated_report_id": None,
            "full_name": "Feedback Reader",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Besiktas, Istanbul, Turkey",
            "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "premium",
            "interpretation_context": interpretation_context,
            "ai_interpretation": "### Summary\nA premium reading.\n\n### Guidance\nMore detailed guidance follows here.",
            "calculation_config": {
                "zodiac": "sidereal",
                "ayanamsa": "lahiri",
                "node_mode": "true",
                "house_system": "whole_sign",
                "engine_version": "test-suite",
            },
        }
        report = db_mod.GeneratedReport(
            user_id=user.id,
            report_type="premium",
            title="Feedback Report",
            full_name="Feedback Reader",
            birth_date="1990-01-01",
            birth_time="08:30",
            birth_city="Besiktas, Istanbul, Turkey",
            normalized_birth_place="Besiktas, Istanbul, Marmara Region, Turkey",
            timezone="Europe/Istanbul",
            interpretation_context_json=json.dumps(interpretation_context),
            result_payload_json=json.dumps(payload),
            access_state="preview",
            is_paid=False,
            pdf_ready=False,
        )
        self.db.add(report)
        self.db.flush()
        payload["generated_report_id"] = report.id
        report.result_payload_json = json.dumps(payload)
        self.db.commit()
        self.db.refresh(report)
        return report, user

    def _bound_user(self, user):
        return self.db.get(db_mod.AppUser, user.id) or self.db.merge(user)


if __name__ == "__main__":
    unittest.main()
