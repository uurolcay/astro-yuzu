import json
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import inspect

import app
import database as db_mod
from services.payments import IyzicoProvider, StripeProvider, get_payment_provider


def sample_interpretation_context():
    return {
        "primary_focus": "career",
        "secondary_focus": "growth",
        "dominant_narratives": ["career expansion"],
        "dominant_life_areas": ["career"],
        "decision_posture": "prepare",
        "timing_strategy": "mixed",
        "confidence_level": "high",
        "top_timing_windows": {
            "peak": {"label": "Peak window", "start": "2026-04-01", "end": "2026-05-01"},
            "opportunity": {"label": "Opportunity window", "start": "2026-05-02", "end": "2026-05-28"},
            "pressure": {"label": "Pressure window", "start": "2026-06-01", "end": "2026-06-21"},
        },
        "signal_layer": {
            "top_anchors": [
                {
                    "rank": 1,
                    "title": "Visible leadership stretch",
                    "summary": "You are being asked to take clearer ownership.",
                    "why_it_matters": "This sets the tone for your current cycle.",
                    "opportunity": "Step into visible responsibility.",
                    "risk": "Do not overextend too early.",
                },
                {
                    "rank": 2,
                    "title": "Private restructuring pressure",
                    "summary": "Behind-the-scenes cleanup is still active.",
                    "why_it_matters": "The hidden workload shapes your pace.",
                    "opportunity": "Simplify commitments.",
                    "risk": "Avoid rushing unstable plans.",
                },
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
                },
                {
                    "title": "Protect recovery time before expansion",
                    "type": "timing",
                    "priority": "medium",
                    "time_window": "Apr 21 - May 5",
                    "reasoning": "Pacing matters before the next expansion window opens.",
                    "linked_anchors": [{"title": "Private restructuring pressure"}],
                },
            ],
            "opportunity_windows": [{"title": "Career opening", "time_window": "May 2 - May 28"}],
            "risk_windows": [{"title": "Pressure cluster", "time_window": "Jun 1 - Jun 21"}],
        },
    }


class _StubProvider:
    def __init__(self, checkout_response=None, verified_payment=None, webhook_payment=None):
        self.checkout_response = checkout_response or {}
        self.verified_payment = verified_payment
        self.webhook_payment = webhook_payment

    def create_checkout_session(self, report, user, success_url, cancel_url):
        return dict(self.checkout_response)

    def verify_payment(self, data):
        return dict(self.verified_payment or {})

    def finalize_purchase(self, report, payment_data):
        payment_reference = payment_data.get("payment_reference")
        if getattr(report, "payment_reference", None) == payment_reference and bool(getattr(report, "is_paid", False)):
            if not getattr(report, "pdf_ready", False):
                report.pdf_ready = True
            return False
        report.access_state = "purchased"
        report.is_paid = True
        report.pdf_ready = True
        report.unlocked_at = getattr(report, "unlocked_at", None) or payment_data.get("completed_at")
        if payment_reference:
            report.payment_reference = payment_reference
        return True

    def verify_webhook(self, request):
        return dict(self.webhook_payment or {}) if self.webhook_payment else None


class MonetizationFlowTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
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
        self.db.query(db_mod.EmailCapture).delete()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def test_checkout_route_requires_auth(self):
        report, _user = self._create_report()
        response = self.client.post(f"/api/v1/reports/{report.id}/checkout")
        self.assertEqual(response.status_code, 401)

    def test_checkout_route_enforces_ownership(self):
        report, _owner = self._create_report(email="owner@example.com")
        intruder = self._create_user("intruder@example.com")
        with patch.object(app, "get_request_user", return_value=intruder):
            response = self.client.post(f"/api/v1/reports/{report.id}/checkout")
        self.assertEqual(response.status_code, 404)

    def test_checkout_route_returns_provider_redirect(self):
        report, user = self._create_report()
        provider = _StubProvider(
            checkout_response={"session_id": "cs_test_123", "redirect_url": "https://checkout.example/session/cs_test_123"}
        )
        with (
            patch.object(app.payments, "payments_enabled", return_value=True),
            patch.object(app.payments, "can_use_beta_free_unlock", return_value=False),
            patch.object(app.payments, "get_payment_provider", return_value=provider),
            patch.object(app, "get_request_user", return_value=user),
        ):
            response = self.client.post(f"/api/v1/reports/{report.id}/checkout")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["checkout_session_id"], "cs_test_123")
        self.assertEqual(payload["redirect_url"], "https://checkout.example/session/cs_test_123")

    def test_success_flow_unlocks_report_safely(self):
        report, user = self._create_report()
        app.capture_email_lead(self.db, email=user.email, report_id=report.id, source="result_page")
        self.db.commit()
        payment_data = {
            "session_id": "cs_paid_1",
            "payment_reference": "pi_paid_1",
            "report_id": report.id,
            "user_id": user.id,
            "payment_status": "paid",
        }
        provider = _StubProvider(verified_payment=payment_data)
        with (
            patch.object(app.payments, "get_payment_provider", return_value=provider),
            patch.object(app, "get_request_user", return_value=user),
        ):
            response = self.client.get(f"/checkout/success?report_id={report.id}&session_id=cs_paid_1", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn(f"/reports/{report.id}?unlocked=1", response.headers["location"])
        self.db.refresh(report)
        self.assertEqual(app.get_report_access_state(report), "purchased")
        self.assertEqual(report.payment_reference, "pi_paid_1")
        capture = self.db.query(db_mod.EmailCapture).filter(db_mod.EmailCapture.report_id == report.id).first()
        self.assertTrue(capture.is_converted)
        self.assertIsNotNone(capture.converted_at)

    def test_cancel_flow_preserves_preview_state(self):
        report, user = self._create_report()
        with patch.object(app, "get_request_user", return_value=user):
            response = self.client.get(f"/checkout/cancel?report_id={report.id}", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn(f"/reports/{report.id}?checkout=cancelled", response.headers["location"])
        self.db.refresh(report)
        self.assertEqual(app.get_report_access_state(report), "preview")

    def test_repeated_success_handling_is_idempotent(self):
        report, user = self._create_report()
        payment_data = {
            "session_id": "cs_paid_repeat",
            "payment_reference": "pi_repeat_1",
            "report_id": report.id,
            "user_id": user.id,
            "payment_status": "paid",
        }
        provider = _StubProvider(verified_payment=payment_data)
        with (
            patch.object(app.payments, "get_payment_provider", return_value=provider),
            patch.object(app, "get_request_user", return_value=user),
        ):
            first = self.client.get(f"/checkout/success?report_id={report.id}&session_id=cs_paid_repeat", follow_redirects=False)
            second = self.client.get(f"/checkout/success?report_id={report.id}&session_id=cs_paid_repeat", follow_redirects=False)
        self.assertEqual(first.status_code, 303)
        self.assertEqual(second.status_code, 303)
        self.db.refresh(report)
        self.assertEqual(app.get_report_access_state(report), "purchased")
        self.assertEqual(report.payment_reference, "pi_repeat_1")

    def test_pdf_access_respects_paid_or_unlocked_state(self):
        report, user = self._create_report()
        with patch.object(app, "get_request_user", return_value=user):
            blocked = self.client.post("/report/pdf", data={"payload_json": report.result_payload_json})
        self.assertEqual(blocked.status_code, 403)

        app.mark_report_as_paid(report, payment_reference="pi_pdf_1")
        self.db.commit()
        with (
            patch.object(app, "get_request_user", return_value=user),
            patch.object(app, "_generate_pdf_bytes_from_report", return_value=b"%PDF-test"),
        ):
            allowed = self.client.post("/report/pdf", data={"payload_json": report.result_payload_json})
        self.assertEqual(allowed.status_code, 200)
        self.db.refresh(report)
        self.assertEqual(app.get_report_access_state(report), "delivered")

    def test_beta_free_unlock_works_only_when_enabled(self):
        report, user = self._create_report()
        with (
            patch.object(app.payments, "can_use_beta_free_unlock", return_value=False),
            patch.object(app, "get_request_user", return_value=user),
        ):
            blocked = self.client.post(f"/api/v1/reports/{report.id}/beta-unlock")
        self.assertEqual(blocked.status_code, 403)

        with (
            patch.object(app.payments, "can_use_beta_free_unlock", return_value=True),
            patch.object(app, "get_request_user", return_value=user),
        ):
            allowed = self.client.post(f"/api/v1/reports/{report.id}/beta-unlock")
        self.assertEqual(allowed.status_code, 200)
        self.db.refresh(report)
        self.assertEqual(app.get_report_access_state(report), "unlocked")
        self.assertEqual(report.payment_reference, "beta-free-unlock")

    def test_beta_allowlist_is_enforced(self):
        user = self._create_user("beta@example.com")
        with (
            patch.dict("os.environ", {"BETA_FREE_UNLOCK": "true", "BETA_UNLOCK_EMAILS": "beta@example.com"}),
        ):
            self.assertTrue(app.payments.can_use_beta_free_unlock(user))
        with patch.dict("os.environ", {"BETA_FREE_UNLOCK": "true", "BETA_UNLOCK_EMAILS": "other@example.com"}):
            self.assertFalse(app.payments.can_use_beta_free_unlock(user))

    def test_payment_fields_can_be_unset_safely_in_dev_mode(self):
        report, _user = self._create_report()
        access = app._build_report_access_context(report)
        self.assertEqual(access["access_state"], "preview")
        self.assertIsNone(access["payment_reference"])
        self.assertFalse(access["can_download_pdf"])

    def test_preview_state_renders_checkout_cta(self):
        report, user = self._create_report()
        with (
            patch.object(app.payments, "payments_enabled", return_value=True),
            patch.object(app.payments, "can_use_beta_free_unlock", return_value=False),
            patch.object(app, "get_request_user", return_value=user),
        ):
            response = self.client.get(f"/reports/{report.id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Unlock Your Full Reading", response.text)
        self.assertIn("Enter your email to save your reading", response.text)
        self.assertIn("Save &amp; Continue", response.text)

    def test_beta_users_see_beta_unlock_cta(self):
        report, user = self._create_report()
        with (
            patch.object(app.payments, "payments_enabled", return_value=False),
            patch.object(app.payments, "can_use_beta_free_unlock", return_value=True),
            patch.object(app, "get_request_user", return_value=user),
        ):
            response = self.client.get(f"/reports/{report.id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Unlock beta access", response.text)

    def test_paid_status_is_reflected_in_history(self):
        preview_report, user = self._create_report(email="history@example.com")
        purchased_report, _ = self._create_report(user=user)
        purchased_report = self.db.get(db_mod.GeneratedReport, purchased_report.id)
        app.mark_report_as_paid(purchased_report, payment_reference="pi_history")
        self.db.commit()
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Preview", response.text)
        self.assertIn("Purchased", response.text)
        self.assertIn("View preview", response.text)
        self.assertIn("View full report", response.text)

    def test_reports_page_product_cards_have_differentiated_copy(self):
        _report, user = self._create_report(email="selection@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Personal Vedic Reading", response.text)
        self.assertIn("Clarity for your own life patterns, current timing, and next-step focus.", response.text)
        self.assertIn("Understand your core life themes and current focus", response.text)
        self.assertIn("Unlock Your Full Reading", response.text)
        self.assertIn("Parent-Child Guidance Report", response.text)
        self.assertIn("Supportive insight into how your child feels, learns, and responds to your approach.", response.text)
        self.assertIn("emotional needs and sensitivity patterns", response.text)
        self.assertIn("Unlock Your Full Child Report", response.text)

    def test_reports_page_includes_pricing_reassurance_and_bundle_note(self):
        _report, user = self._create_report(email="bundle@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("One-time payment. No subscription. Secure checkout.", response.text)
        self.assertIn("A fuller family picture", response.text)
        self.assertIn("pair both readings", response.text)

    def test_reports_page_includes_decision_guidance_block(self):
        _report, user = self._create_report(email="decision@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn('aria-label="report-decision-guidance"', response.text)
        self.assertIn("Which reading is right for you?", response.text)
        self.assertIn("Start with a Personal Reading", response.text)
        self.assertIn("Choose the Child Report", response.text)

    def test_reports_decision_guidance_stays_short_and_does_not_break_cards(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\reports.html").read_text(encoding="utf-8")
        self.assertIn('t("reports.personal_subtitle")', template)
        self.assertIn('t("reports.child_subtitle")', template)
        self.assertIn('t("reports.bundle_copy_1")', template)
        self.assertNotIn("data-reports-i18n", template)

    def test_reports_template_includes_tr_and_en_product_copy_support(self):
        translations_module = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\translations.py").read_text(encoding="utf-8")
        self.assertIn('"personal_cta": "Tüm Raporu Aç"', translations_module)
        self.assertIn('"child_cta": "Çocuk Raporunun Tamamını Aç"', translations_module)
        self.assertIn('"child_cta": "Unlock Your Full Child Report"', translations_module)

    def test_webhook_finalizes_purchase_idempotently(self):
        report, user = self._create_report()
        app.capture_email_lead(self.db, email=user.email, report_id=report.id, source="preview_gate")
        self.db.commit()
        payment_data = {
            "session_id": "cs_webhook_1",
            "payment_reference": "pi_webhook_1",
            "payment_status": "paid",
            "report_id": report.id,
            "user_id": user.id,
        }
        provider = _StubProvider(webhook_payment=payment_data)
        with (
            patch.object(app.payments, "get_payment_provider", return_value=provider),
        ):
            first = self.client.post("/webhooks/payments", content=b"{}", headers={"stripe-signature": "sig"})
            second = self.client.post("/webhooks/payments", content=b"{}", headers={"stripe-signature": "sig"})
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.db.refresh(report)
        self.assertEqual(app.get_report_access_state(report), "purchased")
        self.assertEqual(report.payment_reference, "pi_webhook_1")
        capture = self.db.query(db_mod.EmailCapture).filter(db_mod.EmailCapture.report_id == report.id).first()
        self.assertTrue(capture.is_converted)

    def test_email_capture_endpoint_stores_valid_email(self):
        report, _user = self._create_report()
        response = self.client.post(
            "/api/v1/email-capture",
            json={"email": "reader@example.com", "report_id": report.id, "source": "result_page"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        capture = self.db.query(db_mod.EmailCapture).filter(db_mod.EmailCapture.report_id == report.id).first()
        self.assertEqual(capture.email, "reader@example.com")
        self.assertEqual(capture.source, "result_page")

    def test_invalid_email_capture_is_rejected(self):
        response = self.client.post(
            "/api/v1/email-capture",
            json={"email": "not-an-email", "source": "result_page"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_email")

    def test_duplicate_email_capture_is_handled_safely(self):
        report, _user = self._create_report()
        first = self.client.post(
            "/api/v1/email-capture",
            json={"email": "reader@example.com", "report_id": report.id, "source": "result_page"},
        )
        second = self.client.post(
            "/api/v1/email-capture",
            json={"email": "reader@example.com", "report_id": report.id, "source": "preview_gate"},
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertTrue(first.json()["created"])
        self.assertFalse(second.json()["created"])
        captures = self.db.query(db_mod.EmailCapture).filter(db_mod.EmailCapture.report_id == report.id).all()
        self.assertEqual(len(captures), 1)

    def test_mark_email_capture_converted_updates_capture(self):
        report, user = self._create_report()
        capture, _ = app.capture_email_lead(self.db, email=user.email, report_id=report.id, source="result_page")
        self.db.commit()
        changed = app.mark_email_capture_converted(self.db, report=report, email=user.email)
        self.db.commit()
        self.assertEqual(changed, 1)
        self.db.refresh(capture)
        self.assertTrue(capture.is_converted)
        self.assertIsNotNone(capture.converted_at)

    def test_abandoned_unlock_query_returns_unconverted_rows(self):
        report, _user = self._create_report()
        old_capture = db_mod.EmailCapture(
            email="waiting@example.com",
            report_id=report.id,
            source="result_page",
            created_at=app.datetime.utcnow() - app.timedelta(days=2),
            is_converted=False,
        )
        fresh_capture = db_mod.EmailCapture(
            email="fresh@example.com",
            report_id=None,
            source="bottom_cta",
            created_at=app.datetime.utcnow(),
            is_converted=False,
        )
        converted_capture = db_mod.EmailCapture(
            email="done@example.com",
            report_id=None,
            source="bottom_cta",
            created_at=app.datetime.utcnow() - app.timedelta(days=2),
            is_converted=True,
            converted_at=app.datetime.utcnow() - app.timedelta(days=1),
        )
        self.db.add_all([old_capture, fresh_capture, converted_capture])
        self.db.commit()
        abandoned = app.get_abandoned_unlocks(self.db, days=1)
        self.assertEqual(len(abandoned), 1)
        self.assertEqual(abandoned[0]["email"], "waiting@example.com")

    def test_recovery_email_content_returns_structured_copy(self):
        content = app.build_recovery_email_content(
            {"full_name": "Reader", "recommendation_title": "Visible leadership stretch"}
        )
        self.assertEqual(content["subject"], "Your reading is still waiting")
        self.assertIn("Return to unlock", content["preview_text"])
        self.assertIn("Visible leadership stretch", content["body"])

    def test_provider_selection_defaults_to_stripe(self):
        with patch.dict("os.environ", {"PAYMENT_PROVIDER": "stripe"}):
            provider = get_payment_provider()
        self.assertIsInstance(provider, StripeProvider)

    def test_provider_selection_supports_iyzico(self):
        with patch.dict("os.environ", {"PAYMENT_PROVIDER": "iyzico"}):
            provider = get_payment_provider()
        self.assertIsInstance(provider, IyzicoProvider)

    def _create_user(self, email):
        user = db_mod.AppUser(
            email=email,
            password_hash="hash",
            name="Member",
            plan_code="premium",
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def _create_report(self, user=None, email="member@example.com"):
        if user is None:
            user = self._create_user(email)
        else:
            user = self._bound_user(user)
        interpretation_context = sample_interpretation_context()
        payload = {
            "generated_report_id": None,
            "full_name": "Preview Client",
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
            title="Monetization Report",
            full_name="Preview Client",
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
        identity = inspect(user).identity
        user_id = identity[0] if identity else user.__dict__.get("id")
        return self.db.get(db_mod.AppUser, user_id) or self.db.merge(user)


if __name__ == "__main__":
    unittest.main()
