import json
import re
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


def sample_paid_chart_bundle():
    birth_context = {
        "raw_birth_place_input": "Istanbul, Turkey",
        "normalized_birth_place": "Istanbul, Turkey",
        "latitude": 41.0082,
        "longitude": 28.9784,
        "timezone": "Europe/Istanbul",
        "geocode_provider": "test-suite",
        "geocode_confidence": 0.99,
    }
    return {
        "birth_context": birth_context,
        "natal_data": {
            "planets": [
                {"name": "Sun", "sign_idx": 0, "degree": 10.0, "nakshatra": "Ashwini", "abs_longitude": 10.0},
                {"name": "Moon", "sign_idx": 7, "degree": 21.2, "nakshatra": "Vishakha", "abs_longitude": 201.2},
                {"name": "Mars", "sign_idx": 8, "degree": 5.0, "nakshatra": "Mula", "abs_longitude": 245.0},
                {"name": "Mercury", "sign_idx": 5, "degree": 7.0, "nakshatra": "Hasta", "abs_longitude": 157.0},
                {"name": "Jupiter", "sign_idx": 3, "degree": 28.0, "nakshatra": "Punarvasu", "abs_longitude": 88.0},
                {"name": "Venus", "sign_idx": 11, "degree": 4.0, "nakshatra": "Purva Bhadrapada", "abs_longitude": 334.0},
                {"name": "Saturn", "sign_idx": 6, "degree": 2.0, "nakshatra": "Swati", "abs_longitude": 182.0},
                {"name": "Rahu", "sign_idx": 0, "degree": 25.0, "nakshatra": "Bharani", "abs_longitude": 25.0},
                {"name": "Ketu", "sign_idx": 6, "degree": 25.0, "nakshatra": "Swati", "abs_longitude": 205.0},
            ],
            "ascendant": {"sign_idx": 3, "degree": 4.2, "abs_longitude": 94.2},
        },
        "navamsa_data": {
            "planets": [
                {"name": "Sun", "sign_idx": 0},
                {"name": "Moon", "sign_idx": 7},
                {"name": "Jupiter", "sign_idx": 3},
                {"name": "Venus", "sign_idx": 11},
            ]
        },
        "dasha_data": [
            {"planet": "Jupiter", "start": "2025-01-01", "end": "2027-12-31"},
            {"planet": "Saturn", "start": "2028-01-01", "end": "2030-12-31"},
        ],
        "transit_data": [{"planet": "Jupiter", "title": "Transit emphasis"}],
        "interpretation_context": sample_interpretation_context(),
        "calculation_config": {
            "zodiac": "sidereal",
            "ayanamsa": "lahiri",
            "node_mode": "true",
            "house_system": "whole_sign",
            "engine_version": "test-suite",
        },
        "calculation_metadata": {"engine_version": "test-suite"},
    }


def sample_parent_child_paid_order_payload():
    child_bundle = sample_paid_chart_bundle()
    parent_bundle = sample_paid_chart_bundle()
    parent_bundle["natal_data"] = {
        "planets": [
            {"name": "Sun", "sign_idx": 4, "degree": 9.1, "nakshatra": "Magha", "abs_longitude": 129.1},
            {"name": "Moon", "sign_idx": 3, "degree": 3.2, "nakshatra": "Pushya", "abs_longitude": 93.2},
            {"name": "Mars", "sign_idx": 7, "degree": 26.5, "nakshatra": "Jyeshtha", "abs_longitude": 236.5},
            {"name": "Mercury", "sign_idx": 5, "degree": 23.0, "nakshatra": "Hasta", "abs_longitude": 173.0},
            {"name": "Jupiter", "sign_idx": 1, "degree": 14.3, "nakshatra": "Rohini", "abs_longitude": 44.3},
            {"name": "Venus", "sign_idx": 1, "degree": 17.6, "nakshatra": "Rohini", "abs_longitude": 47.6},
            {"name": "Saturn", "sign_idx": 9, "degree": 11.9, "nakshatra": "Shravana", "abs_longitude": 281.9},
            {"name": "Rahu", "sign_idx": 0, "degree": 1.0, "nakshatra": "Ashwini", "abs_longitude": 1.0},
            {"name": "Ketu", "sign_idx": 6, "degree": 1.0, "nakshatra": "Swati", "abs_longitude": 181.0},
        ],
        "ascendant": {"nakshatra": "Rohini", "abs_longitude": 44.1, "degree": 14.1, "sign_idx": 1},
    }
    return {
        "full_name": "Child Example",
        "email": "family@example.com",
        "birth_date": "2018-04-01",
        "birth_time": "08:30",
        "birth_city": "Istanbul, Turkey",
        "report_type": "parent_child",
        "user_lang": "tr",
        "natal_data": child_bundle["natal_data"],
        "navamsa_data": child_bundle["navamsa_data"],
        "dasha_data": child_bundle["dasha_data"],
        "interpretation_context": child_bundle["interpretation_context"],
        "parent_profile": {
            "full_name": "Parent Example",
            "birth_date": "1986-02-12",
            "birth_time": "09:15",
            "birth_city": "Ankara, Turkey",
            "birth_country": "Turkey",
        },
        "child_profile_meta": {
            "full_name": "Child Example",
            "birth_date": "2018-04-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "birth_country": "Turkey",
        },
        "parent_natal_data": parent_bundle["natal_data"],
        "parent_dasha_data": parent_bundle["dasha_data"],
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
        self.db.query(db_mod.AdminActionLog).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == "site_usd_try_rate").delete()
        self.db.commit()
        app._RATE_LIMIT_BUCKETS.clear()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.EmailCapture).delete()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.AdminActionLog).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == "site_usd_try_rate").delete()
        self.db.commit()
        app._RATE_LIMIT_BUCKETS.clear()
        self.db.close()

    def _csrf_token_from(self, path="/reports/order/career"):
        response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        marker = 'name="csrf_token" value="'
        self.assertIn(marker, response.text)
        return response.text.split(marker, 1)[1].split('"', 1)[0]

    def _set_site_setting(self, key, value):
        row = self.db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == key).first()
        if not row:
            row = db_mod.SiteSetting(key=key)
            self.db.add(row)
        row.value = value
        self.db.commit()

    def test_security_headers_are_added_to_public_pages(self):
        response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("x-content-type-options"), "nosniff")
        self.assertEqual(response.headers.get("x-frame-options"), "DENY")
        self.assertEqual(response.headers.get("referrer-policy"), "strict-origin-when-cross-origin")
        self.assertIn("geolocation=()", response.headers.get("permissions-policy", ""))

    def test_client_ip_uses_proxy_headers_only_when_trusted(self):
        request = type(
            "RequestStub",
            (),
            {
                "headers": {
                    "x-forwarded-for": "203.0.113.10, 10.0.0.1",
                    "cf-connecting-ip": "203.0.113.11",
                },
                "client": type("ClientStub", (), {"host": "127.0.0.1"})(),
            },
        )()
        with patch.object(app, "TRUST_PROXY", False):
            self.assertEqual(app._client_ip(request), "127.0.0.1")
        with patch.object(app, "TRUST_PROXY", True):
            self.assertEqual(app._client_ip(request), "203.0.113.11")

    def test_rate_limit_blocks_after_threshold(self):
        request = type(
            "RequestStub",
            (),
            {
                "headers": {},
                "client": type("ClientStub", (), {"host": "198.51.100.7"})(),
            },
        )()
        app.enforce_rate_limit(request, "unit_test_scope", limit=2, window_seconds=60)
        app.enforce_rate_limit(request, "unit_test_scope", limit=2, window_seconds=60)
        with self.assertRaises(app.HTTPException) as raised:
            app.enforce_rate_limit(request, "unit_test_scope", limit=2, window_seconds=60)
        self.assertEqual(raised.exception.status_code, 429)

    def test_report_order_rejects_missing_csrf_token(self):
        response = self.client.post(
            "/reports/order/career",
            data={
                "full_name": "Aylin Test",
                "email": "csrf-missing@example.com",
                "birth_date": "1990-01-02",
                "birth_time": "08:30",
                "birth_city": "Istanbul, Turkey",
                "selected_report_type": "career",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 403)

    def test_checkout_post_rejects_missing_csrf_token(self):
        order = db_mod.ServiceOrder(
            order_token="csrf_checkout_order",
            public_token="csrf_checkout_order",
            service_type="report",
            product_type="career",
            status="awaiting_payment",
            amount=app.Decimal("1690.00"),
            amount_label="TRY 1690.00",
            currency="TRY",
        )
        self.db.add(order)
        self.db.commit()
        response = self.client.post(f"/checkout/report/{order.order_token}", follow_redirects=False)
        self.assertEqual(response.status_code, 403)

    def test_checkout_post_accepts_valid_csrf_token(self):
        order = db_mod.ServiceOrder(
            order_token="csrf_checkout_valid",
            public_token="csrf_checkout_valid",
            service_type="report",
            product_type="career",
            status="awaiting_payment",
            amount=app.Decimal("1690.00"),
            amount_label="TRY 1690.00",
            currency="TRY",
        )
        self.db.add(order)
        self.db.commit()
        csrf_token = self._csrf_token_from(f"/checkout/report/{order.order_token}")
        response = self.client.post(
            f"/checkout/report/{order.order_token}",
            data={"csrf_token": csrf_token},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("payments_unavailable", response.headers["location"])

    def test_admin_bootstrap_creates_env_configured_admin_user(self):
        admin_email = f"bootstrap-admin-{int(app.time.time() * 1000000)}@example.com"
        with patch.dict(
            "os.environ",
            {
                "ADMIN_EMAIL": admin_email,
                "ADMIN_PASSWORD": "safe-admin-password-123",
            },
        ):
            result = app._bootstrap_admin_user_from_env()
        self.assertEqual(result["status"], "created")
        user = self.db.query(db_mod.AppUser).filter(db_mod.AppUser.email == admin_email).first()
        self.assertIsNotNone(user)
        self.assertTrue(user.is_admin)
        self.assertTrue(user.is_active)
        self.assertTrue(app.check_password_hash(user.password_hash, "safe-admin-password-123"))

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
        csrf_token = self._csrf_token_from("/reports/order/career")
        with patch.object(app, "get_request_user", return_value=user):
            blocked = self.client.post("/report/pdf", data={"payload_json": report.result_payload_json, "csrf_token": csrf_token})
        self.assertEqual(blocked.status_code, 403)

        app.mark_report_as_paid(report, payment_reference="pi_pdf_1")
        self.db.commit()
        with (
            patch.object(app, "get_request_user", return_value=user),
            patch.object(app, "_generate_pdf_bytes_from_report", return_value=b"%PDF-test"),
        ):
            allowed = self.client.post("/report/pdf", data={"payload_json": report.result_payload_json, "csrf_token": csrf_token})
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
        self.assertIn("Get Personal Consultation", response.text)
        self.assertIn("Bu sonuç neyi gösterir, neyi göstermez?", response.text)
        self.assertIn("Danışmanlık neden farklıdır?", response.text)
        self.assertGreaterEqual(response.text.count('href="/personal-consultation"'), 3)

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
            response = self.client.get("/reports", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Preview", response.text)
        self.assertIn("Purchased", response.text)
        self.assertIn("Önizlemeyi görüntüle", response.text)
        self.assertIn("Tam raporu görüntüle", response.text)

    def test_reports_page_product_cards_have_differentiated_copy(self):
        _report, user = self._create_report(email="selection@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Doğum Haritası Karma’sı", response.text)
        self.assertIn("Yıllık Transit", response.text)
        self.assertIn("Kariyer", response.text)
        self.assertIn("Ebeveyn-Çocuk", response.text)
        self.assertIn("Yaşam temalarınızı, karmik örüntülerinizi", response.text)
        self.assertIn("Önümüzdeki dönemin vurgu alanlarını", response.text)
        self.assertIn("Mesleki yönünüzü, çalışma biçiminizi", response.text)
        self.assertIn("Çocuğun doğasını, ebeveyn-çocuk ilişkisindeki akışı", response.text)
        self.assertIn("Bu Raporu Al", response.text)
        self.assertIn("Danışmanlıkla Derinleştir", response.text)
        self.assertNotIn("Start With Calculator", response.text)
        self.assertIn('class="btn btn-primary" href="/reports/order/birth_chart_karma">Bu Raporu Al', response.text)
        self.assertIn('class="btn btn-primary" href="/reports/order/annual_transit">Bu Raporu Al', response.text)
        self.assertIn('class="btn btn-primary" href="/reports/order/career">Bu Raporu Al', response.text)
        self.assertIn('class="btn btn-primary" href="/reports/parent-child">Bu Raporu Al', response.text)

    def test_reports_page_includes_pricing_reassurance_and_bundle_note(self):
        _report, user = self._create_report(email="bundle@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Paket mimarisi", response.text)
        self.assertIn("₺1.900", response.text)
        self.assertIn("₺1.490", response.text)
        self.assertIn("₺1.690", response.text)
        self.assertIn("₺1.790", response.text)
        self.assertIn("Astrology Deep Dive", response.text)
        self.assertIn("Life Path Bundle", response.text)
        self.assertIn("Full Year Insight Bundle", response.text)
        self.assertIn("Deep Family Insight", response.text)
        self.assertIn("₺7.900", response.text)
        self.assertIn("₺3.290", response.text)
        self.assertIn("₺2.890", response.text)
        self.assertIn("₺3.390", response.text)
        self.assertIn("/reports/order/bundle/life_path_bundle", response.text)

    def test_reports_page_formats_english_prices_with_approximate_usd_from_settings(self):
        self._set_site_setting("site_usd_try_rate", "32.5")

        response = self.client.get("/reports", headers={"accept-language": "en"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("₺1,900 TL", response.text)
        self.assertIn("≈ $58", response.text)
        self.assertIn("Charged in Turkish lira. USD amounts are approximate.", response.text)

    def test_reports_page_hides_approximate_usd_when_rate_setting_is_blank(self):
        self._set_site_setting("site_usd_try_rate", "")

        response = self.client.get("/reports", headers={"accept-language": "en"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("₺1,900 TL", response.text)
        self.assertNotIn("≈ $", response.text)
        self.assertIn("Charged in Turkish lira. USD amounts are approximate.", response.text)

    def test_bundle_order_submission_records_bundle_metadata(self):
        csrf_token = self._csrf_token_from("/reports/order/bundle/life_path_bundle")
        payload = {
            "full_name": "Aylin Bundle",
            "email": "bundle-order@example.com",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "optional_note": "Yaşam yönü ve kariyer",
        }
        payload["csrf_token"] = csrf_token
        response = self.client.post("/reports/order/bundle/life_path_bundle", data=payload, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("/checkout/report/", response.headers["location"])
        order = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.customer_email == "bundle-order@example.com").first()
        self.assertIsNotNone(order)
        self.assertEqual(order.status, "awaiting_payment")
        self.assertEqual(order.product_type, "life_path_bundle")
        self.assertEqual(order.bundle_type, "life_path_bundle")
        self.assertIn("birth_chart_karma", order.included_products_json)
        self.assertIn("career", order.included_products_json)
        self.assertEqual(order.amount_label, "₺3.290")

    def test_reports_page_includes_decision_guidance_block(self):
        _report, user = self._create_report(email="decision@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn('aria-label="Hangi rapor size uygun?"', response.text)
        self.assertIn("Hangi rapor size uygun?", response.text)
        self.assertIn("Genel yaşam temaları", response.text)
        self.assertIn("Ebeveyn-çocuk dinamiği", response.text)
        self.assertIn("Birden fazla soru", response.text)

    def test_reports_page_references_consultation_without_major_spotlight(self):
        _report, user = self._create_report(email="consultation-feature@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("/personal-consultation", response.text)
        self.assertIn("Kişisel Danışmanlık Al", response.text)
        self.assertIn("Rapor mu, danışmanlık mı?", response.text)
        self.assertNotIn('class="reports-consultation-feature"', response.text)

    def test_reports_decision_guidance_stays_short_and_does_not_break_cards(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\reports.html").read_text(encoding="utf-8")
        self.assertIn("Doğum Haritası Karma’sı", template)
        self.assertIn("Yıllık Transit", template)
        self.assertIn("Kariyer", template)
        self.assertIn("Ebeveyn-Çocuk", template)
        self.assertIn("60 dk birebir danışmanlık", template)
        self.assertNotIn("data-reports-i18n", template)
        self.assertIn(".reports-card {", template)
        self.assertIn(".reports-card-actions {", template)
        self.assertIn("margin-top:auto;", template)
        self.assertIn("justify-content:center;", template)
        self.assertIn('href="/reports/parent-child">{{ t("common.cta_report_buy") }}', template)
        self.assertNotIn('class="reports-consultation-feature"', template)
        self.assertNotIn("Hesaplayıcı ile Başla", template)

    def test_reports_template_includes_tr_and_en_product_copy_support(self):
        translations_module = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\translations.py").read_text(encoding="utf-8")
        self.assertIn('"order_cta": "Sipariş Ver"', translations_module)
        self.assertIn('"order_cta": "Order Now"', translations_module)
        self.assertIn('"price_placeholder": "Yakında paylaşılacak"', translations_module)
        self.assertIn('"price_placeholder": "Available soon"', translations_module)

    def test_reports_page_renders_turkish_order_cta_without_old_label(self):
        _report, user = self._create_report(email="selection-tr@example.com")
        with patch.object(app, "get_request_user", return_value=self._bound_user(user)):
            response = self.client.get("/reports", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Bu Raporu Al", response.text)
        self.assertNotIn("Hesaplayıcı ile Başla", response.text)

    def test_report_order_form_captures_explicit_report_type(self):
        response = self.client.get("/reports/order/birth_chart_karma", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Doğum Haritası Karma’sı", response.text)
        self.assertIn('name="selected_report_type" value="birth_chart_karma"', response.text)
        self.assertIn('name="full_name"', response.text)
        self.assertIn('name="email"', response.text)
        self.assertIn('name="birth_date"', response.text)
        self.assertIn('name="birth_time"', response.text)
        self.assertIn('name="birth_city"', response.text)
        self.assertIn('name="optional_note"', response.text)
        self.assertIn("en geç 7 gün içinde e-posta ile teslim edilir", response.text)

    def test_report_order_submission_routes_to_payment_before_admin_work(self):
        csrf_token = self._csrf_token_from("/reports/order/career")
        payload = {
            "full_name": "Aylin Test",
            "email": "customer@example.com",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "selected_report_type": "career",
            "optional_note": "Kariyer yönü",
        }
        payload["csrf_token"] = csrf_token
        with (
            patch.object(app, "_generate_report_order_draft", return_value=("Taslak metni", "generated")) as draft_mock,
            patch.object(app, "send_report_draft_to_admin", return_value={"status": "sent"}) as admin_mock,
        ):
            response = self.client.post("/reports/order/career", data=payload, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("/checkout/report/", response.headers["location"])
        draft_mock.assert_not_called()
        admin_mock.assert_not_called()
        order = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.customer_email == "customer@example.com").first()
        self.assertIsNotNone(order)
        self.assertEqual(order.status, "awaiting_payment")
        self.assertEqual(order.product_type, "career")

    def test_report_checkout_page_explains_post_payment_draft_boundary(self):
        order = db_mod.ServiceOrder(
            order_token="report_test_token",
            service_type="report",
            product_type="career",
            status="awaiting_payment",
            customer_name="Aylin Test",
            customer_email="customer@example.com",
            amount_label="₺1.690",
            currency="TRY",
        )
        self.db.add(order)
        self.db.commit()
        response = self.client.get("/checkout/report/report_test_token")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Rapor talebiniz ödeme sonrası inceleme sürecine alınır.", response.text)
        self.assertIn("yalnızca yönetici incelemesine gönderilir", response.text)
        self.assertIn("Rapor Ödemesini Başlat", response.text)
        self.assertIn("Full Year Insight Bundle", response.text)

    def test_parent_child_order_uses_existing_parent_child_flow(self):
        response = self.client.get("/reports/order/parent_child", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers.get("location"), "/reports/parent-child")

    def test_consultation_checkout_page_precedes_booking_handoff(self):
        response = self.client.get("/checkout/consultation")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Randevu seçiminizi ödeme adımıyla kesinleştirin.", response.text)
        self.assertIn("Önce Calendly üzerinden uygun zamanı seçersiniz.", response.text)
        self.assertIn("Ödemeyi Başlat", response.text)
        booking = self.client.get("/personal-consultation/book")
        self.assertEqual(booking.status_code, 200)
        self.assertIn('href="/checkout/consultation"', booking.text)
        self.assertIn("Randevu seçimi tek başına satın alma tamamlandığı anlamına gelmez.", booking.text)

    def test_calendly_invitee_created_creates_consultation_order(self):
        payload = self._calendly_payload()

        response = self.client.post("/webhooks/calendly", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["action"], "consultation_booking_created")
        order = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.calendly_invitee_uri == self._calendly_invitee_uri()).first()
        self.assertIsNotNone(order)
        self.assertEqual(order.service_type, "consultation")
        self.assertEqual(order.product_type, "consultation_60_min")
        self.assertEqual(order.status, "booking_pending_payment")
        self.assertEqual(order.customer_email, "client@example.com")
        self.assertEqual(order.calendly_status, "created")
        self.assertIsNone(order.paid_at)

    def test_calendly_duplicate_invitee_created_updates_without_duplicate_order(self):
        payload = self._calendly_payload()

        first = self.client.post("/webhooks/calendly", json=payload)
        second = self.client.post("/webhooks/calendly", json=payload)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["action"], "consultation_booking_updated")
        orders = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.calendly_invitee_uri == self._calendly_invitee_uri()).all()
        self.assertEqual(len(orders), 1)

    def test_calendly_existing_matching_booking_updates_correctly(self):
        scheduled_start = app.datetime(2026, 5, 1, 10, 0, 0)
        order = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="booking_expired",
            paid=False,
            customer_email="client@example.com",
        )
        order.calendly_event_uri = self._calendly_event_uri()
        order.scheduled_start = scheduled_start
        self.db.commit()

        response = self.client.post("/webhooks/calendly", json=self._calendly_payload())

        self.assertEqual(response.status_code, 200)
        self.db.refresh(order)
        self.assertEqual(response.json()["action"], "consultation_booking_updated")
        self.assertEqual(order.status, "booking_pending_payment")
        self.assertEqual(order.calendly_status, "created")
        self.assertEqual(order.calendly_invitee_uri, self._calendly_invitee_uri())

    def test_calendly_invitee_canceled_expires_unpaid_booking(self):
        self.client.post("/webhooks/calendly", json=self._calendly_payload())
        order = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.calendly_invitee_uri == self._calendly_invitee_uri()).first()

        response = self.client.post("/webhooks/calendly", json=self._calendly_payload(event="invitee.canceled"))

        self.assertEqual(response.status_code, 200)
        self.db.refresh(order)
        self.assertEqual(order.status, "booking_expired")
        self.assertEqual(order.calendly_status, "canceled")
        self.assertIsNotNone(order.calendly_canceled_at)

    def test_calendly_paid_cancellation_does_not_auto_refund(self):
        order = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="paid",
            paid=True,
            customer_email="client@example.com",
        )
        order.calendly_event_uri = self._calendly_event_uri()
        order.calendly_invitee_uri = self._calendly_invitee_uri()
        self.db.commit()

        response = self.client.post("/webhooks/calendly", json=self._calendly_payload(event="invitee.canceled"))

        self.assertEqual(response.status_code, 200)
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")
        self.assertIsNone(order.refund_status)
        self.assertIn("admin review required", order.internal_notes)

    def test_calendly_webhook_signature_validation_when_enabled(self):
        payload = self._calendly_payload()
        body = json.dumps(payload).encode("utf-8")
        timestamp = "1714567200"
        secret = "calendly_test_secret"
        signature = app.hmac.new(secret.encode("utf-8"), f"{timestamp}.{body.decode('utf-8')}".encode("utf-8"), app.hashlib.sha256).hexdigest()

        with patch.dict("os.environ", {"CALENDLY_WEBHOOK_SIGNING_KEY": secret}):
            rejected = self.client.post("/webhooks/calendly", content=body, headers={"content-type": "application/json"})
            accepted = self.client.post(
                "/webhooks/calendly",
                content=body,
                headers={"content-type": "application/json", "Calendly-Webhook-Signature": f"t={timestamp},v1={signature}"},
            )

        self.assertEqual(rejected.status_code, 400)
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(accepted.json()["action"], "consultation_booking_created")

    def test_calendly_booking_routes_to_existing_consultation_checkout_flow(self):
        response = self.client.post("/webhooks/calendly", json=self._calendly_payload())
        order_token = response.json()["order_token"]

        checkout = self.client.get(f"/checkout/consultation/{order_token}")

        self.assertEqual(checkout.status_code, 200)
        self.assertIn(f'action="/checkout/consultation/{order_token}"', checkout.text)
        self.assertIn("booking_pending_payment", checkout.text)

    def test_calendly_stores_scheduling_timestamps_as_utc_naive(self):
        response = self.client.post("/webhooks/calendly", json=self._calendly_payload())
        order = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.order_token == response.json()["order_token"]).first()

        self.assertEqual(order.scheduled_start, app.datetime(2026, 5, 1, 10, 0, 0))
        self.assertEqual(order.scheduled_end, app.datetime(2026, 5, 1, 11, 0, 0))

    def test_admin_orders_route_requires_admin_auth(self):
        response = self.client.get("/admin/orders", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("/login", response.headers["location"])

    def test_admin_dashboard_requires_admin_auth(self):
        response = self.client.get("/admin/dashboard", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("/login", response.headers["location"])

    def test_admin_dashboard_route_renders_kpis(self):
        admin = self._create_admin_user()
        self._service_order(status="paid", paid=True, customer_email="dashboard-paid@example.com")
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            response = self.client.get("/admin/dashboard")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Business Dashboard", response.text)
        self.assertIn("Revenue Today", response.text)
        self.assertIn("Consultation Conversion Rate", response.text)
        self.assertIn("Orders Under Review", response.text)

    def test_dashboard_metrics_calculate_revenue_refunds_and_aov(self):
        now = app.datetime.utcnow()
        self._service_order(
            service_type="report",
            product_type="birth_chart_karma",
            status="delivered",
            paid=True,
            amount=app.Decimal("1900.00"),
            paid_at=now,
            delivered_at=now,
            customer_email="karma-kpi@example.com",
        )
        self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="completed",
            paid=True,
            amount=app.Decimal("4900.00"),
            paid_at=now,
            completed_at=now,
            customer_email="consult-kpi@example.com",
        )
        self._service_order(
            service_type="report",
            product_type="annual_transit",
            status="refunded",
            paid=True,
            amount=app.Decimal("1490.00"),
            paid_at=now,
            refund_amount=app.Decimal("1490.00"),
            refunded_at=now,
            customer_email="refund-kpi@example.com",
        )
        self._service_order(
            service_type="report",
            product_type="career",
            status="cancelled",
            paid=True,
            amount=app.Decimal("1690.00"),
            paid_at=now,
            customer_email="cancelled-kpi@example.com",
        )
        manual_review_refund = self._service_order(
            service_type="report",
            product_type="career",
            status="refunded",
            paid=False,
            amount=app.Decimal("1690.00"),
            refund_amount=app.Decimal("1690.00"),
            refunded_at=now,
            customer_email="manual-review-refund-kpi@example.com",
        )
        manual_review_refund.refund_status = "manual_review_needed"
        self.db.commit()
        self._service_order(status="payment_under_review", paid=False, customer_email="review-kpi@example.com")

        dashboard = app.get_dashboard_metrics(self.db, "7d")
        revenue = dashboard["selected_revenue"]

        self.assertEqual(revenue["gross_revenue"], app.Decimal("8290.00"))
        self.assertEqual(revenue["refunded_amount"], app.Decimal("1490.00"))
        self.assertEqual(revenue["net_revenue"], app.Decimal("6800.00"))
        self.assertEqual(revenue["paid_order_count"], 3)
        self.assertEqual(revenue["average_order_value"], app.Decimal("2763.33"))
        self.assertEqual(revenue["refund_count"], 1)
        under_review = [card for card in dashboard["kpis"] if card["label"] == "Orders Under Review"][0]
        self.assertEqual(under_review["value"], 1)
        net_revenue = [card for card in dashboard["kpis"] if card["label"] == "Net Revenue"][0]
        paid_orders = [card for card in dashboard["kpis"] if card["label"] == "Paid Orders"][0]
        self.assertEqual(net_revenue["value"], "TRY 6,800.00")
        self.assertEqual(paid_orders["value"], 3)

    def test_dashboard_consultation_conversion_uses_calendly_bookings(self):
        paid_booking = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="completed",
            paid=True,
            completed_at=app.datetime.utcnow(),
            customer_email="paid-booking@example.com",
        )
        unpaid_booking = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="booking_pending_payment",
            paid=False,
            customer_email="pending-booking@example.com",
        )
        non_calendly = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="paid",
            paid=True,
            customer_email="manual-booking@example.com",
        )
        paid_booking.booking_source = "calendly"
        unpaid_booking.calendly_event_uri = "https://api.calendly.com/scheduled_events/pending"
        non_calendly.booking_source = ""
        non_calendly.calendly_event_uri = None
        non_calendly.calendly_invitee_uri = None
        self.db.commit()

        conversion = app.get_consultation_conversion(self.db, app._admin_dashboard_range("7d"))

        self.assertEqual(conversion["total_bookings"], 2)
        self.assertEqual(conversion["paid_consultations"], 1)
        self.assertEqual(conversion["conversion_rate_label"], "50.0%")

    def test_dashboard_active_bookings_counts_pending_consultation_bookings(self):
        now = app.datetime.utcnow()
        active = self._service_order(
            service_type="consultation",
            status="booking_pending_payment",
            paid=False,
            customer_email="active-booking@example.com",
        )
        active.scheduled_start = now + app.timedelta(days=1)
        active.calendly_status = "created"
        past = self._service_order(
            service_type="consultation",
            status="booking_pending_payment",
            paid=False,
            customer_email="past-booking@example.com",
        )
        past.scheduled_start = now - app.timedelta(hours=1)
        past.calendly_status = "created"
        canceled = self._service_order(
            service_type="consultation",
            status="booking_pending_payment",
            paid=False,
            customer_email="canceled-booking@example.com",
        )
        canceled.scheduled_start = now + app.timedelta(days=1)
        canceled.calendly_status = "canceled"
        canceled.cancelled_at = now
        self.db.commit()

        dashboard = app.get_dashboard_metrics(self.db, "7d")
        active_bookings = [card for card in dashboard["kpis"] if card["label"] == "Active Bookings"][0]

        self.assertEqual(active_bookings["value"], 3)

    def test_dashboard_orders_under_review_counts_payment_under_review_queue(self):
        self._service_order(service_type="report", status="draft_ready", paid=True, customer_email="draft-review@example.com")
        self._service_order(service_type="report", status="under_review", paid=True, customer_email="under-review@example.com")
        self._service_order(service_type="consultation", status="under_review", paid=True, customer_email="consult-review@example.com")
        self._service_order(service_type="report", status="payment_under_review", paid=False, customer_email="payment-review@example.com")

        dashboard = app.get_dashboard_metrics(self.db, "7d")
        under_review = [card for card in dashboard["kpis"] if card["label"] == "Orders Under Review"][0]

        self.assertEqual(under_review["value"], 1)

    def test_dashboard_product_performance_aggregates_by_product(self):
        now = app.datetime.utcnow()
        self._service_order(
            service_type="report",
            product_type="career",
            status="delivered",
            paid=True,
            amount=app.Decimal("1690.00"),
            paid_at=now,
            delivered_at=now,
            customer_email="career-performance@example.com",
        )
        self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="completed",
            paid=True,
            amount=app.Decimal("4900.00"),
            paid_at=now,
            completed_at=now,
            customer_email="consult-performance@example.com",
        )
        self._service_order(
            service_type="report",
            product_type="career",
            status="delivered",
            paid=True,
            amount=app.Decimal("1690.00"),
            paid_at=now,
            delivered_at=None,
            customer_email="internal-review-not-delivered@example.com",
        )
        self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="completed",
            paid=True,
            amount=app.Decimal("4900.00"),
            paid_at=now,
            completed_at=None,
            customer_email="not-actually-completed@example.com",
        )

        rows = {row["product_type"]: row for row in app.get_product_performance(self.db, app._admin_dashboard_range("7d"))}

        self.assertEqual(rows["career"]["paid_order_count"], 2)
        self.assertEqual(rows["career"]["revenue"], app.Decimal("3380.00"))
        self.assertEqual(rows["career"]["delivered_count"], 2)
        self.assertEqual(rows["consultation_60_min"]["paid_order_count"], 2)
        self.assertEqual(rows["consultation_60_min"]["completed_count"], 2)

    def test_dashboard_status_breakdown_counts_expected_statuses(self):
        self._service_order(status="awaiting_payment", paid=False, customer_email="awaiting-breakdown@example.com")
        self._service_order(status="ready_to_send", paid=True, customer_email="ready-breakdown@example.com")
        self._service_order(status="payment_under_review", paid=False, customer_email="review-breakdown@example.com")

        rows = {row["status"]: row["count"] for row in app.get_status_breakdown(self.db, app._admin_dashboard_range("7d"))}

        self.assertEqual(rows["awaiting_payment"], 1)
        self.assertEqual(rows["ready_to_send"], 1)
        self.assertEqual(rows["payment_under_review"], 1)
        self.assertNotIn("booking_expired", rows)

    def test_dashboard_recent_activity_loads_admin_logs(self):
        order = self._service_order(status="paid", paid=True, customer_email="activity@example.com")
        self.db.add(db_mod.AdminActionLog(order_id=order.id, action="refund", actor="admin", metadata_json='{"amount":"1690.00"}'))
        self.db.commit()

        activity = app.get_recent_activity(self.db, limit=5)

        self.assertEqual(activity[0]["order_id"], order.id)
        self.assertEqual(activity[0]["customer"], "activity@example.com")
        self.assertEqual(activity[0]["action"], "refund")

    def test_dashboard_range_filter_limits_paid_order_metrics(self):
        now = app.datetime.utcnow()
        self._service_order(status="paid", paid=True, paid_at=now, created_at=now, customer_email="current-range@example.com")
        self._service_order(
            status="paid",
            paid=True,
            paid_at=now - app.timedelta(days=10),
            created_at=now - app.timedelta(days=10),
            customer_email="old-range@example.com",
        )

        today = app.get_dashboard_metrics(self.db, "1d")
        thirty_days = app.get_dashboard_metrics(self.db, "30d")

        self.assertEqual(today["selected_revenue"]["paid_order_count"], 1)
        self.assertEqual(thirty_days["selected_revenue"]["paid_order_count"], 2)

    def test_paid_report_appears_in_admin_queue(self):
        admin = self._create_admin_user()
        paid_order = self._service_order(
            service_type="report",
            product_type="career",
            status="paid",
            paid=True,
            customer_email="paid-report@example.com",
        )
        unpaid_order = self._service_order(
            service_type="report",
            product_type="annual_transit",
            status="awaiting_payment",
            paid=False,
            customer_email="unpaid-report@example.com",
        )
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.get("/admin/orders")
        self.assertEqual(response.status_code, 200)
        self.assertIn(f"#{paid_order.id}", response.text)
        self.assertIn("paid-report@example.com", response.text)
        self.assertNotIn(f"#{unpaid_order.id}", response.text)
        self.assertNotIn("unpaid-report@example.com", response.text)

    def test_admin_send_report_rejects_unpaid_order(self):
        admin = self._create_admin_user()
        order = self._service_order(status="ready_to_send", paid=False)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.post(f"/admin/orders/{order.id}/send-report", data={"csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        self.db.refresh(order)
        self.assertNotEqual(order.status, "delivered")

    def test_admin_send_report_queues_delivery_tasks(self):
        admin = self._create_admin_user()
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final human-reviewed report.")
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "enqueue_final_report_delivery_tasks", return_value={"pdf": "task_pdf", "delivery": "task_delivery"}) as enqueue_mock,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/send-report", data={"csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=report_queued", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "ready_to_send")
        enqueue_mock.assert_called_once()
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "send_final_report_queued").first()
        self.assertIsNotNone(action)

    def test_admin_duplicate_send_report_is_blocked(self):
        admin = self._create_admin_user()
        order = self._service_order(status="delivered", paid=True, delivered=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "safe_send_template_email") as email_mock,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/send-report", data={"csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        email_mock.assert_not_called()

    def test_admin_report_routes_are_protected(self):
        for path in ["/admin/reports", "/admin/consultations", "/admin/content", "/admin/content/new", "/admin/emails"]:
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)
                self.assertEqual(response.status_code, 303)
                self.assertIn("/login", response.headers["location"])

    def test_admin_reports_list_and_detail_use_written_report_orders(self):
        admin = self._create_admin_user()
        report_order = self._service_order(
            service_type="report",
            product_type="career",
            status="draft_ready",
            paid=True,
            customer_email="written-report@example.com",
        )
        consultation = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="paid",
            paid=True,
            customer_email="consultation-only@example.com",
        )
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            listing = self.client.get("/admin/reports")
            detail = self.client.get(f"/admin/reports/{report_order.id}")

        self.assertEqual(listing.status_code, 200)
        self.assertIn("written-report@example.com", listing.text)
        self.assertNotIn("consultation-only@example.com", listing.text)
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Report Order", detail.text)
        self.assertIn("written-report@example.com", detail.text)
        self.assertNotIn(f"Report Order #{consultation.id}", detail.text)

    def test_admin_report_approve_updates_status_to_ready_to_send(self):
        admin = self._create_admin_user()
        order = self._service_order(service_type="report", status="under_review", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/reports/{order.id}")
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.post(f"/admin/reports/{order.id}/approve", data={"csrf_token": csrf_token}, follow_redirects=False)

        self.assertEqual(response.status_code, 303)
        self.db.refresh(order)
        self.assertEqual(order.status, "ready_to_send")
        self.assertIsNotNone(order.ready_to_send_at)

    def test_admin_report_send_uses_existing_delivery_queue(self):
        admin = self._create_admin_user()
        order = self._service_order(service_type="report", status="ready_to_send", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/reports/{order.id}")
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "enqueue_final_report_delivery_tasks", return_value={"pdf": "task_pdf", "delivery": "queued_after_pdf_success"}) as enqueue_mock,
        ):
            response = self.client.post(f"/admin/reports/{order.id}/send", data={"csrf_token": csrf_token}, follow_redirects=False)

        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=report_queued", response.headers["location"])
        enqueue_mock.assert_called_once()

    def test_admin_report_regenerate_queues_existing_ai_task(self):
        admin = self._create_admin_user()
        order = self._service_order(service_type="report", status="draft_ready", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/reports/{order.id}")
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch("report_tasks.generate_ai_draft_task.delay") as delay_mock,
        ):
            delay_mock.return_value.id = "regen-task"
            response = self.client.post(f"/admin/reports/{order.id}/regenerate", data={"csrf_token": csrf_token}, follow_redirects=False)

        self.assertEqual(response.status_code, 303)
        self.db.refresh(order)
        self.assertEqual(order.ai_draft_status, "pending")
        delay_mock.assert_called_once_with(order.id)

    def test_delivery_does_not_run_before_pdf_exists(self):
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final report.")
        order.pdf_status = "processing"
        order.final_pdf_path = ""
        self.db.commit()
        with patch.object(app, "safe_send_template_email") as email_mock:
            result = app.deliver_final_report_for_order(self.db, order)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "pdf_not_ready")
        email_mock.assert_not_called()
        self.db.refresh(order)
        self.assertEqual(order.status, "ready_to_send")
        self.assertIsNotNone(order.last_task_error)

    def test_generate_pdf_task_triggers_delivery_after_success(self):
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final report.")
        with (
            patch.object(app, "generate_pdf_for_order", return_value={"status": "completed", "path": "report.pdf"}) as pdf_mock,
            patch("email_tasks.deliver_final_report_task.delay") as delivery_delay,
        ):
            delivery_delay.return_value.id = "delivery-task"
            from report_tasks import generate_pdf_task

            result = generate_pdf_task.delay(order.id, deliver_after=True).get()
        self.assertEqual(result["delivery_task_id"], "delivery-task")
        pdf_mock.assert_called_once()
        delivery_delay.assert_called_once_with(order.id)

    def test_final_report_email_includes_pdf_attachment_path(self):
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final report.")
        pdf_path = Path("tmp_final_report_attachment.pdf").resolve()
        pdf_path.write_bytes(b"%PDF-test")
        self.addCleanup(lambda: pdf_path.exists() and pdf_path.unlink())
        order.pdf_status = "completed"
        order.final_pdf_path = str(pdf_path)
        self.db.commit()
        with patch.object(app, "safe_send_template_email", return_value={"status": "sent", "email_log_id": 77}) as email_mock:
            result = app.send_final_report_delivery_email(self.db, order, actor="admin")
        self.assertEqual(result["status"], "sent")
        attachments = email_mock.call_args.kwargs["attachments"]
        self.assertEqual(attachments[0]["path"], str(pdf_path))
        self.db.refresh(order)
        self.assertEqual(order.status, "delivered")

    def test_missing_pdf_file_prevents_delivered_state(self):
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final report.")
        order.pdf_status = "completed"
        order.final_pdf_path = str(Path("missing_final_report.pdf").resolve())
        self.db.commit()
        with patch.object(app, "safe_send_template_email") as email_mock:
            result = app.deliver_final_report_for_order(self.db, order)
        self.assertEqual(result["reason"], "pdf_not_ready")
        email_mock.assert_not_called()
        self.db.refresh(order)
        self.assertEqual(order.status, "ready_to_send")
        self.assertIsNone(order.delivered_at)

    def test_final_report_email_failure_prevents_delivered_state(self):
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final report.")
        pdf_path = Path("tmp_final_report_failure.pdf").resolve()
        pdf_path.write_bytes(b"%PDF-test")
        self.addCleanup(lambda: pdf_path.exists() and pdf_path.unlink())
        order.pdf_status = "completed"
        order.final_pdf_path = str(pdf_path)
        self.db.commit()
        with patch.object(app, "safe_send_template_email", return_value={"status": "failed"}) as email_mock:
            with self.assertRaises(ValueError):
                app.send_final_report_delivery_email(self.db, order, actor="admin")
        email_mock.assert_called_once()
        self.db.refresh(order)
        self.assertEqual(order.status, "ready_to_send")
        self.assertIsNone(order.delivered_at)

    def test_duplicate_final_delivery_attempt_is_safely_ignored(self):
        order = self._service_order(status="delivered", paid=True, delivered=True)
        with patch.object(app, "safe_send_template_email") as email_mock:
            result = app.deliver_final_report_for_order(self.db, order)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "already_delivered")
        email_mock.assert_not_called()

    def test_paid_consultation_appears_in_admin_queue(self):
        admin = self._create_admin_user()
        order = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="paid",
            paid=True,
            customer_email="consult@example.com",
        )
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.get("/admin/orders?order_type=consultation")
        self.assertEqual(response.status_code, 200)
        self.assertIn(f"#{order.id}", response.text)
        self.assertIn("consult@example.com", response.text)

    def test_admin_consultations_page_shows_consultation_only_data(self):
        admin = self._create_admin_user()
        consultation = self._service_order(
            service_type="consultation",
            product_type="consultation_60_min",
            status="paid",
            paid=True,
            customer_email="monitor-consult@example.com",
        )
        self._service_order(
            service_type="report",
            product_type="career",
            status="paid",
            paid=True,
            customer_email="not-consult@example.com",
        )
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            response = self.client.get("/admin/consultations")

        self.assertEqual(response.status_code, 200)
        self.assertIn("monitor-consult@example.com", response.text)
        self.assertTrue("Yes" in response.text or "Ödendi" in response.text)
        self.assertNotIn("not-consult@example.com", response.text)
        self.assertIn(f"/admin/orders/{consultation.id}", response.text)

    def test_admin_content_create_and_edit_work(self):
        admin = self._create_admin_user()
        unique_slug = f"premium-timing-note-{int(app.time.time() * 1000000)}"
        unique_title = f"Premium Timing Note {unique_slug}"
        csrf_token = self._admin_csrf_token(admin, path="/admin/content/new")
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            create_response = self.client.post(
                "/admin/content/new",
                data={"title": unique_title, "slug": unique_slug, "content": "Draft body", "status": "draft", "csrf_token": csrf_token},
                follow_redirects=False,
            )
        self.assertEqual(create_response.status_code, 303)
        article = self.db.query(db_mod.Article).filter(db_mod.Article.title == unique_title).first()
        self.assertIsNotNone(article)
        self.assertFalse(article.is_published)

        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            list_response = self.client.get("/admin/content")
            edit_csrf = self._admin_csrf_token(admin, path=f"/admin/content/{article.id}/edit")
            edit_response = self.client.post(
                f"/admin/content/{article.id}/edit",
                data={"title": "Premium Timing Note Updated", "slug": unique_slug, "content": "Published body", "status": "published", "csrf_token": edit_csrf},
                follow_redirects=False,
            )
        self.assertEqual(list_response.status_code, 200)
        self.assertIn("Premium Timing Note", list_response.text)
        self.assertEqual(edit_response.status_code, 303)
        self.db.refresh(article)
        self.assertTrue(article.is_published)
        self.assertEqual(article.body, "Published body")

    def test_admin_emails_page_loads_related_order_column(self):
        admin = self._create_admin_user()
        order = self._service_order(service_type="report", status="ready_to_send", paid=True)
        self.db.add(
            db_mod.EmailLog(
                email_type="final_report_delivery",
                recipient_email=order.customer_email,
                subject="Your report is ready",
                status="sent",
                related_event_type="report_delivered",
                related_event_key=f"final_report_delivery:{order.order_token}",
            )
        )
        self.db.commit()
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.get("/admin/emails")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Your report is ready", response.text)
        self.assertIn(f"#{order.id}", response.text)

    def test_admin_internal_notes_can_be_saved(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            response = self.client.post(
                f"/admin/orders/{order.id}/notes",
                data={"internal_notes": "Review tone before final delivery.", "csrf_token": csrf_token},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.refresh(order)
        self.assertEqual(order.internal_notes, "Review tone before final delivery.")
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "save_internal_notes").first()
        self.assertIsNotNone(action)

    def test_admin_invalid_status_transition_is_rejected(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            response = self.client.post(
                f"/admin/orders/{order.id}/transition",
                data={"action": "mark_ready_to_send", "csrf_token": csrf_token},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")

    def test_admin_refund_flow_records_audited_refund(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)),
            patch.object(app, "refund_service_order_payment", return_value={"status": "refunded", "refund_reference": "rf_1"}) as refund_mock,
            patch.object(app, "safe_send_template_email", return_value={"status": "sent", "email_log_id": 52}),
        ):
            response = self.client.post(
                f"/admin/orders/{order.id}/refund",
                data={"refund_amount": "1690.00", "refund_reason": "Customer request", "refund_mode": "provider", "csrf_token": csrf_token},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=refunded", response.headers["location"])
        refund_mock.assert_called_once()
        self.db.refresh(order)
        self.assertEqual(order.status, "refunded")
        self.assertEqual(order.refund_status, "refunded")
        self.assertIsNotNone(order.refunded_at)
        self.assertEqual(order.provider_refund_id, "rf_1")
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "refund").first()
        self.assertIsNotNone(action)
        requested = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "refund_requested").first()
        self.assertIsNotNone(requested)

    def test_admin_refund_failure_does_not_mark_refunded(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)),
            patch.object(app, "refund_service_order_payment", side_effect=app.payments.PaymentVerificationError("provider rejected")),
        ):
            response = self.client.post(
                f"/admin/orders/{order.id}/refund",
                data={"refund_amount": "1690.00", "refund_reason": "Customer request", "refund_mode": "provider", "csrf_token": csrf_token},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")
        self.assertIsNone(order.refunded_at)
        self.assertNotEqual(order.refund_status, "refunded")

    def test_admin_double_refund_is_blocked(self):
        admin = self._create_admin_user()
        order = self._service_order(status="refunded", paid=True)
        order = self.db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == order.id).first()
        order.refund_status = "refunded"
        order.refunded_at = app.datetime.utcnow()
        self.db.commit()
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)),
            patch.object(app, "refund_service_order_payment") as refund_mock,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/refund", data={"refund_mode": "provider", "csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        refund_mock.assert_not_called()

    def test_consultation_cancellation_timing_rules_and_override(self):
        admin = self._create_admin_user()
        late_order = self._service_order(service_type="consultation", product_type="consultation_60_min", status="paid", paid=True)
        late_order.scheduled_start = app.datetime.utcnow() + app.timedelta(hours=3)
        late_order.scheduled_end = late_order.scheduled_start + app.timedelta(hours=1)
        self.db.commit()
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{late_order.id}")
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            blocked = self.client.post(f"/admin/orders/{late_order.id}/cancel", data={"cancellation_reason": "Late request", "csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(blocked.status_code, 303)
        self.assertIn("error=", blocked.headers["location"])
        self.db.refresh(late_order)
        self.assertEqual(late_order.status, "paid")
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)),
            patch.object(app, "safe_send_template_email", return_value={"status": "sent", "email_log_id": 53}),
        ):
            allowed = self.client.post(
                f"/admin/orders/{late_order.id}/cancel",
                data={"cancellation_reason": "Admin approved exception", "admin_override": "1", "csrf_token": csrf_token},
                follow_redirects=False,
            )
        self.assertEqual(allowed.status_code, 303)
        self.db.refresh(late_order)
        self.assertEqual(late_order.status, "cancelled")
        self.assertIsNotNone(late_order.cancelled_at)

    def test_admin_marks_consultation_no_show(self):
        admin = self._create_admin_user()
        order = self._service_order(service_type="consultation", product_type="consultation_60_min", status="confirmed", paid=True)
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)):
            response = self.client.post(f"/admin/orders/{order.id}/mark-no-show", data={"no_show_reason": "Client did not attend", "csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.db.refresh(order)
        self.assertEqual(order.status, "no_show")
        self.assertIsNotNone(order.no_show_at)

    def test_admin_reconcile_payment_success_finalizes_order(self):
        admin = self._create_admin_user()
        order = self._service_order(status="awaiting_payment", paid=False)
        order.provider_token = "reconcile-token"
        order.provider_conversation_id = order.public_token
        self.db.commit()
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")

        class _ReconcileProvider:
            provider_name = "iyzico"

            def retrieve_checkout_form(self, token, conversation_id):
                payload = {
                    "status": "success",
                    "paymentStatus": "SUCCESS",
                    "conversationId": conversation_id,
                    "basketId": str(order.id),
                    "paidPrice": "1690.00",
                    "currency": "TRY",
                    "fraudStatus": 1,
                    "paymentId": "pay_reconciled_1",
                    "itemTransactions": [{"paymentTransactionId": "txn_reconciled_1"}],
                }
                return payload

        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)),
            patch.object(app.payments, "get_payment_provider", return_value=_ReconcileProvider()),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/reconcile-payment", data={"payment_token": "reconcile-token", "csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=reconciled", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")
        self.assertEqual(order.provider_payment_id, "pay_reconciled_1")
        self.assertEqual(order.provider_transaction_id, "txn_reconciled_1")
        triggers.assert_called_once()

    def test_admin_reconcile_payment_detail_success_finalizes_order(self):
        admin = self._create_admin_user()
        order = self._service_order(status="awaiting_payment", paid=False)
        order.provider_conversation_id = order.public_token
        self.db.commit()
        csrf_token = self._admin_csrf_token(admin, path=f"/admin/orders/{order.id}")

        class _DetailProvider:
            provider_name = "iyzico"

            def retrieve_payment_detail(self, payment_id, conversation_id):
                return {
                    "status": "success",
                    "paymentStatus": "SUCCESS",
                    "conversationId": conversation_id,
                    "basketId": str(order.id),
                    "paidPrice": "1690.00",
                    "currency": "TRY",
                    "fraudStatus": 1,
                    "paymentId": payment_id,
                    "itemTransactions": [{"paymentTransactionId": "txn_detail_1"}],
                }

        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin.email)),
            patch.object(app.payments, "get_payment_provider", return_value=_DetailProvider()),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}),
        ):
            response = self.client.post(f"/admin/orders/{order.id}/reconcile-payment", data={"payment_token": "", "payment_id": "pay_detail_1", "csrf_token": csrf_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")
        self.assertEqual(order.provider_payment_id, "pay_detail_1")

    def test_report_post_payment_tasks_are_enqueued_after_verified_payment(self):
        order = self._service_order(status="paid", paid=True)
        with (
            patch("report_tasks.generate_ai_draft_task.delay") as draft_delay,
            patch("email_tasks.send_customer_confirmation_email_task.delay") as customer_delay,
        ):
            draft_delay.return_value.id = "draft-task"
            customer_delay.return_value.id = "customer-task"
            result = app.run_post_payment_triggers(self.db, order, {"provider": "iyzico", "payment_reference": "pay_async"})
        self.assertEqual(result["status"], "queued")
        draft_delay.assert_called_once_with(order.id)
        customer_delay.assert_called_once_with(order.id)

    def test_generate_ai_draft_service_is_idempotent(self):
        order = self._service_order(status="draft_ready", paid=True, ai_draft_text="Existing draft.")
        result = app.generate_ai_draft_for_order(self.db, order)
        self.assertEqual(result["status"], "skipped")
        self.db.refresh(order)
        self.assertEqual(order.ai_draft_text, "Existing draft.")

    def test_customer_confirmation_service_is_idempotent(self):
        order = self._service_order(status="paid", paid=True)
        order.customer_confirmation_sent_at = app.datetime.utcnow()
        self.db.commit()
        with patch.object(app, "safe_send_template_email") as email_mock:
            result = app.send_customer_confirmation_for_order(self.db, order)
        self.assertEqual(result["status"], "skipped")
        email_mock.assert_not_called()

    def test_eager_task_execution_generates_draft(self):
        order = self._service_order(status="paid", paid=True, ai_draft_text="")
        with patch.object(app, "_generate_report_order_draft", return_value=("Generated async draft.", "generated")):
            from report_tasks import generate_ai_draft_task

            result = generate_ai_draft_task.delay(order.id).get()
        self.assertEqual(result["status"], "generated")
        self.db.refresh(order)
        self.assertEqual(order.ai_draft_text, "Generated async draft.")
        self.assertIsNone(order.draft_sent_at)

    def test_draft_generation_stores_report_without_internal_review_email(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True, ai_draft_text="", customer_email="client-review@example.com")
        before_logs = self.db.query(db_mod.EmailLog).filter(db_mod.EmailLog.email_type == "report_order_admin_draft").count()
        with (
            patch.object(app, "_generate_report_order_draft", return_value=("Generated review draft.", "generated")),
            patch.object(app, "safe_send_template_email") as email_mock,
        ):
            result = app.generate_ai_draft_for_order(self.db, order)

        self.assertEqual(result["status"], "generated")
        email_mock.assert_not_called()
        self.db.refresh(order)
        self.assertEqual(order.status, "draft_ready")
        self.assertEqual(order.ai_draft_text, "Generated review draft.")
        self.assertIsNone(order.draft_sent_at)
        self.assertIsNone(order.delivered_at)
        self.assertEqual(
            self.db.query(db_mod.EmailLog).filter(db_mod.EmailLog.email_type == "report_order_admin_draft").count(),
            before_logs,
        )
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.get("/admin/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("client-review@example.com", response.text)
        self.assertIn("draft_ready", response.text)

    def test_paid_report_draft_payload_includes_astro_signal_context_when_birth_data_exists(self):
        order = self._service_order(status="paid", paid=True, ai_draft_text="")

        with (
            patch.object(app, "_build_birth_context", return_value=sample_paid_chart_bundle()["birth_context"]) as birth_context_mock,
            patch.object(app, "_calculate_chart_bundle_from_birth_context", return_value=sample_paid_chart_bundle()) as chart_bundle_mock,
        ):
            payload = app._build_report_order_payload(app._order_data_from_service_order(order), app._service_order_product(order))

        birth_context_mock.assert_called_once()
        chart_bundle_mock.assert_called_once()
        self.assertEqual(payload["report_order_type"], "career")
        self.assertEqual(payload["language"], "tr")
        self.assertIn("natal_data", payload)
        self.assertIn("astro_signal_context", payload)
        self.assertIn("nakshatra_signals", payload["astro_signal_context"])
        self.assertIn("yoga_signals", payload["astro_signal_context"])
        self.assertIn("dasha_activation_signals", payload["astro_signal_context"])
        self.assertIn("dasha_signal_bundle", payload["astro_signal_context"])
        self.assertIn("atmakaraka_signals", payload["astro_signal_context"])

    def test_paid_report_payload_reuses_existing_chart_payload_without_recalculation(self):
        chart_bundle = sample_paid_chart_bundle()
        order = db_mod.ServiceOrder(
            order_token="report_payload_reuse_test",
            public_token="report_payload_reuse_test",
            service_type="report",
            product_type="career",
            status="paid",
            customer_name="Aylin Test",
            customer_email="payload-reuse@example.com",
            birth_date="1990-01-02",
            birth_time="08:30",
            birth_place="Istanbul, Turkey",
            optional_note="Internal fulfillment test.",
            amount=app.Decimal("1690.00"),
            amount_label="TRY 1690.00",
            currency="TRY",
            paid_at=app.datetime.utcnow(),
            provider_name="iyzico",
            provider_payment_id="pay_payload_reuse_test",
            payload_json=json.dumps(
                {
                    "full_name": "Aylin Test",
                    "email": "payload-reuse@example.com",
                    "birth_date": "1990-01-02",
                    "birth_time": "08:30",
                    "birth_city": "Istanbul, Turkey",
                    "report_type": "career",
                    "user_lang": "en",
                    "natal_data": chart_bundle["natal_data"],
                    "navamsa_data": chart_bundle["navamsa_data"],
                    "dasha_data": chart_bundle["dasha_data"],
                    "interpretation_context": chart_bundle["interpretation_context"],
                },
                ensure_ascii=False,
            ),
        )
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        with (
            patch.object(app, "_build_birth_context") as birth_context_mock,
            patch.object(app, "_calculate_chart_bundle_from_birth_context") as chart_bundle_mock,
        ):
            payload = app._build_report_order_payload(app._order_data_from_service_order(order), app._service_order_product(order))

        birth_context_mock.assert_not_called()
        chart_bundle_mock.assert_not_called()
        self.assertEqual(payload["language"], "en")
        self.assertIn("astro_signal_context", payload)

    def test_paid_report_parent_child_payload_builds_dual_signal_context(self):
        order_payload = sample_parent_child_paid_order_payload()
        order = db_mod.ServiceOrder(
            order_token="parent_child_payload_test",
            public_token="parent_child_payload_test",
            service_type="report",
            product_type="parent_child",
            status="paid",
            customer_name="Child Example",
            customer_email="family@example.com",
            birth_date="2018-04-01",
            birth_time="08:30",
            birth_place="Istanbul, Turkey",
            optional_note="Internal fulfillment test.",
            amount=app.Decimal("1790.00"),
            amount_label="TRY 1790.00",
            currency="TRY",
            paid_at=app.datetime.utcnow(),
            provider_name="iyzico",
            provider_payment_id="pay_parent_child_payload_test",
            payload_json=json.dumps(order_payload, ensure_ascii=False),
        )
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)

        payload = app._build_report_order_payload(app._order_data_from_service_order(order), app._service_order_product(order))

        self.assertEqual(payload["report_order_type"], "parent_child")
        self.assertIn("parent_astro_signal_context", payload)
        self.assertIn("child_astro_signal_context", payload)
        self.assertIn("astro_signal_context", payload)
        self.assertIn("parent_profile_signals", payload["astro_signal_context"])
        self.assertIn("child_profile_signals", payload["astro_signal_context"])

    def test_paid_report_draft_missing_chart_data_does_not_crash(self):
        order = self._service_order(status="paid", paid=True, ai_draft_text="")
        order.birth_date = ""
        order.birth_place = ""
        order.payload_json = json.dumps({"full_name": "Aylin Test", "email": order.customer_email}, ensure_ascii=False)
        self.db.commit()
        captured = {}

        def capture_payload(payload):
            captured["payload"] = payload
            return "Generated review draft.", "generated"

        with patch.object(app, "_generate_report_order_draft", side_effect=capture_payload):
            result = app.generate_ai_draft_for_order(self.db, order)

        self.assertEqual(result["status"], "generated")
        self.db.refresh(order)
        self.assertEqual(order.status, "draft_ready")
        self.assertNotIn("astro_signal_context", captured["payload"])
        confidence_notes = (captured["payload"].get("interpretation_context") or {}).get("confidence_notes") or []
        self.assertIn("Chart context unavailable; interpretation generated without signal enrichment.", confidence_notes)

    def test_disabled_internal_review_helper_does_not_send_or_mark_delivered(self):
        order = self._service_order(status="draft_ready", paid=True, ai_draft_text="Reviewed draft.", customer_email="client-pdf-review@example.com")
        pdf_path = Path("tmp_internal_review.pdf").resolve()
        pdf_path.write_bytes(b"%PDF-review")
        self.addCleanup(lambda: pdf_path.exists() and pdf_path.unlink())
        order.pdf_status = "completed"
        order.final_pdf_path = str(pdf_path)
        self.db.commit()
        with patch.object(app, "safe_send_template_email") as email_mock:
            result = app.send_admin_notification_for_order(self.db, order)

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "admin_reports_queue")
        email_mock.assert_not_called()
        self.db.refresh(order)
        self.assertIsNone(order.draft_sent_at)
        self.assertIsNone(order.delivered_at)

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

    def test_provider_selection_defaults_to_iyzico(self):
        with patch.dict("os.environ", {}, clear=True):
            provider = get_payment_provider()
        self.assertIsInstance(provider, IyzicoProvider)

    def test_provider_selection_supports_iyzico(self):
        with patch.dict("os.environ", {"PAYMENT_PROVIDER": "iyzico"}):
            provider = get_payment_provider()
        self.assertIsInstance(provider, IyzicoProvider)

    def test_provider_selection_supports_stripe_for_later_expansion(self):
        with patch.dict("os.environ", {"PAYMENT_PROVIDER": "stripe"}):
            provider = get_payment_provider()
        self.assertIsInstance(provider, StripeProvider)

    @unittest.skip("Replaced by production Checkout Form initialize/callback tests.")
    def test_iyzico_service_payment_link_handoff_is_ready(self):
        order = db_mod.ServiceOrder(
            order_token="consult_payment_link_test",
            service_type="consultation",
            product_type="consultation_60_min",
            status="booking_pending_payment",
            amount_label="₺4.900",
            currency="TRY",
        )
        with patch.dict(
            "os.environ",
            {
                "PAYMENT_PROVIDER": "iyzico",
                "PAYMENTS_ENABLED": "true",
                "IYZICO_CONSULTATION_PAYMENT_LINK": "https://pay.iyzico.example/consultation",
            },
        ):
            session = app.create_consultation_payment_session(order)
        self.assertEqual(session["provider"], "iyzico")
        self.assertEqual(session["mode"], "payment_link")
        self.assertIn("https://pay.iyzico.example/consultation", session["redirect_url"])
        self.assertIn("consult_payment_link_test", session["redirect_url"])

    def test_iyzico_checkout_form_initialize_handoff_is_ready(self):
        order = db_mod.ServiceOrder(
            order_token="consult_checkout_form_test",
            public_token="consult_checkout_form_test",
            service_type="consultation",
            product_type="consultation_60_min",
            status="booking_pending_payment",
            amount=app.Decimal("4900.00"),
            amount_label="TRY 4900.00",
            currency="TRY",
        )
        self.db.add(order)
        self.db.commit()
        with patch.dict(
            "os.environ",
            {
                "PAYMENT_PROVIDER": "iyzico",
                "PAYMENTS_ENABLED": "true",
                "IYZICO_API_KEY": "api-key",
                "IYZICO_SECRET_KEY": "secret-key",
            },
        ), patch.object(
            IyzicoProvider,
            "_post_json",
            return_value={
                "status": "success",
                "token": "iyzico-token-1",
                "paymentPageUrl": "https://sandbox.iyzico.com/checkout/iyzico-token-1",
            },
        ):
            session = app.create_consultation_payment_session(order)
        self.assertEqual(session["provider"], "iyzico")
        self.assertEqual(session["mode"], "checkout_form")
        self.assertEqual(session["provider_token"], "iyzico-token-1")
        self.assertIn("https://sandbox.iyzico.com/checkout/iyzico-token-1", session["redirect_url"])

    def test_iyzico_success_callback_marks_report_paid_after_verification(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_success_1")
        with (
            patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers,
        ):
            response = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token})
        self.assertEqual(response.status_code, 200)
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")
        self.assertEqual(order.provider_payment_id, "pay_success_1")
        self.assertIsNotNone(order.paid_at)
        self.assertIsNotNone(order.payment_verified_at)
        self.assertEqual(order.fraud_status, "1")
        triggers.assert_called_once()
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "payment_verified").first()
        self.assertIsNotNone(action)

    def test_iyzico_callback_does_not_finalize_when_retrieve_fails(self):
        order = self._service_order_for_payment()
        with patch.object(app, "_retrieve_iyzico_payment_for_order", side_effect=app.payments.PaymentVerificationError("retrieve failed")):
            response = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("verification_failed", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "awaiting_payment")
        self.assertIsNone(order.provider_payment_id)

    def test_iyzico_callback_unknown_token_is_rejected(self):
        response = self.client.post("/payments/iyzico/callback/report", data={"token": "wrong-token"}, follow_redirects=False)
        self.assertEqual(response.status_code, 404)

    def test_iyzico_duplicate_callback_is_idempotent(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_duplicate_1")
        with (
            patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers,
        ):
            first = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token})
            second = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token})
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.db.refresh(order)
        self.assertEqual(order.provider_payment_id, "pay_duplicate_1")
        triggers.assert_called_once()

    def test_iyzico_duplicate_after_report_status_progressed_is_idempotent(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_progressed_duplicate_1")
        with patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers:
            first = app.process_verified_service_payment(self.db, order, payload)
            order.status = "draft_ready"
            self.db.commit()
            second = app.process_verified_service_payment(self.db, order, payload)
        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        triggers.assert_called_once()

    def test_iyzico_callback_rejects_mismatched_values(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_mismatch_1")
        payload["basketId"] = "9999"
        with patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload):
            response = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("verification_failed", response.headers["location"])
        self.db.refresh(order)
        self.assertNotEqual(order.status, "paid")
        self.assertIsNone(order.provider_payment_id)

    def test_iyzico_callback_rejects_currency_mismatch(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_currency_mismatch_1")
        payload["currency"] = "USD"
        with patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload):
            response = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("verification_failed", response.headers["location"])
        self.db.refresh(order)
        self.assertNotEqual(order.status, "paid")

    def test_iyzico_callback_rejects_fraud_status_not_one(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_fraud_1")
        payload["fraudStatus"] = 0
        with patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload):
            response = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("verification_failed", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "payment_under_review")
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "payment_under_review").first()
        self.assertIsNotNone(action)

    def test_iyzico_response_signature_failure_blocks_finalization(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_bad_signature_1")
        payload["signature"] = "bad-signature"
        with (
            patch.dict("os.environ", {"IYZICO_SECRET_KEY": "secret-key", "IYZICO_REQUIRE_RESPONSE_SIGNATURE": "true"}),
            patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload),
        ):
            response = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("verification_failed", response.headers["location"])
        self.db.refresh(order)
        self.assertIsNone(order.provider_payment_id)

    def test_iyzico_webhook_valid_signature_finalizes_order(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_webhook_1")
        webhook_payload = {
            "iyziEventType": "CHECKOUT_FORM_AUTH",
            "iyziPaymentId": "pay_webhook_1",
            "token": order.provider_token,
            "paymentConversationId": order.provider_conversation_id,
            "status": "SUCCESS",
        }
        signature = IyzicoProvider.build_hpp_webhook_signature(webhook_payload, secret_key="secret-key")
        with (
            patch.dict("os.environ", {"IYZICO_WEBHOOK_SIGNATURE_REQUIRED": "true", "IYZICO_WEBHOOK_SECRET": "secret-key"}),
            patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers,
        ):
            response = self.client.post("/payments/iyzico/webhook", json=webhook_payload, headers={"X-IYZ-SIGNATURE-V3": signature})
            duplicate = self.client.post("/payments/iyzico/webhook", json=webhook_payload, headers={"X-IYZ-SIGNATURE-V3": signature})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(duplicate.status_code, 200)
        self.db.refresh(order)
        self.assertEqual(order.provider_payment_id, "pay_webhook_1")
        triggers.assert_called_once()

    def test_iyzico_webhook_invalid_signature_is_rejected(self):
        order = self._service_order_for_payment()
        webhook_payload = {
            "iyziEventType": "CHECKOUT_FORM_AUTH",
            "iyziPaymentId": "pay_webhook_bad_sig",
            "token": order.provider_token,
            "paymentConversationId": order.provider_conversation_id,
            "status": "SUCCESS",
        }
        with patch.dict("os.environ", {"IYZICO_WEBHOOK_SIGNATURE_REQUIRED": "true", "IYZICO_WEBHOOK_SECRET": "secret-key"}):
            response = self.client.post("/payments/iyzico/webhook", json=webhook_payload, headers={"X-IYZ-SIGNATURE-V3": "bad"})
        self.assertEqual(response.status_code, 400)
        self.db.refresh(order)
        self.assertIsNone(order.provider_payment_id)

    def test_iyzico_webhook_requires_json_content_type(self):
        order = self._service_order_for_payment()
        response = self.client.post("/payments/iyzico/webhook", content=json.dumps({"token": order.provider_token}), headers={"content-type": "text/plain"})
        self.assertEqual(response.status_code, 415)

    def test_iyzico_callback_and_webhook_race_is_idempotent(self):
        order = self._service_order_for_payment()
        payload = self._iyzico_payload(order, payment_id="pay_race_1")
        webhook_payload = {
            "iyziEventType": "CHECKOUT_FORM_AUTH",
            "iyziPaymentId": "pay_race_1",
            "token": order.provider_token,
            "paymentConversationId": order.provider_conversation_id,
            "status": "SUCCESS",
        }
        with (
            patch.object(app, "_retrieve_iyzico_payment_for_order", return_value=payload),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers,
        ):
            callback = self.client.post("/payments/iyzico/callback/report", data={"token": order.provider_token})
            webhook = self.client.post("/payments/iyzico/webhook", json=webhook_payload)
        self.assertEqual(callback.status_code, 200)
        self.assertEqual(webhook.status_code, 200)
        triggers.assert_called_once()

    def test_checkout_success_redirect_does_not_mark_order_paid(self):
        order = self._service_order_for_payment()
        response = self.client.get(f"/checkout/report/{order.order_token}/success?order_token={order.order_token}&session_id=fake", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.db.refresh(order)
        self.assertEqual(order.status, "awaiting_payment")
        self.assertIsNone(order.paid_at)

    def test_expire_unpaid_consultations_marks_old_pending_payment_orders(self):
        old_order = db_mod.ServiceOrder(
            order_token="old_consult",
            public_token="old_consult",
            service_type="consultation",
            product_type="consultation_60_min",
            status="booking_pending_payment",
            amount=app.Decimal("4900.00"),
            amount_label="TRY 4900.00",
            currency="TRY",
            created_at=app.datetime.utcnow() - app.timedelta(minutes=31),
        )
        fresh_order = db_mod.ServiceOrder(
            order_token="fresh_consult",
            public_token="fresh_consult",
            service_type="consultation",
            product_type="consultation_60_min",
            status="booking_pending_payment",
            amount=app.Decimal("4900.00"),
            amount_label="TRY 4900.00",
            currency="TRY",
            created_at=app.datetime.utcnow(),
        )
        self.db.add_all([old_order, fresh_order])
        self.db.commit()
        expired = app.expire_unpaid_consultations(self.db)
        self.assertEqual(expired, 1)
        self.db.refresh(old_order)
        self.db.refresh(fresh_order)
        self.assertEqual(old_order.status, "booking_expired")
        self.assertEqual(fresh_order.status, "booking_pending_payment")

    def _create_admin_user(self, email="admin@example.com"):
        existing = self.db.query(db_mod.AppUser).filter(db_mod.AppUser.email == email).first()
        if existing:
            existing.is_admin = True
            existing.is_active = True
            self.db.commit()
            self.db.refresh(existing)
            return existing
        user = self._create_user(email)
        user.is_admin = True
        self.db.commit()
        self.db.refresh(user)
        return user

    def _request_admin_user(self, email):
        def _loader(request, db):
            return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == email).first()
        return _loader

    def _calendly_event_uri(self):
        return "https://api.calendly.com/scheduled_events/event_123"

    def _calendly_invitee_uri(self):
        return "https://api.calendly.com/scheduled_events/event_123/invitees/invitee_456"

    def _calendly_payload(self, event="invitee.created"):
        return {
            "event": event,
            "payload": {
                "uri": self._calendly_invitee_uri(),
                "name": "Aylin Client",
                "email": "client@example.com",
                "event": self._calendly_event_uri(),
                "canceled_at": "2026-04-30T09:00:00Z" if event == "invitee.canceled" else None,
                "cancellation": {"reason": "Client requested cancellation"} if event == "invitee.canceled" else None,
                "scheduled_event": {
                    "uri": self._calendly_event_uri(),
                    "event_type": "https://api.calendly.com/event_types/type_789",
                    "start_time": "2026-05-01T10:00:00Z",
                    "end_time": "2026-05-01T11:00:00Z",
                },
            },
        }

    def _service_order(
        self,
        *,
        service_type="report",
        product_type="career",
        status="paid",
        paid=True,
        delivered=False,
        completed_at=None,
        delivered_at=None,
        paid_at=None,
        created_at=None,
        amount=None,
        refund_amount=None,
        refunded_at=None,
        bundle_type=None,
        customer_email="customer@example.com",
        ai_draft_text="AI-assisted draft awaiting human review.",
    ):
        token = f"{service_type}_{product_type}_{status}_{len(customer_email)}_{int(app.time.time() * 1000000)}"
        now = app.datetime.utcnow()
        effective_created_at = created_at or now
        effective_paid_at = paid_at if paid_at is not None else (now if paid else None)
        effective_amount = amount if amount is not None else (app.Decimal("1690.00") if service_type == "report" else app.Decimal("4900.00"))
        order = db_mod.ServiceOrder(
            order_token=token,
            public_token=token,
            service_type=service_type,
            product_type=product_type,
            bundle_type=bundle_type,
            status=status,
            customer_name="Aylin Test",
            customer_email=customer_email,
            birth_date="1990-01-02",
            birth_time="08:30",
            birth_place="Istanbul, Turkey",
            optional_note="Internal fulfillment test.",
            amount=effective_amount,
            amount_label=f"TRY {effective_amount:.2f}",
            currency="TRY",
            paid_at=effective_paid_at,
            provider_name="iyzico" if paid else None,
            provider_payment_id=f"pay_{token}" if paid else None,
            ai_draft_text=ai_draft_text if service_type == "report" else None,
            ai_draft_created_at=now if service_type == "report" and ai_draft_text else None,
            delivered_at=delivered_at or (now if delivered else None),
            completed_at=completed_at,
            refund_status=status if status in {"refunded", "partially_refunded"} else None,
            refund_amount=refund_amount,
            refunded_at=refunded_at,
            scheduled_start=now + app.timedelta(days=2) if service_type == "consultation" else None,
            scheduled_end=now + app.timedelta(days=2, hours=1) if service_type == "consultation" else None,
            calendly_event_uri="https://calendly.com/example/event" if service_type == "consultation" else None,
            created_at=effective_created_at,
            updated_at=effective_created_at,
        )
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        return order

    def _service_order_for_payment(self):
        order = db_mod.ServiceOrder(
            order_token="report_payment_test",
            public_token="report_payment_test",
            service_type="report",
            product_type="career",
            status="awaiting_payment",
            customer_name="Aylin Test",
            customer_email="customer@example.com",
            amount=app.Decimal("1690.00"),
            amount_label="TRY 1690.00",
            currency="TRY",
            provider_name="iyzico",
            provider_token="iyzico-callback-token",
            provider_conversation_id="report_payment_test",
        )
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        return order

    def _iyzico_payload(self, order, payment_id="pay_1"):
        return {
            "status": "success",
            "paymentStatus": "SUCCESS",
            "conversationId": order.provider_conversation_id,
            "basketId": str(order.id),
            "paidPrice": "1690.00",
            "currency": "TRY",
            "fraudStatus": 1,
            "paymentId": payment_id,
        }

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

    def _admin_csrf_token(self, admin, path="/admin/content/new"):
        admin_email = admin if isinstance(admin, str) else getattr(admin, "email", None)
        candidate_paths = [path]
        if path != "/admin/content/new":
            candidate_paths.append("/admin/content/new")
        response = None
        for candidate_path in candidate_paths:
            with patch.object(app, "get_request_user", side_effect=self._request_admin_user(admin_email)):
                response = self.client.get(candidate_path)
            if response.status_code != 200:
                continue
            match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
            if not match:
                match = re.search(r'value=["\']([^"\']+)["\'][^>]*name=["\']csrf_token["\']', response.text)
            if match:
                return match.group(1)
        raise AssertionError(response.text[:500] if response is not None else "No admin CSRF response")


if __name__ == "__main__":
    unittest.main()

