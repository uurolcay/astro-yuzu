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
        self.db.query(db_mod.AdminActionLog).delete()
        self.db.query(db_mod.ServiceOrder).delete()
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
        self.db.query(db_mod.AdminActionLog).delete()
        self.db.query(db_mod.ServiceOrder).delete()
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

    def test_bundle_order_submission_records_bundle_metadata(self):
        payload = {
            "full_name": "Aylin Bundle",
            "email": "bundle-order@example.com",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "optional_note": "Yaşam yönü ve kariyer",
        }
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
        response = self.client.get("/reports/order/birth_chart_karma")
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
        payload = {
            "full_name": "Aylin Test",
            "email": "customer@example.com",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "selected_report_type": "career",
            "optional_note": "Kariyer yönü",
        }
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

    def test_admin_orders_route_requires_admin_auth(self):
        response = self.client.get("/admin/orders", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("/login", response.headers["location"])

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
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.post(f"/admin/orders/{order.id}/send-report", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        self.db.refresh(order)
        self.assertNotEqual(order.status, "delivered")

    def test_admin_send_report_marks_delivered_only_after_email_success(self):
        admin = self._create_admin_user()
        order = self._service_order(status="ready_to_send", paid=True, ai_draft_text="Final human-reviewed report.")
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "safe_send_template_email", return_value={"status": "sent", "email_log_id": 41}) as email_mock,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/send-report", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=report_sent", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "delivered")
        self.assertIsNotNone(order.delivered_at)
        email_mock.assert_called_once()
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "send_final_report").first()
        self.assertIsNotNone(action)

    def test_admin_duplicate_send_report_is_blocked(self):
        admin = self._create_admin_user()
        order = self._service_order(status="delivered", paid=True, delivered=True)
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "safe_send_template_email") as email_mock,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/send-report", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
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

    def test_admin_internal_notes_can_be_saved(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True)
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.post(
                f"/admin/orders/{order.id}/notes",
                data={"internal_notes": "Review tone before final delivery."},
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
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.post(
                f"/admin/orders/{order.id}/transition",
                data={"action": "mark_ready_to_send"},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")

    def test_admin_refund_flow_records_audited_refund(self):
        admin = self._create_admin_user()
        order = self._service_order(status="paid", paid=True)
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "refund_service_order_payment", return_value={"status": "refunded", "refund_reference": "rf_1"}) as refund_mock,
            patch.object(app, "safe_send_template_email", return_value={"status": "sent", "email_log_id": 52}),
        ):
            response = self.client.post(
                f"/admin/orders/{order.id}/refund",
                data={"refund_amount": "1690.00", "refund_reason": "Customer request", "refund_mode": "provider"},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=refunded", response.headers["location"])
        refund_mock.assert_called_once()
        self.db.refresh(order)
        self.assertEqual(order.status, "refunded")
        self.assertEqual(order.refund_status, "refunded")
        self.assertIsNotNone(order.refunded_at)
        action = self.db.query(db_mod.AdminActionLog).filter(db_mod.AdminActionLog.order_id == order.id, db_mod.AdminActionLog.action == "refund").first()
        self.assertIsNotNone(action)

    def test_admin_double_refund_is_blocked(self):
        admin = self._create_admin_user()
        order = self._service_order(status="refunded", paid=True)
        order.refund_status = "refunded"
        order.refunded_at = app.datetime.utcnow()
        self.db.commit()
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "refund_service_order_payment") as refund_mock,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/refund", data={"refund_mode": "provider"}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        refund_mock.assert_not_called()

    def test_consultation_cancellation_timing_rules_and_override(self):
        admin = self._create_admin_user()
        late_order = self._service_order(service_type="consultation", product_type="consultation_60_min", status="paid", paid=True)
        late_order.scheduled_start = app.datetime.utcnow() + app.timedelta(hours=3)
        late_order.scheduled_end = late_order.scheduled_start + app.timedelta(hours=1)
        self.db.commit()
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            blocked = self.client.post(f"/admin/orders/{late_order.id}/cancel", data={"cancellation_reason": "Late request"}, follow_redirects=False)
        self.assertEqual(blocked.status_code, 303)
        self.assertIn("error=", blocked.headers["location"])
        self.db.refresh(late_order)
        self.assertEqual(late_order.status, "paid")
        with (
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app, "safe_send_template_email", return_value={"status": "sent", "email_log_id": 53}),
        ):
            allowed = self.client.post(
                f"/admin/orders/{late_order.id}/cancel",
                data={"cancellation_reason": "Admin approved exception", "admin_override": "1"},
                follow_redirects=False,
            )
        self.assertEqual(allowed.status_code, 303)
        self.db.refresh(late_order)
        self.assertEqual(late_order.status, "cancelled")
        self.assertIsNotNone(late_order.cancelled_at)

    def test_admin_marks_consultation_no_show(self):
        admin = self._create_admin_user()
        order = self._service_order(service_type="consultation", product_type="consultation_60_min", status="confirmed", paid=True)
        with patch.object(app, "get_request_user", return_value=self._bound_user(admin)):
            response = self.client.post(f"/admin/orders/{order.id}/mark-no-show", data={"no_show_reason": "Client did not attend"}, follow_redirects=False)
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
            patch.object(app, "get_request_user", return_value=self._bound_user(admin)),
            patch.object(app.payments, "get_payment_provider", return_value=_ReconcileProvider()),
            patch.object(app, "run_post_payment_triggers", return_value={"status": "ok"}) as triggers,
        ):
            response = self.client.post(f"/admin/orders/{order.id}/reconcile-payment", data={"payment_token": "reconcile-token"}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertIn("notice=reconciled", response.headers["location"])
        self.db.refresh(order)
        self.assertEqual(order.status, "paid")
        self.assertEqual(order.provider_payment_id, "pay_reconciled_1")
        self.assertEqual(order.provider_transaction_id, "txn_reconciled_1")
        triggers.assert_called_once()

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
        user = self._create_user(email)
        user.is_admin = True
        self.db.commit()
        self.db.refresh(user)
        return user

    def _service_order(
        self,
        *,
        service_type="report",
        product_type="career",
        status="paid",
        paid=True,
        delivered=False,
        customer_email="customer@example.com",
        ai_draft_text="AI-assisted draft awaiting human review.",
    ):
        token = f"{service_type}_{product_type}_{status}_{len(customer_email)}_{int(app.time.time() * 1000000)}"
        order = db_mod.ServiceOrder(
            order_token=token,
            public_token=token,
            service_type=service_type,
            product_type=product_type,
            status=status,
            customer_name="Aylin Test",
            customer_email=customer_email,
            birth_date="1990-01-02",
            birth_time="08:30",
            birth_place="Istanbul, Turkey",
            optional_note="Internal fulfillment test.",
            amount=app.Decimal("1690.00") if service_type == "report" else app.Decimal("4900.00"),
            amount_label="TRY 1690.00" if service_type == "report" else "TRY 4900.00",
            currency="TRY",
            paid_at=app.datetime.utcnow() if paid else None,
            provider_name="iyzico" if paid else None,
            provider_payment_id=f"pay_{token}" if paid else None,
            ai_draft_text=ai_draft_text if service_type == "report" else None,
            ai_draft_created_at=app.datetime.utcnow() if service_type == "report" and ai_draft_text else None,
            delivered_at=app.datetime.utcnow() if delivered else None,
            scheduled_start=app.datetime.utcnow() + app.timedelta(days=2) if service_type == "consultation" else None,
            scheduled_end=app.datetime.utcnow() + app.timedelta(days=2, hours=1) if service_type == "consultation" else None,
            calendly_event_uri="https://calendly.com/example/event" if service_type == "consultation" else None,
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


if __name__ == "__main__":
    unittest.main()
