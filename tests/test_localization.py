import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from translations import t as translate_text


class LocalizationTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _create_user(self, email="localization@example.com"):
        user = db_mod.AppUser(email=email, password_hash="hash", name="Reader", plan_code="premium", is_active=True)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def test_translation_helper_returns_correct_values(self):
        self.assertEqual(translate_text("reports.personal_cta", "en"), "Unlock Your Full Reading")
        self.assertEqual(translate_text("reports.personal_cta", "tr"), "Tüm Raporu Aç")

    def test_translation_helper_falls_back_to_english(self):
        self.assertEqual(translate_text("reports.order_cta", "de"), "Order Now")
        self.assertEqual(translate_text("missing.key", "tr"), "missing.key")

    def test_reports_page_renders_in_turkish(self):
        user = self._create_user()
        with patch.object(app, "get_request_user", return_value=user):
            response = self.client.get("/reports", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Hangi rapor size uygun?", response.text)
        self.assertIn("Doğum Haritası Karma’sı", response.text)
        self.assertIn("Yıllık Transit", response.text)
        self.assertIn("Kariyer", response.text)
        self.assertIn("Ebeveyn-Çocuk", response.text)

    def test_personal_consultation_page_renders_in_turkish(self):
        response = self.client.get("/personal-consultation", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Kişisel Danışmanlık", response.text)
        self.assertIn("Önemli Kararlar İçin Daha Netlik", response.text)
        self.assertIn("Neleri keşfediyoruz?", response.text)
        self.assertIn("Ne ile ayrılırsınız?", response.text)
        self.assertIn("Nasıl ilerler?", response.text)
        self.assertIn("Tek ve odaklı 60 dakikalık danışmanlık", response.text)
        self.assertIn("60 dk Birebir Astroloji Danışmanlığı", response.text)
        self.assertIn("Danışmanlık Al", response.text)
        self.assertIn("Calendly tabanlı randevu akışı", response.text)
        self.assertNotIn("T?m Raporu A?", response.text)

    def test_personal_consultation_template_uses_turkish_sales_copy(self):
        html = app.templates.env.get_template("personal_consultation.html").render(
            {
                "request": SimpleNamespace(state=SimpleNamespace(current_user=None, lang="en")),
                "language": "en",
                "t": lambda key: translate_text(key, "en"),
            }
        )
        template_source = app.templates.env.loader.get_source(app.templates.env, "personal_consultation.html")[0]
        self.assertIn("Clarity for the Decisions That Matter", html)
        self.assertIn("One focused 60-minute consultation", html)
        self.assertIn("A classical framework with a more careful modern presentation.", html)
        self.assertIn("Get Personal Consultation", html)
        self.assertNotIn("Kişisel Danışmanlık Al", html)
        self.assertNotIn("{% if is_tr %}", template_source)

    def test_result_page_renders_in_turkish(self):
        html = app.templates.env.get_template("result.html").render(
            {
                "request": SimpleNamespace(state=SimpleNamespace(current_user=None, lang="tr")),
                "language": "tr",
                "full_name": "Okur",
                "birth_date": "1990-01-01",
                "birth_time": "08:30",
                "birth_city": "İstanbul, Türkiye",
                "normalized_birth_place": "İstanbul, Türkiye",
                "timezone": "Europe/Istanbul",
                "report_type": "premium",
                "report_type_config": {"include_pdf": True},
                "interpretation_context": {
                    "signal_layer": {"top_anchors": []},
                    "recommendation_layer": {"top_recommendations": [], "opportunity_windows": [], "risk_windows": []},
                    "top_timing_windows": {},
                },
                "payload_json": {"generated_report_id": 90},
                "report_access": {
                    "is_preview": True,
                    "show_unlock_cta": True,
                    "can_unlock_here": True,
                    "checkout_mode": "payment",
                    "can_view_full_report": False,
                    "can_download_pdf": False,
                    "show_login_hint": False,
                    "unlock_success": False,
                    "access_label": "Önizleme",
                },
                "related_articles": [],
                "natal_data": {},
                "dasha_data": [],
                "navamsa_data": {},
                "transit_data": [],
                "eclipse_data": [],
            }
        )
        self.assertIn(translate_text("result.hero_personal_label", "tr"), html)
        self.assertIn(translate_text("result.personal_decision_title", "tr"), html)
        self.assertIn(translate_text("result.unlock_full_reading", "tr"), html)

    def test_parent_child_copy_is_localized(self):
        html = app.templates.env.get_template("result.html").render(
            {
                "request": SimpleNamespace(state=SimpleNamespace(current_user=None, lang="tr")),
                "language": "tr",
                "full_name": "Çocuk",
                "birth_date": "2018-05-10",
                "birth_time": "09:15",
                "birth_city": "İstanbul, Türkiye",
                "normalized_birth_place": "İstanbul, Türkiye",
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
                "payload_json": {"generated_report_id": 91},
                "report_access": {
                    "is_preview": True,
                    "show_unlock_cta": True,
                    "can_unlock_here": True,
                    "checkout_mode": "payment",
                    "can_view_full_report": False,
                    "can_download_pdf": False,
                    "show_login_hint": False,
                    "unlock_success": False,
                    "access_label": "Önizleme",
                },
                "related_articles": [],
                "natal_data": {},
                "dasha_data": [],
                "navamsa_data": {},
                "transit_data": [],
                "eclipse_data": [],
            }
        )
        self.assertIn(translate_text("result.hero_parent_child_label", "tr"), html)
        self.assertIn(translate_text("result.child_decision_title", "tr"), html)
        self.assertIn(translate_text("result.unlock_full_child_report", "tr"), html)

    def test_articles_page_renders_in_turkish(self):
        response = self.client.get("/articles", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(translate_text("articles.hero_default_title", "tr"), response.text)
        self.assertIn(translate_text("articles.filter_all", "tr"), response.text)
        self.assertIn("Venüs Transiti: Değişim Kaçınılmaz", response.text)

    def test_articles_page_and_detail_render_english_article_content(self):
        listing = self.client.get("/articles", headers={"accept-language": "en"})
        detail = self.client.get("/articles/jupiter-transiti-jupiter-acele-etme-diyor", headers={"accept-language": "en"})
        self.assertEqual(listing.status_code, 200)
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Jupiter Transit: Jupiter Says", listing.text)
        self.assertIn("Jupiter retrograde in Gemini asks for observation", listing.text)
        self.assertIn("Jupiter Transit: Jupiter Says", detail.text)
        self.assertIn("While Jupiter is retrograde in Gemini", detail.text)
        self.assertNotIn("İkizler burcundaki Jüpiter retrosu", detail.text)

    def test_login_and_signup_pages_render_in_turkish(self):
        login_response = self.client.get("/login", headers={"accept-language": "tr"})
        signup_response = self.client.get("/signup", headers={"accept-language": "tr"})
        self.assertEqual(login_response.status_code, 200)
        self.assertEqual(signup_response.status_code, 200)
        self.assertIn(translate_text("auth.login_title", "tr"), login_response.text)
        self.assertIn(translate_text("auth.login_support_title", "tr"), login_response.text)
        self.assertIn(translate_text("auth.signup_title", "tr"), signup_response.text)
        self.assertIn(translate_text("auth.signup_support_title", "tr"), signup_response.text)
        self.assertIn(translate_text("auth.login_cta", "tr"), login_response.text)
        self.assertIn(translate_text("auth.signup_cta", "tr"), signup_response.text)

    def test_parent_child_form_renders_in_turkish(self):
        response = self.client.get("/reports/parent-child", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(translate_text("parent_child_form.hero_title", "tr"), response.text)
        self.assertIn(translate_text("parent_child_form.parent_title", "tr"), response.text)
        self.assertIn(translate_text("parent_child_form.child_title", "tr"), response.text)
        self.assertTrue(
            translate_text("parent_child_form.cta_submit", "tr") in response.text
            or "Harita Analizi Yakında" in response.text
        )

    def test_account_and_dashboard_render_in_turkish(self):
        user = self._create_user("member@example.com")
        with patch.object(app, "get_request_user", return_value=user):
            account_response = self.client.get("/account", headers={"accept-language": "tr"})
            dashboard_response = self.client.get("/dashboard", headers={"accept-language": "tr"})
        self.assertEqual(account_response.status_code, 200)
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertIn(translate_text("account.title", "tr"), account_response.text)
        self.assertIn(translate_text("account.plan_title", "tr"), account_response.text)
        self.assertIn(translate_text("account.plan_change_label", "tr"), account_response.text)
        self.assertIn(translate_text("dashboard.title", "tr"), dashboard_response.text)
        self.assertIn(translate_text("dashboard.next_title", "tr"), dashboard_response.text)
        self.assertIn(translate_text("dashboard.recent_reports_title", "tr"), dashboard_response.text)


if __name__ == "__main__":
    unittest.main()
