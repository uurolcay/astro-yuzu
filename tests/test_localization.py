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
        self.assertEqual(translate_text("reports.child_cta", "de"), "Unlock Your Full Child Report")
        self.assertEqual(translate_text("missing.key", "tr"), "missing.key")

    def test_reports_page_renders_in_turkish(self):
        user = self._create_user()
        with patch.object(app, "get_request_user", return_value=user):
            response = self.client.get("/reports", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Hangi okuma sizin için daha uygun?", response.text)
        self.assertIn("Kişisel Vedik Okuma", response.text)
        self.assertIn("Çocuk Raporunun Tamamını Aç", response.text)

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
                "report_access": {"is_preview": True, "show_unlock_cta": True, "can_unlock_here": True, "checkout_mode": "payment", "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Önizleme"},
                "related_articles": [],
                "natal_data": {},
                "dasha_data": [],
                "navamsa_data": {},
                "transit_data": [],
                "eclipse_data": [],
            }
        )
        self.assertIn("Şu anki ana odağınız", html)
        self.assertIn("Doğru okumayı seçiyorsunuz", html)
        self.assertIn("Tüm Raporu Aç", html)

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
                "report_access": {"is_preview": True, "show_unlock_cta": True, "can_unlock_here": True, "checkout_mode": "payment", "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Önizleme"},
                "related_articles": [],
                "natal_data": {},
                "dasha_data": [],
                "navamsa_data": {},
                "transit_data": [],
                "eclipse_data": [],
            }
        )
        self.assertIn("Çocuğunuzun mevcut duygusal ve gelişimsel paterni", html)
        self.assertIn("Devam etmeden önce kısa bir hatırlatma", html)
        self.assertIn("Çocuk Raporunun Tamamını Aç", html)

    def test_articles_page_renders_in_turkish(self):
        response = self.client.get("/articles", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("İçgörüler ve Makaleler", response.text)
        self.assertIn("Tümü", response.text)
