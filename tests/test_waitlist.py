import inspect
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod


class WaitlistTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.WaitlistEntry).delete()
        self.db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "waitlist-admin@example.com").delete()
        self.admin = db_mod.AppUser(
            email="waitlist-admin@example.com",
            password_hash="hash",
            name="Waitlist Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.db.add(self.admin)
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.WaitlistEntry).delete()
        self.db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "waitlist-admin@example.com").delete()
        self.db.commit()
        self.db.close()

    def _csrf_from_home(self, language="tr"):
        response = self.client.get("/", headers={"accept-language": language})
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match, response.text[:500])
        return match.group(1), response.text

    def _submit(self, email, *, language="tr", csrf_token=None, interests=None):
        if csrf_token is None:
            csrf_token, _ = self._csrf_from_home(language)
        data = {
            "email": email,
            "language": language,
            "source_page": "/reports",
            "csrf_token": csrf_token,
            "interests": interests or ["reports"],
        }
        return self.client.post("/waitlist", data=data)

    def _request_admin_pair(self, request, db):
        admin = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "waitlist-admin@example.com").first()
        return admin, None

    def test_waitlist_form_route_saves_new_email(self):
        response = self._submit("new-person@example.com", interests=["reports", "consultation"])
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertFalse(payload["duplicate"])
        entry = self.db.query(db_mod.WaitlistEntry).filter(db_mod.WaitlistEntry.email == "new-person@example.com").first()
        self.assertIsNotNone(entry)
        self.assertEqual(entry.language, "tr")
        self.assertEqual(entry.source_page, "/reports")
        self.assertIn("reports", entry.interest_json)
        self.assertIn("consultation", entry.interest_json)

    def test_duplicate_email_returns_duplicate_message(self):
        self._submit("duplicate@example.com")
        response = self._submit("duplicate@example.com")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["duplicate"])
        self.assertEqual(payload["message"], "Bu e-posta zaten ön kayıt listesinde.")
        self.assertEqual(self.db.query(db_mod.WaitlistEntry).filter(db_mod.WaitlistEntry.email == "duplicate@example.com").count(), 1)

    def test_invalid_email_returns_error(self):
        response = self._submit("not-an-email")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["message"], "Lütfen geçerli bir e-posta adresi girin.")

    def test_empty_email_returns_error(self):
        response = self._submit("")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["message"], "Lütfen e-posta adresinizi girin.")

    def test_tr_success_message_is_correct(self):
        response = self._submit("tr-success@example.com", language="tr")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["message"],
            "Ön kaydınız alındı. Raporlama veya danışmanlık açıldığında size e-posta göndereceğiz.",
        )

    def test_en_success_message_is_correct(self):
        response = self._submit("en-success@example.com", language="en")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["message"],
            "You’re on the list. We’ll email you when reports or consultations become available.",
        )

    def test_homepage_drawer_has_no_forbidden_word_and_has_tr_cta(self):
        _csrf, html = self._csrf_from_home("tr")
        self.assertNotIn("lansman", html.lower())
        self.assertIn("Ön Kayıt", html)
        self.assertIn('role="dialog"', html)
        self.assertIn("Raporlama veya danışmanlık açıldığında size haber verelim.", html)

    def test_homepage_drawer_has_en_cta(self):
        _csrf, html = self._csrf_from_home("en")
        self.assertIn("Get Notified", html)
        self.assertIn("We’ll notify you when reports or consultations become available.", html)

    def test_admin_waitlist_page_requires_admin(self):
        response = self.client.get("/admin/waitlist", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})

    def test_admin_waitlist_page_uses_pagination(self):
        for index in range(3):
            self.db.add(db_mod.WaitlistEntry(email=f"paged-{index}@example.com", language="tr", interest_json='["reports"]'))
        self.db.commit()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/waitlist?page=1&page_size=2")
        self.assertEqual(response.status_code, 200)
        self.assertIn("paged-2@example.com", response.text)
        self.assertIn("paged-1@example.com", response.text)
        self.assertNotIn("paged-0@example.com", response.text)
        self.assertIn("Next", response.text)
        source = inspect.getsource(app.admin_waitlist)
        self.assertIn(".limit(limit)", source)

    def test_csrf_missing_is_rejected(self):
        response = self.client.post("/waitlist", data={"email": "missing-csrf@example.com", "language": "tr"})
        self.assertEqual(response.status_code, 403)
