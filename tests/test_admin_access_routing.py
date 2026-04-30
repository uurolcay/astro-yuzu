import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from werkzeug.security import generate_password_hash

import app
import database as db_mod


class AdminAccessRoutingTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _create_user(self, *, email, password="password123", is_admin=False):
        user = db_mod.AppUser(
            email=email,
            password_hash=generate_password_hash(password),
            name=email.split("@")[0],
            is_admin=is_admin,
            is_active=True,
            plan_code="free",
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def test_allowlisted_user_reaches_admin_dashboard_from_admin_login_flow(self):
        self._create_user(email="admin-allow@example.com", is_admin=False)
        with patch.dict("os.environ", {"ADMIN_EMAILS": "admin-allow@example.com"}, clear=False):
            response = self.client.post(
                "/login?next=/admin",
                data={"email": "admin-allow@example.com", "password": "password123", "next_path": "/admin"},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/admin")

            admin_response = self.client.get("/admin")
            self.assertEqual(admin_response.status_code, 200)
            self.assertIn("admin-page-title", admin_response.text)
            self.assertIn("/admin/reports", admin_response.text)
            self.assertNotIn('class="dashboard-shell"', admin_response.text)

            whoami = self.client.get("/admin/debug/whoami")
            self.assertEqual(whoami.status_code, 200)
            payload = whoami.json()
            self.assertEqual(payload["authenticated_email"], "admin-allow@example.com")
            self.assertTrue(payload["is_admin"])
            self.assertTrue(payload["in_admin_allowlist"])

    def test_non_admin_user_does_not_see_admin_dashboard(self):
        self._create_user(email="member@example.com", is_admin=False)
        response = self.client.post(
            "/login?next=/admin",
            data={"email": "member@example.com", "password": "password123", "next_path": "/admin"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

        admin_response = self.client.get("/admin", follow_redirects=False)
        self.assertEqual(admin_response.status_code, 403)
        self.assertNotIn("/admin/reports", admin_response.text)

    def test_dashboard_route_remains_distinct_from_admin_route(self):
        self._create_user(email="real-admin@example.com", is_admin=True)
        login_response = self.client.post(
            "/login?next=/admin",
            data={"email": "real-admin@example.com", "password": "password123", "next_path": "/admin"},
            follow_redirects=False,
        )
        self.assertEqual(login_response.status_code, 303)
        self.assertEqual(login_response.headers["location"], "/admin")

        account_dashboard = self.client.get("/dashboard")
        admin_dashboard = self.client.get("/admin")

        self.assertEqual(account_dashboard.status_code, 200)
        self.assertEqual(admin_dashboard.status_code, 200)
        self.assertIn('class="dashboard-shell"', account_dashboard.text)
        self.assertIn("admin-page-title", admin_dashboard.text)

    def test_bootstrap_promotes_existing_user_matching_admin_email(self):
        user = self._create_user(email="bootstrap-admin@example.com", is_admin=False)
        with patch.dict("os.environ", {"ADMIN_EMAIL": "bootstrap-admin@example.com", "ADMIN_PASSWORD": ""}, clear=False):
            result = app._bootstrap_admin_user_from_env()
        self.db.refresh(user)
        self.assertEqual(result["status"], "verified")
        self.assertTrue(user.is_admin)
        self.assertTrue(user.is_active)


if __name__ == "__main__":
    unittest.main()
