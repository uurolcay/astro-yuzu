import inspect
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from werkzeug.security import generate_password_hash

import app
import database as db_mod


class AdminAccessRoutingTests(unittest.TestCase):
    SECURITY_SESSION_ERROR = "G\u00fcvenlik oturumu s\u00fcresi doldu. L\u00fctfen sayfay\u0131 yenileyip tekrar deneyin."

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

    def _login_csrf_token(self, next_path="/admin"):
        response = self.client.get(f"/login?next={next_path}")
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name="csrf_token"\s+value="([^"]+)"', response.text)
        self.assertIsNotNone(match, response.text[:500])
        self.assertIn('method="post"', response.text)
        self.assertIn('action="/login"', response.text)
        self.assertIn('name="next_path"', response.text)
        self.assertIn("login-template-version: csrf-v4-raw", response.text)
        return match.group(1)

    def _login(self, *, email, password="password123", next_path="/admin", csrf_token=None):
        if csrf_token is None:
            csrf_token = self._login_csrf_token(next_path)
        return self.client.post(
            f"/login?next={next_path}",
            data={
                "email": email,
                "password": password,
                "csrf_token": csrf_token,
                "next_path": next_path,
            },
            follow_redirects=False,
        )

    def _post_login_routes(self):
        return [
            route
            for route in app.app.routes
            if getattr(route, "path", None) == "/login" and "POST" in (getattr(route, "methods", set()) or set())
        ]

    def test_post_login_route_is_unique(self):
        routes = self._post_login_routes()
        self.assertEqual(len(routes), 1)
        self.assertEqual(getattr(routes[0], "endpoint", None).__name__, "login_submit")

    def test_post_login_handler_uses_request_only_signature(self):
        signature = inspect.signature(app.login_submit)
        self.assertEqual(list(signature.parameters), ["request"])

    def test_debug_version_endpoint_returns_login_version(self):
        response = self.client.get("/debug/version")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"app_version": "csrf-v4-raw", "login_template_version": "csrf-v4-raw"},
        )

    def test_login_page_returns_csrf_hidden_input_and_template_version(self):
        response = self.client.get("/login?next=/admin")
        self.assertEqual(response.status_code, 200)
        self.assertIn("login-template-version: csrf-v4-raw", response.text)
        match = re.search(r'name="csrf_token"\s+value="([^"]+)"', response.text)
        self.assertIsNotNone(match, response.text[:500])
        self.assertTrue(match.group(1).strip())
        self.assertIn('name="next_path"', response.text)
        self.assertIn('name="next_path" value="/admin"', response.text)
        self.assertIn('method="post"', response.text)
        self.assertIn('action="/login"', response.text)

    def test_allowlisted_user_reaches_admin_dashboard_from_admin_login_flow(self):
        self._create_user(email="admin-allow@example.com", is_admin=False)
        with patch.dict("os.environ", {"ADMIN_EMAILS": "admin-allow@example.com"}, clear=False):
            response = self._login(email="admin-allow@example.com")
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
        response = self._login(email="member@example.com")
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/dashboard")

        admin_response = self.client.get("/admin", follow_redirects=False)
        self.assertEqual(admin_response.status_code, 403)
        self.assertNotIn("/admin/reports", admin_response.text)

    def test_dashboard_route_remains_distinct_from_admin_route(self):
        self._create_user(email="real-admin@example.com", is_admin=True)
        login_response = self._login(email="real-admin@example.com")
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

    def test_admin_credentials_login_redirects_to_admin(self):
        self._create_user(email="admin@example.com", is_admin=True)
        response = self._login(email="admin@example.com")
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/admin")

    def test_wrong_password_shows_clear_error_message(self):
        self._create_user(email="admin@example.com", is_admin=True)
        response = self._login(email="admin@example.com", password="wrong-password")
        self.assertEqual(response.status_code, 400)
        self.assertIn("E-posta veya \u015fifre hatal\u0131.", response.text)
        self.assertIn('value="/admin"', response.text)

    def test_login_rejects_missing_csrf_token(self):
        self._create_user(email="admin@example.com", is_admin=True)
        with self.assertLogs(app.logger, level="INFO"):
            response = self.client.post(
                "/login?next=/admin",
                data={"email": "admin@example.com", "password": "password123", "next_path": "/admin"},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 403)
        self.assertNotEqual(response.text.strip(), "")
        self.assertNotEqual(response.text, "Invalid CSRF token")
        self.assertIn("login-template-version: csrf-v4-raw", response.text)
        self.assertIn(self.SECURITY_SESSION_ERROR, response.text)
        self.assertRegex(response.text, r'name="csrf_token"\s+value="[^"]+"')
        self.assertIn('name="next_path" value="/admin"', response.text)

    def test_login_invalid_csrf_renders_visible_security_session_error(self):
        self._create_user(email="admin@example.com", is_admin=True)
        self.client.get("/login?next=/admin")
        with self.assertLogs(app.logger, level="INFO"):
            response = self.client.post(
                "/login?next=/admin",
                data={
                    "email": "admin@example.com",
                    "password": "password123",
                    "csrf_token": "invalid-token",
                    "next_path": "/admin",
                },
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 403)
        self.assertNotEqual(response.text.strip(), "")
        self.assertNotEqual(response.text, "Invalid CSRF token")
        self.assertIn("login-template-version: csrf-v4-raw", response.text)
        self.assertIn(self.SECURITY_SESSION_ERROR, response.text)
        self.assertRegex(response.text, r'name="csrf_token"\s+value="[^"]+"')
        self.assertIn('name="next_path" value="/admin"', response.text)

    def test_login_preserves_admin_next_path(self):
        self._create_user(email="admin-next@example.com", is_admin=True)
        csrf_token = self._login_csrf_token("/admin")
        response = self._login(email="admin-next@example.com", next_path="/admin", csrf_token=csrf_token)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/admin")


if __name__ == "__main__":
    unittest.main()
