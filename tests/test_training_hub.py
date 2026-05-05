import unittest
from unittest.mock import patch

from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient

import app
import database as db_mod


class TrainingHubTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.TrainingTask).delete()
        self.db.query(db_mod.KnowledgeGap).delete()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == app.ADMIN_COVERAGE_CACHE_KEY).delete()
        self.admin = db_mod.AppUser(
            email="training-admin@example.com",
            password_hash="hash",
            name="Training Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.member = db_mod.AppUser(
            email="member@example.com",
            password_hash="hash",
            name="Member",
            is_admin=False,
            is_active=True,
            plan_code="free",
        )
        self.db.add_all([self.admin, self.member])
        self.db.commit()
        self.db.add(
            db_mod.KnowledgeItem(
                title="Saturn Dasha",
                item_type="dasha",
                language="tr",
                body_text="Saturn dasha body",
                entities_json='["saturn","saturn_dasha"]',
                coverage_entities_json='["saturn","saturn_dasha"]',
                status="active",
            )
        )
        self.db.add(
            db_mod.KnowledgeGap(
                report_type="career",
                language="tr",
                missing_entities_json='["saturn"]',
                status="open",
            )
        )
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.TrainingTask).delete()
        self.db.query(db_mod.KnowledgeGap).delete()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.query(db_mod.SiteSetting).filter(db_mod.SiteSetting.key == app.ADMIN_COVERAGE_CACHE_KEY).delete()
        self.db.commit()
        self.db.close()

    def _request_admin_user(self, request, db):
        return (
            db.query(db_mod.AppUser)
            .filter(db_mod.AppUser.email == "training-admin@example.com")
            .first()
        )

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _request_member_pair(self, request, db):
        user = (
            db.query(db_mod.AppUser)
            .filter(db_mod.AppUser.email == "member@example.com")
            .first()
        )
        return user, HTMLResponse("Admin access denied.", status_code=403)

    def test_admin_training_hub_returns_200(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/training")
        self.assertEqual(response.status_code, 200)
        self.assertIn("AI Astrologer Training", response.text)

    def test_training_hub_does_not_rebuild_coverage_on_get(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.coverage_svc,
            "compute_knowledge_coverage",
            side_effect=AssertionError("training hub must use cached coverage"),
        ):
            response = self.client.get("/admin/training")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Coverage not calculated yet", response.text)

    def test_shared_admin_context_does_not_query_unread_contact_count(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app,
            "_unread_contact_count",
            side_effect=AssertionError("sidebar must not run DB count on every admin page"),
        ):
            response = self.client.get("/admin/training")
        self.assertEqual(response.status_code, 200)

    def test_admin_training_write_returns_200(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/training/write")
        self.assertEqual(response.status_code, 200)
        self.assertIn("El ile", response.text)

    def test_admin_training_write_prefills_entity(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/training/write?entity=saturn")
        self.assertEqual(response.status_code, 200)
        self.assertIn('value="saturn"', response.text)

    def test_admin_training_qa_returns_200(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/training/qa")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Q&amp;A Ekle", response.text)

    def test_existing_knowledge_route_still_works(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Knowledge K", response.text)
        self.assertIn("Training Hub", response.text)

    def test_knowledge_library_route_is_paginated(self):
        for index in range(3):
            self.db.add(
                db_mod.KnowledgeItem(
                    title=f"Paged Library {index}",
                    item_type="reference",
                    language="tr",
                    body_text="Paged body",
                    status="active",
                )
            )
        self.db.commit()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge?page_size=2")
        self.assertEqual(response.status_code, 200)
        self.assertLessEqual(response.text.count("Paged Library"), 2)

    def test_existing_documents_route_still_works(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/documents")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Knowledge Documents", response.text)
        self.assertIn("Upload PDF", response.text)

    def test_non_admin_is_blocked_on_new_routes(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_member_pair):
            response_one = self.client.get("/admin/training", follow_redirects=False)
            response_two = self.client.get("/admin/training/write", follow_redirects=False)
            response_three = self.client.get("/admin/training/qa", follow_redirects=False)
        self.assertIn(response_one.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_two.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_three.status_code, {302, 303, 307, 401, 403})

    def test_training_hub_context_contains_expected_keys(self):
        captured = {}

        def fake_template_response(*, request, name, context, status_code=200, **kwargs):
            captured["name"] = name
            captured["context"] = context
            return HTMLResponse("ok", status_code=status_code)

        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.templates,
            "TemplateResponse",
            side_effect=fake_template_response,
        ):
            response = self.client.get("/admin/training")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["name"], "admin/training_hub.html")
        for key in ("stats", "coverage", "recent_items", "priority_gaps"):
            self.assertIn(key, captured["context"])

    def test_training_write_context_contains_expected_keys(self):
        captured = {}

        def fake_template_response(*, request, name, context, status_code=200, **kwargs):
            captured["name"] = name
            captured["context"] = context
            return HTMLResponse("ok", status_code=status_code)

        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.templates,
            "TemplateResponse",
            side_effect=fake_template_response,
        ):
            response = self.client.get("/admin/training/write?entity=saturn")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["name"], "admin/training_write.html")
        self.assertIn("prefill_entity", captured["context"])
        self.assertIn("item_type_options", captured["context"])

    def test_no_service_order_created_by_new_routes(self):
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.get("/admin/training")
            self.client.get("/admin/training/write")
            self.client.get("/admin/training/qa")
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)


if __name__ == "__main__":
    unittest.main()
