import json
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from services import knowledge_service, retrieval_service


class KnowledgeReviewWorkflowTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="knowledge-review-admin@example.com",
            password_hash="hash",
            name="Knowledge Review Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.db.add(self.admin)
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _request_admin_user(self, request, db):
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "knowledge-review-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _review_item(self, *, title="Ashwini Review Chunk", entities=None, metadata=None):
        source_document = db_mod.SourceDocument(title="Nakshatra PDF", document_type="book")
        self.db.add(source_document)
        self.db.flush()
        item = knowledge_service.create_knowledge_item(
            self.db,
            title=title,
            body_text="Classical View: Ashwini brings initiative and quick response.",
            language="tr",
            item_type="nakshatra",
            summary_text="A calm synthesis sentence.",
            entities=entities or ["ashwini", "career"],
            source_document=source_document,
            metadata=metadata or {
                "review_required": True,
                "status": "review_required",
                "category": "nakshatra",
                "primary_entity": "ashwini",
                "source_title": "Nakshatra PDF",
                "confidence_level": "medium",
                "sensitivity_level": "low",
                "classical_view": "Ashwini supports initiative.",
                "premium_synthesis_sentence": "Ashwini can express as poised initiative when supported by the full chart.",
                "coverage_entities": ["ashwini", "career"],
            },
            created_by_user_id=self.admin.id,
            status="review_required",
        )
        self.db.commit()
        return item

    def _csrf(self, path):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_review_required_item_appears_in_list(self):
        item = self._review_item()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn(item.title, response.text)

    def test_detail_page_opens(self):
        item = self._review_item()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(f"/admin/knowledge/review/{item.id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Knowledge Review", response.text)

    def test_edit_form_saves(self):
        item = self._review_item()
        csrf = self._csrf(f"/admin/knowledge/review/{item.id}")
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item.id}",
                data={
                    "csrf_token": csrf,
                    "title": "Ashwini Reviewed",
                    "classical_view": "Edited classical note",
                    "modern_synthesis": "Edited synthesis",
                    "interpretation_logic": "Edited logic",
                    "strong_condition": "When Mars supports initiative",
                    "weak_condition": "When Saturn constrains expression",
                    "risk_pattern": "Impatience",
                    "opportunity_pattern": "Fast mobilization",
                    "dasha_activation": "Stronger in Mars periods",
                    "transit_activation": "Triggered by Mars transits",
                    "safe_language_notes": "Use advisory language.",
                    "what_not_to_say": "Do not promise certainty.",
                    "premium_synthesis_sentence": "Ashwini may express as refined initiative.",
                    "confidence_level": "high",
                    "sensitivity_level": "medium",
                    "tags": "ashwini, review",
                    "coverage_entities": "ashwini, career",
                },
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        refreshed = self.db.query(db_mod.KnowledgeItem).filter_by(id=item.id).first()
        metadata = json.loads(refreshed.metadata_json)
        self.assertEqual(refreshed.title, "Ashwini Reviewed")
        self.assertEqual(metadata["classical_view"], "Edited classical note")
        self.assertEqual(metadata["premium_synthesis_sentence"], "Ashwini may express as refined initiative.")
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)

    def test_publish_item_makes_it_retrieval_visible(self):
        item = self._review_item(title="Career Ashwini", entities=["career", "ashwini"])
        payload = {"report_type": "career", "language": "tr", "natal_data": {"planets": [{"name": "Mars"}]}}
        before = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertEqual(before["chunks"], [])
        csrf = self._csrf(f"/admin/knowledge/review/{item.id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item.id}/publish",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        after = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertGreaterEqual(len(after["chunks"]), 1)

    def test_reject_item_excludes_it_from_retrieval(self):
        item = self._review_item(title="Rejectable Ashwini", entities=["career", "ashwini"])
        csrf = self._csrf(f"/admin/knowledge/review/{item.id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post(f"/admin/knowledge/review/{item.id}/publish", data={"csrf_token": csrf}, follow_redirects=False)
        payload = {"report_type": "career", "language": "tr", "natal_data": {"planets": [{"name": "Mars"}]}}
        self.db.expire_all()
        visible = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertGreaterEqual(len(visible["chunks"]), 1)
        csrf = self._csrf(f"/admin/knowledge/review/{item.id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item.id}/reject",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        hidden = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertEqual(hidden["chunks"], [])

    def test_non_admin_access_is_blocked(self):
        item = self._review_item()
        response_one = self.client.get("/admin/knowledge/review", follow_redirects=False)
        response_two = self.client.get(f"/admin/knowledge/review/{item.id}", follow_redirects=False)
        self.assertIn(response_one.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_two.status_code, {302, 303, 307, 401, 403})

    def test_csrf_is_required(self):
        item = self._review_item()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item.id}",
                data={"title": "Missing token"},
                follow_redirects=False,
            )
        self.assertIn(response.status_code, {400, 403})

    def test_no_service_order_is_created(self):
        item = self._review_item()
        before = self.db.query(db_mod.ServiceOrder).count()
        csrf = self._csrf(f"/admin/knowledge/review/{item.id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post(f"/admin/knowledge/review/{item.id}/publish", data={"csrf_token": csrf}, follow_redirects=False)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before)
