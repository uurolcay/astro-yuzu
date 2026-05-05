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

    def _review_item(self, *, title="Ashwini Review Chunk", entities=None, metadata=None, status="review_required", body_text=None):
        source_document = db_mod.SourceDocument(title="Nakshatra PDF", document_type="book")
        self.db.add(source_document)
        self.db.flush()
        default_metadata = {
            "review_required": status == "review_required",
            "status": status,
            "category": "nakshatra",
            "primary_entity": "ashwini",
            "source_title": "Nakshatra PDF",
            "confidence_level": "medium",
            "sensitivity_level": "low",
            "classical_view": "Ashwini supports initiative.",
            "premium_synthesis_sentence": "Ashwini can express as poised initiative when supported by the full chart.",
            "coverage_entities": ["ashwini", "career"],
        }
        if metadata:
            default_metadata.update(metadata)
        item = knowledge_service.create_knowledge_item(
            self.db,
            title=title,
            body_text=body_text or "Classical View: Ashwini brings initiative and quick response.",
            language="tr",
            item_type="nakshatra",
            summary_text="A calm synthesis sentence.",
            entities=entities or ["ashwini", "career"],
            source_document=source_document,
            metadata=default_metadata,
            created_by_user_id=self.admin.id,
            status=status,
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
        item_title = item.title
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn(item_title, response.text)

    def test_review_list_is_paginated(self):
        for index in range(3):
            self._review_item(title=f"Paged Review {index}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/review?page_size=2")
        self.assertEqual(response.status_code, 200)
        self.assertLessEqual(response.text.count("Paged Review"), 2)

    def test_review_sorting_by_title_status_sensitivity_and_date_works(self):
        alpha = self._review_item(title="Alpha Review", metadata={"sensitivity_level": "low"})
        zulu = self._review_item(title="Zulu Review", metadata={"sensitivity_level": "high"})
        active = self._review_item(title="Active Review", status="active")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            by_title = self.client.get("/admin/knowledge/review?sort=title&direction=asc&status=all")
            by_status = self.client.get("/admin/knowledge/review?sort=status&direction=asc&status=all")
            by_sensitivity = self.client.get("/admin/knowledge/review?sort=sensitivity&direction=desc&status=all")
            by_date = self.client.get("/admin/knowledge/review?sort=created_at&direction=desc&status=all")
        self.assertEqual(by_title.status_code, 200)
        self.assertEqual(by_status.status_code, 200)
        self.assertEqual(by_sensitivity.status_code, 200)
        self.assertEqual(by_date.status_code, 200)
        self.assertLess(by_title.text.index("Active Review"), by_title.text.index("Alpha Review"))
        self.assertLess(by_sensitivity.text.index("Zulu Review"), by_sensitivity.text.index("Alpha Review"))
        self.assertIn(str(active.id), by_status.text)
        self.assertIn(str(zulu.id), by_date.text)

    def test_review_filtering_by_query_category_sensitivity_status_and_source(self):
        target = self._review_item(
            title="Filter Target Dhanishta",
            metadata={"category": "dasha", "sensitivity_level": "high", "primary_entity": "dhanishta"},
            body_text="Specific searchable dasha body for Dhanishta review.",
        )
        self._review_item(title="Filter Other", metadata={"category": "nakshatra", "sensitivity_level": "low"})
        source_id = target.source_document_id
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(
                f"/admin/knowledge/review?q=searchable+dasha&category=dasha&sensitivity=high&status=review_required&source_document_id={source_id}"
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn("Filter Target Dhanishta", response.text)
        self.assertNotIn("Filter Other", response.text)

    def test_unknown_title_repair_creates_meaningful_fallback_title(self):
        item = self._review_item(
            title="Dhanishta - Unknown",
            metadata={
                "category": "nakshatra",
                "primary_entity": "dhanishta",
                "source_page_start": 42,
                "confidence_level": "medium",
                "sensitivity_level": "low",
            },
        )
        csrf = self._csrf("/admin/knowledge/review")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post("/admin/knowledge/repair-titles", data={"csrf_token": csrf}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        repaired = self.db.query(db_mod.KnowledgeItem).filter_by(id=item.id).first()
        self.assertEqual(repaired.title, "Dhanishta — Nakshatra Knowledge — p.42")

    def test_review_ui_renders_localized_status_and_sensitivity_labels(self):
        self._review_item(title="Localized Labels", metadata={"sensitivity_level": "medium"})
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/review")
        self.assertEqual(response.status_code, 200)
        self.assertIn("İnceleme Bekliyor", response.text)
        self.assertIn("Orta", response.text)

    def test_detail_page_opens(self):
        item = self._review_item()
        item_id = item.id
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(f"/admin/knowledge/review/{item_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("İçerik İnceleme", response.text)

    def test_edit_form_saves(self):
        item = self._review_item()
        item_id = item.id
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}",
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
        refreshed = self.db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        metadata = json.loads(refreshed.metadata_json)
        self.assertEqual(refreshed.title, "Ashwini Reviewed")
        self.assertEqual(metadata["classical_view"], "Edited classical note")
        self.assertEqual(metadata["premium_synthesis_sentence"], "Ashwini may express as refined initiative.")
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)

    def test_publish_item_makes_it_retrieval_visible(self):
        item = self._review_item(title="Career Ashwini", entities=["career", "ashwini"])
        item_id = item.id
        payload = {"report_type": "career", "language": "tr", "natal_data": {"planets": [{"name": "Mars"}]}}
        before = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertEqual(before["chunks"], [])
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}/publish",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        after = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertGreaterEqual(len(after["chunks"]), 1)

    def test_publish_does_not_rebuild_coverage_synchronously(self):
        item = self._review_item(title="No Sync Coverage", entities=["career", "ashwini"])
        item_id = item.id
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.coverage_svc,
            "compute_knowledge_coverage",
            side_effect=AssertionError("coverage rebuild must stay manual"),
        ):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}/publish",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)

    def test_reject_item_excludes_it_from_retrieval(self):
        item = self._review_item(title="Rejectable Ashwini", entities=["career", "ashwini"])
        item_id = item.id
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post(f"/admin/knowledge/review/{item_id}/publish", data={"csrf_token": csrf}, follow_redirects=False)
        payload = {"report_type": "career", "language": "tr", "natal_data": {"planets": [{"name": "Mars"}]}}
        self.db.expire_all()
        visible = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertGreaterEqual(len(visible["chunks"]), 1)
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}/reject",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        hidden = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertEqual(hidden["chunks"], [])

    def test_non_admin_access_is_blocked(self):
        item = self._review_item()
        item_id = item.id
        response_one = self.client.get("/admin/knowledge/review", follow_redirects=False)
        response_two = self.client.get(f"/admin/knowledge/review/{item_id}", follow_redirects=False)
        self.assertIn(response_one.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_two.status_code, {302, 303, 307, 401, 403})

    def test_csrf_is_required(self):
        item = self._review_item()
        item_id = item.id
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}",
                data={"title": "Missing token"},
                follow_redirects=False,
            )
        self.assertIn(response.status_code, {400, 403})

    def test_no_service_order_is_created(self):
        item = self._review_item()
        item_id = item.id
        before = self.db.query(db_mod.ServiceOrder).count()
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post(f"/admin/knowledge/review/{item_id}/publish", data={"csrf_token": csrf}, follow_redirects=False)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before)
