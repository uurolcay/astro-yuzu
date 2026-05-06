import json
import re
import unittest
from unittest.mock import patch

from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient

import app
import database as db_mod
from services import knowledge_service


class BulkReviewWorkflowTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="bulk-review-admin@example.com",
            password_hash="hash",
            name="Bulk Review Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.member = db_mod.AppUser(
            email="bulk-review-member@example.com",
            password_hash="hash",
            name="Member",
            is_admin=False,
            is_active=True,
            plan_code="free",
        )
        self.db.add_all([self.admin, self.member])
        self.db.commit()
        self.admin_id = self.admin.id
        self.member_id = self.member.id
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
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "bulk-review-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _request_member_pair(self, request, db):
        user = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "bulk-review-member@example.com").first()
        return user, HTMLResponse("Admin access denied.", status_code=403)

    def _review_item(self, *, title, confidence_level="medium", sensitivity_level="low", metadata=None, body_text=None):
        source_document = db_mod.SourceDocument(title="Bulk Review PDF", document_type="book")
        self.db.add(source_document)
        self.db.flush()
        default_metadata = {
            "review_required": True,
            "status": "review_required",
            "category": "nakshatra",
            "primary_entity": "ashwini",
            "source_title": "Bulk Review PDF",
            "confidence_level": confidence_level,
            "sensitivity_level": sensitivity_level,
            "coverage_entities": ["ashwini", "career"],
            "premium_synthesis_sentence": "A measured synthesis sentence.",
        }
        if metadata:
            default_metadata.update(metadata)
        item = knowledge_service.create_knowledge_item(
            self.db,
            title=title,
            body_text=body_text or "Review body text for deterministic testing.",
            language="tr",
            item_type="nakshatra",
            summary_text="Summary",
            entities=["ashwini", "career"],
            source_document=source_document,
            metadata=default_metadata,
            created_by_user_id=self.admin_id,
            status="review_required",
        )
        self.db.commit()
        return item.id

    def _csrf(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/review")
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_bulk_approve_changes_status(self):
        item_one_id = self._review_item(title="Bulk Approve One")
        item_two_id = self._review_item(title="Bulk Approve Two")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/knowledge/review/bulk-approve",
                data={"csrf_token": csrf, "knowledge_ids": [str(item_one_id), str(item_two_id)]},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed_one = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_one_id).first()
            refreshed_two = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_two_id).first()
        finally:
            lookup_db.close()
        self.assertEqual(refreshed_one.status, "published")
        self.assertEqual(refreshed_two.status, "published")
        self.assertFalse(json.loads(refreshed_one.metadata_json).get("review_required"))

    def test_bulk_approve_does_not_rebuild_coverage_synchronously(self):
        item_id = self._review_item(title="Bulk No Coverage Rebuild")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.coverage_svc,
            "compute_knowledge_coverage",
            side_effect=AssertionError("coverage rebuild must stay manual"),
        ):
            response = self.client.post(
                "/admin/knowledge/review/bulk-approve",
                data={"csrf_token": csrf, "knowledge_ids": [str(item_id)]},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)

    def test_bulk_reject_works(self):
        item_one_id = self._review_item(title="Bulk Reject One")
        item_two_id = self._review_item(title="Bulk Reject Two")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/knowledge/review/bulk-reject",
                data={"csrf_token": csrf, "knowledge_ids": [str(item_one_id), str(item_two_id)]},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed_one = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_one_id).first()
            refreshed_two = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_two_id).first()
        finally:
            lookup_db.close()
        self.assertEqual(refreshed_one.status, "rejected")
        self.assertEqual(refreshed_two.status, "rejected")

    def test_auto_approve_filters_correctly(self):
        publishable_id = self._review_item(title="High Safe", confidence_level="high", sensitivity_level="low")
        sensitive_id = self._review_item(title="High Sensitive", confidence_level="high", sensitivity_level="sensitive")
        low_conf_id = self._review_item(title="Low Safe", confidence_level="low", sensitivity_level="low")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/knowledge/review/auto-approve",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            publishable = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=publishable_id).first()
            sensitive = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=sensitive_id).first()
            low_conf = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=low_conf_id).first()
        finally:
            lookup_db.close()
        self.assertEqual(publishable.status, "published")
        self.assertEqual(sensitive.status, "review_required")
        self.assertEqual(low_conf.status, "review_required")

    def test_sensitive_items_are_not_auto_approved(self):
        item_id = self._review_item(title="Sensitive Item", confidence_level="high", sensitivity_level="sensitive")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post("/admin/knowledge/review/auto-approve", data={"csrf_token": csrf}, follow_redirects=False)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        finally:
            lookup_db.close()
        self.assertEqual(refreshed.status, "review_required")

    def test_low_confidence_items_are_not_auto_approved(self):
        item_id = self._review_item(title="Low Confidence", confidence_level="low", sensitivity_level="low")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post("/admin/knowledge/review/auto-approve", data={"csrf_token": csrf}, follow_redirects=False)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        finally:
            lookup_db.close()
        self.assertEqual(refreshed.status, "review_required")

    def test_auto_approve_safe_filtered_items_skips_sensitive_noisy_toc_and_index(self):
        safe_id = self._review_item(title="Safe Medium", confidence_level="medium", sensitivity_level="low")
        toc_id = self._review_item(title="Contents", confidence_level="high", metadata={"is_toc": True})
        index_id = self._review_item(title="Index", confidence_level="high", metadata={"is_index": True})
        noisy_id = self._review_item(title="Noisy", confidence_level="high", metadata={"noise_score": 0.9})
        unknown_short_id = self._review_item(title="Ashwini - Unknown", confidence_level="high", body_text="short body")
        sensitive_id = self._review_item(title="Sensitive", confidence_level="high", sensitivity_level="sensitive")
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/knowledge/review/auto-approve",
                data={"csrf_token": csrf, "status": "review_required", "page_size": "100"},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            statuses = {
                item_id: lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first().status
                for item_id in [safe_id, toc_id, index_id, noisy_id, unknown_short_id, sensitive_id]
            }
        finally:
            lookup_db.close()
        self.assertEqual(statuses[safe_id], "published")
        self.assertEqual(statuses[toc_id], "review_required")
        self.assertEqual(statuses[index_id], "review_required")
        self.assertEqual(statuses[noisy_id], "review_required")
        self.assertEqual(statuses[unknown_short_id], "review_required")
        self.assertEqual(statuses[sensitive_id], "review_required")

    def test_unfiltered_auto_approve_does_not_publish_entire_db_unexpectedly(self):
        item_ids = [
            self._review_item(title=f"Safe Visible Item {index}", confidence_level="medium", sensitivity_level="low")
            for index in range(55)
        ]
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/knowledge/review/auto-approve",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            published_count = (
                lookup_db.query(db_mod.KnowledgeItem)
                .filter(db_mod.KnowledgeItem.id.in_(item_ids), db_mod.KnowledgeItem.status == "published")
                .count()
            )
            review_count = (
                lookup_db.query(db_mod.KnowledgeItem)
                .filter(db_mod.KnowledgeItem.id.in_(item_ids), db_mod.KnowledgeItem.status == "review_required")
                .count()
            )
        finally:
            lookup_db.close()
        self.assertEqual(published_count, app.DEFAULT_ADMIN_PAGE_SIZE)
        self.assertEqual(review_count, 55 - app.DEFAULT_ADMIN_PAGE_SIZE)

    def test_non_admin_access_blocked(self):
        item_id = self._review_item(title="Blocked Item")
        with patch.object(app, "_require_admin_user", side_effect=self._request_member_pair):
            response_one = self.client.post("/admin/knowledge/review/bulk-approve", data={"knowledge_ids": [str(item_id)]}, follow_redirects=False)
            response_two = self.client.post("/admin/knowledge/review/bulk-reject", data={"knowledge_ids": [str(item_id)]}, follow_redirects=False)
            response_three = self.client.post("/admin/knowledge/review/auto-approve", data={}, follow_redirects=False)
        self.assertIn(response_one.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_two.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_three.status_code, {302, 303, 307, 401, 403})

    def test_no_service_order_created(self):
        item_id = self._review_item(title="No Order Item", confidence_level="high", sensitivity_level="low")
        before = self.db.query(db_mod.ServiceOrder).count()
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            self.client.post(
                "/admin/knowledge/review/bulk-approve",
                data={"csrf_token": csrf, "knowledge_ids": [str(item_id)]},
                follow_redirects=False,
            )
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before)


if __name__ == "__main__":
    unittest.main()
