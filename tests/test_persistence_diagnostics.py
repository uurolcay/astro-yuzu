import json
import os
import re
import tempfile
import unittest
import base64
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

import app
import database as db_mod
from services import knowledge_service


class PersistenceDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="persistence-admin@example.com",
            password_hash="hash",
            name="Persistence Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.member = db_mod.AppUser(
            email="persistence-member@example.com",
            password_hash="hash",
            name="Persistence Member",
            is_admin=False,
            is_active=True,
            plan_code="free",
        )
        self.db.add_all([self.admin, self.member])
        self.db.commit()
        self.admin_id = self.admin.id
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
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "persistence-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _request_member_pair(self, request, db):
        user = db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "persistence-member@example.com").first()
        return user, None if getattr(user, "is_admin", False) else app.HTMLResponse("Admin access denied.", status_code=403)

    def _csrf(self, path):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def _review_item(self, *, title="Persist Me"):
        source_document = db_mod.SourceDocument(title="Persist Source", document_type="book")
        self.db.add(source_document)
        self.db.flush()
        item = knowledge_service.create_knowledge_item(
            self.db,
            title=title,
            body_text="Persistence body text that produces stable chunks.",
            language="tr",
            item_type="reference",
            summary_text="Persistence summary",
            entities=["saturn", "career"],
            source_document=source_document,
            metadata={
                "review_required": True,
                "status": "review_required",
                "confidence_level": "high",
                "sensitivity_level": "low",
                "coverage_entities": ["saturn", "career"],
            },
            created_by_user_id=self.admin_id,
            status="review_required",
        )
        self.db.commit()
        return item.id

    def test_storage_debug_route_returns_200_for_admin(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/debug/storage")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("db_dialect", payload)
        self.assertIn("knowledge_items_count", payload)
        for key in (
            "database_url_missing",
            "production_sqlite_detected",
            "upload_dir_may_be_ephemeral",
            "is_postgresql",
            "is_sqlite",
            "db_pool_pre_ping",
            "db_pool_recycle_seconds",
            "db_pool_size",
            "db_max_overflow",
            "database_url_uses_internal_hint",
            "db_disconnect_patterns_enabled",
        ):
            self.assertIn(key, payload)
        for key in (
            "database_url_missing",
            "production_sqlite_detected",
            "upload_dir_may_be_ephemeral",
            "is_postgresql",
            "is_sqlite",
            "db_disconnect_patterns_enabled",
        ):
            self.assertIsInstance(payload[key], bool)

    def test_storage_debug_route_blocks_non_admin(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_member_pair):
            response = self.client.get("/admin/debug/storage", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})

    def test_storage_debug_counts_return_safely(self):
        self._review_item()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/debug/storage")
        payload = response.json()
        self.assertGreaterEqual(payload["source_documents_count"], 1)
        self.assertGreaterEqual(payload["knowledge_items_count"], 1)
        self.assertGreaterEqual(payload["knowledge_chunks_count"], 0)
        self.assertIn("warnings", payload)

    def test_upload_dir_env_is_honored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "persistent" / "uploads"
            with patch.dict(os.environ, {"UPLOAD_DIR": str(target)}, clear=False):
                upload_dir = app.get_upload_dir()
            self.assertEqual(upload_dir, target.resolve())
            self.assertTrue(upload_dir.exists())

    def test_default_local_uploads_still_work(self):
        with patch.dict(os.environ, {}, clear=True):
            upload_dir = app.get_upload_dir()
        self.assertEqual(upload_dir, (Path(app.BASE_DIR) / "uploads").resolve())
        self.assertTrue(upload_dir.exists())

    def test_storage_debug_shows_env_based_upload_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"UPLOAD_DIR": tmpdir, "RENDER": "true"}, clear=False):
                with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
                    response = self.client.get("/admin/debug/storage")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["uploads_dir_path"], str(Path(tmpdir).resolve()))
        self.assertIn("Upload dir may be ephemeral", payload.get("warnings", []))
        self.assertTrue(payload["upload_dir_may_be_ephemeral"])

    def test_postgres_database_url_does_not_report_production_sqlite(self):
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://user:pass@example.com/db", "RENDER": "true"}, clear=False):
            with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
                response = self.client.get("/admin/debug/storage")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["database_url_missing"])
        self.assertTrue(payload["is_postgresql"])
        self.assertFalse(payload["is_sqlite"])
        self.assertFalse(payload["production_sqlite_detected"])

    def test_missing_database_url_on_render_reports_production_sqlite(self):
        with patch.dict(os.environ, {"DATABASE_URL": "", "RENDER": "true"}, clear=False):
            with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
                response = self.client.get("/admin/debug/storage")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["database_url_missing"])
        self.assertTrue(payload["is_sqlite"])
        self.assertFalse(payload["is_postgresql"])
        self.assertTrue(payload["production_sqlite_detected"])

    def test_missing_upload_dir_on_render_reports_ephemeral_upload_dir(self):
        with patch.dict(os.environ, {"RENDER": "true"}, clear=False):
            os.environ.pop("UPLOAD_DIR", None)
            with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
                response = self.client.get("/admin/debug/storage")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["upload_dir_may_be_ephemeral"])

    def test_postgres_engine_options_include_pool_pre_ping(self):
        kwargs = db_mod.build_engine_kwargs("postgresql://user:pass@localhost/db")
        self.assertTrue(kwargs["pool_pre_ping"])
        self.assertEqual(kwargs["pool_timeout"], int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30")))
        self.assertIn("pool_size", kwargs)
        self.assertIn("max_overflow", kwargs)

    def test_postgres_pool_recycle_env_is_read(self):
        with patch.dict(os.environ, {"DB_POOL_RECYCLE_SECONDS": "123"}, clear=False):
            kwargs = db_mod.build_engine_kwargs("postgresql://user:pass@localhost/db")
        self.assertEqual(kwargs["pool_recycle"], 123)

    def test_disconnect_error_detects_ssl_bad_record_mac(self):
        exc = OperationalError(
            "SELECT 1",
            {},
            Exception("SSL error: decryption failed or bad record mac"),
        )
        self.assertTrue(db_mod.is_db_disconnect_error(exc))

    def test_signed_cookie_user_lookup_operational_error_returns_none(self):
        class _BrokenQuery:
            def filter(self, *args, **kwargs):
                return self

            def first(self):
                raise OperationalError(
                    "SELECT app_users",
                    {},
                    Exception("SSL error: decryption failed or bad record mac"),
                )

        class _BrokenDb:
            def __init__(self):
                self.rolled_back = False

            def query(self, *args, **kwargs):
                return _BrokenQuery()

            def rollback(self):
                self.rolled_back = True

        class _RetryDb(_BrokenDb):
            def close(self):
                pass

        payload = base64.b64encode(json.dumps({"user_id": self.admin_id}).encode("utf-8")).decode("utf-8")
        signed = app.TimestampSigner(str(app.SESSION_SECRET_KEY)).sign(payload.encode("utf-8")).decode("utf-8")
        request = SimpleNamespace(cookies={"session": signed})
        broken_db = _BrokenDb()
        retry_db = _RetryDb()
        with patch.object(db_mod, "SessionLocal", return_value=retry_db), patch.object(db_mod.engine, "dispose") as dispose:
            user = app._request_user_from_signed_cookie(request, broken_db)
        self.assertIsNone(user)
        self.assertTrue(broken_db.rolled_back)
        self.assertTrue(retry_db.rolled_back)
        self.assertTrue(dispose.called)

    def test_init_db_does_not_delete_existing_data(self):
        self._review_item()
        before_items = self.db.query(db_mod.KnowledgeItem).count()
        before_docs = self.db.query(db_mod.SourceDocument).count()
        db_mod.init_db()
        lookup_db = db_mod.SessionLocal()
        try:
            self.assertEqual(lookup_db.query(db_mod.KnowledgeItem).count(), before_items)
            self.assertEqual(lookup_db.query(db_mod.SourceDocument).count(), before_docs)
        finally:
            lookup_db.close()

    def test_publish_commit_is_persistent(self):
        item_id = self._review_item(title="Publish Persist")
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}/publish",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        finally:
            lookup_db.close()
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed.status, "published")

    def test_reject_commit_is_persistent(self):
        item_id = self._review_item(title="Reject Persist")
        csrf = self._csrf(f"/admin/knowledge/review/{item_id}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/knowledge/review/{item_id}/reject",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        finally:
            lookup_db.close()
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed.status, "rejected")


if __name__ == "__main__":
    unittest.main()
