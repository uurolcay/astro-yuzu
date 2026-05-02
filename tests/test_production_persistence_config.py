import io
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from services import knowledge_service


class ProductionPersistenceConfigTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="prod-persist-admin@example.com",
            password_hash="hash",
            name="Prod Persist Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.member = db_mod.AppUser(
            email="prod-persist-member@example.com",
            password_hash="hash",
            name="Prod Persist Member",
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
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "prod-persist-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _csrf(self, path="/admin/documents"):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def _review_item(self, title="Persist Check"):
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

    def test_postgres_url_is_normalized(self):
        resolved = db_mod.resolve_database_url("postgres://user:pass@localhost/dbname")
        self.assertTrue(resolved.startswith("postgresql://"))

    def test_missing_database_url_uses_local_sqlite_fallback(self):
        with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False):
            resolved = db_mod.resolve_database_url(None)
        self.assertTrue(resolved.startswith("sqlite:///"))
        self.assertIn("astro_logic.db", resolved)

    def test_upload_dir_env_is_honored_and_created(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "nested" / "uploads"
            with patch.dict(os.environ, {"UPLOAD_DIR": str(target)}, clear=False):
                upload_dir = app.get_upload_dir()
            self.assertEqual(upload_dir, target.resolve())
            self.assertTrue(upload_dir.exists())

    def test_storage_debug_shows_env_based_upload_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"UPLOAD_DIR": tmpdir, "RENDER": "true", "DATABASE_URL": ""}, clear=False):
                with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
                    response = self.client.get("/admin/debug/storage")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["uploads_dir_path"], str(Path(tmpdir).resolve()))
        self.assertIn("db_dialect", payload)
        self.assertIn("warnings", payload)

    def test_init_db_does_not_reset_existing_data(self):
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

    def test_source_document_upload_path_uses_configured_upload_dir(self):
        csrf = self._csrf("/admin/documents")
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"UPLOAD_DIR": tmpdir}, clear=False):
                with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
                    app.document_parser,
                    "parse_pdf_with_diagnostics",
                    return_value={
                        "blocks": [],
                        "page_blocks": [],
                        "page_count": 0,
                        "block_count": 0,
                        "preview": "",
                        "parser_used": "pypdf",
                        "error": None,
                    },
                ):
                    response = self.client.post(
                        "/admin/documents/upload",
                        data={"csrf_token": csrf, "title": "Configured Upload", "document_type": "book"},
                        files={"file": ("configured.pdf", io.BytesIO(b"%PDF-1.4 fake pdf"), "application/pdf")},
                        follow_redirects=False,
                    )
        self.assertEqual(response.status_code, 303)
        document = self.db.query(db_mod.SourceDocument).order_by(db_mod.SourceDocument.id.desc()).first()
        self.assertIsNotNone(document)
        self.assertTrue(str(document.file_path).startswith(str(Path(tmpdir).resolve())))


if __name__ == "__main__":
    unittest.main()
