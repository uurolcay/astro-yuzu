import io
import re
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

import app
import database as db_mod
from services import document_chunker, document_parser


class DocumentIngestionTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="documents-admin@example.com",
            password_hash="hash",
            name="Documents Admin",
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
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "documents-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _csrf(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/documents")
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_parse_pdf_missing_file_returns_empty_list(self):
        self.assertEqual(document_parser.parse_pdf("missing-file.pdf"), [])

    def test_parse_pdf_with_diagnostics_returns_blocks_when_parser_reads_text(self):
        class _FakePage:
            def extract_text(self):
                return "ASHWINI\nRuler: Ketu"

        class _FakeReader:
            def __init__(self, _path):
                self.pages = [_FakePage()]

        fake_module = SimpleNamespace(PdfReader=_FakeReader)
        with tempfile.NamedTemporaryFile(suffix=".pdf") as handle, patch.dict(sys.modules, {"pypdf": fake_module}):
            diagnostics = document_parser.parse_pdf_with_diagnostics(handle.name)
        self.assertGreater(diagnostics["block_count"], 0)
        self.assertEqual(diagnostics["page_count"], 1)
        self.assertEqual(diagnostics["parser_used"], "pypdf")

    def test_chunk_text_blocks_splits_and_classifies(self):
        chunks = document_chunker.chunk_text_blocks(
            [
                "MARS DASHA\n\nMars dasha in the 6th house can intensify conflict handling and work pressure.",
                "SATURN\n\nSaturn in Dhanishta gives discipline and responsibility.",
            ]
        )
        self.assertGreaterEqual(len(chunks), 2)
        self.assertTrue(any(chunk["category"] in {"dasha", "planet"} for chunk in chunks))

    def test_admin_documents_page_returns_200_for_admin(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/documents")
        self.assertEqual(response.status_code, 200)
        self.assertIn('type="file"', response.text)
        self.assertIn('name="file"', response.text)
        self.assertIn('enctype="multipart/form-data"', response.text)
        self.assertIn("/admin/documents", response.text)
        self.assertIn("Parser", response.text)
        self.assertIn("Preview", response.text)

    def test_sidebar_contains_documents_link(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/documents")
        self.assertEqual(response.status_code, 200)
        self.assertIn('href="/admin/documents"', response.text)

    def test_upload_pdf_creates_source_document_and_knowledge_chunks(self):
        csrf = self._csrf()
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": [
                    "MARS DASHA\n\nMars dasha in the 6th house supports decisive work output.",
                    "ASHWINI\n\nAshwini can indicate quick response and initiative.",
                ],
                "page_count": 2,
                "block_count": 2,
                "preview": "MARS DASHA",
                "parser_used": "pypdf",
                "error": None,
            },
        ):
            response = self.client.post(
                "/admin/documents/upload",
                data={
                    "csrf_token": csrf,
                    "title": "Imported Doctrine",
                    "document_type": "book",
                },
                files={"file": ("import.pdf", io.BytesIO(b"%PDF-1.4 fake pdf"), "application/pdf")},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        self.assertEqual(self.db.query(db_mod.SourceDocument).count(), 1)
        self.assertGreaterEqual(self.db.query(db_mod.KnowledgeItem).count(), 1)
        self.assertGreaterEqual(self.db.query(db_mod.KnowledgeChunk).count(), 1)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)

    def test_parse_empty_notice_is_shown_and_document_metadata_keeps_diagnostics(self):
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": [],
                "page_count": 3,
                "block_count": 0,
                "preview": "",
                "parser_used": "none",
                "error": "no_pdf_parser_available",
            },
        ):
            response = self.client.post(
                "/admin/documents/upload",
                data={"csrf_token": csrf, "title": "Empty Parse", "document_type": "book"},
                files={"file": ("import.pdf", io.BytesIO(b"%PDF-1.4 fake pdf"), "application/pdf")},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("parse_empty", response.headers.get("location", ""))
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents?notice=parse_empty")
        self.assertEqual(page.status_code, 200)
        self.assertIn("parse sonucu bos dondu", page.text)
        self.assertIn("Parser", page.text)
        self.assertIn("Diagnostics", page.text)

    def test_upload_route_rejects_non_pdf(self):
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/documents/upload",
                data={
                    "csrf_token": csrf,
                    "title": "Bad Import",
                    "document_type": "notes",
                },
                files={"file": ("import.txt", io.BytesIO(b"not pdf"), "text/plain")},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("invalid_pdf", response.headers.get("location", ""))

    def test_upload_route_db_operational_error_returns_visible_notice(self):
        class _FakeQuery:
            def __init__(self, user):
                self.user = user

            def filter(self, *args, **kwargs):
                return self

            def first(self):
                return self.user

        class _BrokenUploadDb:
            def __init__(self, user):
                self.user = user
                self.rolled_back = False

            def query(self, *args, **kwargs):
                return _FakeQuery(self.user)

            def add(self, *args, **kwargs):
                raise OperationalError(
                    "INSERT INTO source_documents",
                    {},
                    Exception("SSL error: decryption failed or bad record mac"),
                )

            def rollback(self):
                self.rolled_back = True

            def close(self):
                pass

        csrf = self._csrf()
        broken_db = _BrokenUploadDb(self.admin)

        def override_get_db():
            yield broken_db

        with tempfile.TemporaryDirectory() as tmpdir:
            app.app.dependency_overrides[app.get_db] = override_get_db
            try:
                with patch.dict(app.os.environ, {"UPLOAD_DIR": tmpdir}, clear=False), patch.object(
                    app, "_require_admin_user", side_effect=self._request_admin_pair
                ), patch.object(db_mod.engine, "dispose") as dispose:
                    response = self.client.post(
                        "/admin/documents/upload",
                        data={
                            "csrf_token": csrf,
                            "title": "Broken Import",
                            "document_type": "book",
                        },
                        files={"file": ("import.pdf", io.BytesIO(b"%PDF-1.4 fake pdf"), "application/pdf")},
                        follow_redirects=False,
                    )
            finally:
                app.app.dependency_overrides.clear()
        self.assertEqual(response.status_code, 303)
        self.assertIn("db_temp_failed", response.headers.get("location", ""))
        self.assertTrue(broken_db.rolled_back)
        self.assertTrue(dispose.called)
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents?notice=db_temp_failed")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Database connection temporarily failed. Please try again.", page.text)

    def test_non_admin_access_is_blocked(self):
        response = self.client.get("/admin/documents", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})
