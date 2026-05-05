import io
import json
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

    def _uploaded_document(self, *, title="Queued PDF", file_path=None):
        document = db_mod.SourceDocument(
            title=title,
            file_path=file_path or __file__,
            document_type="book",
            source_label="queued.pdf",
            processing_status="uploaded",
            processing_cursor_page=0,
        )
        self.db.add(document)
        self.db.commit()
        return document.id

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

    def test_upload_route_does_not_synchronously_parse_pdf_by_default(self):
        csrf = self._csrf()
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            side_effect=AssertionError("upload must not parse"),
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
        self.assertIn("upload_saved", response.headers.get("location", ""))
        self.db.expire_all()
        self.assertEqual(self.db.query(db_mod.SourceDocument).count(), 1)
        document = self.db.query(db_mod.SourceDocument).first()
        self.assertEqual(document.processing_status, "uploaded")
        self.assertEqual(self.db.query(db_mod.KnowledgeItem).count(), 0)
        self.assertEqual(self.db.query(db_mod.KnowledgeChunk).count(), 0)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)

    def test_process_route_creates_knowledge_chunks(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Imported Doctrine")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": [
                    "MARS DASHA\n\nMars dasha in the 6th house supports decisive work output.",
                    "ASHWINI\n\nAshwini can indicate quick response and initiative.",
                ],
                "page_blocks": [
                    {"page": 1, "text": "MARS DASHA\n\nMars dasha in the 6th house supports decisive work output."},
                    {"page": 2, "text": "ASHWINI\n\nAshwini can indicate quick response and initiative."},
                ],
                "page_count": 2,
                "block_count": 2,
                "range_end_page": 2,
                "has_more_pages": False,
                "preview": "MARS DASHA",
                "parser_used": "pypdf",
                "error": None,
            },
        ):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("processing_completed", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        self.assertEqual(document.processing_status, "completed")
        self.assertEqual(document.processing_cursor_page, 2)
        metadata = json.loads(document.metadata_json or "{}")
        self.assertNotIn("last_processing_error_type", metadata)
        self.assertEqual(metadata["parser_used"], "pypdf")
        self.assertEqual(metadata["page_count"], 2)
        self.assertEqual(metadata["block_count"], 2)
        self.assertGreaterEqual(self.db.query(db_mod.KnowledgeItem).count(), 1)
        self.assertGreaterEqual(self.db.query(db_mod.KnowledgeChunk).count(), 1)

    def test_process_missing_file_sets_file_missing_diagnostics_without_500(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Missing PDF", file_path="missing-temporary-upload.pdf")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("file_missing", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(document.processing_status, "failed")
        self.assertEqual(metadata["last_processing_error_type"], "file_missing")
        self.assertFalse(metadata["file_exists"])
        self.assertIn("geçici depolamada bulunamadı", metadata["last_processing_error"])

    def test_process_parse_empty_notice_is_shown_and_document_metadata_keeps_diagnostics(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Empty Parse")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": [],
                "page_blocks": [],
                "page_count": 3,
                "block_count": 0,
                "range_end_page": 3,
                "has_more_pages": False,
                "preview": "",
                "parser_used": "none",
                "error": "no_pdf_parser_available",
            },
        ):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("parse_empty", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(document.processing_status, "parse_empty")
        self.assertEqual(metadata["last_processing_error_type"], "parse_empty")
        self.assertEqual(metadata["page_count"], 3)
        self.assertEqual(metadata["block_count"], 0)
        self.assertIn("metin katmanı", metadata["last_processing_error"])
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents?notice=parse_empty")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Bu PDF metin katmanı içermiyor olabilir", page.text)
        self.assertIn("parse_empty", page.text)
        self.assertIn("Parser", page.text)
        self.assertIn("Diagnostics", page.text)

    def test_process_route_handles_parser_exception_without_500(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Parser Broken")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            side_effect=RuntimeError("pypdf exploded"),
        ):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("parser_error", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(document.processing_status, "failed")
        self.assertEqual(metadata["last_processing_error_type"], "parser_error")
        self.assertIn("PDF okunurken hata oluştu", metadata["last_processing_error"])
        self.assertIn("pypdf exploded", metadata["last_processing_warning"])

    def test_process_route_handles_zero_chunks_without_500(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="No Chunks")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": ["INDEX\n\n1 2 3 4 5"],
                "page_blocks": [{"page": 1, "text": "INDEX\n\n1 2 3 4 5"}],
                "page_count": 1,
                "block_count": 1,
                "range_end_page": 1,
                "has_more_pages": False,
                "preview": "INDEX",
                "parser_used": "pypdf",
                "error": None,
            },
        ), patch.object(app.document_chunker, "chunk_text_blocks", return_value=[]):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("no_chunks_generated", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(document.processing_status, "failed")
        self.assertEqual(metadata["last_processing_error_type"], "no_chunks_generated")
        self.assertIn("anlamlı bilgi parçası üretilemedi", metadata["last_processing_error"])

    def test_process_route_handles_db_operational_error_without_blank_500(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="DB Broken")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": ["SATURN\n\nSaturn gives discipline and responsibility."],
                "page_blocks": [{"page": 1, "text": "SATURN\n\nSaturn gives discipline and responsibility."}],
                "page_count": 1,
                "block_count": 1,
                "range_end_page": 1,
                "has_more_pages": False,
                "preview": "SATURN",
                "parser_used": "pypdf",
                "error": None,
            },
        ), patch.object(
            app.knowledge_service,
            "create_knowledge_item",
            side_effect=OperationalError("INSERT", {}, Exception("SSL error: decryption failed or bad record mac")),
        ), patch.object(db_mod.engine, "dispose") as dispose:
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("db_error", response.headers.get("location", ""))
        self.assertTrue(dispose.called)
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(document.processing_status, "failed")
        self.assertEqual(metadata["last_processing_error_type"], "db_error")
        self.assertIn("Veritabanı bağlantısı", metadata["last_processing_error"])

    def test_large_document_processes_in_batches(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Large Doctrine")
        with patch.object(app, "MAX_PDF_PAGES_PER_REQUEST", 1), patch.object(
            app, "MAX_CHUNKS_PER_REQUEST", 100
        ), patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": ["JUPITER\n\nJupiter expands wisdom and teaching."],
                "page_blocks": [{"page": 1, "text": "JUPITER\n\nJupiter expands wisdom and teaching."}],
                "page_count": 2,
                "block_count": 1,
                "range_end_page": 1,
                "has_more_pages": True,
                "preview": "JUPITER",
                "parser_used": "pypdf",
                "error": None,
            },
        ):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("processing_started", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        self.assertEqual(document.processing_status, "processing")
        self.assertEqual(document.processing_cursor_page, 1)

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
        self.assertIn("Veritabanı bağlantısı geçici olarak başarısız oldu. Lütfen tekrar deneyin.", page.text)

    def test_documents_page_shows_process_button_and_status(self):
        self._uploaded_document(title="Needs Processing")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("uploaded", page.text)
        self.assertIn("Process", page.text)
        self.assertIn("/process", page.text)

    def test_documents_page_renders_error_type_and_friendly_message(self):
        document_id = self._uploaded_document(title="Failed Diagnostics", file_path="missing-temporary-upload.pdf")
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        document.processing_status = "failed"
        document.processing_error = "Yüklenen PDF dosyası geçici depolamada bulunamadı. Lütfen tekrar yükleyin."
        document.metadata_json = json.dumps(
            {
                "processing_status": "failed",
                "last_processing_error_type": "file_missing",
                "last_processing_error": "Yüklenen PDF dosyası geçici depolamada bulunamadı. Lütfen tekrar yükleyin.",
                "last_processing_warning": "missing-temporary-upload.pdf",
                "file_exists": False,
                "file_size": 0,
                "parser_used": "none",
                "page_count": 0,
                "block_count": 0,
                "chunk_count": 0,
                "processing_cursor_page": 0,
                "last_processed_page": 0,
            },
            ensure_ascii=False,
        )
        self.db.commit()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Error type", page.text)
        self.assertIn("file_missing", page.text)
        self.assertIn("geçici depolamada bulunamadı", page.text)
        self.assertIn("File: missing", page.text)

    def test_documents_page_is_paginated_for_large_document_lists(self):
        for index in range(3):
            self._uploaded_document(title=f"Paged Document {index}")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents?page_size=2")
        self.assertEqual(page.status_code, 200)
        self.assertLessEqual(page.text.count("Paged Document"), 2)

    def test_non_admin_access_is_blocked(self):
        response = self.client.get("/admin/documents", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})
