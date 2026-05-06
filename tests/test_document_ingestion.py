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

    def _document_with_related_knowledge(self, *, title="Related PDF", file_path=None, status="failed"):
        document_id = self._uploaded_document(title=title, file_path=file_path)
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        document.processing_status = status
        item = db_mod.KnowledgeItem(
            source_document=document,
            title=f"{title} item",
            item_type="reference",
            language="tr",
            summary_text="Generated summary",
            body_text="Generated knowledge body.",
            status="published",
        )
        self.db.add(item)
        self.db.flush()
        self.db.add(
            db_mod.KnowledgeChunk(
                knowledge_item_id=item.id,
                chunk_index=0,
                chunk_text="Generated knowledge chunk.",
                token_count=3,
            )
        )
        self.db.commit()
        return document_id

    def _knowledge_item_for_document(
        self,
        document_id,
        *,
        title="Safe Source Item",
        status="review_required",
        metadata=None,
        body_text=None,
        confidence_level="medium",
        sensitivity_level="low",
        entities=None,
    ):
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        default_metadata = {
            "review_required": status == "review_required",
            "status": status,
            "category": "nakshatra",
            "primary_entity": "dhanishta",
            "source_title": document.title if document else "Trusted Source",
            "source_page_start": 42,
            "confidence_level": confidence_level,
            "sensitivity_level": sensitivity_level,
            "coverage_entities": entities or ["dhanishta", "nakshatra"],
        }
        if metadata:
            default_metadata.update(metadata)
        body = body_text or (
            "Dhanishta nakshatra material with practical interpretive context. "
            "This paragraph is intentionally long enough to pass the safe publish threshold. "
            "It contains useful structured knowledge for review, synthesis, and retrieval. "
            "The source is a trusted astrology reference with meaningful explanatory content."
        )
        item = db_mod.KnowledgeItem(
            source_document=document,
            title=title,
            item_type="nakshatra",
            language="tr",
            summary_text="Structured summary",
            body_text=body,
            status=status,
            metadata_json=json.dumps(default_metadata, ensure_ascii=False),
            entities_json=json.dumps(entities or ["dhanishta", "nakshatra"], ensure_ascii=False),
            coverage_entities_json=json.dumps(entities or ["dhanishta", "nakshatra"], ensure_ascii=False),
        )
        self.db.add(item)
        self.db.commit()
        return item.id

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
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(metadata["trust_level"], "trusted")
        self.assertEqual(metadata["review_policy"], "auto_publish_safe")
        self.assertTrue(metadata["auto_publish_safe_chunks"])
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

    def test_process_auto_publishes_safe_chunks_and_rejects_noise(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Auto Curated Doctrine")
        safe_body = (
            "Dhanishta nakshatra gives a disciplined rhythm for work, craft, and social contribution. "
            "When the source describes this placement in a practical way, the interpretation can connect "
            "initiative, stamina, group responsibility, and timing without making fatalistic claims. "
            "This source paragraph is intentionally long enough to be useful for retrieval and synthesis."
        )
        low_body = (
            "Ashwini nakshatra can show quick initiative and rapid response, but this note is still brief "
            "and should remain for review because confidence is deliberately low."
        )
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": ["DUMMY"],
                "page_blocks": [{"page": 1, "text": "DUMMY"}],
                "page_count": 1,
                "block_count": 1,
                "range_end_page": 1,
                "has_more_pages": False,
                "preview": "DUMMY",
                "parser_used": "pypdf",
                "error": None,
            },
        ), patch.object(
            app.document_chunker,
            "chunk_text_blocks",
            return_value=[
                {
                    "title": "Dhanishta Work Pattern",
                    "text": safe_body,
                    "category": "nakshatra",
                    "entity": "dhanishta",
                    "topic": "career",
                    "coverage_entities": ["dhanishta", "career"],
                    "source_page_start": 10,
                    "source_page_end": 10,
                    "is_toc": False,
                    "is_index": False,
                    "noise_score": 0.1,
                    "confidence_level": "medium",
                    "sensitivity_level": "low",
                },
                {
                    "title": "Table of Contents",
                    "text": "Contents\n1\n2\n3",
                    "category": "general",
                    "entity": "",
                    "topic": "general",
                    "coverage_entities": [],
                    "source_page_start": 1,
                    "source_page_end": 1,
                    "is_toc": True,
                    "is_index": False,
                    "noise_score": 0.95,
                    "confidence_level": "high",
                    "sensitivity_level": "low",
                },
                {
                    "title": "Ashwini Brief Note",
                    "text": low_body,
                    "category": "nakshatra",
                    "entity": "ashwini",
                    "topic": "general",
                    "coverage_entities": ["ashwini"],
                    "source_page_start": 12,
                    "source_page_end": 12,
                    "is_toc": False,
                    "is_index": False,
                    "noise_score": 0.1,
                    "confidence_level": "low",
                    "sensitivity_level": "low",
                },
                {
                    "title": "Sensitive Timing",
                    "text": safe_body,
                    "category": "dasha",
                    "entity": "saturn",
                    "topic": "timing",
                    "coverage_entities": ["saturn"],
                    "source_page_start": 13,
                    "source_page_end": 13,
                    "is_toc": False,
                    "is_index": False,
                    "noise_score": 0.1,
                    "confidence_level": "high",
                    "sensitivity_level": "sensitive",
                },
            ],
        ):
            response = self.client.post(
                f"/admin/documents/{document_id}/process",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        items = {item.title: item for item in self.db.query(db_mod.KnowledgeItem).all()}
        self.assertEqual(items["Dhanishta Work Pattern"].status, "published")
        self.assertEqual(items["Table of Contents"].status, "rejected")
        self.assertEqual(items["Ashwini Brief Note"].status, "review_required")
        self.assertEqual(items["Sensitive Timing"].status, "review_required")
        safe_metadata = json.loads(items["Dhanishta Work Pattern"].metadata_json or "{}")
        noise_metadata = json.loads(items["Table of Contents"].metadata_json or "{}")
        self.assertTrue(safe_metadata["auto_published"])
        self.assertEqual(noise_metadata["auto_reject_reason"], "noise_or_index")

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
        self.assertIn("Veritabani baglantisi gecici olarak basarisiz oldu. Lutfen tekrar deneyin.", page.text)

    def test_documents_page_shows_process_button_and_status(self):
        self._uploaded_document(title="Needs Processing")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Yüklendi", page.text)
        self.assertIn("Icerigi Isle", page.text)
        self.assertIn("Sil", page.text)
        self.assertIn("/delete", page.text)
        self.assertIn("/process", page.text)

    def test_documents_page_extract_button_uses_polished_label(self):
        self._document_with_related_knowledge(title="Extractable PDF", status="completed")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Icerik Cikar", page.text)
        self.assertIn("PDF'den yapilandirilmis icerik cikarir.", page.text)

    def test_failed_document_can_be_deleted_from_admin_route(self):
        csrf = self._csrf()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as handle:
            handle.write(b"%PDF-1.4 temporary")
            file_path = handle.name
        document_id = self._document_with_related_knowledge(title="Delete Me", file_path=file_path, status="failed")
        self.assertTrue(app.Path(file_path).exists())
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/delete",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_deleted", response.headers.get("location", ""))
        self.db.expire_all()
        self.assertIsNone(self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first())
        self.assertEqual(self.db.query(db_mod.KnowledgeItem).count(), 0)
        self.assertEqual(self.db.query(db_mod.KnowledgeChunk).count(), 0)
        self.assertFalse(app.Path(file_path).exists())

    def test_delete_file_missing_does_not_fail_db_deletion(self):
        csrf = self._csrf()
        document_id = self._document_with_related_knowledge(
            title="Missing File Delete",
            file_path="already-missing-delete.pdf",
            status="failed",
        )
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/delete",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_deleted", response.headers.get("location", ""))
        self.db.expire_all()
        self.assertIsNone(self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first())
        self.assertEqual(self.db.query(db_mod.KnowledgeItem).count(), 0)
        self.assertEqual(self.db.query(db_mod.KnowledgeChunk).count(), 0)

    def test_delete_missing_document_redirects_with_notice(self):
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/documents/999999/delete",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_not_found", response.headers.get("location", ""))

    def test_delete_route_requires_admin(self):
        document_id = self._uploaded_document(title="Protected Delete")
        response = self.client.post(
            f"/admin/documents/{document_id}/delete",
            data={"csrf_token": "invalid"},
            follow_redirects=False,
        )
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})
        self.db.expire_all()
        self.assertIsNotNone(self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first())

    def test_delete_handles_db_error_without_blank_500(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Delete DB Error")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app,
            "_delete_source_document",
            side_effect=OperationalError("DELETE", {}, Exception("SSL error: decryption failed or bad record mac")),
        ), patch.object(db_mod.engine, "dispose") as dispose:
            response = self.client.post(
                f"/admin/documents/{document_id}/delete",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_delete_failed", response.headers.get("location", ""))
        self.assertTrue(dispose.called)
        self.db.expire_all()
        self.assertIsNotNone(self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first())

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

    def test_document_preview_is_snippet_limited(self):
        document_id = self._uploaded_document(title="Long Preview")
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        document.metadata_json = json.dumps(
            {
                "processing_status": "failed",
                "parser_diagnostics": {"preview": "A" * 500, "parser_used": "pypdf"},
            },
            ensure_ascii=False,
        )
        self.db.commit()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("A" * 200, page.text)
        self.assertNotIn("A" * 301, page.text)

    def test_trusted_source_metadata_update_works(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Trusted Source")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/trust",
                data={
                    "csrf_token": csrf,
                    "trust_level": "high",
                    "review_policy": "auto_publish_safe",
                    "source_domain": "nakshatra",
                    "auto_publish_safe_chunks": "1",
                },
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_trust_updated", response.headers.get("location", ""))
        self.db.expire_all()
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(metadata["trust_level"], "high")
        self.assertEqual(metadata["review_policy"], "auto_publish_safe")
        self.assertEqual(metadata["source_domain"], "nakshatra")
        self.assertTrue(metadata["auto_publish_safe_chunks"])
        self.assertEqual(metadata["approved_by_admin_email"], "documents-admin@example.com")

    def test_publish_safe_publishes_only_safe_items_from_selected_source(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Selected Source")
        other_document_id = self._uploaded_document(title="Other Source")
        selected_item_id = self._knowledge_item_for_document(document_id, title="Selected Safe")
        other_item_id = self._knowledge_item_for_document(other_document_id, title="Other Safe")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/publish-safe",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_safe_published:1", response.headers.get("location", ""))
        lookup_db = db_mod.SessionLocal()
        try:
            selected_item = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=selected_item_id).first()
            other_item = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=other_item_id).first()
            selected_metadata = json.loads(selected_item.metadata_json or "{}")
        finally:
            lookup_db.close()
        self.assertEqual(selected_item.status, "published")
        self.assertTrue(selected_metadata["auto_published"])
        self.assertEqual(selected_metadata["auto_publish_reason"], "trusted_source_safe_content")
        self.assertEqual(other_item.status, "review_required")

    def test_publish_safe_does_not_publish_sensitive_noisy_toc_or_index_items(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Unsafe Source")
        item_ids = [
            self._knowledge_item_for_document(document_id, title="Sensitive", sensitivity_level="sensitive"),
            self._knowledge_item_for_document(document_id, title="Noisy", metadata={"noise_score": 0.9}),
            self._knowledge_item_for_document(document_id, title="Contents", metadata={"is_toc": True}),
            self._knowledge_item_for_document(document_id, title="Index", metadata={"is_index": True}),
        ]
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/publish-safe",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            statuses = [lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first().status for item_id in item_ids]
        finally:
            lookup_db.close()
        self.assertEqual(statuses, ["review_required", "review_required", "review_required", "review_required"])

    def test_publish_safe_repairs_unknown_title_when_possible(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Nakshatra Kitabı")
        item_id = self._knowledge_item_for_document(document_id, title="Dhanishta - Unknown")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/publish-safe",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        item = self.db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        metadata = json.loads(item.metadata_json or "{}")
        self.assertEqual(item.status, "published")
        self.assertNotIn("Unknown", item.title)
        self.assertIn("Dhanishta", item.title)
        self.assertTrue(metadata["auto_published"])

    def test_publish_safe_leaves_unsafe_unknown_title_in_review(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Short Unknown Source")
        item_id = self._knowledge_item_for_document(document_id, title="Unknown", body_text="short")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/publish-safe",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.db.expire_all()
        item = self.db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        metadata = json.loads(item.metadata_json or "{}")
        self.assertEqual(item.status, "review_required")
        self.assertFalse(metadata.get("auto_published", False))

    def test_reject_noise_rejects_noisy_items_from_selected_source(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Noise Source")
        noisy_id = self._knowledge_item_for_document(document_id, title="Table of Contents", metadata={"is_toc": True})
        safe_id = self._knowledge_item_for_document(document_id, title="Safe Remaining")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/reject-noise",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_noise_rejected:1", response.headers.get("location", ""))
        lookup_db = db_mod.SessionLocal()
        try:
            noisy = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=noisy_id).first()
            safe = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=safe_id).first()
            noisy_metadata = json.loads(noisy.metadata_json or "{}")
        finally:
            lookup_db.close()
        self.assertEqual(noisy.status, "rejected")
        self.assertEqual(noisy_metadata["auto_reject_reason"], "noise_or_index")
        self.assertEqual(safe.status, "review_required")

    def test_documents_page_renders_source_level_actions_and_review_queue_link(self):
        document_id = self._uploaded_document(title="Source Actions")
        self._knowledge_item_for_document(document_id, title="Reviewable Source Item")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Otomatik Duzenle", page.text)
        self.assertIn("Sorunlu Parcalari Gor", page.text)
        self.assertIn("Guvenilir Kaynak Yap", page.text)
        self.assertIn("Guvenli Icerikleri Yayinla", page.text)
        self.assertIn("Noise Icerikleri Reddet", page.text)
        self.assertIn(f"source_document_id={document_id}&status=review_required", page.text)

    def test_documents_page_renders_grouped_source_counts(self):
        document_id = self._uploaded_document(title="Counted Source")
        self._knowledge_item_for_document(document_id, title="Published", status="published")
        self._knowledge_item_for_document(document_id, title="Review Required")
        self._knowledge_item_for_document(document_id, title="Rejected", status="rejected")
        self._knowledge_item_for_document(document_id, title="Auto Published", status="published", metadata={"auto_published": True})
        self._knowledge_item_for_document(document_id, title="Sensitive Count", sensitivity_level="sensitive")
        self._knowledge_item_for_document(document_id, title="Noise Count", metadata={"noise_score": 0.95})
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/documents")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Yayinlandi: 2", page.text)
        self.assertIn("Inceleme Bekliyor: 3", page.text)
        self.assertIn("Reddedildi: 1", page.text)
        self.assertIn("Otomatik Yayinlandi: 1", page.text)
        self.assertIn("Hassas: 1", page.text)
        self.assertIn("Noise: 1", page.text)

    def test_auto_curate_document_route_updates_safe_noise_and_review_items(self):
        csrf = self._csrf()
        document_id = self._uploaded_document(title="Curate Source")
        safe_id = self._knowledge_item_for_document(document_id, title="Safe Curate")
        noise_id = self._knowledge_item_for_document(document_id, title="Index", metadata={"is_index": True})
        sensitive_id = self._knowledge_item_for_document(document_id, title="Sensitive Curate", sensitivity_level="sensitive")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                f"/admin/documents/{document_id}/auto-curate",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("document_auto_curated:1:1:1:3", response.headers.get("location", ""))
        self.db.expire_all()
        statuses = {
            item_id: self.db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first().status
            for item_id in [safe_id, noise_id, sensitive_id]
        }
        self.assertEqual(statuses[safe_id], "published")
        self.assertEqual(statuses[noise_id], "rejected")
        self.assertEqual(statuses[sensitive_id], "review_required")
        document = self.db.query(db_mod.SourceDocument).filter_by(id=document_id).first()
        metadata = json.loads(document.metadata_json or "{}")
        self.assertEqual(metadata["trust_level"], "trusted")
        self.assertEqual(metadata["review_policy"], "auto_publish_safe")

    def test_auto_curate_all_is_limited_and_timeout_safe(self):
        csrf = self._csrf()
        first_document_id = self._uploaded_document(title="First Curate")
        second_document_id = self._uploaded_document(title="Second Curate")
        first_item_id = self._knowledge_item_for_document(first_document_id, title="First Safe")
        second_item_id = self._knowledge_item_for_document(second_document_id, title="Second Safe")
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post(
                "/admin/knowledge/auto-curate-all?limit=1",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("knowledge_auto_curated", response.headers.get("location", ""))
        self.db.expire_all()
        statuses = [
            self.db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first().status
            for item_id in [first_item_id, second_item_id]
        ]
        self.assertEqual(statuses.count("published"), 1)
        self.assertEqual(statuses.count("review_required"), 1)

    def test_non_admin_access_is_blocked(self):
        response = self.client.get("/admin/documents", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})
