import io
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

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
            "parse_pdf",
            return_value=[
                "MARS DASHA\n\nMars dasha in the 6th house supports decisive work output.",
                "ASHWINI\n\nAshwini can indicate quick response and initiative.",
            ],
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

    def test_non_admin_access_is_blocked(self):
        response = self.client.get("/admin/documents", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})
