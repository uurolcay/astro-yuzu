import io
import json
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from services import document_chunker, knowledge_service, nakshatra_extraction_service as svc


class DocumentChunkNoiseFilteringTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="noise-admin@example.com",
            password_hash="hash",
            name="Noise Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.db.add(self.admin)
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
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "noise-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _csrf(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/review")
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_table_of_contents_chunk_is_not_produced(self):
        chunks = document_chunker.chunk_text_blocks(
            [{"page": 1, "text": "Table of Contents\nASHWINI 17\nBHARANI 41\nINDEX 320"}]
        )
        self.assertEqual(chunks, [])

    def test_nakshatra_page_number_list_chunk_is_not_produced(self):
        chunks = document_chunker.chunk_text_blocks(
            [{"page": 2, "text": "ASHWINI 17\nBHARANI 41\nKRITTIKA 59"}]
        )
        self.assertEqual(chunks, [])

    def test_real_ashwini_section_with_metadata_is_detected(self):
        sections = svc.detect_nakshatra_sections(
            [
                {"page": 1, "text": "Contents\nAshwini 17\nBharani 41"},
                {"page": 17, "text": "ASHWINI\nBurç Aralığı: 0°00 Koç - 13°20 Koç\nRuler: Ketu\nDeity: Ashvini Kumars\n\nAshwini expresses quick initiative in a healing style."},
            ]
        )
        self.assertEqual(len(sections), 1)
        self.assertEqual(sections[0]["nakshatra"], "ashwini")

    def test_toc_ashwini_is_not_section_start(self):
        sections = svc.detect_nakshatra_sections(
            [
                {"page": 1, "text": "İçindekiler\nASHWINI 17\nBHARANI 41"},
                {"page": 2, "text": "Random notes without metadata and without long explanation."},
            ]
        )
        self.assertEqual(sections, [])

    def test_auto_approve_does_not_publish_toc_or_noise_chunks(self):
        source_document = db_mod.SourceDocument(title="TOC Source", document_type="book")
        self.db.add(source_document)
        self.db.flush()
        item = knowledge_service.create_knowledge_item(
            self.db,
            title="Contents Chunk",
            body_text="ASHWINI 17\nBHARANI 41",
            language="tr",
            item_type="reference",
            summary_text="TOC",
            entities=["ashwini", "bharani"],
            source_document=source_document,
            metadata={
                "review_required": True,
                "status": "review_required",
                "confidence_level": "high",
                "sensitivity_level": "low",
                "is_toc": True,
                "noise_score": 0.95,
                "coverage_entities": ["ashwini", "bharani"],
            },
            created_by_user_id=self.admin_id,
            status="review_required",
        )
        self.db.commit()
        item_id = item.id
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.post("/admin/knowledge/review/auto-approve", data={"csrf_token": csrf}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        lookup_db = db_mod.SessionLocal()
        try:
            refreshed = lookup_db.query(db_mod.KnowledgeItem).filter_by(id=item_id).first()
        finally:
            lookup_db.close()
        self.assertEqual(refreshed.status, "review_required")

    def test_source_page_metadata_is_preserved(self):
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": ["ASHWINI\n\nAshwini explanatory paragraph with enough length to chunk."],
                "page_blocks": [{"page": 17, "text": "ASHWINI\n\nAshwini explanatory paragraph with enough length to chunk."}],
                "page_count": 1,
                "block_count": 1,
                "preview": "ASHWINI",
                "parser_used": "pypdf",
                "error": None,
            },
        ):
            response = self.client.post(
                "/admin/documents/upload",
                data={"csrf_token": csrf, "title": "Paged Source", "document_type": "book"},
                files={"file": ("import.pdf", io.BytesIO(b"%PDF-1.4 fake pdf"), "application/pdf")},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        item = self.db.query(db_mod.KnowledgeItem).order_by(db_mod.KnowledgeItem.id.desc()).first()
        metadata = json.loads(item.metadata_json)
        self.assertEqual(metadata.get("source_page_start"), 17)
        self.assertEqual(metadata.get("source_page_end"), 17)


if __name__ == "__main__":
    unittest.main()
