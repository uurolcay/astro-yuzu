import re
import unittest
import io
from datetime import datetime
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from services import nakshatra_extraction_service as svc


class NakshatraExtractionServiceTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="nakshatra-admin@example.com",
            password_hash="hash",
            name="Nakshatra Admin",
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
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "nakshatra-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _csrf(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/documents")
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_normalize_nakshatra_name_variations(self):
        self.assertEqual(svc.normalize_nakshatra_name("MRIGAŞİR"), "mrigashira")
        self.assertEqual(svc.normalize_nakshatra_name("KRİTİKA"), "krittika")
        self.assertEqual(svc.normalize_nakshatra_name("ŞATABHİŞA"), "shatabhisha")
        self.assertEqual(svc.normalize_nakshatra_name("Moola"), "mula")
        self.assertEqual(svc.normalize_nakshatra_name("ASHWINİ"), "ashwini")

    def test_detect_nakshatra_sections_simple_fixture(self):
        blocks = [
            "ASHWINI\nRuler: Ketu\nDeity: Ashvini Kumars",
            "More on Ashwini symbolism.",
            "BHARANI\nRuler: Venus\nDeity: Yama",
        ]
        sections = svc.detect_nakshatra_sections(blocks)
        self.assertEqual(len(sections), 2)
        self.assertEqual(sections[0]["nakshatra"], "ashwini")
        self.assertEqual(sections[1]["nakshatra"], "bharani")

    def test_detect_nakshatra_sections_supports_turkish_heading_variations(self):
        blocks = [
            "KRİTİKA\nRuler: Sun\nDeity: Agni",
            "MRIGAŞİR\nRuler: Mars\nDeity: Soma",
        ]
        sections = svc.detect_nakshatra_sections(blocks)
        self.assertEqual([item["nakshatra"] for item in sections], ["krittika", "mrigashira"])

    def test_long_text_soft_split_works_for_inline_nakshatra_mentions(self):
        blocks = [
            "Intro text\nASHWINI quick response and initiative.\nMore text continues.\nBHARANI restraint and containment.",
        ]
        sections = svc.detect_nakshatra_sections(blocks)
        self.assertGreaterEqual(len(sections), 2)
        self.assertEqual(sections[0]["nakshatra"], "ashwini")
        self.assertEqual(sections[1]["nakshatra"], "bharani")

    def test_extract_nakshatra_metadata_ashwini_example(self):
        metadata = svc.extract_nakshatra_metadata(
            "Ashwini\nZodiac Range: 0°00 Aries - 13°20 Aries\nRuler: Ketu\nDeity: Ashvini Kumars\nSymbol: Horse head\nAnimal: Horse\nSounds: Chu, Che"
        )
        self.assertEqual(metadata["ruler"], "Ketu")
        self.assertEqual(metadata["deity"], "Ashvini Kumars")
        self.assertEqual(metadata["animal"], "Horse")

    def test_classify_animal_paragraph(self):
        self.assertEqual(svc.classify_nakshatra_paragraph("The animal symbolism shows instinct and mate pattern."), "animal_logic")

    def test_classify_deity_paragraph(self):
        self.assertEqual(svc.classify_nakshatra_paragraph("The deity mythology reveals the divine healing pattern."), "deity_mythology")

    def test_classify_observation_paragraph(self):
        self.assertEqual(svc.classify_nakshatra_paragraph("Observation: in some cases this may be seen as social reserve."), "observation_notes")

    def test_build_suggested_nakshatra_chunks_sets_review_required(self):
        section = {
            "nakshatra": "ashwini",
            "start_page": 1,
            "end_page": 2,
            "source_title": "Test Source",
            "raw_text": "ASHWINI\n\nThe deity mythology emphasizes healing and quick restoration.",
        }
        chunks = svc.build_suggested_nakshatra_chunks(section)
        self.assertTrue(chunks)
        self.assertTrue(all(chunk["review_required"] for chunk in chunks))

    def test_sensitive_paragraph_raises_sensitivity_level(self):
        section = {
            "nakshatra": "bharani",
            "start_page": 1,
            "end_page": 1,
            "source_title": "Test Source",
            "raw_text": "BHARANI\n\nHealth and pregnancy topics should be handled with care and not framed as certainty.",
        }
        chunks = svc.build_suggested_nakshatra_chunks(section)
        self.assertTrue(any(chunk["sensitivity_level"] in {"moderate", "high"} for chunk in chunks))

    def test_extraction_empty_input_does_not_fail(self):
        result = svc.extract_nakshatra_knowledge_from_document([], "Test Source")
        self.assertEqual(result["section_count"], 0)
        self.assertEqual(result["chunk_count"], 0)

    def test_route_smoke_test_admin_works(self):
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": [
                    "ASHWINI\nRuler: Ketu\nDeity: Ashvini Kumars\n\nThe deity mythology emphasizes healing and responsiveness.",
                    "BHARANI\nRuler: Venus\nDeity: Yama\n\nObservation: in some cases this may be seen as intense containment.",
                ],
                "page_count": 2,
                "block_count": 2,
                "preview": "ASHWINI",
                "parser_used": "pypdf",
                "error": None,
            },
        ):
            upload_response = self.client.post(
                "/admin/documents/upload",
                data={"csrf_token": csrf, "title": "Nakshatra Source", "document_type": "book"},
                files={"file": ("import.pdf", io.BytesIO(b"%PDF-1.4 fake pdf"), "application/pdf")},
                follow_redirects=False,
            )
            self.assertEqual(upload_response.status_code, 303)
            lookup_db = db_mod.SessionLocal()
            try:
                document_id = lookup_db.query(db_mod.SourceDocument).order_by(db_mod.SourceDocument.id.desc()).first().id
            finally:
                lookup_db.close()
            response = self.client.post(
                f"/admin/documents/{document_id}/extract-nakshatra",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertGreaterEqual(self.db.query(db_mod.KnowledgeItem).count(), 1)

    def test_empty_parse_result_does_not_crash_route(self):
        document = db_mod.SourceDocument(
            title="Empty Source",
            file_path=__file__,
            document_type="book",
            uploaded_at=datetime.utcnow(),
        )
        self.db.add(document)
        self.db.commit()
        lookup_db = db_mod.SessionLocal()
        try:
            document_id = lookup_db.query(db_mod.SourceDocument).order_by(db_mod.SourceDocument.id.desc()).first().id
        finally:
            lookup_db.close()
        csrf = self._csrf()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.document_parser,
            "parse_pdf_with_diagnostics",
            return_value={
                "blocks": [],
                "page_count": 1,
                "block_count": 0,
                "preview": "",
                "parser_used": "none",
                "error": "no_pdf_parser_available",
            },
        ):
            response = self.client.post(
                f"/admin/documents/{document_id}/extract-nakshatra",
                data={"csrf_token": csrf},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("parse_empty", response.headers.get("location", ""))

    def test_non_admin_access_is_blocked(self):
        response = self.client.post("/admin/documents/1/extract-nakshatra", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})
