import json
import unittest
from unittest.mock import patch

import ai_interpreter
import agent_pipeline
import database as db_mod
from services import admin_astro_workspace as workspace
from services import embedding_service, evaluation_service, retrieval_service


class _FakeResponse:
    def __init__(self, text):
        self.text = text


class _FakeModels:
    def __init__(self):
        self.prompts = []

    def generate_content(self, model, contents):
        self.prompts.append(contents)
        if "You are the Insight Agent" in contents:
            return _FakeResponse("Insight output")
        if "You are the Timing Agent" in contents:
            return _FakeResponse("Timing output")
        if "You are the Guidance Agent" in contents:
            return _FakeResponse("Guidance output")
        if "You are the Composer Agent" in contents:
            return _FakeResponse("Composer draft")
        return _FakeResponse("Final report")


class _FakeClient:
    def __init__(self):
        self.models = _FakeModels()


class KnowledgeTraceIntegrationTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeGap).delete()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="phase4-admin@example.com",
            password_hash="hash",
            name="Phase4 Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.db.add(self.admin)
        self.db.commit()

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.KnowledgeGap).delete()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _knowledge_item(self, *, title="Swati Career Source", entity="Swati", domain="career"):
        source = db_mod.SourceDocument(title="Trusted Swati Book", source_label="trusted-swati.pdf", document_type="book", processing_status="completed")
        self.db.add(source)
        self.db.flush()
        metadata = {
            "review_required": False,
            "status": "published",
            "category": "nakshatra",
            "primary_entity": entity.lower(),
            "confidence_level": "high",
            "sensitivity_level": "low",
            "source_title": source.title,
        }
        text = f"{entity} {domain} independence decision pattern from a published Vedic astrology source."
        item = db_mod.KnowledgeItem(
            source_document=source,
            title=title,
            item_type="nakshatra",
            language="tr",
            summary_text=text,
            body_text=text,
            status="published",
            metadata_json=json.dumps(metadata, ensure_ascii=False),
            entities_json=json.dumps([entity.lower(), domain], ensure_ascii=False),
            coverage_entities_json=json.dumps([entity.lower(), domain], ensure_ascii=False),
        )
        self.db.add(item)
        self.db.flush()
        chunk = db_mod.KnowledgeChunk(
            knowledge_item_id=item.id,
            chunk_index=0,
            chunk_text=text,
            token_count=len(text.split()),
            embedding_json=embedding_service.serialize_embedding(embedding_service.generate_embedding(text)),
            entities_json=json.dumps([entity.lower(), domain], ensure_ascii=False),
            coverage_entities_json=json.dumps([entity.lower(), domain], ensure_ascii=False),
        )
        self.db.add(chunk)
        self.db.commit()
        return item, chunk

    def _payload(self):
        return {
            "language": "tr",
            "report_type": "career",
            "natal_data": {
                "planets": [{"name": "Moon", "house": 10, "nakshatra": "Swati", "pada": 2}],
                "active_dasha": {"planet": "Rahu"},
            },
            "astro_signal_context": {
                "dominant_signals": [{"nakshatra": "Swati", "domain": "career", "pada": 2}],
            },
        }

    def test_ai_interpreter_includes_published_knowledge_context_when_available(self):
        _item, chunk = self._knowledge_item()
        fake_client = _FakeClient()
        with patch.object(agent_pipeline, "_get_model_client", return_value=fake_client), patch.object(
            agent_pipeline, "_get_model_name", return_value="fake-model"
        ):
            result = ai_interpreter.generate_interpretation(self._payload(), db=self.db)
        self.assertEqual(result, "Final report")
        self.assertIn("Published Knowledge Context", fake_client.models.prompts[0])
        self.assertIn(str(chunk.id), fake_client.models.prompts[0])
        self.assertIn("Do not cite unpublished or review-pending knowledge.", fake_client.models.prompts[0])

    def test_ai_interpreter_works_without_knowledge_context(self):
        fake_client = _FakeClient()
        with patch.object(retrieval_service, "build_prompt_knowledge_context", side_effect=RuntimeError("no db")), patch.object(
            agent_pipeline, "_get_model_client", return_value=fake_client
        ), patch.object(agent_pipeline, "_get_model_name", return_value="fake-model"):
            result = ai_interpreter.generate_interpretation(self._payload(), db=self.db)
        self.assertEqual(result, "Final report")

    def test_used_chunk_ids_json_is_persisted(self):
        _item, chunk = self._knowledge_item()
        payload = self._payload()
        prompt_payload = workspace.build_workspace_prompt_payload(payload, db=self.db)
        self.assertIn(chunk.id, prompt_payload["_used_chunk_ids"])
        interpretation = workspace.save_internal_interpretation(
            self.db,
            report_type="career",
            payload=payload,
            interpretation_text="Saved source-grounded text",
            admin_user=self.admin,
        )
        self.db.commit()
        self.assertEqual(json.loads(interpretation.used_chunk_ids_json), [chunk.id])
        saved_payload = json.loads(interpretation.input_payload_json)
        self.assertIn("knowledge_context", saved_payload)
        self.assertIn("_knowledge_trace", saved_payload)

    def test_missing_query_creates_deduped_knowledge_gap(self):
        signal_context = {"dominant_signals": [{"nakshatra": "Swati", "domain": "career", "pada": 2}]}
        first = retrieval_service.build_published_knowledge_context(signal_context, "career", "tr", db=self.db)
        second = retrieval_service.build_published_knowledge_context(signal_context, "career", "tr", db=self.db)
        self.assertTrue(first["no_source_available"])
        self.assertTrue(second["no_source_available"])
        gaps = self.db.query(db_mod.KnowledgeGap).all()
        self.assertEqual(len(gaps), 1)
        context = json.loads(gaps[0].context_json)
        self.assertEqual(context["reason"], "no_source_available")
        self.assertEqual(context["entity"], "Swati")

    def test_evaluation_includes_source_coverage_score(self):
        result = evaluation_service.evaluate_interpretation(
            "Swati career guidance references Moon and Swati.",
            {
                "natal_data": {"planets": [{"name": "Moon", "nakshatra": "Swati"}]},
                "_knowledge_trace": {
                    "used_chunk_ids": [1, 2, 3],
                    "missing_entities": ["Rahu dasha"],
                    "source_coverage_score": 0.75,
                    "retrieval_queries": [{"entity": "Swati"}],
                },
            },
        )
        self.assertEqual(result["source_coverage_score"], 0.75)
        self.assertTrue(result["knowledge_trace_available"])
        self.assertIn("Used 3 published knowledge chunks.", result["knowledge_grounding_summary"])


if __name__ == "__main__":
    unittest.main()
