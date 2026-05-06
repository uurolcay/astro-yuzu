import json
import unittest

import database as db_mod
from services import embedding_service, retrieval_service


class KnowledgeGroundedRetrievalTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.commit()

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.commit()
        self.db.close()

    def _item(self, *, title, status="published", metadata=None, chunk_text=None, entities=None):
        source = db_mod.SourceDocument(title=f"{title} Source", document_type="book", processing_status="completed")
        self.db.add(source)
        self.db.flush()
        item_metadata = {
            "review_required": False,
            "status": status,
            "category": "nakshatra",
            "primary_entity": "swati",
            "confidence_level": "high",
            "sensitivity_level": "low",
        }
        if metadata:
            item_metadata.update(metadata)
        item = db_mod.KnowledgeItem(
            source_document=source,
            title=title,
            item_type="nakshatra",
            language="tr",
            summary_text="Swati career summary",
            body_text=chunk_text or "Swati career independence decision pattern with strong source detail.",
            status=status,
            metadata_json=json.dumps(item_metadata, ensure_ascii=False),
            entities_json=json.dumps(entities or ["swati", "career"], ensure_ascii=False),
            coverage_entities_json=json.dumps(entities or ["swati", "career"], ensure_ascii=False),
        )
        self.db.add(item)
        self.db.flush()
        text = chunk_text or "Swati career independence decision pattern with strong source detail."
        chunk = db_mod.KnowledgeChunk(
            knowledge_item_id=item.id,
            chunk_index=0,
            chunk_text=text,
            token_count=len(text.split()),
            embedding_json=embedding_service.serialize_embedding(embedding_service.generate_embedding(text)),
            entities_json=json.dumps(entities or ["swati", "career"], ensure_ascii=False),
            coverage_entities_json=json.dumps(entities or ["swati", "career"], ensure_ascii=False),
        )
        self.db.add(chunk)
        self.db.commit()
        return item, chunk

    def test_only_published_and_active_non_review_knowledge_is_retrieved(self):
        published, _ = self._item(title="Published Swati")
        active, _ = self._item(title="Active Swati", status="active", metadata={"review_required": False, "status": "active"})
        self._item(title="Review Pending", status="published", metadata={"review_required": True})
        self._item(title="Rejected", status="rejected", metadata={"review_required": False, "status": "rejected"})
        rows = retrieval_service.retrieve_relevant_chunks(
            self.db,
            query_text="Swati career decision pattern",
            entities=["swati", "career"],
            top_k=10,
            language="tr",
        )
        item_ids = {row["knowledge_item_id"] for row in rows}
        self.assertIn(published.id, item_ids)
        self.assertIn(active.id, item_ids)
        self.assertEqual(len(item_ids), 2)

    def test_noisy_index_toc_and_deleted_items_are_excluded(self):
        safe, _ = self._item(title="Safe Swati")
        self._item(title="TOC", metadata={"is_toc": True})
        self._item(title="Index", metadata={"is_index": True})
        self._item(title="Noisy", metadata={"noise_score": 0.95})
        self._item(title="Deleted", metadata={"deleted": True})
        rows = retrieval_service.retrieve_relevant_chunks(
            self.db,
            query_text="Swati career decision pattern",
            entities=["swati", "career"],
            top_k=10,
            language="tr",
        )
        self.assertEqual([row["knowledge_item_id"] for row in rows], [safe.id])

    def test_published_knowledge_context_respects_max_chunks_and_dedupes(self):
        self._item(title="Swati Career One")
        self._item(title="Swati Career Two")
        context = retrieval_service.build_published_knowledge_context(
            {
                "dominant_signals": [
                    {"nakshatra": "Swati", "domain": "career", "pada": 2},
                    {"nakshatra": "Swati", "domain": "career", "pada": 2},
                ]
            },
            "career",
            "tr",
            max_chunks=1,
            db=self.db,
            create_gaps=False,
        )
        self.assertEqual(len(context["chunks"]), 1)
        self.assertEqual(len(context["chunk_ids"]), 1)
        self.assertEqual(len(set(context["chunk_ids"])), 1)
        self.assertIn("Swati", context["matched_entities"])


if __name__ == "__main__":
    unittest.main()
