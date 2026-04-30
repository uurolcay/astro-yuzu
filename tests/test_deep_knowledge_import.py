import json
import unittest

import database as db_mod
from services import knowledge_import_service


class DeepKnowledgeImportTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="deep-knowledge-admin@example.com",
            password_hash="hash",
            name="Knowledge Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.db.add(self.admin)
        self.db.commit()

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _payload(self, **overrides):
        payload = {
            "title": "Mars in the 6th House During Mars Dasha",
            "category": "timing_logic",
            "entity_type": "mixed",
            "primary_entity": "mars",
            "secondary_entity": "6th_house",
            "coverage_entities": ["mars", "6th_house", "mars_dasha", "career"],
            "source_type": "classical_text",
            "source_title": "Brihat Parashara Hora Shastra",
            "source_author": "Parashara",
            "source_reference": "Chapter 42",
            "classical_view": "Mars in the 6th can increase conflict capacity and tactical persistence.",
            "modern_synthesis": "This can be productive in work settings when pressure is structured.",
            "interpretation_logic": "Use when Mars is active and the chart emphasizes service, work, or conflict handling.",
            "condition_context": "Prefer when Mars is supported and not severely destabilized.",
            "strong_condition": "Mars dasha or strong Mars transit to work houses.",
            "weak_condition": "Weak if Mars is heavily compromised and timing is not activated.",
            "contradiction_notes": "Do not flatten this into guaranteed conflict.",
            "risk_pattern": "Overwork, defensiveness, unnecessary battles.",
            "opportunity_pattern": "Decisive action, tactical problem solving, grit under pressure.",
            "dasha_activation": "Strongly activated in Mars dasha and Mars antardasha.",
            "transit_activation": "Strengthens when Mars or Saturn activate the 6th axis.",
            "report_type_usage": ["career", "annual_transit"],
            "safe_language_notes": "Avoid deterministic health claims.",
            "what_not_to_say": "You will definitely get sick or enter conflict.",
            "premium_synthesis_sentence": "Mars pressure can become disciplined momentum when conflict is given a structured outlet.",
            "tags": ["mars", "timing", "career"],
            "confidence_level": "high",
            "sensitivity_level": "moderate",
            "interpretation_modes": ["timing_sensitive", "action_guided"],
        }
        payload.update(overrides)
        return payload

    def test_valid_payload_imports(self):
        imported = knowledge_import_service.import_deep_knowledge_items(self.db, [self._payload()], admin_user=self.admin)
        self.db.commit()
        self.assertEqual(len(imported), 1)
        self.assertEqual(self.db.query(db_mod.KnowledgeItem).count(), 1)

    def test_missing_required_field_fails(self):
        with self.assertRaises(ValueError):
            knowledge_import_service.import_deep_knowledge_items(
                self.db,
                [self._payload(title="")],
                admin_user=self.admin,
            )

    def test_invalid_category_fails(self):
        with self.assertRaises(ValueError):
            knowledge_import_service.import_deep_knowledge_items(
                self.db,
                [self._payload(category="bad_category")],
                admin_user=self.admin,
            )

    def test_invalid_source_type_fails(self):
        with self.assertRaises(ValueError):
            knowledge_import_service.import_deep_knowledge_items(
                self.db,
                [self._payload(source_type="bad_source")],
                admin_user=self.admin,
            )

    def test_coverage_entities_are_stored(self):
        imported = knowledge_import_service.import_deep_knowledge_items(self.db, [self._payload()], admin_user=self.admin)
        self.db.commit()
        item = imported[0]
        self.assertIn("mars", json.loads(item.coverage_entities_json or "[]"))

    def test_what_not_to_say_is_preserved(self):
        imported = knowledge_import_service.import_deep_knowledge_items(self.db, [self._payload()], admin_user=self.admin)
        self.db.commit()
        metadata = json.loads(imported[0].metadata_json or "{}")
        self.assertEqual(metadata.get("what_not_to_say"), "You will definitely get sick or enter conflict.")

    def test_premium_synthesis_sentence_is_preserved(self):
        imported = knowledge_import_service.import_deep_knowledge_items(self.db, [self._payload()], admin_user=self.admin)
        self.db.commit()
        metadata = json.loads(imported[0].metadata_json or "{}")
        self.assertEqual(
            metadata.get("premium_synthesis_sentence"),
            "Mars pressure can become disciplined momentum when conflict is given a structured outlet.",
        )

    def test_export_returns_valid_json_shape(self):
        knowledge_import_service.import_deep_knowledge_items(self.db, [self._payload()], admin_user=self.admin)
        self.db.commit()
        exported = knowledge_import_service.export_deep_knowledge_items(self.db, category="timing_logic", entity="mars", report_type="career")
        self.assertIn("items", exported)
        self.assertIn("count", exported)
        self.assertEqual(exported["count"], 1)
        self.assertEqual(exported["items"][0]["title"], "Mars in the 6th House During Mars Dasha")

    def test_no_service_order_is_created(self):
        before = self.db.query(db_mod.ServiceOrder).count()
        knowledge_import_service.import_deep_knowledge_items(self.db, [self._payload()], admin_user=self.admin)
        knowledge_import_service.export_deep_knowledge_items(self.db)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before)
