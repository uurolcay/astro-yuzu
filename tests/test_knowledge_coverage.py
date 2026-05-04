import json
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from services import knowledge_coverage_service as coverage_svc
from services import retrieval_service


class KnowledgeCoverageTests(unittest.TestCase):
    def setUp(self):
        db_mod.init_db()
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.TrainingTask).delete()
        self.db.query(db_mod.KnowledgeGap).delete()
        self.db.query(db_mod.EvaluationResult).delete()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.InterpretationReview).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="coverage-admin@example.com",
            password_hash="hash",
            name="Coverage Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.profile = db_mod.InternalProfile(
            full_name="Coverage Profile",
            birth_date="1990-01-01",
            birth_time="08:30",
            birth_place_label="Istanbul",
        )
        self.db.add_all([self.admin, self.profile])
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.TrainingTask).delete()
        self.db.query(db_mod.KnowledgeGap).delete()
        self.db.query(db_mod.EvaluationResult).delete()
        self.db.query(db_mod.KnowledgeChunk).delete()
        self.db.query(db_mod.KnowledgeItem).delete()
        self.db.query(db_mod.SourceDocument).delete()
        self.db.query(db_mod.InterpretationReview).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _request_admin_user(self, request, db):
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "coverage-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def _knowledge_item(self, title="Saturn Dhanishta", text="Saturn in Dhanishta strengthens discipline and career timing.", entities=None):
        item = db_mod.KnowledgeItem(
            title=title,
            body_text=text,
            language="en",
            item_type="reference",
            status="active",
            entities_json=json.dumps(entities or [], ensure_ascii=False),
            coverage_entities_json=json.dumps(entities or [], ensure_ascii=False),
        )
        self.db.add(item)
        self.db.flush()
        chunk = db_mod.KnowledgeChunk(
            knowledge_item=item,
            chunk_index=0,
            chunk_text=text,
            entities_json=json.dumps(entities or [], ensure_ascii=False),
            coverage_entities_json=json.dumps(entities or [], ensure_ascii=False),
            token_count=len(text.split()),
        )
        self.db.add(chunk)
        self.db.commit()
        return item, chunk

    def test_normalize_entity_purva_phalguni(self):
        self.assertEqual(coverage_svc.normalize_entity("Purva Phalguni"), "purva_phalguni")

    def test_normalize_entity_6th_house(self):
        self.assertEqual(coverage_svc.normalize_entity("6th house"), "6th_house")

    def test_normalize_entity_turkish_house(self):
        self.assertEqual(coverage_svc.normalize_entity("6. ev"), "6th_house")

    def test_normalize_entity_mars_dasha(self):
        self.assertEqual(coverage_svc.normalize_entity("Mars Dasha"), "mars_dasha")

    def test_extract_entities_from_text_contains_saturn_and_dhanishta(self):
        entities = coverage_svc.extract_entities_from_text("Saturn in Dhanishta")
        self.assertIn("saturn", entities)
        self.assertIn("dhanishta", entities)

    def test_extract_entities_from_text_empty_returns_empty(self):
        self.assertEqual(coverage_svc.extract_entities_from_text(""), [])

    def test_extract_entities_from_text_none_returns_empty(self):
        self.assertEqual(coverage_svc.extract_entities_from_text(None), [])

    def test_extract_entities_from_text_mars_mahadasha_contains_entities(self):
        entities = coverage_svc.extract_entities_from_text("Mars mahadasha with 6th house activation")
        self.assertIn("mars_dasha", entities)
        self.assertIn("6th_house", entities)

    def test_extract_entities_from_chart_data_returns_expected_entities(self):
        entities = coverage_svc.extract_entities_from_chart_data(
            {"planets": [{"name": "Mars", "house": 6, "nakshatra": "Hasta", "pada": 3}]}
        )
        self.assertIn("mars", entities)
        self.assertIn("6th_house", entities)
        self.assertIn("hasta", entities)
        self.assertIn("pada_3", entities)

    def test_extract_entities_from_chart_data_empty_returns_empty(self):
        self.assertEqual(coverage_svc.extract_entities_from_chart_data({}), [])

    def test_compute_knowledge_coverage_empty_db_returns_valid_shape(self):
        coverage = coverage_svc.compute_knowledge_coverage(self.db)
        self.assertGreater(coverage["summary"]["total_entities"], 0)
        self.assertGreater(coverage["summary"]["missing_count"], 0)
        self.assertEqual(coverage["summary"]["overall_pct"], 0.0)

    def test_compute_knowledge_coverage_counts_chunks(self):
        self._knowledge_item(entities=["saturn", "dhanishta"])
        coverage = coverage_svc.compute_knowledge_coverage(self.db)
        self.assertNotEqual(coverage["by_category"]["planets"]["saturn"]["level"], "missing")
        self.assertNotEqual(coverage["by_category"]["nakshatras"]["dhanishta"]["level"], "missing")

    def test_detect_unused_knowledge_returns_empty_when_entity_in_output(self):
        _item, chunk = self._knowledge_item(text="Saturn discipline and responsibility", entities=["saturn"])
        unused = coverage_svc.detect_unused_knowledge([chunk.id], "Saturn shows discipline and responsibility.", self.db)
        self.assertEqual(unused, [])

    def test_detect_unused_knowledge_flags_when_entity_absent(self):
        _item, chunk = self._knowledge_item(text="Saturn discipline", entities=["saturn"])
        unused = coverage_svc.detect_unused_knowledge([chunk.id], "Venus shows relationship harmony.", self.db)
        self.assertEqual(len(unused), 1)
        self.assertEqual(unused[0]["chunk_id"], chunk.id)

    def test_get_knowledge_trace_returns_safe_empty_valid_structure(self):
        interpretation = db_mod.InternalInterpretation(
            profile=self.profile,
            report_type="career",
            input_payload_json=json.dumps({}, ensure_ascii=False),
            interpretation_text="",
        )
        self.db.add(interpretation)
        self.db.commit()
        trace = coverage_svc.get_knowledge_trace_for_interpretation(self.db, interpretation)
        self.assertEqual(set(trace.keys()), {
            "used_chunks", "used_entity_set", "chart_expected_entities", "output_entities",
            "missing_from_knowledge", "retrieved_but_weak", "not_in_output", "unused_chunks", "gap_types",
        })

    def test_get_knowledge_trace_detects_missing_knowledge(self):
        interpretation = db_mod.InternalInterpretation(
            profile=self.profile,
            report_type="career",
            input_payload_json=json.dumps(
                {"natal_data": {"planets": [{"name": "Mars", "nakshatra": "Hasta"}]}},
                ensure_ascii=False,
            ),
            interpretation_text="Career focus.",
        )
        self.db.add(interpretation)
        self.db.commit()
        trace = coverage_svc.get_knowledge_trace_for_interpretation(self.db, interpretation)
        self.assertTrue("mars" in trace["missing_from_knowledge"] or "hasta" in trace["missing_from_knowledge"])

    def test_get_knowledge_trace_detects_unused_knowledge(self):
        _item, chunk = self._knowledge_item(text="Saturn discipline", entities=["saturn"])
        interpretation = db_mod.InternalInterpretation(
            profile=self.profile,
            report_type="career",
            input_payload_json=json.dumps({"natal_data": {"planets": [{"name": "Saturn"}]}}, ensure_ascii=False),
            interpretation_text="Venus supports harmony.",
            used_chunk_ids_json=json.dumps([chunk.id], ensure_ascii=False),
        )
        self.db.add(interpretation)
        self.db.commit()
        trace = coverage_svc.get_knowledge_trace_for_interpretation(self.db, interpretation)
        self.assertTrue("saturn" in trace["not_in_output"] or trace["unused_chunks"])

    def test_build_suggested_training_tasks_from_trace_creates_priorities(self):
        tasks = coverage_svc.build_suggested_training_tasks_from_trace(
            {
                "gap_types": {
                    "mars": "missing_knowledge",
                    "hasta": "low_depth",
                    "saturn": "unused_knowledge",
                }
            }
        )
        priorities = {task["reason"]: task["priority"] for task in tasks}
        self.assertEqual(priorities["missing_knowledge"], "high")
        self.assertEqual(priorities["low_depth"], "medium")
        self.assertEqual(priorities["unused_knowledge"], "low")

    def test_retrieval_service_build_prompt_knowledge_context_returns_chunk_ids_key(self):
        self._knowledge_item(entities=["career"])
        payload = {"report_type": "career", "language": "en", "natal_data": {"planets": [{"name": "Saturn"}]}}
        context = retrieval_service.build_prompt_knowledge_context(payload, db=self.db)
        self.assertIn("chunk_ids", context)

    def test_no_service_order_is_created_by_coverage_service_functions(self):
        before = self.db.query(db_mod.ServiceOrder).count()
        coverage_svc.compute_knowledge_coverage(self.db)
        coverage_svc.detect_unused_knowledge([], "", self.db)
        coverage_svc.build_suggested_training_tasks_from_trace({"gap_types": {"mars": "missing_knowledge"}})
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before)

    def test_admin_knowledge_coverage_route_returns_200_for_admin(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/knowledge/coverage")
        self.assertEqual(response.status_code, 200)

    def test_admin_knowledge_coverage_route_uses_limited_scan(self):
        safe_coverage = {
            "summary": {"total_entities": 0, "missing_count": 0, "weak_count": 0, "moderate_count": 0, "strong_count": 0, "overall_pct": 0},
            "by_category": {},
            "missing_entities": [],
            "weak_entities": [],
        }
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.coverage_svc,
            "compute_knowledge_coverage",
            return_value=safe_coverage,
        ) as compute:
            response = self.client.get("/admin/knowledge/coverage")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(compute.call_args.kwargs.get("max_chunks"), app.ADMIN_COVERAGE_MAX_CHUNKS)

    def test_coverage_rebuild_is_manual_post_route(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            page = self.client.get("/admin/knowledge/coverage")
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', page.text)
        self.assertIsNotNone(match)
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair), patch.object(
            app.coverage_svc,
            "compute_knowledge_coverage",
            return_value={"summary": {}, "by_category": {}, "missing_entities": [], "weak_entities": []},
        ) as compute:
            response = self.client.post(
                "/admin/knowledge/coverage/rebuild",
                data={"csrf_token": match.group(1)},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertIn("coverage_refreshed", response.headers.get("location", ""))
        self.assertEqual(compute.call_args.kwargs.get("max_chunks"), app.ADMIN_COVERAGE_MAX_CHUNKS)

    def test_admin_interpretation_knowledge_route_returns_200_for_admin(self):
        interpretation = db_mod.InternalInterpretation(
            profile=self.profile,
            report_type="career",
            input_payload_json=json.dumps({"natal_data": {"planets": [{"name": "Mars"}]}}, ensure_ascii=False),
            interpretation_text="Mars indicates effort.",
        )
        self.db.add(interpretation)
        self.db.commit()
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get(f"/admin/astro-workspace/quality/{interpretation.id}/knowledge")
        self.assertEqual(response.status_code, 200)

    def test_non_admin_access_to_new_routes_is_blocked(self):
        response_one = self.client.get("/admin/knowledge/coverage", follow_redirects=False)
        response_two = self.client.get("/admin/astro-workspace/quality/1/knowledge", follow_redirects=False)
        self.assertIn(response_one.status_code, {302, 303, 307, 401, 403})
        self.assertIn(response_two.status_code, {302, 303, 307, 401, 403})
