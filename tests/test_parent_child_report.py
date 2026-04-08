import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from core.dual_chart import build_parent_child_ai_summary, build_parent_child_interpretation
from tests.test_interpretation_layer import SAMPLE_DASHA, SAMPLE_NATAL_DATA, SAMPLE_TRANSITS


def _sample_signal_layer():
    return {
        "top_anchors": [
            {
                "rank": 1,
                "title": "Visible emotional pattern",
                "summary": "The child shows feelings quickly.",
                "why_it_matters": "Emotional tone shapes behavior.",
                "opportunity": "Support with calm reflection.",
                "risk": "Pressure can be internalized fast.",
            }
        ],
        "recommendation_layer": {
            "top_recommendations": [
                {
                    "title": "Lead with calm language",
                    "type": "focus",
                    "priority": "high",
                    "time_window": "Current phase",
                    "reasoning": "The child listens better after emotional settling.",
                    "linked_anchors": [{"title": "Visible emotional pattern"}],
                }
            ],
            "opportunity_windows": [],
            "risk_windows": [],
        },
    }


def _bundle(name):
    return {
        "name": name,
        "birth_summary": "2018-04-01 | 08:30 | Istanbul, Turkey",
        "natal_data": SAMPLE_NATAL_DATA,
        "dasha_data": SAMPLE_DASHA,
        "navamsa_data": {},
        "transit_data": SAMPLE_TRANSITS,
        "eclipse_data": [],
        "fullmoon_data": [],
        "interpretation_context": {
            "signal_layer": _sample_signal_layer(),
            "recommendation_layer": _sample_signal_layer()["recommendation_layer"],
        },
    }


class ParentChildReportTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def test_parent_child_report_type_is_accepted(self):
        report_type, config = app.get_report_type_config("parent_child")
        self.assertEqual(report_type, "parent_child")
        self.assertTrue(config["include_pdf"])

    def test_both_natal_charts_are_calculated_through_existing_pipeline(self):
        sample_birth_context = {
            "local_datetime": datetime(2018, 4, 1, 8, 30),
            "utc_datetime": datetime(2018, 4, 1, 5, 30),
            "latitude": 41.0,
            "longitude": 29.0,
            "timezone": "Europe/Istanbul",
            "raw_birth_place_input": "Besiktas, Istanbul, Turkey",
            "normalized_birth_place": "Besiktas, Istanbul, Marmara Region, Turkey",
            "geocode_provider": "test",
            "geocode_confidence": 0.9,
        }
        with (
            patch.object(app.engines_natal, "calculate_natal_data", return_value=SAMPLE_NATAL_DATA) as natal_mock,
            patch.object(app.engines_dasha, "calculate_vims_dasha", return_value=SAMPLE_DASHA),
            patch.object(app.engines_navamsa, "calculate_navamsa", return_value={}),
            patch.object(app.engines_transits, "get_current_transits", return_value=[]),
            patch.object(app.engines_transits, "score_current_impact", return_value=SAMPLE_TRANSITS),
            patch.object(app.engines_eclipses, "calculate_upcoming_eclipses", return_value=[]),
            patch.object(app, "_build_interpretation_accuracy_context", return_value=_sample_signal_layer()),
            patch.object(app, "_log_chart_calculation_audit"),
        ):
            app._calculate_chart_bundle_from_birth_context(sample_birth_context)
            app._calculate_chart_bundle_from_birth_context(sample_birth_context)
        self.assertEqual(natal_mock.call_count, 2)

    def test_dual_interpretation_layer_returns_structured_sections(self):
        interpretation = build_parent_child_interpretation(_bundle("Parent"), _bundle("Child"))
        self.assertEqual(interpretation["report_type"], "parent_child")
        self.assertIn("child_profile", interpretation)
        self.assertIn("relationship_dynamics", interpretation)
        self.assertIn("school_guidance", interpretation)
        self.assertIn("parenting_guidance", interpretation)
        self.assertIn("growth_guidance", interpretation)

    def test_parent_child_anchors_and_recommendations_are_returned(self):
        interpretation = build_parent_child_interpretation(_bundle("Parent"), _bundle("Child"))
        self.assertLessEqual(len(interpretation["top_anchors"]), 3)
        self.assertTrue(interpretation["recommendation_layer"]["top_recommendations"])

    def test_parent_child_form_route_renders_safely(self):
        response = self.client.get("/reports/parent-child")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Parent-Child Guidance", response.text)
        self.assertIn("Generate Parent-Child Report", response.text)

    def test_reports_page_shows_parent_child_product_card(self):
        user = db_mod.AppUser(email="parent@example.com", password_hash="hash", name="Parent", plan_code="premium", is_active=True)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        with patch.object(app, "get_request_user", return_value=user):
            response = self.client.get("/reports")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Parent-Child Guidance Report", response.text)
        self.assertIn("/reports/parent-child", response.text)
        self.assertIn("Guidance, not labeling.", response.text)
        self.assertIn("Unlock Your Full Child Report", response.text)

    def test_result_template_supports_parent_child_sections(self):
        interpretation = build_parent_child_interpretation(_bundle("Parent"), _bundle("Child"))
        html = app.templates.env.get_template("result.html").render(
            {
                "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
                "full_name": "Child",
                "birth_date": "2018-04-01",
                "birth_time": "08:30",
                "birth_city": "Istanbul, Turkey",
                "normalized_birth_place": "Istanbul, Turkey",
                "timezone": "Europe/Istanbul",
                "report_type": "parent_child",
                "report_type_config": {"include_pdf": True},
                "interpretation_context": interpretation,
                "payload_json": {},
                "report_access": {"is_preview": True, "show_unlock_cta": False, "can_view_full_report": False, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Preview"},
                "related_articles": [],
                "natal_data": SAMPLE_NATAL_DATA,
                "dasha_data": SAMPLE_DASHA,
                "navamsa_data": {},
                "transit_data": SAMPLE_TRANSITS,
                "eclipse_data": [],
            }
        )
        self.assertIn("Parent-Child Guidance", html)
        self.assertIn("Child Core Nature", html)
        self.assertIn("Recommended Approach", html)
        self.assertIn("This is only part of your child's profile", html)

    def test_report_pdf_supports_parent_child_sections_with_missing_timing(self):
        interpretation = build_parent_child_interpretation(_bundle("Parent"), _bundle("Child"))
        interpretation["timing_notes"] = []
        context = {
            "language": "en",
            "report_title": "Parent-Child Guidance Report",
            "report_subtitle": "Premium family guidance.",
            "report_type": "parent_child",
            "report_type_label": "Parent-Child",
            "client_name": "Child",
            "birth_summary": "2018-04-01 | 08:30 | Istanbul, Turkey",
            "top_anchors": interpretation["top_anchors"],
            "top_recommendations": [
                {
                    "title": item["title"],
                    "type_label": item["type"],
                    "time_window": item["time_window"],
                    "reasoning": item["reasoning"],
                    "priority_label": item["priority"],
                    "linked_anchor_title": (item.get("linked_anchors") or [{}])[0].get("title"),
                }
                for item in interpretation["recommendation_layer"]["top_recommendations"]
            ],
            "opportunity_windows_report": [],
            "risk_windows_report": [],
            "show_action_guidance": True,
            "methodology_notes": [],
            "child_profile_report": interpretation["child_profile"],
            "relationship_dynamics_report": interpretation["relationship_dynamics"],
            "school_guidance_report": interpretation["school_guidance"],
            "parenting_guidance_report": interpretation["parenting_guidance"],
            "watch_areas_report": interpretation["watch_areas"],
            "growth_guidance_report": interpretation["growth_guidance"],
            "timing_notes_report": [],
            "ai_interpretation": build_parent_child_ai_summary(interpretation),
            "request": None,
            "report_access": {"is_preview": False},
        }
        html = app.templates.env.get_template("report_pdf.html").render(context)
        self.assertIn("Parent-Child Guidance", html)
        self.assertIn("Child Core Nature", html)
        self.assertIn("Calculation Notes", html)


if __name__ == "__main__":
    unittest.main()
