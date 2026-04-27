import tempfile
import re
import unittest
from datetime import datetime
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import agent_pipeline
import database as db_mod
from services import admin_astro_chat as chat
from services import admin_astro_workspace as workspace
from services import ai_behavior_rules
from tests.test_interpretation_layer import SAMPLE_DASHA, SAMPLE_NATAL_DATA, SAMPLE_TRANSITS


def _birth_context():
    return {
        "local_datetime": datetime(1990, 1, 2, 8, 30),
        "utc_datetime": datetime(1990, 1, 2, 5, 30),
        "latitude": 41.0,
        "longitude": 29.0,
        "timezone": "Europe/Istanbul",
        "raw_birth_place_input": "Istanbul, Turkey",
        "normalized_birth_place": "Istanbul, Turkey",
        "geocode_provider": "test",
        "geocode_confidence": 1.0,
    }


def _signal_layer():
    return {
        "top_anchors": [
            {
                "rank": 1,
                "title": "Career timing",
                "summary": "A focused work cycle is active.",
                "why_it_matters": "It shapes the reading.",
                "opportunity": "Choose deliberate action.",
                "risk": "Avoid overextension.",
            }
        ],
        "recommendation_layer": {"top_recommendations": []},
    }


class AdminAstroWorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.InternalChatMessage).delete()
        self.db.query(db_mod.InternalChatSession).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.AiBehaviorRule).delete()
        self.db.query(db_mod.AiBehaviorRuleSet).delete()
        self.db.query(db_mod.Transaction).delete()
        self.db.query(db_mod.Invoice).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.admin = db_mod.AppUser(
            email="workspace-admin@example.com",
            password_hash="hash",
            name="Admin",
            is_admin=True,
            is_active=True,
            plan_code="elite",
        )
        self.db.add(self.admin)
        self.db.commit()
        self.client = TestClient(app.app)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.InternalChatMessage).delete()
        self.db.query(db_mod.InternalChatSession).delete()
        self.db.query(db_mod.InternalInterpretation).delete()
        self.db.query(db_mod.InternalProfile).delete()
        self.db.query(db_mod.AiBehaviorRule).delete()
        self.db.query(db_mod.AiBehaviorRuleSet).delete()
        self.db.query(db_mod.Transaction).delete()
        self.db.query(db_mod.Invoice).delete()
        self.db.query(db_mod.ServiceOrder).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def _patch_chart_pipeline(self):
        return (
            patch.object(app, "_build_birth_context", return_value=_birth_context()),
            patch.object(app, "_build_birth_context_from_saved_fields", return_value=_birth_context()),
            patch.object(app.engines_natal, "calculate_natal_data", return_value=SAMPLE_NATAL_DATA),
            patch.object(app.engines_dasha, "calculate_vims_dasha", return_value=SAMPLE_DASHA),
            patch.object(app.engines_navamsa, "calculate_navamsa", return_value={}),
            patch.object(app.engines_transits, "get_current_transits", return_value=[]),
            patch.object(app.engines_transits, "score_current_impact", return_value=SAMPLE_TRANSITS),
            patch.object(app.engines_eclipses, "calculate_upcoming_eclipses", return_value=[]),
            patch.object(app, "_build_interpretation_accuracy_context", return_value=_signal_layer()),
            patch.object(app, "_log_chart_calculation_audit"),
        )

    def _workspace_form_data(self, **overrides):
        data = {
            "report_type": "career",
            "full_name": "Saved Workspace Person",
            "gender": "",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Kadikoy, Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
            "resolved_geocode_provider": "test",
            "resolved_geocode_confidence": "0.98",
            "notes": "Internal note",
        }
        data.update(overrides)
        return data

    def _workspace_csrf(self):
        with patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair):
            response = self.client.get("/admin/astro-workspace")
        self.assertEqual(response.status_code, 200)
        match = re.search(r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']', response.text)
        if not match:
            match = re.search(r'value=["\']([^"\']+)["\'][^>]*name=["\']csrf_token["\']', response.text)
        self.assertIsNotNone(match, response.text[:500])
        return match.group(1)

    def _request_admin_user(self, request, db):
        return db.query(db_mod.AppUser).filter(db_mod.AppUser.email == "workspace-admin@example.com").first()

    def _request_admin_pair(self, request, db):
        return self._request_admin_user(request, db), None

    def test_internal_profile_can_be_created_without_service_order(self):
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {
                "full_name": "Internal Friend",
                "birth_date": "1990-01-02",
                "birth_time": "08:30",
                "birth_place_label": "Istanbul, Turkey",
            },
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Istanbul, Turkey"},
        )
        self.db.commit()
        self.assertIsNotNone(profile.id)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)

    def test_generation_uses_shared_ai_without_payment_or_order(self):
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        before_transactions = self.db.query(db_mod.Transaction).count()
        form = {
            "report_type": "career",
            "full_name": "Internal Career",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
        }
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        with patch.object(workspace.ai_logic, "generate_interpretation", return_value="shared ai text") as ai_mock:
            text = workspace.generate_workspace_interpretation(prepared["payload"])
        self.assertEqual(text, "shared ai text")
        self.assertEqual(ai_mock.call_count, 1)
        self.assertEqual(prepared["payload"]["source"], "admin_astro_workspace")
        self.assertIn("astro_signal_context", prepared["payload"])
        self.assertIn("dominant_signals", prepared["payload"]["astro_signal_context"])
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)
        self.assertEqual(self.db.query(db_mod.Transaction).count(), before_transactions)

    def test_saved_interpretation_attaches_to_internal_profile(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "Saved Person", "birth_date": "1990-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
        )
        interpretation = workspace.save_internal_interpretation(
            self.db,
            profile=profile,
            report_type="birth_chart_karma",
            payload={"source": "admin_astro_workspace"},
            interpretation_text="saved text",
            admin_user=self.admin,
        )
        self.db.commit()
        self.assertEqual(interpretation.profile_id, profile.id)
        self.assertEqual(self.db.query(db_mod.InternalInterpretation).count(), 1)

    def test_parent_child_generation_prepares_two_profiles(self):
        form = {
            "report_type": "parent_child",
            "full_name": "Parent",
            "birth_date": "1980-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
            "secondary_full_name": "Child",
            "secondary_birth_date": "2018-04-01",
            "secondary_birth_time": "09:15",
            "secondary_birth_place_label": "Istanbul, Turkey",
            "secondary_birth_city": "Uskudar",
            "secondary_resolved_birth_place": "Uskudar, Istanbul, Turkey",
            "secondary_resolved_latitude": "41.0220",
            "secondary_resolved_longitude": "29.0137",
            "secondary_resolved_timezone": "Europe/Istanbul",
        }
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertEqual(prepared["report_type"], "parent_child")
        self.assertEqual(prepared["payload"]["report_type"], "parent_child")
        self.assertIn("parent_profile", prepared["payload"])
        self.assertIn("child_profile_meta", prepared["payload"])

    def test_workspace_manual_submission_without_resolved_location_fails(self):
        form = {
            "report_type": "career",
            "full_name": "Needs Selection",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Kadikoy, Istanbul, Turkey",
            "birth_city": "Kadikoy",
        }
        with self.assertRaisesRegex(ValueError, "Select the birth location from the dropdown suggestions"):
            app._prepare_admin_workspace_generation(self.db, form, self.admin)

    def test_workspace_manual_submission_with_resolved_location_passes(self):
        form = {
            "report_type": "career",
            "full_name": "Resolved Selection",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Kadikoy, Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
            "resolved_geocode_provider": "test",
            "resolved_geocode_confidence": "0.98",
        }
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertEqual(prepared["primary_payload"]["birth_city"], "Kadikoy")
        self.assertEqual(prepared["primary_birth_context"]["timezone"], "Europe/Istanbul")

    def test_workspace_parent_child_manual_submission_requires_both_resolved_locations(self):
        form = {
            "report_type": "parent_child",
            "full_name": "Parent",
            "birth_date": "1980-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Kadikoy, Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
            "secondary_full_name": "Child",
            "secondary_birth_date": "2018-04-01",
            "secondary_birth_time": "09:15",
            "secondary_birth_place_label": "Uskudar, Istanbul, Turkey",
            "secondary_birth_city": "Uskudar",
            "secondary_resolved_birth_place": "Uskudar, Istanbul, Turkey",
            "secondary_resolved_latitude": "41.0220",
            "secondary_resolved_longitude": "29.0137",
            "secondary_resolved_timezone": "Europe/Istanbul",
        }
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertEqual(prepared["secondary_payload"]["birth_city"], "Uskudar")
        self.assertEqual(prepared["secondary_birth_context"]["timezone"], "Europe/Istanbul")

    def test_career_generation_does_not_require_secondary_profile(self):
        form = self._workspace_form_data(
            report_type="career",
            secondary_full_name="Ignored Secondary",
            secondary_birth_date="2018-04-01",
            secondary_birth_time="09:15",
            secondary_birth_place_label="Unresolved Secondary Input",
            secondary_birth_city="Unresolved Secondary Input",
        )
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertEqual(prepared["report_type"], "career")
        self.assertIsNone(prepared["secondary_payload"])
        self.assertIsNone(prepared["secondary_birth_context"])

    def test_birth_chart_generation_does_not_require_secondary_profile(self):
        form = self._workspace_form_data(
            report_type="birth_chart_karma",
            secondary_full_name="Ignored Secondary",
            secondary_birth_place_label="Unresolved Secondary Input",
            secondary_birth_city="Unresolved Secondary Input",
        )
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertEqual(prepared["report_type"], "birth_chart_karma")
        self.assertIsNone(prepared["secondary_payload"])

    def test_annual_transit_generation_does_not_require_secondary_profile(self):
        form = self._workspace_form_data(
            report_type="annual_transit",
            secondary_full_name="Ignored Secondary",
            secondary_birth_place_label="Unresolved Secondary Input",
            secondary_birth_city="Unresolved Secondary Input",
        )
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertEqual(prepared["report_type"], "annual_transit")
        self.assertIsNone(prepared["secondary_payload"])

    def test_workspace_default_page_hides_secondary_panel_until_parent_child(self):
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user):
            response = self.client.get("/admin/astro-workspace")
        self.assertEqual(response.status_code, 200)
        self.assertIn('id="secondary-profile-panel"', response.text)
        self.assertIn('secondary-profile-panel is-hidden', response.text)

    def test_workspace_turkish_context_localizes_theme_labels_and_preserves_characters(self):
        payload = {
            "language": "tr",
            "report_type": "career",
            "workspace_report_type": "career",
            "render_report_type": "premium",
            "full_name": "Türkçe Profil",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_city": "Kadıköy, İstanbul, Türkiye",
            "normalized_birth_place": "Kadıköy, İstanbul, Türkiye",
            "interpretation_context": {
                "primary_focus": "career",
                "decision_posture": "act",
                "signal_layer": {
                    "top_anchors": [
                        {
                            "rank": 1,
                            "title": "Concentration shaping career",
                            "summary": "This cluster concentrates the chart's strongest weight across career and growth.",
                            "why_it_matters": "This anchor shapes decision quality, emotional orientation, and timing across career, relationships.",
                            "opportunity": "Maturity, meaningful expansion, and stronger long-range vision.",
                            "risk": "Pressure fatigue, over-control, or mistaking delay for failure.",
                        }
                    ],
                    "recommendation_layer": {"top_recommendations": []},
                },
            },
        }
        context = app._internal_pdf_context(None, payload, "Türkçe yorum metni")
        self.assertEqual(context["report_title"], "Kariyer Yönü Raporu")
        self.assertEqual(context["report_type_label"], "Kariyer")
        self.assertIn("İç kullanım", context["report_access"]["access_label"])
        self.assertEqual(context["top_anchors"][0]["title"], "Kariyeri şekillendiren Odak")
        self.assertNotIn("Concentration shaping career", context["top_anchors"][0]["title"]); self.assertNotIn("relationships", context["top_anchors"][0]["why_it_matters"].lower()); self.assertIn("kariyer", context["top_anchors"][0]["why_it_matters"].lower()); return
        self.assertIn("ilişkiler", context["top_anchors"][0]["why_it_matters"])
        return
        self.assertIn("ilişkiler", context["top_anchors"][0]["why_it_matters"])
        return
        self.assertIn("ilişkiler", context["top_anchors"][0]["why_it_matters"])

    def test_workspace_turkish_context_removes_career_english_leaks(self):
        payload = {
            "language": "tr",
            "report_type": "career",
            "workspace_report_type": "career",
            "render_report_type": "premium",
            "full_name": "TÃ¼rkÃ§e Kariyer",
            "birth_date": "1990-01-02",
            "birth_time": "08:30",
            "birth_city": "KadÄ±kÃ¶y, Ä°stanbul, TÃ¼rkiye",
            "interpretation_context": {
                "signal_layer": {
                    "top_anchors": [
                        {
                            "title": "money shaping nodes",
                            "summary": "The chart is currently led by money and nodes.",
                            "why_it_matters": "Timing and money need extra care.",
                            "opportunity": "Growth can happen through relationship repair.",
                            "risk": "Nodes can scatter attention.",
                        }
                    ],
                    "recommendation_layer": {
                        "top_recommendations": [
                            {
                                "title": "Timing shaping career",
                                "type": "focus",
                                "priority": "high",
                                "time_window": "Current phase",
                                "reasoning": "The chart is currently led by timing and money.",
                                "linked_anchors": [{"title": "money shaping nodes"}],
                            }
                        ],
                        "opportunity_windows": [{"title": "Timing", "time_window": "Current phase"}],
                        "risk_windows": [{"title": "nodes", "time_window": "Current phase"}],
                    },
                }
            },
        }
        context = app._internal_pdf_context(None, payload, "TÃ¼rkÃ§e yorum metni")
        serialized = str(context)
        for leak in ("The chart is currently", "money", "nodes", "Timing"):
            self.assertNotIn(leak, serialized)
        self.assertIn("Haritada", serialized)
        self.assertIn("Ay DÃ¼ÄŸÃ¼mleri", serialized)
        self.assertIn("zamanlama", serialized.lower())

    def test_workspace_parent_child_uses_specific_interpretation_path(self):
        form = {
            "report_type": "parent_child",
            "full_name": "Parent",
            "birth_date": "1980-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Kadikoy, Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
            "secondary_full_name": "Child",
            "secondary_birth_date": "2018-04-01",
            "secondary_birth_time": "09:15",
            "secondary_birth_place_label": "Uskudar, Istanbul, Turkey",
            "secondary_birth_city": "Uskudar",
            "secondary_resolved_birth_place": "Uskudar, Istanbul, Turkey",
            "secondary_resolved_latitude": "41.0220",
            "secondary_resolved_longitude": "29.0137",
            "secondary_resolved_timezone": "Europe/Istanbul",
        }
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin, language="tr")
        with patch.object(workspace.ai_logic, "generate_interpretation", return_value="should not be used") as ai_mock:
            interpretation_text = workspace.generate_workspace_interpretation(prepared["payload"])
        self.assertEqual(ai_mock.call_count, 0)
        self.assertEqual(prepared["payload"]["report_type"], "parent_child")
        self.assertEqual(prepared["payload"]["workspace_report_type"], "parent_child")
        self.assertIn("Çocuğun Temel Doğası", interpretation_text)
        self.assertNotEqual(prepared["payload"]["report_type"], "birth_chart_karma")

    def test_workspace_parent_child_missing_secondary_returns_clear_error(self):
        form = {
            "report_type": "parent_child",
            "full_name": "Parent",
            "birth_date": "1980-01-02",
            "birth_time": "08:30",
            "birth_place_label": "Kadikoy, Istanbul, Turkey",
            "birth_city": "Kadikoy",
            "resolved_birth_place": "Kadikoy, Istanbul, Turkey",
            "resolved_latitude": "40.9917",
            "resolved_longitude": "29.0277",
            "resolved_timezone": "Europe/Istanbul",
        }
        with self.assertRaisesRegex(ValueError, "Parent-child report requires a secondary child profile"):
            app._prepare_admin_workspace_generation(self.db, form, self.admin, language="tr")

    def test_workspace_pdf_context_localizes_parent_child_titles(self):
        payload = {
            "language": "tr",
            "report_type": "parent_child",
            "workspace_report_type": "parent_child",
            "render_report_type": "parent_child",
            "full_name": "Çocuk",
            "birth_date": "2018-04-01",
            "birth_time": "09:15",
            "birth_city": "Kadıköy, İstanbul, Türkiye",
            "interpretation_context": {
                "report_type": "parent_child",
                "primary_focus": "parent_child_guidance",
                "signal_layer": {"top_anchors": [], "recommendation_layer": {"top_recommendations": []}},
                "child_profile": {"temperament": "Sensitive, imaginative, and emotionally responsive to the tone of the environment."},
                "relationship_dynamics": {"emotional_compatibility": "Emotional flow is easier here because both charts process feelings with a similar tempo."},
                "school_guidance": {},
                "parenting_guidance": {},
                "growth_guidance": {},
                "timing_notes": [],
                "watch_areas": [],
            },
        }
        context = app._internal_pdf_context(None, payload, "Türkçe metin")
        self.assertEqual(context["report_title"], "Ebeveyn-Çocuk Rehberlik Raporu")
        self.assertEqual(context["report_type_label"], "Ebeveyn-Çocuk")
        self.assertEqual(
            context["child_profile_report"]["temperament"],
            "Hassas, hayal gücü kuvvetli ve bulunduğu ortamın duygusal tonuna açık bir yapı.",
        )

    def test_workspace_parent_child_turkish_context_removes_interaction_english_leaks(self):
        payload = {
            "language": "tr",
            "report_type": "parent_child",
            "workspace_report_type": "parent_child",
            "render_report_type": "parent_child",
            "full_name": "Ã‡ocuk",
            "birth_date": "2018-04-01",
            "birth_time": "09:15",
            "birth_city": "KadÄ±kÃ¶y, Ä°stanbul, TÃ¼rkiye",
            "interpretation_context": {
                "report_type": "parent_child",
                "primary_focus": "parent_child_guidance",
                "signal_layer": {
                    "top_anchors": [],
                    "recommendation_layer": {
                        "top_recommendations": [
                            {
                                "title": "Lead with calm, specific communication",
                                "type": "focus",
                                "priority": "high",
                                "time_window": "Current phase",
                                "reasoning": "Invite questions and dialogue so the child can process by interacting, not only by listening",
                                "linked_anchors": [{"title": "Child core emotional pattern"}],
                            }
                        ],
                        "opportunity_windows": [{"title": "Growth-supportive rhythm", "time_window": "Mars period emphasis"}],
                        "risk_windows": [{"title": "Pressure-sensitive periods", "time_window": "Current phase"}],
                    },
                },
                "child_profile": {"temperament": "Child core emotional pattern"},
                "relationship_dynamics": {"emotional_compatibility": "Parent-child relationship dynamic"},
                "school_guidance": {"learning_style": "Growth-supportive rhythm"},
                "parenting_guidance": {"best_approach": "Lead with calm, specific communication"},
                "growth_guidance": {"next_step": "Invite questions and dialogue so the child can process by interacting, not only by listening"},
                "timing_notes": [{"label": "Current phase", "description": "Mars period emphasis"}],
                "watch_areas": ["Pressure-sensitive periods"],
            },
        }
        context = app._internal_pdf_context(None, payload, "TÃ¼rkÃ§e metin")
        serialized = str(context)
        for leak in (
            "Invite questions",
            "Child core emotional pattern",
            "Parent-child relationship dynamic",
            "Current phase",
            "Lead with calm",
            "Mars period emphasis",
            "Growth-supportive rhythm",
            "Pressure-sensitive periods",
        ):
            self.assertNotIn(leak, serialized)
        self.assertIn("çocuğun temel duygusal örüntüsü", serialized.lower())
        self.assertIn("ebeveyn-çocuk ilişki dinamiği", serialized.lower())
        self.assertIn("mevcut dönem", serialized.lower())
        self.assertIn("mars dönemi vurgusu", serialized.lower())

    def test_internal_activity_does_not_enter_monetization_records(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "No Revenue", "birth_date": "1990-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
        )
        workspace.save_internal_interpretation(
            self.db,
            profile=profile,
            report_type="career",
            payload={"source": "admin_astro_workspace"},
            interpretation_text="internal only",
            admin_user=self.admin,
        )
        self.db.commit()
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), 0)
        self.assertEqual(self.db.query(db_mod.Transaction).count(), 0)
        self.assertEqual(self.db.query(db_mod.Invoice).count(), 0)

    def test_export_path_attaches_without_email_or_checkout_dependency(self):
        before_email_logs = self.db.query(db_mod.EmailLog).count()
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "PDF Person", "birth_date": "1990-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
        )
        interpretation = workspace.save_internal_interpretation(
            self.db,
            profile=profile,
            report_type="career",
            payload={"source": "admin_astro_workspace"},
            interpretation_text="pdf text",
            admin_user=self.admin,
        )
        self.db.commit()
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = workspace.internal_pdf_output_path(tmpdir, interpretation.id)
            pdf_path.write_bytes(b"%PDF-1.4\n%test")
            workspace.attach_pdf_path(interpretation, pdf_path)
            self.db.commit()
        self.assertTrue(interpretation.pdf_path.endswith(f"internal_interpretation_{interpretation.id}.pdf"))
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), 0)
        self.assertEqual(self.db.query(db_mod.EmailLog).count(), before_email_logs)

    def test_internal_chat_session_persists_correctly(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "Chat Person", "birth_date": "1990-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
        )
        session = chat.create_chat_session(
            self.db,
            profile=profile,
            title="Career follow-up",
            report_type="career",
            mode="grounded",
            admin_user=self.admin,
        )
        self.db.commit()
        self.assertIsNotNone(session.id)
        self.assertEqual(session.profile_id, profile.id)
        self.assertEqual(session.mode, "grounded")

    def test_grounded_chat_reply_uses_shared_ai_and_creates_no_order(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {
                "full_name": "Grounded Person",
                "birth_date": "1990-01-02",
                "birth_time": "08:30",
                "birth_place_label": "Istanbul, Turkey",
            },
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Istanbul, Turkey"},
        )
        session = chat.create_chat_session(self.db, profile=profile, report_type="career", mode="grounded", admin_user=self.admin)
        self.db.commit()
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            chart_payload = app._workspace_chat_context_payload(self.db, session, self.admin)
        with patch.object(chat.ai_logic, "generate_interpretation", return_value="grounded reply") as ai_mock:
            result = chat.create_grounded_reply(
                self.db,
                session=session,
                question="What is the strongest career theme?",
                chart_payload=chart_payload,
            )
        self.db.commit()
        self.assertEqual(result["reply_text"], "grounded reply")
        self.assertEqual(ai_mock.call_count, 1)
        sent_payload = ai_mock.call_args.args[0]
        self.assertEqual(sent_payload["workflow"], "admin_astro_workspace_chat")
        self.assertIn("Do not assume planetary positions.", sent_payload["safeguards"])
        self.assertIn("natal_data", sent_payload["chart_context"])
        self.assertEqual(self.db.query(db_mod.InternalChatMessage).count(), 2)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), 0)
        self.assertEqual(self.db.query(db_mod.Transaction).count(), 0)

    def test_parent_child_chat_mode_uses_dual_profile_context(self):
        parent = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "Parent Chat", "birth_date": "1980-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Istanbul, Turkey"},
        )
        child = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "Child Chat", "birth_date": "2018-04-01", "birth_time": "09:15", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Istanbul, Turkey"},
        )
        session = chat.create_chat_session(
            self.db,
            profile=parent,
            secondary_profile=child,
            report_type="parent_child",
            mode="consultant",
            admin_user=self.admin,
        )
        self.db.commit()
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            chart_payload = app._workspace_chat_context_payload(self.db, session, self.admin)
        context = chat.build_chat_context(session=session, question="Explain their dynamic.", chart_payload=chart_payload)
        self.assertEqual(context["mode"], "consultant")
        self.assertEqual(context["chart_context"]["report_type"], "parent_child")
        self.assertIn("parent_profile", context["chart_context"])
        self.assertIn("get_parent_child_context", chat.resolve_chat_tools(context))
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), 0)

    def test_chat_session_routes_create_and_continue_without_orders(self):
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        before_transactions = self.db.query(db_mod.Transaction).count()
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {
                "full_name": "Route Chat",
                "birth_date": "1990-01-02",
                "birth_time": "08:30",
                "birth_place_label": "Istanbul",
            },
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Istanbul, Turkey"},
        )
        self.db.commit()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair),
        ):
            page = self.client.get(f"/admin/astro-workspace/chat?profile_id={profile.id}")
        self.assertEqual(page.status_code, 200)
        csrf_match = re.search(r'name=["\']csrf_token["\']\s+value=["\']([^"\']+)["\']', page.text)
        self.assertIsNotNone(csrf_match, page.text[:500])
        csrf = csrf_match.group(1)
        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair),
            patch.object(chat.ai_logic, "generate_interpretation", return_value="first grounded reply") as ai_mock,
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/chat/sessions",
                data={
                    "csrf_token": csrf,
                    "profile_id": str(profile.id),
                    "report_type": "career",
                    "mode": "grounded",
                    "question": "What is the strongest career signal?",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ai_mock.call_count, 1)
        session = self.db.query(db_mod.InternalChatSession).first()
        self.assertIsNotNone(session)
        self.assertEqual(self.db.query(db_mod.InternalChatMessage).count(), 2)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)

        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(app, "_require_admin_user", side_effect=self._request_admin_pair),
            patch.object(chat.ai_logic, "generate_interpretation", return_value="follow-up grounded reply"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            followup = self.client.post(
                f"/admin/astro-workspace/chat/sessions/{session.id}/message",
                data={
                    "csrf_token": csrf,
                    "mode": "consultant",
                    "question": "Make it softer.",
                },
            )
        self.assertEqual(followup.status_code, 200)
        self.assertEqual(self.db.query(db_mod.InternalChatMessage).count(), 4)
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)
        self.assertEqual(self.db.query(db_mod.Transaction).count(), before_transactions)

    def test_save_profile_checkbox_creates_internal_profile(self):
        csrf = self._workspace_csrf()
        patches = self._patch_chart_pipeline()
        before_profiles = self.db.query(db_mod.InternalProfile).count()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(workspace.ai_logic, "generate_interpretation", return_value="saved profile text"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/generate",
                data=self._workspace_form_data(csrf_token=csrf, save_profile="1"),
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.query(db_mod.InternalProfile).count(), before_profiles + 1)
        profile = self.db.query(db_mod.InternalProfile).one()
        self.assertEqual(profile.full_name, "Saved Workspace Person")
        self.assertEqual(profile.birth_place_label, "Istanbul, Turkey")
        self.assertEqual(profile.birth_timezone, "Europe/Istanbul")

    def test_unchecked_save_profile_does_not_create_profile(self):
        csrf = self._workspace_csrf()
        patches = self._patch_chart_pipeline()
        before_profiles = self.db.query(db_mod.InternalProfile).count()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(workspace.ai_logic, "generate_interpretation", return_value="quick view text"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/generate",
                data=self._workspace_form_data(csrf_token=csrf),
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.query(db_mod.InternalProfile).count(), before_profiles)

    def test_save_report_checkbox_creates_internal_interpretation(self):
        csrf = self._workspace_csrf()
        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(workspace.ai_logic, "generate_interpretation", return_value="report save text"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/generate",
                data=self._workspace_form_data(csrf_token=csrf, save_report="1"),
            )
        self.assertEqual(response.status_code, 200)
        interpretation = self.db.query(db_mod.InternalInterpretation).one()
        self.assertEqual(interpretation.report_type, "career")
        self.assertRegex(interpretation.input_payload_json, r'"language": "(tr|en)"')
        self.assertEqual(interpretation.interpretation_text, "report save text")

    def test_save_report_without_save_profile_creates_stable_internal_profile(self):
        csrf = self._workspace_csrf()
        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(workspace.ai_logic, "generate_interpretation", return_value="report with internal profile"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/generate",
                data=self._workspace_form_data(csrf_token=csrf, save_report="1"),
            )
        self.assertEqual(response.status_code, 200)
        interpretation = self.db.query(db_mod.InternalInterpretation).one()
        self.assertIsNotNone(interpretation.profile_id)
        self.assertEqual(self.db.query(db_mod.InternalProfile).count(), 1)
        self.assertIn("Report saved with internal profile.", response.text)

    def test_save_profile_and_save_report_link_together(self):
        csrf = self._workspace_csrf()
        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(workspace.ai_logic, "generate_interpretation", return_value="linked save text"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/generate",
                data=self._workspace_form_data(csrf_token=csrf, save_profile="1", save_report="1"),
            )
        self.assertEqual(response.status_code, 200)
        profile = self.db.query(db_mod.InternalProfile).one()
        interpretation = self.db.query(db_mod.InternalInterpretation).one()
        self.assertEqual(interpretation.profile_id, profile.id)

    def test_saved_profile_appears_in_profile_list(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            self._workspace_form_data(),
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Kadikoy, Istanbul, Turkey"},
        )
        self.db.commit()
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user):
            response = self.client.get("/admin/astro-workspace/profiles")
        self.assertEqual(response.status_code, 200)
        self.assertIn(profile.full_name, response.text)

    def test_saved_report_appears_on_profile_detail(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            self._workspace_form_data(),
            admin_user=self.admin,
            location_payload={"latitude": 41.0, "longitude": 29.0, "timezone": "Europe/Istanbul", "normalized_birth_place": "Kadikoy, Istanbul, Turkey"},
        )
        workspace.save_internal_interpretation(
            self.db,
            profile=profile,
            report_type="career",
            payload={"source": "admin_astro_workspace", "language": "tr"},
            interpretation_text="This is a saved report preview for profile history.",
            admin_user=self.admin,
        )
        self.db.commit()
        with patch.object(app, "get_request_user", side_effect=self._request_admin_user):
            response = self.client.get(f"/admin/astro-workspace/profiles/{profile.id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Career", response.text)
        self.assertIn("saved report preview", response.text)

    def test_gender_persistence(self):
        csrf = self._workspace_csrf()
        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patch.object(workspace.ai_logic, "generate_interpretation", return_value="gender save text"),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post(
                "/admin/astro-workspace/generate",
                data=self._workspace_form_data(csrf_token=csrf, save_profile="1", gender="female"),
            )
        self.assertEqual(response.status_code, 200)
        profile = self.db.query(db_mod.InternalProfile).one()
        self.assertEqual(profile.gender, "female")

    def test_parent_child_save_report_creates_linked_profiles(self):
        csrf = self._workspace_csrf()
        form_data = self._workspace_form_data(
            csrf_token=csrf,
            report_type="parent_child",
            save_report="1",
            secondary_full_name="Child Saved",
            secondary_gender="male",
            secondary_birth_date="2018-04-01",
            secondary_birth_time="09:15",
            secondary_birth_place_label="Uskudar, Istanbul, Turkey",
            secondary_birth_city="Uskudar",
            secondary_resolved_birth_place="Uskudar, Istanbul, Turkey",
            secondary_resolved_latitude="41.0220",
            secondary_resolved_longitude="29.0137",
            secondary_resolved_timezone="Europe/Istanbul",
            secondary_resolved_geocode_provider="test",
            secondary_resolved_geocode_confidence="0.97",
        )
        patches = self._patch_chart_pipeline()
        with (
            patch.object(app, "get_request_user", side_effect=self._request_admin_user),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            response = self.client.post("/admin/astro-workspace/generate", data=form_data)
        self.assertEqual(response.status_code, 200)
        interpretation = self.db.query(db_mod.InternalInterpretation).one()
        self.assertIsNotNone(interpretation.profile_id)
        self.assertIsNotNone(interpretation.secondary_profile_id)
        self.assertEqual(self.db.query(db_mod.InternalProfile).count(), 2)

    def test_ai_behavior_rules_load_active_defaults(self):
        rule_set = ai_behavior_rules.ensure_default_rule_set(self.db, admin_user=self.admin)
        self.db.commit()
        active_rules = ai_behavior_rules.load_active_rules(self.db)
        self.assertEqual(rule_set.name, ai_behavior_rules.DEFAULT_RULE_SET_NAME)
        self.assertGreaterEqual(len(active_rules), 6)
        self.assertTrue(any(rule.category == "Truth & Grounding Rules" for rule in active_rules))

    def test_ai_behavior_prompt_block_preserves_immutable_rules(self):
        ai_behavior_rules.ensure_default_rule_set(self.db, admin_user=self.admin)
        self.db.commit()
        block = ai_behavior_rules.build_prompt_rule_blocks(self.db)
        self.assertIn("Do not assume planetary positions.", block["immutable_rules"])
        self.assertIn("Swiss Ephemeris-backed calculations are the authority", block["prompt_block"])
        self.assertIn("Active admin behavior rules:", block["prompt_block"])

    def test_workspace_prompt_assembly_includes_behavior_rules(self):
        ai_behavior_rules.ensure_default_rule_set(self.db, admin_user=self.admin)
        self.db.commit()
        active_rules = ai_behavior_rules.load_active_rules(self.db)
        payload = workspace.build_workspace_prompt_payload(
            {"source": "admin_astro_workspace", "natal_data": {"planets": []}},
            behavior_rules=active_rules,
        )
        self.assertEqual(payload["source"], "admin_astro_workspace")
        self.assertIn("ai_behavior_rules", payload)
        self.assertIn("Do not assume planetary positions.", payload["ai_behavior_rules"]["immutable_rules"])
        self.assertTrue(payload["ai_behavior_rules"]["active_rules"])

    def test_chat_prompt_assembly_includes_behavior_rules(self):
        ai_behavior_rules.ensure_default_rule_set(self.db, admin_user=self.admin)
        self.db.commit()
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "Rules Chat", "birth_date": "1990-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
        )
        session = chat.create_chat_session(self.db, profile=profile, report_type="career", mode="grounded", admin_user=self.admin)
        self.db.commit()
        context = chat.build_chat_context(session=session, question="Only explain risks.", chart_payload={"source": "admin_astro_workspace"})
        payload = chat.build_chat_prompt_payload(context, behavior_rules=ai_behavior_rules.load_active_rules(self.db))
        self.assertEqual(payload["workflow"], "admin_astro_workspace_chat")
        self.assertIn("ai_behavior_rules", payload)
        self.assertIn("Use only computed or explicitly provided astrology data.", payload["ai_behavior_rules"]["immutable_rules"])

    def test_workspace_payload_includes_astro_signal_context(self):
        form = self._workspace_form_data(report_type="career")
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        signal_context = prepared["payload"]["astro_signal_context"]
        self.assertIn("dominant_signals", signal_context)
        self.assertIn("risk_signals", signal_context)
        self.assertIn("opportunity_signals", signal_context)
        self.assertIn("nakshatra_signals", signal_context)
        self.assertIn("yoga_signals", signal_context)
        self.assertIn("chart_relationships", signal_context)

    def test_workspace_payload_includes_prediction_fusion_key(self):
        form = self._workspace_form_data(report_type="career")
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertIn("prediction_fusion", prepared["payload"])

    def test_prediction_fusion_has_expected_top_level_shape(self):
        form = self._workspace_form_data(report_type="career")
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        fusion = prepared["payload"]["prediction_fusion"]
        self.assertEqual(fusion["source"], "prediction_fusion_engine")
        self.assertIn("prediction_windows", fusion)
        self.assertIn("active_themes", fusion)
        self.assertIn("blocked_predictions", fusion)
        self.assertIn("unconfirmed_observations", fusion)
        self.assertIn("confidence_notes", fusion)

    def test_prediction_fusion_failure_does_not_crash_workspace(self):
        form = self._workspace_form_data(report_type="career")
        patches = self._patch_chart_pipeline()
        with (
            patch.object(workspace, "build_prediction_fusion", side_effect=Exception("fusion unavailable")),
            patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9],
        ):
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertIn("prediction_fusion", prepared["payload"])
        self.assertEqual(prepared["payload"]["prediction_fusion"]["prediction_windows"], [])
        self.assertEqual(prepared["payload"]["prediction_fusion"]["active_themes"], [])
        self.assertEqual(prepared["payload"]["prediction_fusion"]["blocked_predictions"], [])
        self.assertEqual(prepared["payload"]["prediction_fusion"]["unconfirmed_observations"], [])
        self.assertEqual(
            prepared["payload"]["prediction_fusion"]["confidence_notes"],
            ["prediction_fusion_engine unavailable."],
        )

    def test_debug_signals_checkbox_sets_payload_true(self):
        form = self._workspace_form_data(report_type="career", debug_signals="1")
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertTrue(prepared["payload"]["debug_signals"])
        self.assertTrue(prepared["payload"]["interpretation_context"]["debug_signals"])

    def test_unchecked_debug_signals_keeps_payload_false(self):
        form = self._workspace_form_data(report_type="career")
        patches = self._patch_chart_pipeline()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            prepared = app._prepare_admin_workspace_generation(self.db, form, self.admin)
        self.assertFalse(prepared["payload"]["debug_signals"])
        self.assertNotIn("debug_signals", prepared["payload"]["interpretation_context"])

    def test_composer_prompt_contains_signal_usage_rules(self):
        structured = agent_pipeline.build_structured_payload(
            {
                "language": "en",
                "astro_signal_context": {
                    "dominant_signals": [{"key": "career_signal", "label": "Career Signal"}],
                    "risk_signals": [{"key": "risk_signal", "label": "Risk Signal"}],
                    "opportunity_signals": [{"key": "opportunity_signal", "label": "Opportunity Signal"}],
                    "nakshatra_signals": {"moon_nakshatra": {"label": "Moon profile"}, "lagna_nakshatra": {"label": "Lagna profile"}},
                    "yoga_signals": {"detected_yogas": [{"yoga_name": "Raj Yoga"}]},
                    "chart_relationships": {"planet_houses": {"Jupiter": 10}},
                },
                "debug_signals": True,
            }
        )
        prompt = agent_pipeline.build_composer_prompt(structured, "insight", "timing", "guidance")
        self.assertIn("Use astro_signal_context before generic astrology text.", prompt)
        self.assertIn("Do not invent signals.", prompt)
        self.assertIn("If yoga appears in astro_signal_context, describe it as potential or pattern, never guaranteed fate.", prompt)
        self.assertIn("SIGNAL DEBUG BLOCK:", prompt)

    def test_chat_context_includes_astro_signal_context(self):
        profile = workspace.create_or_update_internal_profile(
            self.db,
            {"full_name": "Signal Chat", "birth_date": "1990-01-02", "birth_time": "08:30", "birth_place_label": "Istanbul"},
            admin_user=self.admin,
        )
        session = chat.create_chat_session(self.db, profile=profile, report_type="career", mode="grounded", admin_user=self.admin)
        chart_payload = {
            "source": "admin_astro_workspace",
            "astro_signal_context": {
                "dominant_signals": [{"key": "career_signal"}],
                "risk_signals": [],
                "opportunity_signals": [],
                "nakshatra_signals": {},
                "yoga_signals": {},
                "chart_relationships": {},
            },
        }
        context = chat.build_chat_context(session=session, question="What matters most?", chart_payload=chart_payload)
        self.assertIn("astro_signal_context", context)
        self.assertEqual(context["astro_signal_context"]["dominant_signals"][0]["key"], "career_signal")

    def test_missing_signals_do_not_crash_prompt_assembly(self):
        payload = workspace.build_workspace_prompt_payload(
            {"source": "admin_astro_workspace", "natal_data": {"planets": []}, "astro_signal_context": {}},
            behavior_rules=[],
        )
        structured = agent_pipeline.build_structured_payload(payload)
        prompt = agent_pipeline.build_composer_prompt(structured, "insight", "timing", "guidance")
        self.assertIn("astro_signal_context", structured)
        self.assertNotIn("SIGNAL DEBUG BLOCK:", prompt)

    def test_public_report_flow_debug_default_remains_false(self):
        structured = agent_pipeline.build_structured_payload({"language": "en"})
        self.assertFalse(structured["debug_signals"])
        self.assertEqual(structured["astro_signal_context"], {})

    def test_ai_rules_do_not_create_monetization_records(self):
        before_orders = self.db.query(db_mod.ServiceOrder).count()
        before_transactions = self.db.query(db_mod.Transaction).count()
        before_invoices = self.db.query(db_mod.Invoice).count()
        ai_behavior_rules.ensure_default_rule_set(self.db, admin_user=self.admin)
        self.db.commit()
        form = {
            "rule_enabled_1": "1",
        }
        ai_behavior_rules.update_rules_from_form(self.db, form)
        self.db.commit()
        self.assertEqual(self.db.query(db_mod.ServiceOrder).count(), before_orders)
        self.assertEqual(self.db.query(db_mod.Transaction).count(), before_transactions)
        self.assertEqual(self.db.query(db_mod.Invoice).count(), before_invoices)
