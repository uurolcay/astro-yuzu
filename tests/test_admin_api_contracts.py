import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
from tests.admin_api_client import (
    assert_admin_error_envelope,
    assert_admin_success_envelope,
    assert_has_data_keys,
    fetch_admin_docs_json,
    fetch_admin_export_metadata_json,
    fetch_admin_health_json,
    fetch_admin_insights_json,
    fetch_admin_revenue_json,
    fetch_admin_segments_json,
    fetch_admin_summary_json,
)


class AdminApiContractTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.client = TestClient(app.app)
        self.admin_user = self._create_user("admin@example.com", is_admin=True)
        self.member_user = self._create_user("member@example.com", is_admin=False)

    def tearDown(self):
        self.db.rollback()
        self.db.query(db_mod.RecommendationFollowup).delete()
        self.db.query(db_mod.RecommendationFeedback).delete()
        self.db.query(db_mod.InterpretationFeedback).delete()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.commit()
        self.db.close()

    def test_admin_api_requires_auth_with_standard_error_envelope(self):
        payload = fetch_admin_summary_json(self.client, expected_status=401)
        assert_admin_error_envelope(self, payload, "/api/admin/summary", error_code="authentication_required")

    def test_admin_api_rejects_non_admin_with_standard_error_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.member_user):
            payload = fetch_admin_summary_json(self.client, expected_status=403)
        assert_admin_error_envelope(self, payload, "/api/admin/summary", error_code="admin_access_denied")

    def test_admin_summary_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_summary_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/summary")
        assert_has_data_keys(self, payload, ["kpis", "scorecards", "trends", "watchlist", "priority_focus", "weekly_summary", "links"])

    def test_admin_revenue_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_revenue_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/revenue")
        assert_has_data_keys(self, payload, ["core_metrics", "conversion_metrics", "usage_metrics", "revenue_proxy", "funnel", "engagement", "billing_signals", "top_users"])

    def test_admin_insights_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_insights_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/insights")
        assert_has_data_keys(self, payload, ["headline_kpis", "insights", "upsell_candidates", "churn_watchlist", "quick_metrics"])

    def test_admin_segments_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_segments_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/segments")
        assert_has_data_keys(self, payload, ["filters", "limit_behavior", "summary", "segments"])

    def test_admin_export_metadata_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_export_metadata_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/segments/export-metadata")
        assert_has_data_keys(self, payload, ["default_view", "views", "filters_supported", "filter_notes", "limit_behavior"])

    def test_admin_health_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_health_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/health")
        assert_has_data_keys(self, payload, ["signals"])

    def test_admin_docs_endpoint_returns_standard_success_envelope(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_docs_json(self.client)
        assert_admin_success_envelope(self, payload, "/api/admin/docs")
        assert_has_data_keys(self, payload, ["auth", "schema_version_meaning", "response_envelope", "error_format", "endpoints"])

    def test_docs_lists_all_current_admin_endpoints(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_docs_json(self.client)
        endpoints = payload["data"]["endpoints"]
        for path in (
            "/api/admin/docs",
            "/api/admin/summary",
            "/api/admin/revenue",
            "/api/admin/insights",
            "/api/admin/segments",
            "/api/admin/segments/export-metadata",
            "/api/admin/health",
        ):
            self.assertIn(path, endpoints)
        self.assertTrue(payload["data"]["auth"]["required"])
        self.assertTrue(payload["data"]["auth"]["admin_only"])
        self.assertIn("error", payload["data"]["error_format"])
        self.assertIn("filters", endpoints["/api/admin/segments"])

    def test_docs_describes_segments_and_export_metadata_filters(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_docs_json(self.client)
        endpoints = payload["data"]["endpoints"]
        self.assertIn("limit", endpoints["/api/admin/segments"]["filters"])
        self.assertEqual(endpoints["/api/admin/segments/export-metadata"]["filters"], [])
        self.assertIn("response_keys", endpoints["/api/admin/segments/export-metadata"])

    def test_admin_segments_echoes_filters_and_limit_behavior(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_segments_json(self.client, segment="NEW_SIGNUPS", limit=2)
        assert_admin_success_envelope(self, payload, "/api/admin/segments")
        self.assertEqual(payload["data"]["filters"]["segment"], "NEW_SIGNUPS")
        self.assertEqual(payload["data"]["filters"]["limit"], 2)
        self.assertEqual(payload["data"]["limit_behavior"], "per_segment")
        self.assertIn("summary", payload["data"])

    def test_admin_segments_objects_use_predictable_shape(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_segments_json(self.client, limit=1)
        segments = payload["data"]["segments"]
        self.assertIsInstance(segments, list)
        if segments:
            segment = segments[0]
            for key in ("segment_name", "meta", "campaign_brief", "count", "rows"):
                self.assertIn(key, segment)
            self.assertIsInstance(segment["rows"], list)
            if segment["rows"]:
                self.assertIsInstance(segment["rows"][0], dict)

    def test_admin_segments_rejects_invalid_filter_with_json_400(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_segments_json(self.client, expected_status=400, segment="NOT_A_REAL_SEGMENT")
        assert_admin_error_envelope(self, payload, "/api/admin/segments", error_code="invalid_filters")

    def test_admin_segments_rejects_invalid_limit_with_json_400(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_segments_json(self.client, expected_status=400, limit="oops")
        assert_admin_error_envelope(self, payload, "/api/admin/segments", error_code="invalid_limit")

    def test_admin_segments_accepts_valid_limit_and_keeps_rows_serializable(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_segments_json(self.client, limit=1)
        assert_admin_success_envelope(self, payload, "/api/admin/segments")
        self.assertIn(payload["data"]["limit_behavior"], {"per_segment", "unbounded"})
        for segment in payload["data"]["segments"]:
            self.assertIsInstance(segment["rows"], list)
            for row in segment["rows"]:
                self.assertIsInstance(row, dict)

    def test_export_metadata_contract_is_fully_documented(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_export_metadata_json(self.client)
        data = payload["data"]
        self.assertEqual(data["default_view"], "crm")
        self.assertIn("views", data)
        self.assertIn("filters_supported", data)
        self.assertIn("filter_notes", data)
        self.assertIn("limit_behavior", data)
        for view_name in ("crm", "email", "support", "minimal"):
            self.assertIn(view_name, data["views"])
            self.assertIn("columns", data["views"][view_name])
            self.assertIsInstance(data["views"][view_name]["columns"], list)

    def test_admin_health_contract_contains_normalized_signal_aliases(self):
        with patch.object(app, "get_request_user", return_value=self.admin_user):
            payload = fetch_admin_health_json(self.client)
        signals = payload["data"]["signals"]
        for key in (
            "email_failures",
            "email_failure_count",
            "payment_failed_signals",
            "payment_failed_signal_count",
            "inactive_paid_users",
            "inactive_paid_users_count",
        ):
            self.assertIn(key, signals)

    def test_admin_builder_failure_returns_standard_error_envelope(self):
        with (
            patch.object(app, "get_request_user", return_value=self.admin_user),
            patch.object(app, "build_admin_summary_api_payload", side_effect=RuntimeError("boom")),
        ):
            payload = fetch_admin_summary_json(self.client, expected_status=500)
        assert_admin_error_envelope(self, payload, "/api/admin/summary", error_code="summary_build_failed")
        self.assertEqual(payload["schema_version"], app.get_admin_api_schema_version())

    def _create_user(self, email, is_admin=False):
        user = db_mod.AppUser(
            email=email,
            password_hash="hash",
            name="Admin User" if is_admin else "Member",
            plan_code="premium",
            is_admin=is_admin,
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user
