import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

import app
import database as db_mod
import utils
from services import geocoding


class MockLocation:
    def __init__(self, address, latitude, longitude, raw=None):
        self.address = address
        self.latitude = latitude
        self.longitude = longitude
        self.raw = raw or {
            "display_name": address,
            "address": {},
            "type": "city",
        }


class BirthLocationPipelineTests(unittest.TestCase):
    def setUp(self):
        self.cache_dir = tempfile.TemporaryDirectory()
        self.cache_path = Path(self.cache_dir.name) / "geocode_cache.json"
        geocoding._MEMORY_CACHE.clear()
        self.cache_patch = patch.object(geocoding, "GEOCODE_CACHE_FILE", self.cache_path)
        self.cache_patch.start()

    def tearDown(self):
        self.cache_patch.stop()
        geocoding._MEMORY_CACHE.clear()
        self.cache_dir.cleanup()

    def test_district_level_input_resolves_correctly(self):
        district_result = MockLocation(
            "Besiktas, Istanbul, Marmara Region, Turkey",
            41.0422,
            29.0083,
            raw={
                "display_name": "Besiktas, Istanbul, Marmara Region, Turkey",
                "address": {"suburb": "Besiktas", "city": "Istanbul", "country": "Turkey"},
                "type": "suburb",
            },
        )
        with patch.object(geocoding, "_PROVIDER") as provider, patch.object(geocoding, "_resolve_timezone", return_value="Europe/Istanbul"):
            provider.search.return_value = [district_result]
            result = geocoding.resolve_birth_place("Besiktas, Istanbul, Turkey")

        self.assertEqual(result["normalized_place"], "Besiktas, Istanbul, Marmara Region, Turkey")
        self.assertEqual(result["timezone"], "Europe/Istanbul")
        self.assertAlmostEqual(result["latitude"], 41.0422)
        self.assertAlmostEqual(result["longitude"], 29.0083)

    def test_generic_city_input_resolves_and_stores_distinctly(self):
        city_result = MockLocation(
            "Istanbul, Marmara Region, Turkey",
            41.0082,
            28.9784,
            raw={
                "display_name": "Istanbul, Marmara Region, Turkey",
                "address": {"city": "Istanbul", "country": "Turkey"},
                "type": "city",
            },
        )
        with patch.object(geocoding, "_PROVIDER") as provider, patch.object(geocoding, "_resolve_timezone", return_value="Europe/Istanbul"):
            provider.search.return_value = [city_result]
            result = geocoding.resolve_birth_place("Istanbul, Turkey")

        self.assertEqual(result["raw_input"], "Istanbul, Turkey")
        self.assertEqual(result["normalized_place"], "Istanbul, Marmara Region, Turkey")

    def test_same_raw_text_uses_cache_on_repeated_resolution(self):
        city_result = MockLocation(
            "Istanbul, Marmara Region, Turkey",
            41.0082,
            28.9784,
            raw={
                "display_name": "Istanbul, Marmara Region, Turkey",
                "address": {"city": "Istanbul", "country": "Turkey"},
                "type": "city",
            },
        )
        with patch.object(geocoding, "_PROVIDER") as provider, patch.object(geocoding, "_resolve_timezone", return_value="Europe/Istanbul"):
            provider.search.return_value = [city_result]
            geocoding.resolve_birth_place("Istanbul, Turkey")
            geocoding.resolve_birth_place("Istanbul, Turkey")

        self.assertEqual(provider.search.call_count, 1)

    def test_birthplace_suggestion_endpoint_returns_structured_results(self):
        client = TestClient(app.app)
        district_result = MockLocation(
            "Besiktas, Istanbul, Marmara Region, Turkey",
            41.0422,
            29.0083,
            raw={
                "display_name": "Besiktas, Istanbul, Marmara Region, Turkey",
                "address": {"suburb": "Besiktas", "city": "Istanbul", "country": "Turkey"},
                "type": "suburb",
            },
        )
        with patch.object(geocoding, "_PROVIDER") as provider, patch.object(geocoding, "_resolve_timezone", return_value="Europe/Istanbul"):
            provider.search.return_value = [district_result]
            response = client.get("/api/v1/birthplace-suggestions", params={"q": "besik"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        self.assertEqual(payload[0]["display_name"], "Besiktas, Istanbul, Turkey")
        self.assertEqual(payload[0]["timezone"], "Europe/Istanbul")
        self.assertIn("latitude", payload[0])
        self.assertIn("longitude", payload[0])

    def test_very_short_suggestion_query_returns_empty_safely(self):
        client = TestClient(app.app)
        response = client.get("/api/v1/birthplace-suggestions", params={"q": "b"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])

    def test_frontend_template_includes_birth_place_autocomplete_hook(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\calculator.html").read_text(encoding="utf-8")
        self.assertIn('id="birth_city"', template)
        self.assertIn('id="birthplace-suggestions"', template)
        self.assertIn('name="resolved_birth_place"', template)
        self.assertIn("birthplaceSuggestionSelected", template)
        self.assertIn("autocomplete_submit_warning", template)
        self.assertIn('aria-expanded="false"', template)
        self.assertIn('aria-controls="birthplace-suggestions"', template)

    def test_template_includes_keyboard_navigation_hooks(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\calculator.html").read_text(encoding="utf-8")
        self.assertIn('event.key === "ArrowDown"', template)
        self.assertIn('event.key === "ArrowUp"', template)
        self.assertIn('event.key === "Enter"', template)
        self.assertIn('event.key === "Escape"', template)
        self.assertIn("setHighlightedIndex", template)

    def test_template_includes_escape_and_outside_close_behavior(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\calculator.html").read_text(encoding="utf-8")
        self.assertIn('document.addEventListener("pointerdown"', template)
        self.assertIn("closeSuggestions()", template)

    def test_template_includes_mobile_friendly_selection_path(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\calculator.html").read_text(encoding="utf-8")
        self.assertIn('button.addEventListener("pointerdown"', template)
        self.assertIn("button._payload = item", template)
        self.assertIn('observeBirthplaceEvent("suggestion_selected"', template)

    def test_astrology_calculation_path_consumes_lat_lon_timezone(self):
        with patch.object(utils, "resolve_birth_location", return_value={
            "raw_input": "Besiktas, Istanbul, Turkey",
            "normalized_place": "Besiktas, Istanbul, Turkey",
            "latitude": 41.0422,
            "longitude": 29.0083,
            "timezone": "Europe/Istanbul",
            "provider": "mock",
            "confidence": 0.9,
        }):
            birth_context = app._build_birth_context("2024-01-01T10:30", "Besiktas, Istanbul", "Turkey")

        self.assertEqual(birth_context["timezone"], "Europe/Istanbul")
        self.assertEqual(birth_context["latitude"], 41.0422)
        self.assertEqual(birth_context["longitude"], 29.0083)
        self.assertIsNotNone(birth_context["utc_datetime"])

    def test_form_submission_uses_selected_resolved_location_if_provided(self):
        resolved = app._resolved_birth_location_payload_from_form(
            birth_city="Besiktas, Istanbul, Turkey",
            country="Turkey",
            resolved_birth_place="Besiktas, Istanbul, Marmara Region, Turkey",
            resolved_latitude="41.0422",
            resolved_longitude="29.0083",
            resolved_timezone="Europe/Istanbul",
            geocode_provider="nominatim",
            geocode_confidence="0.9",
        )
        self.assertEqual(resolved["normalized_birth_place"], "Besiktas, Istanbul, Marmara Region, Turkey")
        self.assertEqual(resolved["location_source"], "suggestion_selection")
        self.assertAlmostEqual(resolved["latitude"], 41.0422)

    def test_backend_does_not_trust_stale_resolved_fields(self):
        with patch.object(app, "_resolve_birth_location_payload", return_value={
            "raw_birth_place_input": "Kadikoy, Istanbul, Turkey",
            "normalized_birth_place": "Kadikoy, Istanbul, Marmara Region, Turkey",
            "latitude": 40.9909,
            "longitude": 29.0280,
            "timezone": "Europe/Istanbul",
            "geocode_provider": "mock",
            "geocode_confidence": 0.86,
            "location_source": "provider:mock",
            "geocode_cache_hit": False,
        }) as resolver:
            resolved = app._resolved_birth_location_payload_from_form(
                birth_city="Kadikoy, Istanbul, Turkey",
                country="Turkey",
                resolved_birth_place="Besiktas, Istanbul, Marmara Region, Turkey",
                resolved_latitude="41.0422",
                resolved_longitude="29.0083",
                resolved_timezone="Europe/Istanbul",
                geocode_provider="nominatim",
                geocode_confidence="0.9",
            )
        resolver.assert_called_once()
        self.assertEqual(resolved["normalized_birth_place"], "Kadikoy, Istanbul, Marmara Region, Turkey")
        self.assertEqual(resolved["location_source"], "provider:mock")

    def test_manual_typed_fallback_still_works(self):
        with patch.object(app, "_resolve_birth_location_payload", return_value={
            "raw_birth_place_input": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Marmara Region, Turkey",
            "latitude": 41.0082,
            "longitude": 28.9784,
            "timezone": "Europe/Istanbul",
            "geocode_provider": "mock",
            "geocode_confidence": 0.8,
            "location_source": "provider:mock",
            "geocode_cache_hit": False,
        }) as resolver:
            resolved = app._resolved_birth_location_payload_from_form(
                birth_city="Istanbul",
                country="Turkey",
            )
        resolver.assert_called_once()
        self.assertEqual(resolved["normalized_birth_place"], "Istanbul, Marmara Region, Turkey")

    def test_result_template_shows_resolved_birth_place_transparently(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("resolved_birthplace_label", template)
        self.assertIn("normalized_birth_place", template)

    def test_query_change_reset_hook_and_stale_field_clearing_remain_present(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\calculator.html").read_text(encoding="utf-8")
        self.assertIn("highlightedIndex = -1", template)
        self.assertIn("clearResolvedFields()", template)

    def test_homepage_template_uses_focus_astrology_branding(self):
        base_template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\base.html").read_text(encoding="utf-8")
        index_template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\index.html").read_text(encoding="utf-8")
        self.assertIn('aria-label="Focus Astrology"', base_template)
        self.assertIn('src="/static/focus-logo.png"', base_template)
        self.assertNotIn("brand-subtitle", base_template)
        self.assertIn('t("index.page_title")', index_template)
        self.assertNotIn("Jyotish | Vedik Astroloji Platformu", index_template)

    def test_homepage_reports_and_how_links_point_to_valid_targets(self):
        base_template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\base.html").read_text(encoding="utf-8")
        index_template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\index.html").read_text(encoding="utf-8")
        self.assertIn('href="/reports"', base_template)
        self.assertIn('href="/articles"', base_template)
        self.assertIn('href="/calculator"', base_template)
        self.assertIn('href="/#about-focus"', base_template)
        self.assertNotIn('id="full-reading"', index_template)
        self.assertNotIn('id="calculator-entry"', index_template)
        self.assertIn('id="how-it-works"', index_template)

    def test_turkish_birthplace_helper_and_gender_select_markup_are_present(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\calculator.html").read_text(encoding="utf-8")
        self.assertIn('translation_namespace("index.birthplace")', template)
        self.assertIn('class="form-select"', template)
        self.assertIn('data-testid="gender-select"', template)
        self.assertIn(".form-select option", template)

    def test_logo_asset_reference_is_present_in_header(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\base.html").read_text(encoding="utf-8")
        self.assertIn('/static/focus-logo.png', template)
        self.assertIn('class="auth-links"', template)
        self.assertNotIn('class="btn btn-primary">{{ t("common.nav_reports") }}</a>', template)

    def test_homepage_render_contains_focus_astrology_and_turkish_helper_text(self):
        client = TestClient(app.app)
        response = client.get("/", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('/static/focus-logo.png', response.text)
        self.assertIn("Yorumunu Başlat", response.text)

    def test_calculator_render_contains_turkish_helper_text(self):
        client = TestClient(app.app)
        response = client.get("/calculator", headers={"accept-language": "tr"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("En doğru sonuç için ilçe / şehir / ülke şeklinde yazıp listeden doğru yeri seçin.", response.text)

    def test_homepage_keeps_primary_cta_and_moves_tools_to_dedicated_pages(self):
        client = TestClient(app.app)
        response = client.get("/")
        self.assertIn('href="/calculator"', response.text)
        self.assertIn('href="/articles"', response.text)
        self.assertIn("Exact birth chart reading", response.text)
        self.assertNotIn("Insights &amp; Articles", response.text)
        self.assertNotIn("Parent-Child Guidance Report", response.text)
        self.assertNotIn("Unlock Your Full Child Report", response.text)

    def test_homepage_contains_core_themes_and_focus_preview(self):
        client = TestClient(app.app)
        response = client.get("/")
        self.assertIn("Understand Your Core Patterns", response.text)
        self.assertIn("What to Focus on Now", response.text)
        self.assertIn("Growth through strategic restraint", response.text)
        self.assertIn("Prioritize deliberate career positioning", response.text)
        self.assertNotIn("Choose the reading that matches your question", response.text)

    def test_calculator_route_renders_successfully(self):
        client = TestClient(app.app)
        response = client.get("/calculator")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Start with your birth details", response.text)
        self.assertIn("birthplace-suggestions", response.text)
        self.assertIn("gender-select", response.text)

    def test_articles_route_renders_successfully(self):
        client = TestClient(app.app)
        response = client.get("/articles")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Insights &amp; Articles", response.text)

    def test_ambiguous_or_failed_geocoding_returns_user_facing_validation_failure(self):
        client = TestClient(app.app)
        with patch.object(app, "_build_birth_context", side_effect=geocoding.BirthPlaceResolutionError("ambiguous", code="ambiguous_place")):
            response = client.post(
                "/calculate",
                data={
                    "full_name": "Test User",
                    "birth_date": "2024-01-01",
                    "birth_time": "10:30",
                    "birth_city": "Istanbul",
                    "country": "Turkey",
                    "report_type": "premium",
                },
            )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Dogum yeriniz fazla genel veya belirsiz", response.text)

    def test_legacy_record_with_place_string_only_uses_fallback_resolution(self):
        with patch.object(app, "_build_birth_context", return_value={
            "raw_birth_place_input": "Istanbul",
            "normalized_birth_place": "Istanbul, Turkey",
            "latitude": 41.0082,
            "longitude": 28.9784,
            "timezone": "Europe/Istanbul",
            "geocode_provider": "mock",
            "geocode_confidence": 0.8,
            "local_datetime": None,
            "utc_datetime": None,
        }) as helper:
            result = app._build_birth_context_from_saved_fields(
                "2024-01-01T10:30",
                latitude=None,
                longitude=None,
                timezone=None,
                fallback_place_text="Istanbul",
            )

        helper.assert_called_once()
        self.assertEqual(result["normalized_birth_place"], "Istanbul, Turkey")


class BirthplaceAdminAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.query(db_mod.BirthplaceEventLog).delete()
        self.db.commit()

    def tearDown(self):
        self.db.query(db_mod.GeneratedReport).delete()
        self.db.query(db_mod.UserProfile).delete()
        self.db.query(db_mod.AppUser).delete()
        self.db.query(db_mod.BirthplaceEventLog).delete()
        self.db.commit()
        self.db.close()

    def _admin_user(self):
        return SimpleNamespace(
            id=1,
            email="admin@example.com",
            name="Admin",
            plan_code="elite",
            is_admin=True,
            is_active=True,
        )

    def _add_user(self, email, is_admin=False):
        user = db_mod.AppUser(
            email=email,
            password_hash="test-hash",
            name=email.split("@")[0].title(),
            plan_code="elite",
            is_admin=is_admin,
            is_active=True,
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def _add_report(self, user, access_state, *, pdf_ready=False, is_paid=False):
        report = db_mod.GeneratedReport(
            user_id=user.id,
            report_type="premium",
            title=f"{access_state.title()} Reading",
            full_name="Test User",
            birth_date="1990-01-01",
            birth_time="10:30",
            birth_city="Istanbul",
            birth_country="Turkey",
            result_payload_json="{}",
            access_state=access_state,
            is_paid=is_paid,
            pdf_ready=pdf_ready,
            unlocked_at=datetime.utcnow() if access_state in {"unlocked", "purchased", "delivered"} else None,
            delivered_at=datetime.utcnow() if access_state == "delivered" else None,
        )
        self.db.add(report)
        self.db.commit()
        self.db.refresh(report)
        return report

    def _add_event(self, event_name, **kwargs):
        event = db_mod.BirthplaceEventLog(
            event_name=event_name,
            provider=kwargs.get("provider"),
            outcome=kwargs.get("outcome"),
            location_source=kwargs.get("location_source"),
            confidence=kwargs.get("confidence"),
            suggestion_count=kwargs.get("suggestion_count"),
            created_at=kwargs.get("created_at", datetime.utcnow()),
        )
        self.db.add(event)
        self.db.commit()
        return event

    def test_birthplace_observability_summary_returns_structured_aggregates(self):
        self._add_event("suggestion_results_returned", provider="nominatim", outcome="success", location_source="suggestion_lookup", suggestion_count=4)
        self._add_event("suggestion_selected", provider="nominatim", outcome="selected", location_source="suggestion_selection", confidence=0.91)
        self._add_event("submit_with_selected_suggestion", outcome="submitted", location_source="suggestion_selection")
        self._add_event("resolved_birthplace_success", provider="nominatim", outcome="success", location_source="suggestion_selection", confidence=0.91)

        summary = app.get_birthplace_observability_summary(self.db)

        self.assertEqual(summary["metrics"]["total_suggestion_queries"], 1)
        self.assertEqual(summary["metrics"]["suggestion_selected_count"], 1)
        self.assertEqual(summary["metrics"]["submit_with_selected_suggestion"], 1)
        self.assertEqual(summary["metrics"]["resolved_birthplace_success_count"], 1)
        self.assertEqual(summary["confidence_buckets"]["high"], 2)

    def test_birthplace_observability_summary_computes_safe_derived_metrics(self):
        self._add_event("submit_with_selected_suggestion", outcome="submitted", location_source="suggestion_selection")
        self._add_event("submit_without_selected_suggestion", outcome="submitted", location_source="manual_input")
        self._add_event("resolved_birthplace_success", outcome="success", location_source="manual_input")
        self._add_event("resolved_birthplace_failure", outcome="ambiguous_place", location_source="manual_input")

        summary = app.get_birthplace_observability_summary(self.db)

        self.assertEqual(summary["metrics"]["suggestion_selection_rate"], 0.5)
        self.assertEqual(summary["metrics"]["resolution_success_rate"], 0.5)
        self.assertEqual(summary["metrics"]["fallback_rate"], 0.5)

    def test_birthplace_observability_summary_handles_empty_data(self):
        summary = app.get_birthplace_observability_summary(self.db)

        self.assertEqual(summary["metrics"]["total_suggestion_queries"], 0)
        self.assertIsNone(summary["metrics"]["average_suggestion_count"])
        self.assertIsNone(summary["metrics"]["suggestion_selection_rate"])
        self.assertEqual(summary["recent_events"], [])

    def test_birthplace_observability_time_filter_is_safe(self):
        self._add_event("resolved_birthplace_success", created_at=datetime.utcnow() - timedelta(days=2))
        self._add_event("resolved_birthplace_success", created_at=datetime.utcnow() - timedelta(days=12))

        summary_7d = app.get_birthplace_observability_summary(self.db, time_window="7d")
        summary_invalid = app.get_birthplace_observability_summary(self.db, time_window="weird")

        self.assertEqual(summary_7d["time_window"], "7d")
        self.assertEqual(summary_7d["metrics"]["resolved_birthplace_success_count"], 1)
        self.assertEqual(summary_invalid["time_window"], "all")
        self.assertEqual(summary_invalid["metrics"]["resolved_birthplace_success_count"], 2)

    def test_admin_birthplace_analytics_requires_authentication(self):
        client = TestClient(app.app)
        response = client.get("/admin/birthplace-analytics", follow_redirects=False)

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/login")

    def test_admin_birthplace_analytics_denies_non_admin_users(self):
        client = TestClient(app.app)
        non_admin = SimpleNamespace(
            id=2,
            email="member@example.com",
            name="Member",
            plan_code="free",
            is_admin=False,
            is_active=True,
        )
        with patch.object(app, "_require_admin_user", return_value=(non_admin, app.HTMLResponse("Admin access denied.", status_code=403))):
            response = client.get("/admin/birthplace-analytics")

        self.assertEqual(response.status_code, 403)
        self.assertIn("Admin access denied.", response.text)

    def test_admin_birthplace_analytics_route_renders_successfully(self):
        self._add_event("suggestion_results_returned", suggestion_count=3, outcome="success", location_source="suggestion_lookup")
        client = TestClient(app.app)
        with patch.object(app, "_require_admin_user", return_value=(self._admin_user(), None)):
            response = client.get("/admin/birthplace-analytics")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Birthplace Analytics", response.text)
        self.assertIn("Birthplace Suggestion Funnel", response.text)
        self.assertIn("Resolution Outcomes", response.text)
        self.assertIn("Window: all", response.text)

    def test_admin_birthplace_analytics_supports_all_period_filters(self):
        self._add_event("resolved_birthplace_success", created_at=datetime.utcnow() - timedelta(days=3))
        self._add_event("resolved_birthplace_success", created_at=datetime.utcnow() - timedelta(days=20))
        client = TestClient(app.app)

        with patch.object(app, "_require_admin_user", return_value=(self._admin_user(), None)):
            response_all = client.get("/admin/birthplace-analytics?period=all")
            response_7d = client.get("/admin/birthplace-analytics?period=7d")
            response_30d = client.get("/admin/birthplace-analytics?period=30d")

        self.assertEqual(response_all.status_code, 200)
        self.assertEqual(response_7d.status_code, 200)
        self.assertEqual(response_30d.status_code, 200)
        self.assertIn("Window: all", response_all.text)
        self.assertIn("Window: 7d", response_7d.text)
        self.assertIn("Window: 30d", response_30d.text)

    def test_admin_birthplace_analytics_empty_state_does_not_crash(self):
        client = TestClient(app.app)
        with patch.object(app, "_require_admin_user", return_value=(self._admin_user(), None)):
            response = client.get("/admin/birthplace-analytics")

        self.assertEqual(response.status_code, 200)
        self.assertIn("No birthplace analytics events have been captured yet.", response.text)

    def test_admin_birthplace_analytics_recent_events_table_renders_safe_values(self):
        self._add_event(
            "resolved_birthplace_failure",
            created_at=datetime.utcnow(),
            provider=None,
            outcome=None,
            location_source=None,
        )
        client = TestClient(app.app)
        with patch.object(app, "_require_admin_user", return_value=(self._admin_user(), None)):
            response = client.get("/admin/birthplace-analytics")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Recent Birthplace Events", response.text)
        self.assertIn("resolved_birthplace_failure", response.text)
        self.assertIn("<td>-</td>", response.text)

    def test_admin_reports_reflect_report_access_states(self):
        user = self._add_user("reader@example.com")
        self._add_report(user, "preview")
        self._add_report(user, "unlocked", pdf_ready=True)
        self._add_report(user, "purchased", pdf_ready=True, is_paid=True)
        self._add_report(user, "delivered", pdf_ready=True, is_paid=True)
        client = TestClient(app.app)

        with patch.object(app, "_require_admin_user", return_value=(self._admin_user(), None)):
            response = client.get("/admin/reports")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Preview", response.text)
        self.assertIn("Unlocked", response.text)
        self.assertIn("Purchased", response.text)
        self.assertIn("Delivered", response.text)
        self.assertIn("Full Access", response.text)
        self.assertIn("PDF Enabled", response.text)


if __name__ == "__main__":
    unittest.main()
