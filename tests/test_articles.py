import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

import app
import database as db_mod
from core.article_matching import match_articles_to_result
from core.interpretation import build_interpretation_layer
from tests.test_interpretation_layer import SAMPLE_DASHA, SAMPLE_NATAL_DATA, SAMPLE_TRANSITS


class ArticleSystemTests(unittest.TestCase):
    def setUp(self):
        self.db = db_mod.SessionLocal()
        self.db.query(db_mod.Article).delete()
        self.db.commit()

    def tearDown(self):
        self.db.query(db_mod.Article).delete()
        self.db.commit()
        self.db.close()

    def _add_article(self, title, *, category="foundations", slug=None, published=True, published_at=None):
        article = db_mod.Article(
            title=title,
            slug=slug or app._unique_article_slug(self.db, title),
            category=category,
            excerpt=f"{title} excerpt",
            body=f"{title} body paragraph one.\n\n{title} body paragraph two.",
            is_published=published,
            published_at=published_at or datetime.utcnow(),
            author_name="Focus Astrology",
            reading_time=5,
            language="en",
        )
        self.db.add(article)
        self.db.commit()
        self.db.refresh(article)
        return article

    def test_articles_listing_renders_successfully(self):
        client = TestClient(app.app)
        response = client.get("/articles")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Insights & Articles", response.text)
        self.assertIn("What Is Vedic Astrology", response.text)

    def test_article_cards_render_from_stored_content(self):
        article = self._add_article("Latest Editorial Signal", category="timing", published_at=datetime.utcnow() + timedelta(days=1))
        client = TestClient(app.app)
        response = client.get("/articles")
        self.assertEqual(response.status_code, 200)
        self.assertIn(article.title, response.text)
        self.assertIn(article.slug, response.text)

    def test_article_detail_renders_successfully(self):
        article = self._add_article("Authority Detail Article", category="chart-reading")
        client = TestClient(app.app)
        response = client.get(f"/articles/{article.slug}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(article.title, response.text)
        self.assertIn(article.excerpt, response.text)

    def test_category_filter_route_works(self):
        self._add_article("Timing Piece", category="timing")
        self._add_article("Foundations Piece", category="foundations")
        client = TestClient(app.app)
        response = client.get("/articles/category/timing")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Timing Piece", response.text)
        self.assertNotIn("Foundations Piece", response.text)

    def test_slug_uniqueness_is_handled_safely(self):
        first = self._add_article("Duplicate Title")
        second_slug = app._unique_article_slug(self.db, "Duplicate Title")
        self.assertEqual(first.slug, "duplicate-title")
        self.assertEqual(second_slug, "duplicate-title-2")

    def test_related_articles_helper_returns_structured_output(self):
        target = self._add_article("Target Article", category="timing")
        self._add_article("Related One", category="timing")
        self._add_article("Related Two", category="timing")
        related = app.get_related_articles(self.db, target, limit=2)
        self.assertEqual(len(related), 2)
        self.assertIn("title", related[0])
        self.assertIn("slug", related[0])
        self.assertEqual(related[0]["category"]["slug"], "timing")

    def test_homepage_article_preview_uses_real_articles(self):
        article = self._add_article("Homepage Authority Piece", category="life-guidance", published_at=datetime.utcnow() + timedelta(days=2))
        client = TestClient(app.app)
        response = client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(article.title, response.text)
        self.assertIn(f"/articles/{article.slug}", response.text)

    def test_unpublished_articles_are_not_shown_publicly(self):
        article = self._add_article("Hidden Draft", published=False)
        client = TestClient(app.app)
        listing = client.get("/articles")
        detail = client.get(f"/articles/{article.slug}")
        self.assertEqual(listing.status_code, 200)
        self.assertNotIn(article.title, listing.text)
        self.assertEqual(detail.status_code, 404)

    def test_matching_function_returns_deterministic_results(self):
        articles = [
            {"title": "Saturn Periods and Life Pressure", "slug": "saturn-periods", "excerpt": "pressure and discipline", "category": "timing", "is_published": True},
            {"title": "What Is Vedic Astrology", "slug": "vedic-astrology", "excerpt": "foundations for growth", "category": "foundations", "is_published": True},
        ]
        prioritized_signals = [{"planet": "Saturn"}]
        anchors = [{"title": "Career pressure under Saturn"}]
        domain_scores = {"timing": 9, "career": 7}
        a = match_articles_to_result(prioritized_signals, anchors, domain_scores, articles)
        b = match_articles_to_result(prioritized_signals, anchors, domain_scores, articles)
        self.assertEqual(a, b)

    def test_domain_based_matching_works(self):
        matched = match_articles_to_result(
            [],
            [],
            {"timing": 10},
            [
                {"title": "Timing vs Free Will", "slug": "timing-free-will", "excerpt": "timing and cycles", "category": "timing", "is_published": True},
                {"title": "Foundations of Chart Reading", "slug": "chart-foundations", "excerpt": "core chart structure", "category": "foundations", "is_published": True},
            ],
        )
        self.assertEqual(matched[0]["category"], "timing")

    def test_keyword_based_matching_works(self):
        matched = match_articles_to_result(
            [{"planet": "Jupiter"}],
            [{"title": "Growth through expansion"}],
            {"growth": 8},
            [
                {"title": "Jupiter in the First House", "slug": "jupiter-first-house", "excerpt": "growth and expansion", "category": "chart-reading", "is_published": True},
                {"title": "Saturn Periods and Life Pressure", "slug": "saturn-pressure", "excerpt": "discipline and pressure", "category": "timing", "is_published": True},
            ],
        )
        self.assertEqual(matched[0]["slug"], "jupiter-first-house")

    def test_only_published_articles_are_considered_by_matching(self):
        matched = match_articles_to_result(
            [{"planet": "Saturn"}],
            [{"title": "Saturn pressure"}],
            {"timing": 9},
            [
                {"title": "Visible Saturn Article", "slug": "visible-saturn", "excerpt": "saturn timing", "category": "timing", "is_published": True},
                {"title": "Hidden Saturn Article", "slug": "hidden-saturn", "excerpt": "saturn timing", "category": "timing", "is_published": False},
            ],
        )
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0]["slug"], "visible-saturn")

    def test_matching_returns_max_three_articles(self):
        articles = [
            {"title": f"Timing Article {index}", "slug": f"timing-{index}", "excerpt": "timing cycles pressure", "category": "timing", "is_published": True}
            for index in range(5)
        ]
        matched = match_articles_to_result([{"planet": "Saturn"}], [{"title": "Timing pressure"}], {"timing": 10}, articles)
        self.assertLessEqual(len(matched), 3)

    def test_result_template_contains_related_insights_guard(self):
        template = Path("C:\\Users\\uolca\\Documents\\Chatgpt Codex\\astro-yuzu\\templates\\result.html").read_text(encoding="utf-8")
        self.assertIn("{% if related_articles %}", template)
        self.assertIn("Related Insights", template)

    def test_result_template_renders_related_articles_when_present(self):
        layer = build_interpretation_layer(SAMPLE_NATAL_DATA, dasha_data=SAMPLE_DASHA, transit_data=SAMPLE_TRANSITS)
        context = {
            "request": SimpleNamespace(state=SimpleNamespace(current_user=None)),
            "full_name": "Insight Reader",
            "birth_date": "1990-01-01",
            "birth_time": "08:30",
            "birth_city": "Istanbul, Turkey",
            "normalized_birth_place": "Istanbul, Turkey",
            "timezone": "Europe/Istanbul",
            "report_type": "premium",
            "report_type_config": {"include_pdf": True},
            "interpretation_context": {
                "signal_layer": layer["anchors"],
                "recommendation_layer": layer["recommendation_layer"],
                "top_timing_windows": {},
            },
            "payload_json": {},
            "report_access": {"is_preview": False, "show_unlock_cta": False, "can_view_full_report": True, "can_download_pdf": False, "show_login_hint": False, "unlock_success": False, "access_label": "Purchased"},
            "related_articles": [
                {"title": "Saturn Periods and Life Pressure", "slug": "saturn-periods-and-life-pressure", "excerpt": "A calm guide to pressure periods.", "category": "timing"}
            ],
            "natal_data": {},
            "dasha_data": [],
            "navamsa_data": {},
            "transit_data": [],
            "eclipse_data": [],
        }
        html = app.templates.env.get_template("result.html").render(context)
        self.assertIn("Related Insights", html)
        self.assertIn("Saturn Periods and Life Pressure", html)


if __name__ == "__main__":
    unittest.main()
