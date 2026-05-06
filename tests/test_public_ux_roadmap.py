import os
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import app


ROOT = Path(__file__).resolve().parents[1]


def _template(name):
    return (ROOT / "templates" / name).read_text(encoding="utf-8")


def test_theme_inline_script_defaults_to_auto_and_prevents_initial_flash():
    base = _template("base.html")
    head = base.split("</head>", 1)[0]

    assert 'const storageKey = "focus-astrology-theme-mode"' in head
    assert "window.localStorage.getItem(storageKey) || \"auto\"" in head
    assert "prefers-color-scheme: dark" in head
    assert "document.documentElement.dataset.themeMode = mode" in head
    assert "document.documentElement.dataset.theme = resolved" in head
    assert "document.documentElement.dataset.themeReady = \"false\"" in head
    assert 'html:not([data-theme-ready="true"])' in head


def test_theme_runtime_persists_mode_and_updates_auto_on_system_change():
    base = _template("base.html")

    assert "localStorage.setItem(this.storageKey, nextMode)" in base
    assert "return [\"dark\", \"light\", \"auto\"].includes(mode) ? mode : \"auto\"" in base
    assert "window.APP_THEME.media.addEventListener(\"change\", syncAutoTheme)" in base
    assert "window.APP_THEME.apply(\"auto\")" in base
    assert "document.documentElement.dataset.themeReady = \"true\"" in base


def test_homepage_report_cards_include_delivery_badges_and_suitability_labels():
    html = _template("index.html")

    assert "reports-curated-card" in html
    assert "grid-template-rows: auto auto auto 1fr auto" in html
    assert "report-delivery-badge" in html
    assert "PDF · 7 gün" in html
    assert "PDF · 7 days" in html
    assert "Kendini ve yaşam temasını derinlemesine anlamak isteyenler için." in html
    assert "Önümüzdeki dönemin zamanlamasını görmek isteyenler için." in html
    assert "Kariyer yönü, iş değişimi veya mesleki karar aşamasında olanlar için." in html
    assert "Çocuğunun doğasını daha bilinçli anlamak isteyen ebeveynler için." in html
    assert "For those who want to understand their life themes more deeply." in html
    assert "For those who want to understand the timing of the year ahead." in html
    assert "For career direction, job transitions, or professional decisions." in html
    assert "For parents who want to understand their child’s nature more consciously." in html


def test_reports_page_cards_include_delivery_badges_and_suitability_labels():
    html = _template("reports.html")

    assert "reports-offer-grid { grid-template-columns:repeat(2, minmax(0, 1fr)); align-items:stretch; }" in html
    assert "grid-template-rows:auto auto auto auto 1fr auto" in html
    assert "reports-card-topline" in html
    assert "report-delivery-badge" in html
    assert "PDF · 7 gün" in html
    assert "PDF · 7 days" in html
    assert "Kendini ve yaşam temasını derinlemesine anlamak isteyenler için." in html
    assert "Önümüzdeki dönemin zamanlamasını görmek isteyenler için." in html
    assert "Kariyer yönü, iş değişimi veya mesleki karar aşamasında olanlar için." in html
    assert "Çocuğunun doğasını daha bilinçli anlamak isteyen ebeveynler için." in html


def test_public_pages_render_localized_report_card_additions():
    client = TestClient(app.app)

    tr_home = client.get("/", headers={"accept-language": "tr"})
    assert tr_home.status_code == 200
    assert "PDF · 7 gün" in tr_home.text
    assert "Kendini ve yaşam temasını derinlemesine anlamak isteyenler için." in tr_home.text

    en_reports = client.get("/reports", headers={"accept-language": "en"})
    assert en_reports.status_code == 200
    assert "PDF · 7 days" in en_reports.text
    assert "For career direction, job transitions, or professional decisions." in en_reports.text


def test_reports_launch_payment_gating_remains_in_place():
    client = TestClient(app.app)
    with patch.dict(os.environ, {"ENABLE_PAYMENTS": "false", "ENABLE_CONSULTATION_BOOKING": "false"}, clear=False):
        response = client.get("/reports", headers={"accept-language": "en"})

    assert response.status_code == 200
    assert "Available Soon" in response.text
    assert "Get Notified" in response.text
    assert 'href="/reports/order/birth_chart_karma"' not in response.text
