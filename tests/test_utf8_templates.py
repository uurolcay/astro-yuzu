import importlib
import os
import sys
from pathlib import Path

from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_FILES = [
    ROOT / "templates" / "index.html",
    ROOT / "templates" / "base.html",
    ROOT / "templates" / "reports.html",
    ROOT / "templates" / "calculator.html",
    ROOT / "templates" / "result.html",
    ROOT / "templates" / "personal_consultation.html",
    ROOT / "templates" / "report_order.html",
    ROOT / "templates" / "service_checkout.html",
    ROOT / "templates" / "parent_child_form.html",
    ROOT / "templates" / "coming_soon.html",
    ROOT / "templates" / "components" / "coming_soon_panel.html",
    ROOT / "translations.py",
]
BAD_PATTERNS = [
    "\u00c3",
    "\u00c4",
    "\u00c5",
    "&#",
    "&amp;#",
    "Do\u00c4",
    "Haritam\u00c4",
    "Yak&#",
    "\u00c4\u00b0",
]


def _load_app_module():
    os.environ["LAUNCH_MODE"] = "true"
    os.environ["ENABLE_PAYMENTS"] = "false"
    os.environ["ENABLE_FREE_CALCULATOR"] = "false"
    os.environ["ENABLE_AI_INTERPRETATION"] = "false"
    os.environ["ENABLE_CONSULTATION_BOOKING"] = "false"
    os.chdir(ROOT)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import app
    return importlib.reload(app)


def test_templates_do_not_contain_mojibake_or_entities():
    for path in TEMPLATE_FILES:
        text = path.read_text(encoding="utf-8")
        for pattern in BAD_PATTERNS:
            assert pattern not in text, f"{pattern!r} found in {path}"


def test_launch_homepage_renders_clean_turkish_notice_and_preserves_founder_panel():
    index_text = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
    panel_text = (ROOT / "templates" / "components" / "coming_soon_panel.html").read_text(encoding="utf-8")

    assert "launch-notice-panel" in panel_text
    assert "Yak\u0131nda Yay\u0131nda" in panel_text
    assert "Focus Astrology \u015fu anda ki\u015fisel raporlar, dan\u0131\u015fmanl\u0131k hizmetleri ve \u00f6deme altyap\u0131s\u0131 i\u00e7in son haz\u0131rl\u0131k s\u00fcrecindedir." in panel_text
    assert "founder-hero-card" in index_text
    assert 'coming_soon_variant = "notice"' in index_text

    app_module = _load_app_module()
    client = TestClient(app_module.app)
    response = client.get("/", headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert 'lang="tr"' in response.text
    for pattern in ("Haritam\u00c4", "Do\u00c4", "Yak&#", "&amp;#", "\u00c4\u00b0\u00e7g\u00f6r\u00fc"):
        assert pattern not in response.text


def test_articles_pages_render_without_turkish_encoding_leaks():
    app_module = _load_app_module()
    client = TestClient(app_module.app)
    for path in ("/articles", "/articles/venus-transiti-degisim-kacinilmaz"):
        response = client.get(path, headers={"accept-language": "tr"})
        assert response.status_code == 200
        for pattern in ("\u00c3", "\u00c4", "\u00c5", "Yak&#", "&amp;#", "Do\u00c4", "Haritam\u00c4", "\u00c4\u00b0"):
            assert pattern not in response.text, f"{pattern!r} leaked on {path}"
