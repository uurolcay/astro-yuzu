from unittest.mock import patch

from fastapi.testclient import TestClient

import app


LAUNCH_ENV = {
    "LAUNCH_MODE": "true",
    "ENABLE_PAYMENTS": "false",
    "ENABLE_FREE_CALCULATOR": "false",
    "ENABLE_AI_INTERPRETATION": "false",
    "ENABLE_CONSULTATION_BOOKING": "false",
}


def test_calculate_is_blocked_before_chart_pipeline_runs():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False), patch.object(app, "_build_birth_context") as build_birth_context:
        response = client.post(
            "/calculate",
            data={
                "full_name": "Launch Test",
                "birth_date": "1990-01-01",
                "birth_time": "10:00",
                "birth_city": "Istanbul",
            },
            headers={"accept-language": "tr"},
        )
    assert response.status_code == 200
    assert "YakÄ±nda YayÄ±nda" in response.text
    assert "Focus Astrology ÅŸu anda kiÅŸisel raporlar, danÄ±ÅŸmanlÄ±k hizmetleri ve Ã¶deme altyapÄ±sÄ± iÃ§in son hazÄ±rlÄ±k sÃ¼recindedir." in response.text
    build_birth_context.assert_not_called()


def test_interpret_returns_clean_json_when_ai_is_disabled():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False), patch.object(app.ai_logic, "generate_interpretation") as generate_interpretation:
        response = client.post("/interpret", data={"payload_json": "{}"}, headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert response.json() == {"ok": False, "launch_mode": True, "message": "YakÄ±nda YayÄ±nda"}
    generate_interpretation.assert_not_called()


def test_report_checkout_does_not_start_payment_when_payments_are_disabled():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False), patch.object(app, "create_report_payment_session") as create_session:
        response = client.post("/checkout/report/fake-order-token", headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert "YakÄ±nda YayÄ±nda" in response.text
    create_session.assert_not_called()


def test_consultation_checkout_does_not_start_payment_when_disabled():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False), patch.object(app, "create_consultation_payment_session") as create_session:
        response = client.post("/checkout/consultation", headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert "YakÄ±nda YayÄ±nda" in response.text
    create_session.assert_not_called()


def test_reports_page_shows_launch_cta_copy_when_payments_are_disabled():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False):
        response = client.get("/reports", headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert "YakÄ±nda AÃ§Ä±lacak" in response.text


def test_calculator_page_shows_launch_button_when_calculator_is_disabled():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False):
        response = client.get("/calculator", headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert "Harita Analizi YakÄ±nda" in response.text


def test_personal_consultation_booking_route_renders_coming_soon_panel():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False):
        response = client.get("/personal-consultation/book", headers={"accept-language": "tr"})
    assert response.status_code == 200
    assert "YakÄ±nda YayÄ±nda" in response.text
    assert "Focus Astrology ÅŸu anda kiÅŸisel raporlar, danÄ±ÅŸmanlÄ±k hizmetleri ve Ã¶deme altyapÄ±sÄ± iÃ§in son hazÄ±rlÄ±k sÃ¼recindedir." in response.text

def test_english_launch_mode_panel_is_fully_english():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False):
        response = client.get("/personal-consultation/book", headers={"accept-language": "en"})
    assert response.status_code == 200
    assert "Coming Soon" in response.text
    assert "Back to Home" in response.text
    assert "Get Information" in response.text
    assert "YakÄ±nda YayÄ±nda" not in response.text


def test_report_order_page_uses_english_launch_copy_when_payments_disabled():
    client = TestClient(app.app)
    with patch.dict("os.environ", LAUNCH_ENV, clear=False):
        response = client.get("/reports/order/career", headers={"accept-language": "en"})
    assert response.status_code == 200
    assert "Coming Soon" in response.text
    assert "Back to Home" in response.text
    assert "YakÄ±nda YayÄ±nda" not in response.text


