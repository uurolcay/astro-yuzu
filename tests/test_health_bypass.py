import time
from unittest.mock import patch

from fastapi.testclient import TestClient

import app


def test_health_bypasses_database_session():
    client = TestClient(app.app)
    with patch.object(app.db_mod, "SessionLocal", side_effect=AssertionError("DB touched")):
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_debug_version_bypasses_database_session_and_auth():
    client = TestClient(app.app)
    with patch.object(app.db_mod, "SessionLocal", side_effect=AssertionError("DB touched")):
        response = client.get("/debug/version")

    assert response.status_code == 200
    payload = response.json()
    assert payload["app_version"] == app.APP_VERSION
    assert payload["login_template_version"] == app.LOGIN_TEMPLATE_VERSION


def test_health_and_debug_version_return_quickly():
    client = TestClient(app.app)

    started = time.perf_counter()
    health_response = client.get("/health")
    health_duration = time.perf_counter() - started

    started = time.perf_counter()
    version_response = client.get("/debug/version")
    version_duration = time.perf_counter() - started

    assert health_response.status_code == 200
    assert version_response.status_code == 200
    assert health_duration < 1.0
    assert version_duration < 1.0


def test_public_request_tracing_logs_health_request():
    client = TestClient(app.app)

    with patch.object(app.logger, "info") as info_log:
        response = client.get("/health")

    assert response.status_code == 200
    messages = [call.args[0] % call.args[1:] for call in info_log.call_args_list]
    assert any(message.startswith("PUBLIC_REQUEST_START GET /health") for message in messages)
    assert any(message.startswith("PUBLIC_REQUEST_END GET /health 200") for message in messages)
