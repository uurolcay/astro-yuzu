from urllib.parse import urlencode


def fetch_admin_api_json(client, path, expected_status=200):
    response = client.get(path)
    assert response.status_code == expected_status, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def fetch_admin_summary_json(client, expected_status=200):
    return fetch_admin_api_json(client, "/api/admin/summary", expected_status=expected_status)


def fetch_admin_revenue_json(client, expected_status=200):
    return fetch_admin_api_json(client, "/api/admin/revenue", expected_status=expected_status)


def fetch_admin_insights_json(client, expected_status=200):
    return fetch_admin_api_json(client, "/api/admin/insights", expected_status=expected_status)


def fetch_admin_segments_json(client, expected_status=200, **filters):
    normalized_filters = {key: value for key, value in filters.items() if value not in (None, "")}
    query = urlencode(normalized_filters)
    path = "/api/admin/segments"
    if query:
        path = f"{path}?{query}"
    return fetch_admin_api_json(client, path, expected_status=expected_status)


def fetch_admin_export_metadata_json(client, expected_status=200):
    return fetch_admin_api_json(client, "/api/admin/segments/export-metadata", expected_status=expected_status)


def fetch_admin_health_json(client, expected_status=200):
    return fetch_admin_api_json(client, "/api/admin/health", expected_status=expected_status)


def fetch_admin_docs_json(client, expected_status=200):
    return fetch_admin_api_json(client, "/api/admin/docs", expected_status=expected_status)


def assert_admin_success_envelope(testcase, payload, endpoint):
    testcase.assertIsInstance(payload, dict)
    testcase.assertTrue(payload.get("ok"))
    testcase.assertIn("schema_version", payload)
    testcase.assertIn("generated_at", payload)
    testcase.assertEqual(payload.get("endpoint"), endpoint)
    testcase.assertIn("data", payload)
    testcase.assertIsInstance(payload["data"], dict)


def assert_admin_error_envelope(testcase, payload, endpoint, error_code=None):
    testcase.assertIsInstance(payload, dict)
    testcase.assertFalse(payload.get("ok"))
    testcase.assertIn("schema_version", payload)
    testcase.assertIn("error", payload)
    testcase.assertIn("message", payload)
    testcase.assertEqual(payload.get("endpoint"), endpoint)
    if error_code is not None:
        testcase.assertEqual(payload.get("error"), error_code)


def assert_has_data_keys(testcase, payload, required_keys):
    testcase.assertIn("data", payload)
    for key in required_keys:
        testcase.assertIn(key, payload["data"])
