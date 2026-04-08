"""Shared read-only admin API envelope and docs helpers."""

from datetime import datetime

from fastapi.responses import JSONResponse

from services.admin_segments import build_export_columns_for_view

ADMIN_API_SCHEMA_VERSION = "2026-04-06"
ADMIN_API_ERROR_MESSAGES = {
    "authentication_required": "Authentication is required.",
    "admin_access_denied": "Admin access is required.",
    "summary_build_failed": "Admin summary could not be generated.",
    "revenue_build_failed": "Admin revenue data could not be generated.",
    "insights_build_failed": "Admin insights could not be generated.",
    "segments_build_failed": "Admin segments could not be generated.",
    "export_metadata_build_failed": "Export metadata could not be generated.",
    "health_build_failed": "Admin health signals could not be generated.",
    "docs_build_failed": "Admin API docs could not be generated.",
    "invalid_limit": "Limit must be a positive integer between 1 and 500.",
    "invalid_filters": "One or more admin API filters are invalid.",
}


def get_admin_api_schema_version():
    return ADMIN_API_SCHEMA_VERSION


def safe_endpoint_name(endpoint=None):
    if hasattr(endpoint, "url") and getattr(endpoint.url, "path", None):
        return endpoint.url.path
    if hasattr(endpoint, "scope"):
        return endpoint.scope.get("path")
    endpoint_name = str(endpoint or "").strip()
    return endpoint_name or None


def _is_admin_api_endpoint(endpoint=None):
    endpoint_name = safe_endpoint_name(endpoint)
    return bool(endpoint_name and endpoint_name.startswith("/api/admin"))


def _admin_success_envelope(data, endpoint=None):
    return {
        "ok": True,
        "schema_version": get_admin_api_schema_version(),
        "generated_at": _generated_at_iso(),
        "endpoint": safe_endpoint_name(endpoint),
        "data": data or {},
    }


def _admin_error_envelope(error_code, message=None, endpoint=None):
    return {
        "ok": False,
        "schema_version": get_admin_api_schema_version(),
        "error": error_code,
        "message": message or ADMIN_API_ERROR_MESSAGES.get(error_code) or "Admin API request failed.",
        "endpoint": safe_endpoint_name(endpoint),
    }


def json_admin_error(error_code, status_code=400, message=None, endpoint=None):
    return JSONResponse(
        status_code=status_code,
        content=_admin_error_envelope(error_code, message=message, endpoint=endpoint),
    )


def _generated_at_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def json_ok(payload, endpoint=None):
    if _is_admin_api_endpoint(endpoint):
        return JSONResponse(content=_admin_success_envelope(payload, endpoint=endpoint))
    content = {"ok": True, "generated_at": _generated_at_iso()}
    content.update(payload or {})
    return JSONResponse(content=content)


def build_admin_segments_export_metadata_payload():
    view_descriptions = {
        "crm": "Full operational CRM export with lifecycle and campaign guidance fields.",
        "email": "Email-ready export with messaging and follow-up fields for campaign drafting.",
        "support": "Support-oriented export focused on account context and intervention notes.",
        "minimal": "Lean export for lightweight operational review or external joins.",
    }
    views = {}
    for view_name in ("crm", "email", "support", "minimal"):
        _, columns = build_export_columns_for_view(view_name)
        views[view_name] = {
            "columns": columns or [],
            "description": view_descriptions.get(view_name, ""),
        }
    return {
        "default_view": "crm",
        "views": views,
        "filters_supported": ["segment", "group", "priority", "channel", "message_type", "view", "limit"],
        "filter_notes": [
            "Filters combine with logical AND semantics.",
            "The `limit` filter is applied per segment after filters are resolved.",
            "The `view` filter only affects CSV export column selection.",
        ],
        "limit_behavior": "per_segment",
    }


def build_admin_api_docs_payload():
    response_envelope = {
        "success": {
            "ok": True,
            "schema_version": get_admin_api_schema_version(),
            "generated_at": "ISO-8601 UTC timestamp",
            "endpoint": "/api/admin/summary",
            "data": {"...": "endpoint payload"},
        },
        "error": _admin_error_envelope(
            "admin_access_denied",
            message="Admin access is required.",
            endpoint="/api/admin/summary",
        ),
    }
    return {
        "auth": {
            "required": True,
            "admin_only": True,
            "behavior": "Unauthenticated requests return 401. Non-admin authenticated requests return 403.",
        },
        "schema_version_meaning": "Bump this when admin API response contracts change in a consumer-visible way.",
        "response_envelope": response_envelope,
        "error_format": response_envelope["error"],
        "endpoints": {
            "/api/admin/docs": {
                "method": "GET",
                "description": "Machine-readable admin API contract reference.",
                "filters": [],
                "response_keys": ["auth", "schema_version_meaning", "response_envelope", "error_format", "endpoints"],
            },
            "/api/admin/summary": {
                "method": "GET",
                "description": "Executive summary KPIs, scorecards, watchlist, and quick admin links.",
                "filters": [],
                "response_keys": ["kpis", "scorecards", "trends", "watchlist", "priority_focus", "weekly_summary", "links"],
            },
            "/api/admin/revenue": {
                "method": "GET",
                "description": "Read-only revenue proxy, funnel, billing, and paid-user breakdowns.",
                "filters": [],
                "response_keys": ["core_metrics", "conversion_metrics", "usage_metrics", "revenue_proxy", "funnel", "engagement", "billing_signals", "top_users"],
            },
            "/api/admin/insights": {
                "method": "GET",
                "description": "Headline KPIs and prioritized product or lifecycle insights.",
                "filters": [],
                "response_keys": ["headline_kpis", "insights", "upsell_candidates", "churn_watchlist", "quick_metrics"],
            },
            "/api/admin/segments": {
                "method": "GET",
                "description": "Campaign-ready lifecycle segments with echoed filters and per-segment rows.",
                "filters": ["segment", "group", "priority", "channel", "message_type", "limit"],
                "response_keys": ["filters", "limit_behavior", "summary", "segments"],
            },
            "/api/admin/segments/export-metadata": {
                "method": "GET",
                "description": "CSV export capabilities, supported views, and filter behavior notes.",
                "filters": [],
                "response_keys": ["default_view", "views", "filters_supported", "filter_notes", "limit_behavior"],
            },
            "/api/admin/health": {
                "method": "GET",
                "description": "Operational warning signals derived from executive KPI thresholds.",
                "filters": [],
                "response_keys": ["signals"],
            },
        },
    }
