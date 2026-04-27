import argparse
import json
import os
from dataclasses import dataclass
from typing import Any

import requests

API_BASE_URL = "https://api.calendly.com"
DEFAULT_EVENTS = ["invitee.created", "invitee.canceled"]
DEFAULT_SCOPE = "organization"
DEFAULT_TIMEOUT = 15


class CalendlyClientError(RuntimeError):
    pass


@dataclass
class CalendlyIdentity:
    user_uri: str
    organization_uri: str


class CalendlyClient:
    def __init__(
        self,
        pat: str | None = None,
        callback_url: str | None = None,
        default_scope: str | None = None,
        base_url: str = API_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        session: Any | None = None,
    ):
        self.pat = (pat if pat is not None else os.getenv("CALENDLY_PAT", "")).strip()
        self.callback_url = (callback_url if callback_url is not None else os.getenv("CALENDLY_WEBHOOK_CALLBACK_URL", "")).strip()
        self.default_scope = (default_scope if default_scope is not None else os.getenv("CALENDLY_WEBHOOK_SCOPE", DEFAULT_SCOPE)).strip() or DEFAULT_SCOPE
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    def _require_pat(self) -> str:
        if not self.pat:
            raise CalendlyClientError("CALENDLY_PAT is required.")
        return self.pat

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._require_pat()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.base_url}{path}"
        kwargs.setdefault("timeout", self.timeout)
        try:
            response = self.session.request(method, url, headers=self._headers(), **kwargs)
        except requests.RequestException as exc:
            raise CalendlyClientError(f"Calendly request failed: {exc}") from exc
        if response.status_code >= 400:
            body = response.text[:500] if getattr(response, "text", None) else ""
            raise CalendlyClientError(f"Calendly API error {response.status_code}: {body}")
        try:
            return response.json()
        except ValueError as exc:
            raise CalendlyClientError("Calendly API returned malformed JSON.") from exc

    def get_current_user(self) -> dict:
        return self._request("GET", "/users/me")

    @staticmethod
    def parse_user_and_organization_uris(payload: dict) -> CalendlyIdentity:
        resource = payload.get("resource") if isinstance(payload, dict) else None
        if not isinstance(resource, dict):
            raise CalendlyClientError("Calendly /users/me response is missing resource.")
        user_uri = str(resource.get("uri") or "").strip()
        organization_uri = str(resource.get("current_organization") or "").strip()
        if not user_uri:
            raise CalendlyClientError("Calendly /users/me response is missing resource.uri.")
        if not organization_uri:
            raise CalendlyClientError("Calendly /users/me response is missing resource.current_organization.")
        return CalendlyIdentity(user_uri=user_uri, organization_uri=organization_uri)

    def get_user_and_organization_uris(self) -> tuple[str, str]:
        identity = self.parse_user_and_organization_uris(self.get_current_user())
        return identity.user_uri, identity.organization_uri

    def _resolve_identity(self, user_uri: str | None = None, organization_uri: str | None = None) -> CalendlyIdentity:
        if user_uri and organization_uri:
            return CalendlyIdentity(user_uri=user_uri, organization_uri=organization_uri)
        current = self.parse_user_and_organization_uris(self.get_current_user())
        return CalendlyIdentity(user_uri=user_uri or current.user_uri, organization_uri=organization_uri or current.organization_uri)

    def build_webhook_subscription_payload(
        self,
        events: list[str] | None = None,
        scope: str | None = None,
        user_uri: str | None = None,
        organization_uri: str | None = None,
        callback_url: str | None = None,
    ) -> dict:
        callback = (callback_url or self.callback_url).strip()
        if not callback:
            raise CalendlyClientError("CALENDLY_WEBHOOK_CALLBACK_URL is required.")
        resolved_scope = (scope or self.default_scope or DEFAULT_SCOPE).strip().lower()
        if resolved_scope not in {"organization", "user"}:
            raise CalendlyClientError("CALENDLY_WEBHOOK_SCOPE must be 'organization' or 'user'.")
        identity = self._resolve_identity(user_uri=user_uri, organization_uri=organization_uri)
        payload = {
            "url": callback,
            "events": events or list(DEFAULT_EVENTS),
            "organization": identity.organization_uri,
            "scope": resolved_scope,
        }
        if resolved_scope == "user":
            payload["user"] = identity.user_uri
        return payload

    def create_webhook_subscription(
        self,
        events: list[str] | None = None,
        scope: str | None = None,
        user_uri: str | None = None,
        organization_uri: str | None = None,
    ) -> dict:
        payload = self.build_webhook_subscription_payload(
            events=events,
            scope=scope,
            user_uri=user_uri,
            organization_uri=organization_uri,
        )
        return self._request("POST", "/webhook_subscriptions", json=payload)

    def build_webhook_list_params(
        self,
        organization_uri: str | None = None,
        user_uri: str | None = None,
        scope: str | None = None,
    ) -> dict:
        resolved_scope = (scope or self.default_scope or DEFAULT_SCOPE).strip().lower()
        if resolved_scope not in {"organization", "user"}:
            raise CalendlyClientError("CALENDLY_WEBHOOK_SCOPE must be 'organization' or 'user'.")
        identity = self._resolve_identity(user_uri=user_uri, organization_uri=organization_uri)
        params = {"organization": identity.organization_uri, "scope": resolved_scope}
        if resolved_scope == "user":
            params["user"] = identity.user_uri
        return params

    def list_webhook_subscriptions(
        self,
        organization_uri: str | None = None,
        user_uri: str | None = None,
        scope: str | None = None,
    ) -> dict:
        params = self.build_webhook_list_params(
            organization_uri=organization_uri,
            user_uri=user_uri,
            scope=scope,
        )
        return self._request("GET", "/webhook_subscriptions", params=params)

    def diagnose(self) -> dict:
        user_uri, organization_uri = self.get_user_and_organization_uris()
        subscriptions = self.list_webhook_subscriptions(
            organization_uri=organization_uri,
            user_uri=user_uri,
            scope=self.default_scope,
        )
        return {
            "ok": True,
            "user_uri": user_uri,
            "organization_uri": organization_uri,
            "scope": self.default_scope,
            "subscriptions": subscriptions,
        }


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Calendly PAT helper for Focus Astrology.")
    parser.add_argument("command", choices=["diagnose", "create-webhook", "list-webhooks", "users-me"])
    parser.add_argument("--scope", choices=["organization", "user"], default=None)
    args = parser.parse_args()
    client = CalendlyClient(default_scope=args.scope)
    if args.command == "users-me":
        _print_json(client.get_current_user())
    elif args.command == "diagnose":
        _print_json(client.diagnose())
    elif args.command == "create-webhook":
        _print_json(client.create_webhook_subscription(scope=args.scope))
    elif args.command == "list-webhooks":
        _print_json(client.list_webhook_subscriptions(scope=args.scope))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
