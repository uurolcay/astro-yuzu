import unittest
from unittest.mock import Mock

from services.calendly_client import CalendlyClient, CalendlyClientError


USER_URI = "https://api.calendly.com/users/USER"
ORG_URI = "https://api.calendly.com/organizations/ORG"


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


class CalendlyClientTests(unittest.TestCase):
    def _client(self, response_payload=None):
        session = Mock()
        session.request.return_value = FakeResponse(
            payload=response_payload
            or {"resource": {"uri": USER_URI, "current_organization": ORG_URI}}
        )
        return CalendlyClient(
            pat="pat_test",
            callback_url="https://focusastrology.com/webhooks/calendly",
            session=session,
        ), session

    def test_parse_users_me_response(self):
        identity = CalendlyClient.parse_user_and_organization_uris(
            {"resource": {"uri": USER_URI, "current_organization": ORG_URI}}
        )
        self.assertEqual(identity.user_uri, USER_URI)
        self.assertEqual(identity.organization_uri, ORG_URI)

    def test_get_user_and_organization_uris_calls_users_me(self):
        client, session = self._client()
        self.assertEqual(client.get_user_and_organization_uris(), (USER_URI, ORG_URI))
        session.request.assert_called_with(
            "GET",
            "https://api.calendly.com/users/me",
            headers={
                "Authorization": "Bearer pat_test",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=15,
        )

    def test_webhook_creation_payload_for_organization_scope(self):
        client, _session = self._client()
        payload = client.build_webhook_subscription_payload()
        self.assertEqual(payload["url"], "https://focusastrology.com/webhooks/calendly")
        self.assertEqual(payload["events"], ["invitee.created", "invitee.canceled"])
        self.assertEqual(payload["organization"], ORG_URI)
        self.assertEqual(payload["scope"], "organization")
        self.assertNotIn("user", payload)

    def test_webhook_creation_payload_for_user_scope(self):
        client, _session = self._client()
        payload = client.build_webhook_subscription_payload(scope="user", events=["invitee.created"])
        self.assertEqual(payload["scope"], "user")
        self.assertEqual(payload["user"], USER_URI)
        self.assertEqual(payload["events"], ["invitee.created"])

    def test_create_webhook_subscription_posts_payload(self):
        client, session = self._client(response_payload={"resource": {"uri": "subscription"}})
        response = client.create_webhook_subscription(
            user_uri=USER_URI,
            organization_uri=ORG_URI,
        )
        self.assertEqual(response["resource"]["uri"], "subscription")
        method, url = session.request.call_args.args[:2]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://api.calendly.com/webhook_subscriptions")
        self.assertEqual(session.request.call_args.kwargs["json"]["organization"], ORG_URI)

    def test_list_webhook_subscriptions_params_for_user_scope(self):
        client, session = self._client(response_payload={"collection": []})
        client.list_webhook_subscriptions(scope="user", user_uri=USER_URI, organization_uri=ORG_URI)
        kwargs = session.request.call_args.kwargs
        self.assertEqual(kwargs["params"], {"organization": ORG_URI, "scope": "user", "user": USER_URI})

    def test_missing_pat_raises_clear_error(self):
        client = CalendlyClient(pat="", callback_url="https://focusastrology.com/webhooks/calendly")
        with self.assertRaisesRegex(CalendlyClientError, "CALENDLY_PAT"):
            client.get_current_user()

    def test_missing_callback_url_raises_clear_error(self):
        client = CalendlyClient(pat="pat_test", callback_url="")
        with self.assertRaisesRegex(CalendlyClientError, "CALENDLY_WEBHOOK_CALLBACK_URL"):
            client.build_webhook_subscription_payload(user_uri=USER_URI, organization_uri=ORG_URI)

    def test_malformed_users_me_raises_clear_error(self):
        with self.assertRaisesRegex(CalendlyClientError, "current_organization"):
            CalendlyClient.parse_user_and_organization_uris({"resource": {"uri": USER_URI}})


if __name__ == "__main__":
    unittest.main()
