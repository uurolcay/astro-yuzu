import os
import json
import uuid
import hmac
import hashlib
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib import request as urlrequest
from urllib.parse import urlencode, urljoin

from .base import PaymentConfigurationError, PaymentProvider, PaymentVerificationError


def _env(name, default=""):
    return str(os.getenv(name, default)).strip()


def _env_flag(name, default=False):
    raw = _env(name, "true" if default else "false").lower()
    return raw in {"1", "true", "yes", "on"}


def _amount(value):
    raw = str(value or "").strip()
    raw = raw.replace("₺", "")
    raw = raw.replace("₺", "").replace("TRY", "").replace("TL", "").replace(" ", "")
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "." in raw and len(raw.rsplit(".", 1)[-1]) == 3:
        raw = raw.replace(".", "")
    else:
        raw = raw.replace(",", "")
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError):
        amount = Decimal("0")
    return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _compact(value):
    return str(value or "").strip()


def _hmac_sha256_hex(secret_key, message):
    return hmac.new(
        str(secret_key or "").encode("utf-8"),
        str(message or "").encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_iyzico_hpp_webhook_signature(payload, secret_key=None):
    """Build iyzico X-IYZ-SIGNATURE-V3 for Checkout Form/HPP webhooks."""
    secret = _compact(secret_key) or _env("IYZICO_WEBHOOK_SECRET") or _env("IYZICO_SECRET_KEY")
    if not secret:
        raise PaymentConfigurationError("Iyzico webhook signature secret is not configured.")
    key = "".join(
        [
            secret,
            _compact((payload or {}).get("iyziEventType")),
            _compact((payload or {}).get("iyziPaymentId") or (payload or {}).get("paymentId")),
            _compact((payload or {}).get("token")),
            _compact((payload or {}).get("paymentConversationId")),
            _compact((payload or {}).get("status")),
        ]
    )
    return _hmac_sha256_hex(secret, key)


def verify_iyzico_hpp_webhook_signature(payload, signature, secret_key=None):
    signature = _compact(signature).lower()
    if not signature:
        return False
    expected = build_iyzico_hpp_webhook_signature(payload or {}, secret_key=secret_key).lower()
    return hmac.compare_digest(expected, signature)


class IyzicoProvider(PaymentProvider):
    provider_name = "iyzico"

    def create_checkout_session(self, report, user, success_url, cancel_url):
        raise NotImplementedError("Iyzico checkout session initialization is not implemented yet.")

    @staticmethod
    def build_hpp_webhook_signature(payload, secret_key=None):
        return build_iyzico_hpp_webhook_signature(payload, secret_key=secret_key)

    @staticmethod
    def verify_hpp_webhook_signature(payload, signature, secret_key=None):
        return verify_iyzico_hpp_webhook_signature(payload, signature, secret_key=secret_key)

    @staticmethod
    def response_signature_payload(kind, payload):
        """Isolated best-effort canonicalization for iyzico response signatures.

        iyzico exposes a `signature` field on some responses, but endpoint-level
        canonical fields can differ. Keeping this isolated makes sandbox-driven
        adjustment safe without touching payment finalization logic.
        """
        data = payload or {}
        normalized_kind = str(kind or "").strip().lower()
        if normalized_kind == "checkout_initialize":
            return "".join([
                _compact(data.get("conversationId")),
                _compact(data.get("token") or data.get("checkoutFormToken")),
                _compact(data.get("status")),
            ])
        if normalized_kind == "checkout_retrieve":
            return "".join([
                _compact(data.get("conversationId")),
                _compact(data.get("paymentId")),
                _compact(data.get("paymentStatus")),
                _compact(data.get("basketId")),
                _compact(data.get("currency")),
                _amount(data.get("paidPrice")),
            ])
        if normalized_kind == "refund":
            return "".join([
                _compact(data.get("conversationId")),
                _compact(data.get("paymentTransactionId") or data.get("paymentId")),
                _amount(data.get("price")),
                _compact(data.get("currency")),
                _compact(data.get("status")),
            ])
        return ""

    @classmethod
    def verify_response_signature_payload(cls, kind, payload, *, required=None):
        signature = _compact((payload or {}).get("signature"))
        require_signature = _env_flag("IYZICO_REQUIRE_RESPONSE_SIGNATURE", default=False) if required is None else bool(required)
        if not signature:
            if require_signature:
                raise PaymentVerificationError("Iyzico response signature is missing.")
            return True
        secret = _env("IYZICO_SECRET_KEY")
        if not secret:
            if require_signature:
                raise PaymentConfigurationError("IYZICO_SECRET_KEY is required for response signature validation.")
            return True
        message = cls.response_signature_payload(kind, payload)
        if not message:
            if require_signature:
                raise PaymentVerificationError("Iyzico response signature canonical payload is unavailable.")
            return True
        expected = _hmac_sha256_hex(secret, message).lower()
        if not hmac.compare_digest(expected, signature.lower()):
            raise PaymentVerificationError("Iyzico response signature validation failed.")
        return True

    def _base_url(self):
        return _env("IYZICO_BASE_URL", "https://api.iyzipay.com").rstrip("/")

    def _api_key(self):
        api_key = _env("IYZICO_API_KEY")
        if not api_key:
            raise PaymentConfigurationError("IYZICO_API_KEY is not configured.")
        return api_key

    def _secret_key(self):
        secret_key = _env("IYZICO_SECRET_KEY")
        if not secret_key:
            raise PaymentConfigurationError("IYZICO_SECRET_KEY is not configured.")
        return secret_key

    def _auth_headers(self, body):
        api_key = self._api_key()
        secret_key = self._secret_key()
        random_key = str(uuid.uuid4())
        signature_payload = f"{api_key}{random_key}{secret_key}{body}"
        signature = hmac.new(secret_key.encode("utf-8"), signature_payload.encode("utf-8"), hashlib.sha256).hexdigest()
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"IYZWSv2 {api_key}:{random_key}:{signature}",
            "x-iyzi-rnd": random_key,
        }

    def _post_json(self, path, payload):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        req = urlrequest.Request(
            urljoin(self._base_url() + "/", path.lstrip("/")),
            data=body.encode("utf-8"),
            headers=self._auth_headers(body),
            method="POST",
        )
        timeout = int(_env("IYZICO_HTTP_TIMEOUT_SECONDS", "25") or "25")
        try:
            with urlrequest.urlopen(req, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
        except Exception as exc:
            raise PaymentVerificationError("Iyzico request failed.") from exc
        try:
            return json.loads(raw)
        except Exception as exc:
            raise PaymentVerificationError("Iyzico response was not valid JSON.") from exc

    def _buyer_payload(self, order):
        email = getattr(order, "customer_email", None) or _env("IYZICO_DEFAULT_BUYER_EMAIL", "customer@example.com")
        name = getattr(order, "customer_name", None) or "Focus Astrology Client"
        parts = str(name).split()
        first_name = parts[0] if parts else "Focus"
        last_name = " ".join(parts[1:]) if len(parts) > 1 else "Client"
        return {
            "id": str(getattr(order, "id", "")),
            "name": first_name,
            "surname": last_name,
            "gsmNumber": _env("IYZICO_DEFAULT_GSM", "+905350000000"),
            "email": email,
            "identityNumber": _env("IYZICO_DEFAULT_IDENTITY_NUMBER", "11111111111"),
            "lastLoginDate": "2026-01-01 00:00:00",
            "registrationDate": "2026-01-01 00:00:00",
            "registrationAddress": _env("IYZICO_DEFAULT_ADDRESS", "Istanbul, Turkey"),
            "ip": _env("IYZICO_DEFAULT_IP", "127.0.0.1"),
            "city": _env("IYZICO_DEFAULT_CITY", "Istanbul"),
            "country": _env("IYZICO_DEFAULT_COUNTRY", "Turkey"),
            "zipCode": _env("IYZICO_DEFAULT_ZIP", "34000"),
        }

    def _address_payload(self):
        return {
            "contactName": _env("IYZICO_DEFAULT_CONTACT_NAME", "Focus Astrology"),
            "city": _env("IYZICO_DEFAULT_CITY", "Istanbul"),
            "country": _env("IYZICO_DEFAULT_COUNTRY", "Turkey"),
            "address": _env("IYZICO_DEFAULT_ADDRESS", "Istanbul, Turkey"),
            "zipCode": _env("IYZICO_DEFAULT_ZIP", "34000"),
        }

    def _initialize_payload(self, order, callback_url):
        amount = _amount(getattr(order, "amount", None) or getattr(order, "amount_label", ""))
        conversation_id = getattr(order, "public_token", None) or getattr(order, "order_token", "")
        product_name = getattr(order, "product_type", "service_order")
        return {
            "locale": "tr",
            "conversationId": conversation_id,
            "price": amount,
            "paidPrice": amount,
            "currency": getattr(order, "currency", None) or "TRY",
            "basketId": str(getattr(order, "id", "")),
            "paymentGroup": "PRODUCT",
            "callbackUrl": callback_url,
            "enabledInstallments": [1],
            "buyer": self._buyer_payload(order),
            "shippingAddress": self._address_payload(),
            "billingAddress": self._address_payload(),
            "basketItems": [
                {
                    "id": str(getattr(order, "id", "")),
                    "name": str(product_name),
                    "category1": str(getattr(order, "service_type", "service")),
                    "itemType": "VIRTUAL",
                    "price": amount,
                }
            ],
        }

    def initialize_payment_for_order(self, order, callback_url):
        payload = self._initialize_payload(order, callback_url)
        response = self._post_json("/payment/iyzipos/checkoutform/initialize/auth/ecom", payload)
        self.verify_response_signature_payload("checkout_initialize", response)
        if str(response.get("status", "")).lower() != "success":
            raise PaymentVerificationError(response.get("errorMessage") or "Iyzico checkout form initialization failed.")
        token = response.get("token")
        payment_page_url = response.get("paymentPageUrl") or response.get("checkoutFormContent")
        if not token or not payment_page_url:
            raise PaymentVerificationError("Iyzico initialize response is missing token or paymentPageUrl.")
        return {
            "provider": self.provider_name,
            "session_id": token,
            "provider_token": token,
            "provider_conversation_id": payload["conversationId"],
            "redirect_url": payment_page_url,
            "paymentPageUrl": payment_page_url,
            "mode": "checkout_form",
            "raw": response,
        }

    def retrieve_checkout_form(self, token, conversation_id):
        payload = {
            "locale": "tr",
            "conversationId": conversation_id,
            "token": token,
        }
        response = self._post_json("/payment/iyzipos/checkoutform/auth/ecom/detail", payload)
        self.verify_response_signature_payload("checkout_retrieve", response)
        return response

    def refund_order_payment(self, order, amount, reason=""):
        transaction_id = str(getattr(order, "provider_transaction_id", "") or "").strip()
        if not transaction_id:
            raise PaymentConfigurationError("Iyzico refund requires provider_transaction_id from the verified payment payload.")
        refund_amount = _amount(amount)
        payload = {
            "locale": "tr",
            "conversationId": getattr(order, "provider_conversation_id", None) or getattr(order, "public_token", None) or getattr(order, "order_token", ""),
            "paymentTransactionId": transaction_id,
            "price": refund_amount,
            "currency": getattr(order, "currency", None) or "TRY",
            "ip": _env("IYZICO_DEFAULT_IP", "127.0.0.1"),
            "reason": str(reason or "")[:256],
        }
        response = self._post_json("/payment/refund", payload)
        self.verify_response_signature_payload("refund", response)
        if str(response.get("status", "")).lower() != "success":
            raise PaymentVerificationError(response.get("errorMessage") or "Iyzico refund failed.")
        return {
            "provider": self.provider_name,
            "status": "refunded",
            "refund_amount": refund_amount,
            "refund_reference": response.get("refundId") or response.get("paymentId") or response.get("conversationId") or transaction_id,
            "provider_status": response.get("status"),
            "raw": response,
        }

    def retrieve_payment_detail(self, payment_id, conversation_id=None):
        if not payment_id:
            raise PaymentVerificationError("Iyzico payment detail requires paymentId.")
        payload = {
            "locale": "tr",
            "conversationId": conversation_id or str(uuid.uuid4()),
            "paymentId": str(payment_id),
        }
        response = self._post_json(_env("IYZICO_PAYMENT_DETAIL_PATH", "/payment/detail"), payload)
        self.verify_response_signature_payload("checkout_retrieve", response)
        return response

    def _configured_payment_link(self, order):
        service_type = str(getattr(order, "service_type", "") or "").strip().lower()
        product_type = str(getattr(order, "product_type", "") or "").strip().upper()
        candidates = []
        if service_type == "consultation":
            candidates.append("IYZICO_CONSULTATION_PAYMENT_LINK")
        elif service_type == "report":
            candidates.extend([
                f"IYZICO_REPORT_PAYMENT_LINK_{product_type}",
                "IYZICO_REPORT_PAYMENT_LINK",
            ])
        candidates.append("IYZICO_PAYMENT_LINK_BASE")
        for key in candidates:
            value = str(os.getenv(key, "")).strip()
            if value:
                return value
        raise PaymentConfigurationError(
            "Iyzico payment link is not configured for this service order."
        )

    def create_service_payment_link(self, order, success_url, cancel_url):
        payment_link = self._configured_payment_link(order)
        separator = "&" if "?" in payment_link else "?"
        handoff_query = urlencode({
            "order_token": getattr(order, "order_token", ""),
            "service_type": getattr(order, "service_type", ""),
            "product_type": getattr(order, "product_type", ""),
            "success_url": success_url,
            "cancel_url": cancel_url,
        })
        return {
            "provider": self.provider_name,
            "session_id": getattr(order, "order_token", ""),
            "redirect_url": f"{payment_link}{separator}{handoff_query}",
            "mode": "payment_link",
        }

    def create_service_checkout_session(self, order, success_url, cancel_url):
        return self.initialize_payment_for_order(order, success_url)

    def verify_payment(self, data):
        raise NotImplementedError("Iyzico payment verification is not implemented yet.")

    def verify_service_payment(self, data, order=None):
        token = str(data or "").strip()
        conversation_id = getattr(order, "provider_conversation_id", None) or getattr(order, "public_token", None) or getattr(order, "order_token", "")
        return self.retrieve_checkout_form(token, conversation_id)

    def finalize_purchase(self, report, payment_data):
        payment_reference = payment_data.get("payment_reference")
        if getattr(report, "payment_reference", None) == payment_reference and bool(getattr(report, "is_paid", False)):
            if not getattr(report, "pdf_ready", False):
                report.pdf_ready = True
            return False
        report.access_state = "purchased"
        report.is_paid = True
        report.pdf_ready = True
        if payment_reference:
            report.payment_reference = payment_reference
        if not getattr(report, "unlocked_at", None):
            report.unlocked_at = payment_data.get("completed_at")
        return True

    def verify_webhook(self, request):
        raise NotImplementedError("Iyzico webhook/callback verification is not implemented yet.")
