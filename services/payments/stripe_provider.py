import os
from datetime import datetime

from .base import PaymentConfigurationError, PaymentProvider, PaymentVerificationError


def _report_price_currency():
    return str(os.getenv("REPORT_PRICE_CURRENCY", "usd")).strip().lower() or "usd"


def _report_price_amount():
    raw = str(os.getenv("REPORT_PRICE_AMOUNT", "")).strip()
    return int(raw) if raw else None


def _report_price_id():
    return str(os.getenv("REPORT_PRICE_ID", "")).strip() or None


def _require_stripe_module():
    secret_key = str(os.getenv("STRIPE_SECRET_KEY", "")).strip()
    if not secret_key:
        raise PaymentConfigurationError("STRIPE_SECRET_KEY is not configured.")
    try:
        import stripe
    except Exception as exc:
        raise PaymentConfigurationError("Stripe SDK is not installed.") from exc
    stripe.api_key = secret_key
    return stripe


def _checkout_metadata(report, user):
    return {
        "kind": "report_unlock",
        "report_id": str(getattr(report, "id", "")),
        "user_id": str(getattr(user, "id", "")),
        "user_email": str(getattr(user, "email", "")),
    }


class StripeProvider(PaymentProvider):
    provider_name = "stripe"

    def create_checkout_session(self, report, user, success_url, cancel_url):
        stripe = _require_stripe_module()
        line_item = None
        price_id = _report_price_id()
        amount = _report_price_amount()
        if price_id:
            line_item = {"price": price_id, "quantity": 1}
        elif amount:
            line_item = {
                "price_data": {
                    "currency": _report_price_currency(),
                    "unit_amount": amount,
                    "product_data": {
                        "name": "Full Vedic Reading Unlock",
                        "description": "Unlock the full reading, premium report access, and PDF delivery.",
                    },
                },
                "quantity": 1,
            }
        else:
            raise PaymentConfigurationError("Configure REPORT_PRICE_ID or REPORT_PRICE_AMOUNT.")

        session = stripe.checkout.Session.create(
            mode="payment",
            success_url=success_url,
            cancel_url=cancel_url,
            line_items=[line_item],
            metadata=_checkout_metadata(report, user),
            client_reference_id=str(getattr(report, "id", "")),
            customer_email=str(getattr(user, "email", "")),
        )
        return {
            "provider": self.provider_name,
            "session_id": session.get("id"),
            "redirect_url": session.get("url"),
            "raw": session,
        }

    def verify_payment(self, data):
        stripe = _require_stripe_module()
        session_id = str(data or "").strip()
        session = stripe.checkout.Session.retrieve(session_id, expand=["payment_intent"])
        metadata = session.get("metadata") or {}
        if session.get("payment_status") != "paid":
            raise PaymentVerificationError("Checkout session is not paid.")
        report_id = metadata.get("report_id")
        user_id = metadata.get("user_id")
        if not report_id or not user_id:
            raise PaymentVerificationError("Checkout session metadata is incomplete.")
        payment_intent = session.get("payment_intent")
        payment_reference = payment_intent.get("id") if isinstance(payment_intent, dict) else None
        payment_reference = payment_reference or session.get("payment_intent") or session.get("id")
        return {
            "provider": self.provider_name,
            "session_id": session.get("id"),
            "payment_reference": payment_reference,
            "payment_status": session.get("payment_status"),
            "report_id": int(report_id),
            "user_id": int(user_id),
            "customer_email": metadata.get("user_email") or session.get("customer_email"),
            "completed_at": datetime.utcnow(),
            "raw": session,
        }

    def finalize_purchase(self, report, payment_data):
        payment_reference = payment_data.get("payment_reference")
        if getattr(report, "payment_reference", None) == payment_reference and bool(getattr(report, "is_paid", False)):
            if not getattr(report, "pdf_ready", False):
                report.pdf_ready = True
            if not getattr(report, "unlocked_at", None):
                report.unlocked_at = payment_data.get("completed_at") or datetime.utcnow()
            if getattr(report, "access_state", None) not in {"purchased", "delivered"}:
                report.access_state = "purchased"
            return False
        report.access_state = "purchased"
        report.is_paid = True
        report.pdf_ready = True
        report.unlocked_at = getattr(report, "unlocked_at", None) or payment_data.get("completed_at") or datetime.utcnow()
        if payment_reference:
            report.payment_reference = payment_reference
        return True

    def verify_webhook(self, request):
        stripe = _require_stripe_module()
        webhook_secret = str(os.getenv("STRIPE_WEBHOOK_SECRET", "")).strip()
        if not webhook_secret:
            raise PaymentConfigurationError("STRIPE_WEBHOOK_SECRET is not configured.")
        payload_bytes = request.get("payload_bytes") or b""
        signature_header = request.get("signature_header") or ""
        try:
            event = stripe.Webhook.construct_event(payload_bytes, signature_header, webhook_secret)
        except Exception as exc:
            raise PaymentVerificationError("Webhook signature verification failed.") from exc
        event_type = str((event or {}).get("type") or "").strip()
        data_object = ((event or {}).get("data") or {}).get("object") or {}
        metadata = data_object.get("metadata") or {}
        if event_type != "checkout.session.completed" or data_object.get("payment_status") != "paid":
            return None
        report_id = metadata.get("report_id")
        user_id = metadata.get("user_id")
        if not report_id or not user_id:
            raise PaymentVerificationError("Webhook payload is missing report metadata.")
        return {
            "provider": self.provider_name,
            "session_id": data_object.get("id"),
            "payment_reference": data_object.get("payment_intent") or data_object.get("id"),
            "payment_status": data_object.get("payment_status"),
            "report_id": int(report_id),
            "user_id": int(user_id),
            "customer_email": metadata.get("user_email") or data_object.get("customer_email"),
            "completed_at": datetime.utcnow(),
            "raw": event,
        }
