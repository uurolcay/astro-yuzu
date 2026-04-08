import os

from .base import PaymentConfigurationError, PaymentError, PaymentProvider, PaymentVerificationError
from .iyzico_provider import IyzicoProvider
from .stripe_provider import StripeProvider


def _env_flag(name, default=False):
    raw = str(os.getenv(name, "true" if default else "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def payment_provider():
    return str(os.getenv("PAYMENT_PROVIDER", "stripe")).strip().lower() or "stripe"


def payments_enabled():
    return _env_flag("PAYMENTS_ENABLED", default=False)


def beta_free_unlock_enabled():
    return _env_flag("BETA_FREE_UNLOCK", default=False)


def beta_unlock_allowlist():
    raw = os.getenv("BETA_UNLOCK_EMAILS", "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def can_use_beta_free_unlock(user):
    if not user or not beta_free_unlock_enabled():
        return False
    email = str(getattr(user, "email", "")).strip().lower()
    allowlist = beta_unlock_allowlist()
    return bool(email and allowlist and email in allowlist)


def get_payment_provider():
    provider = payment_provider()
    if provider == "iyzico":
        return IyzicoProvider()
    return StripeProvider()


__all__ = [
    "PaymentConfigurationError",
    "PaymentError",
    "PaymentProvider",
    "PaymentVerificationError",
    "StripeProvider",
    "IyzicoProvider",
    "beta_free_unlock_enabled",
    "beta_unlock_allowlist",
    "can_use_beta_free_unlock",
    "get_payment_provider",
    "payment_provider",
    "payments_enabled",
]
