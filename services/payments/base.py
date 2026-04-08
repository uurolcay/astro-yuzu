from abc import ABC, abstractmethod


class PaymentError(Exception):
    """Base payment error."""


class PaymentConfigurationError(PaymentError):
    """Raised when provider configuration is missing or invalid."""


class PaymentVerificationError(PaymentError):
    """Raised when a payment session or webhook cannot be verified."""


class PaymentProvider(ABC):
    @abstractmethod
    def create_checkout_session(self, report, user, success_url, cancel_url):
        raise NotImplementedError

    @abstractmethod
    def verify_payment(self, data):
        raise NotImplementedError

    @abstractmethod
    def finalize_purchase(self, report, payment_data):
        raise NotImplementedError

    @abstractmethod
    def verify_webhook(self, request):
        raise NotImplementedError
