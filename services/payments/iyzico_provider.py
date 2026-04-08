from .base import PaymentProvider


class IyzicoProvider(PaymentProvider):
    provider_name = "iyzico"

    def create_checkout_session(self, report, user, success_url, cancel_url):
        raise NotImplementedError("Iyzico checkout session initialization is not implemented yet.")

    def verify_payment(self, data):
        raise NotImplementedError("Iyzico payment verification is not implemented yet.")

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
