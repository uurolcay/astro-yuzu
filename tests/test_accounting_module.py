import unittest
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import database as db_mod
from services.accounting import document_service, invoice_service, month_close_service, reminders_service, tax_service, transaction_service
from services.accounting.calculations import estimated_tax, estimated_vat, invoice_coverage_ratio, net_after_commission


class AccountingModuleTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        db_mod.Base.metadata.create_all(bind=engine)
        self.Session = sessionmaker(bind=engine)
        self.db = self.Session()

    def tearDown(self):
        self.db.close()

    def _paid_order(self, **overrides):
        data = {
            "order_token": f"tok-{datetime.utcnow().timestamp()}",
            "service_type": "report",
            "product_type": "natal",
            "status": "paid",
            "customer_name": "Test User",
            "customer_email": "test@example.com",
            "amount": Decimal("1200.00"),
            "currency": "TRY",
            "paid_at": datetime.utcnow(),
        }
        data.update(overrides)
        order = db_mod.ServiceOrder(**data)
        self.db.add(order)
        self.db.commit()
        return order

    def test_transaction_creation_from_paid_order(self):
        order = self._paid_order()
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        self.assertIsNotNone(tx)
        self.assertEqual(tx.service_order_id, order.id)
        self.assertEqual(tx.payment_status, "paid")
        self.assertEqual(tx.invoice_status, "uninvoiced")

    def test_uninvoiced_queue_logic(self):
        order = self._paid_order()
        transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        queue = transaction_service.uninvoiced_paid_transactions(self.db)
        self.assertEqual(len(queue), 1)

    def test_invoice_draft_and_issue_flow(self):
        order = self._paid_order()
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        invoice = invoice_service.create_draft_invoice(self.db, tx.id)
        self.assertEqual(invoice.status, "draft")
        self.assertEqual(tx.invoice_status, "draft")
        issued = invoice_service.issue_invoice(self.db, invoice.id)
        self.assertEqual(issued.status, "issued")
        self.assertTrue(issued.invoice_number.startswith("FA-"))
        self.assertEqual(tx.invoice_status, "issued")

    def test_dashboard_metric_helpers(self):
        order = self._paid_order()
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        self.assertGreater(net_after_commission([tx]), Decimal("0.00"))
        self.assertEqual(invoice_coverage_ratio(2, 1), Decimal("50.00"))

    def test_tax_estimate_helpers(self):
        self.assertEqual(estimated_vat(Decimal("120.00"), 20), Decimal("20.00"))
        self.assertEqual(estimated_tax(Decimal("1000.00"), 20), Decimal("200.00"))

    def test_reminder_creation(self):
        order = self._paid_order()
        transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        reminders = reminders_service.create_operational_reminders(self.db)
        self.db.commit()
        self.assertTrue(any(r.reminder_type == "uninvoiced_transaction" for r in reminders))
        self.assertGreater(self.db.query(db_mod.Reminder).count(), 0)

    def test_blocked_transaction_shows_missing_info_state(self):
        order = self._paid_order(customer_name="", customer_email="missing@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        readiness = invoice_service.invoice_readiness(self.db, tx)
        self.assertEqual(readiness["status"], "missing_info")
        self.assertIn("full name", readiness["missing_fields"])
        self.assertIn("individual identity number", readiness["missing_fields"])

    def test_ready_transaction_can_create_draft_from_queue(self):
        order = self._paid_order(customer_email="ready@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        customer = self.db.query(db_mod.Customer).filter(db_mod.Customer.id == tx.customer_id).first()
        customer.tax_id = "11111111111"
        customer.billing_address = "Istanbul"
        self.db.commit()
        rows, summary = invoice_service.invoice_queue_rows(self.db, tab="ready")
        self.assertEqual(summary["ready"], 1)
        self.assertEqual(len(rows), 1)
        invoice = invoice_service.create_draft_invoice(self.db, rows[0]["transaction"].id)
        self.assertEqual(invoice.status, "draft")

    def test_existing_draft_changes_available_action_state(self):
        order = self._paid_order(customer_email="draft@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        customer = self.db.query(db_mod.Customer).filter(db_mod.Customer.id == tx.customer_id).first()
        customer.tax_id = "11111111111"
        customer.billing_address = "Istanbul"
        self.db.commit()
        invoice = invoice_service.create_draft_invoice(self.db, tx.id)
        self.db.commit()
        readiness = invoice_service.invoice_readiness(self.db, tx)
        self.assertEqual(readiness["status"], "draft_exists")
        self.assertEqual(readiness["invoice"].id, invoice.id)
        self.assertFalse(readiness["can_create"])

    def test_bulk_draft_creation_skips_blocked_rows(self):
        ready_order = self._paid_order(order_token="ready-bulk", customer_email="ready-bulk@example.com")
        blocked_order = self._paid_order(order_token="blocked-bulk", customer_name="", customer_email="blocked-bulk@example.com")
        ready_tx = transaction_service.sync_transaction_from_order(self.db, ready_order)
        blocked_tx = transaction_service.sync_transaction_from_order(self.db, blocked_order)
        ready_customer = self.db.query(db_mod.Customer).filter(db_mod.Customer.id == ready_tx.customer_id).first()
        ready_customer.tax_id = "11111111111"
        ready_customer.billing_address = "Istanbul"
        self.db.commit()
        result = invoice_service.create_drafts_for_ready_transactions(self.db, [ready_tx.id, blocked_tx.id])
        self.assertEqual(len(result["created"]), 1)
        self.assertEqual(len(result["skipped"]), 1)

    def test_queue_summary_counts_and_filters(self):
        ready_order = self._paid_order(order_token="ready-filter", product_type="career", customer_email="ready-filter@example.com")
        blocked_order = self._paid_order(order_token="blocked-filter", product_type="natal", customer_name="", customer_email="blocked-filter@example.com")
        ready_tx = transaction_service.sync_transaction_from_order(self.db, ready_order)
        transaction_service.sync_transaction_from_order(self.db, blocked_order)
        ready_customer = self.db.query(db_mod.Customer).filter(db_mod.Customer.id == ready_tx.customer_id).first()
        ready_customer.tax_id = "11111111111"
        ready_customer.billing_address = "Istanbul"
        self.db.commit()
        rows, summary = invoice_service.invoice_queue_rows(self.db, tab="all")
        self.assertEqual(summary["ready"], 1)
        self.assertGreaterEqual(summary["missing_info"], 1)
        filtered_rows, _ = invoice_service.invoice_queue_rows(self.db, tab="ready", product_type="career")
        self.assertEqual(len(filtered_rows), 1)
        self.assertEqual(filtered_rows[0]["transaction"].product_type, "career")

    def test_updating_missing_individual_fields_changes_readiness_to_ready(self):
        order = self._paid_order(order_token="individual-fix", customer_name="", customer_email="individual-fix@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        result = invoice_service.update_customer_billing_info(self.db, tx.id, {
            "customer_type": "individual",
            "full_name": "Ayse Yilmaz",
            "email": "ayse@example.com",
            "identity_number": "12345678901",
            "address_line": "Istanbul",
            "city": "Istanbul",
            "country": "TR",
        })
        self.assertTrue(result["ok"])
        self.assertEqual(result["readiness"]["status"], "ready")
        self.assertTrue(result["readiness"]["can_create"])

    def test_updating_missing_company_fields_changes_readiness_to_ready(self):
        order = self._paid_order(order_token="company-fix", customer_name="", customer_email="company-fix@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        result = invoice_service.update_customer_billing_info(self.db, tx.id, {
            "customer_type": "company",
            "company_name": "Focus Test Ltd",
            "email": "finance@focus-test.com",
            "tax_number": "1234567890",
            "tax_office": "Besiktas",
            "address_line": "Istanbul",
            "city": "Istanbul",
            "country": "TR",
        })
        self.assertTrue(result["ok"])
        self.assertEqual(result["readiness"]["status"], "ready")

    def test_invalid_billing_save_keeps_row_blocked(self):
        order = self._paid_order(order_token="invalid-fix", customer_name="", customer_email="invalid-fix@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        result = invoice_service.update_customer_billing_info(self.db, tx.id, {
            "customer_type": "individual",
            "full_name": "",
            "email": "not-an-email",
            "identity_number": "",
            "address_line": "",
        })
        self.assertFalse(result["ok"])
        readiness = invoice_service.invoice_readiness(self.db, tx)
        self.assertEqual(readiness["status"], "missing_info")

    def test_draft_creation_available_after_billing_info_fix(self):
        order = self._paid_order(order_token="fix-to-draft", customer_name="", customer_email="fix-to-draft@example.com")
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        result = invoice_service.update_customer_billing_info(self.db, tx.id, {
            "customer_type": "individual",
            "full_name": "Ready Person",
            "email": "ready-person@example.com",
            "identity_number": "12345678901",
            "address_line": "Istanbul",
        })
        self.assertTrue(result["readiness"]["can_create"])
        invoice = invoice_service.create_draft_invoice(self.db, tx.id)
        self.assertEqual(invoice.status, "draft")

    def test_blocked_row_exposes_fix_billing_info_action(self):
        template = Path("templates/admin/accounting/invoices.html").read_text(encoding="utf-8")
        self.assertIn("Fix Billing Info", template)
        self.assertIn("return_to", template)
        self.assertIn("/admin/accounting/transactions/{{ t.id }}/billing-info", template)

    def _issued_invoice(self, email="pdf@example.com"):
        order = self._paid_order(order_token=f"invoice-{email}", customer_email=email)
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        customer = self.db.query(db_mod.Customer).filter(db_mod.Customer.id == tx.customer_id).first()
        customer.tax_id = "11111111111"
        customer.identity_number = "11111111111"
        customer.billing_address = "Istanbul"
        self.db.commit()
        invoice = invoice_service.create_draft_invoice(self.db, tx.id)
        invoice_service.issue_invoice(self.db, invoice.id)
        self.db.commit()
        return invoice

    def test_invoice_list_template_shows_pdf_and_send_state(self):
        template = Path("templates/admin/accounting/invoices.html").read_text(encoding="utf-8")
        self.assertIn("PDF</th><th>Send", template)
        self.assertIn("PDF ready", template)
        self.assertIn("Retry Send", template)

    def test_draft_invoice_without_pdf_shows_generate_action(self):
        template = Path("templates/admin/accounting/invoices.html").read_text(encoding="utf-8")
        self.assertIn("Generate PDF", template)
        self.assertIn("Send Invoice", template)

    def test_pdf_ready_invoice_enables_download(self):
        invoice = self._issued_invoice()
        with tempfile.TemporaryDirectory() as tmp:
            invoice_service.generate_invoice_pdf(invoice, tmp)
            self.assertEqual(invoice.pdf_status, "ready")
            self.assertTrue(Path(invoice.pdf_path).exists())

    def test_issued_pdf_ready_not_sent_is_awaiting_send(self):
        invoice = self._issued_invoice()
        invoice.pdf_status = "ready"
        invoice.pdf_path = __file__
        invoice.send_status = "not_sent"
        self.assertTrue(invoice_service.is_awaiting_send(invoice))

    def test_send_failure_shows_retry_state(self):
        invoice = self._issued_invoice()
        invoice.pdf_status = "ready"
        invoice.pdf_path = "missing.pdf"
        ok = invoice_service.send_invoice_email(invoice, lambda **kwargs: True)
        self.assertFalse(ok)
        self.assertEqual(invoice.send_status, "failed")
        self.assertIn("PDF", invoice.send_error_message)

    def test_retry_send_updates_status_when_successful(self):
        invoice = self._issued_invoice()
        invoice.pdf_status = "ready"
        invoice.pdf_path = __file__
        invoice.send_status = "failed"
        ok = invoice_service.send_invoice_email(invoice, lambda **kwargs: True)
        self.assertTrue(ok)
        self.assertEqual(invoice.send_status, "sent")
        self.assertIsNotNone(invoice.sent_at)

    def test_missing_customer_email_blocks_send_readably(self):
        invoice = self._issued_invoice(email="missing-email@example.com")
        invoice.customer.email = ""
        invoice.pdf_status = "ready"
        invoice.pdf_path = __file__
        ok = invoice_service.send_invoice_email(invoice, lambda **kwargs: True)
        self.assertFalse(ok)
        self.assertEqual(invoice.send_status, "failed")
        self.assertIn("email", invoice.send_error_message.lower())

    def test_month_summary_metrics_calculate_correctly(self):
        order = self._paid_order(order_token="month-metrics", paid_at=datetime(2026, 4, 10), amount=Decimal("1200.00"))
        tx = transaction_service.sync_transaction_from_order(self.db, order)
        tx.refunded_amount = Decimal("200.00")
        tx.net_amount = Decimal("958.00")
        self.db.add(db_mod.Expense(expense_date=datetime(2026, 4, 12), supplier="Tool", category="software", amount=Decimal("100.00")))
        self.db.commit()
        summary = month_close_service.monthly_summary(self.db, 2026, 4)
        self.assertEqual(summary["gross_revenue"], Decimal("1200.00"))
        self.assertEqual(summary["refunded_amount"], Decimal("200.00"))
        self.assertEqual(summary["commission_total"], Decimal("42.00"))
        self.assertEqual(summary["total_expense_amount"], Decimal("100.00"))

    def test_month_readiness_checklist_identifies_blockers(self):
        order = self._paid_order(order_token="month-blocker", paid_at=datetime(2026, 4, 10), customer_name="", customer_email="blocker@example.com")
        transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        checks = month_close_service.readiness_checklist(self.db, 2026, 4)
        states = {check["key"]: check["state"] for check in checks}
        self.assertEqual(states["all_paid_invoiced"], "blocker")
        self.assertEqual(states["missing_billing_info"], "blocker")

    def test_mark_reviewed_action_stores_reviewed_status_and_note(self):
        result = month_close_service.mark_reviewed(self.db, 2026, 4, "admin@example.com", note="Reviewed with accountant.", confirmed=True)
        self.assertTrue(result["ok"])
        self.db.commit()
        period = self.db.query(db_mod.TaxPeriod).filter(db_mod.TaxPeriod.period_key == "2026-04").first()
        self.assertIsNotNone(period.reviewed_at)
        self.assertEqual(period.reviewed_by, "admin@example.com")
        self.assertIn("Reviewed", period.notes)
        self.assertTrue(period.snapshot_json)

    def test_warning_blocker_logic_requires_confirmation(self):
        order = self._paid_order(order_token="month-confirm", paid_at=datetime(2026, 4, 10), customer_name="", customer_email="confirm@example.com")
        transaction_service.sync_transaction_from_order(self.db, order)
        self.db.commit()
        result = month_close_service.mark_reviewed(self.db, 2026, 4, "admin@example.com", confirmed=False)
        self.assertFalse(result["ok"])
        self.assertIn("blockers", result["reason"].lower())

    def test_month_export_links_context_exists(self):
        template = Path("templates/admin/accounting/month_close.html").read_text(encoding="utf-8")
        self.assertIn("/admin/accounting/exports/monthly.zip?{{ export_query }}", template)
        self.assertIn("Month-End Review", template)

    def test_month_close_page_is_admin_protected(self):
        from fastapi.testclient import TestClient
        from app import app

        response = TestClient(app).get("/admin/accounting/month-close", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})

    def test_invoice_pdfs_appear_in_archive(self):
        invoice = self._issued_invoice(email="archive-invoice@example.com")
        with tempfile.TemporaryDirectory() as tmp:
            invoice_service.generate_invoice_pdf(invoice, tmp)
            docs, summary = document_service.archive_documents(self.db, base_dir=tmp, document_type="invoice_pdf")
            self.assertTrue(any(doc["invoice_id"] == invoice.id and doc["file_status"] == "ready" for doc in docs))
            self.assertGreaterEqual(summary["invoice_pdfs"], 1)

    def test_monthly_exports_appear_in_archive(self):
        month_close_service.get_or_create_period(self.db, 2026, 4)
        self.db.commit()
        docs, summary = document_service.archive_documents(self.db, year=2026, month=4, month_close_only=True)
        self.assertTrue(any(doc["document_type"] == "monthly_zip" and doc["related_month"] == "2026-04" for doc in docs))
        self.assertGreaterEqual(summary["monthly_exports"], 1)

    def test_expense_receipts_appear_in_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt = Path(tmp) / "static" / "accounting" / "receipt.pdf"
            receipt.parent.mkdir(parents=True)
            receipt.write_text("receipt", encoding="utf-8")
            expense = db_mod.Expense(expense_date=datetime(2026, 4, 3), supplier="Receipt Co", amount=Decimal("50.00"), receipt_path="static/accounting/receipt.pdf")
            self.db.add(expense)
            self.db.commit()
            docs, summary = document_service.archive_documents(self.db, base_dir=tmp, expense_only=True)
            self.assertTrue(any(doc["expense_id"] == expense.id and doc["file_status"] == "ready" for doc in docs))
            self.assertEqual(summary["expense_receipts"], 1)

    def test_missing_file_state_is_detected(self):
        invoice = self._issued_invoice(email="missing-file@example.com")
        invoice.pdf_status = "ready"
        invoice.pdf_path = "static/accounting/missing.pdf"
        self.db.commit()
        docs, _summary = document_service.archive_documents(self.db, base_dir=".", document_type="invoice_pdf")
        row = next(doc for doc in docs if doc["invoice_id"] == invoice.id)
        self.assertEqual(row["file_status"], "missing")

    def test_archive_filters_work(self):
        month_close_service.get_or_create_period(self.db, 2026, 4)
        month_close_service.get_or_create_period(self.db, 2026, 5)
        self.db.commit()
        docs, _summary = document_service.archive_documents(self.db, year=2026, month=4, document_type="monthly_zip")
        self.assertTrue(docs)
        self.assertTrue(all(doc["related_month"] == "2026-04" for doc in docs))

    def test_archive_related_links_actions_render(self):
        template = Path("templates/admin/accounting/documents.html").read_text(encoding="utf-8")
        self.assertIn("Open Detail", template)
        self.assertIn("Download", template)
        self.assertIn("Generate PDF", template)

    def test_documents_archive_is_admin_protected(self):
        from fastapi.testclient import TestClient
        from app import app

        response = TestClient(app).get("/admin/accounting/documents", follow_redirects=False)
        self.assertIn(response.status_code, {302, 303, 307, 401, 403})


if __name__ == "__main__":
    unittest.main()
