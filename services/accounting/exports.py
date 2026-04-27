from __future__ import annotations

import csv
import io
import zipfile


def transactions_csv(transactions) -> str:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id", "order_id", "customer", "product_type", "gross", "commission", "refunded", "net", "payment_status", "invoice_status", "paid_at"])
    for t in transactions:
        writer.writerow([
            t.id,
            t.service_order_id,
            getattr(t.customer, "email", ""),
            t.product_type,
            t.gross_amount,
            t.commission_amount,
            t.refunded_amount,
            t.net_amount,
            t.payment_status,
            t.invoice_status,
            t.paid_at,
        ])
    return out.getvalue()


def invoices_csv(invoices) -> str:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["invoice_number", "customer", "transaction_id", "issue_date", "total", "status"])
    for invoice in invoices:
        writer.writerow([
            invoice.invoice_number or invoice.id,
            getattr(invoice.customer, "email", ""),
            invoice.transaction_id,
            invoice.issue_date,
            invoice.total_amount,
            invoice.status,
        ])
    return out.getvalue()


def monthly_finance_zip(*, transactions, invoices) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("transactions.csv", transactions_csv(transactions))
        zf.writestr("invoices.csv", invoices_csv(invoices))
        zf.writestr("README.txt", "Operational finance export only. Not an official tax filing package.\n")
    return buffer.getvalue()
