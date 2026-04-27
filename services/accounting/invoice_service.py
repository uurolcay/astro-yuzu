from __future__ import annotations

import os
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import database as db_mod
from services.accounting.calculations import estimated_vat, money
from weasyprint import HTML as WeasyHTML


def next_invoice_number(db, issue_date=None) -> str:
    issue_date = issue_date or datetime.utcnow()
    prefix = f"FA-{issue_date:%Y%m}"
    count = db.query(db_mod.Invoice).filter(db_mod.Invoice.invoice_number.like(f"{prefix}-%")).count() + 1
    return f"{prefix}-{count:04d}"


def create_draft_invoice(db, transaction_id: int):
    transaction = db.query(db_mod.Transaction).filter(db_mod.Transaction.id == transaction_id).first()
    if not transaction:
        raise ValueError("Transaction not found")
    existing = db.query(db_mod.Invoice).filter(db_mod.Invoice.transaction_id == transaction.id, db_mod.Invoice.status != "cancelled").first()
    if existing:
        return existing
    subtotal = money(transaction.net_amount)
    vat = estimated_vat(subtotal, 20)
    invoice = db_mod.Invoice(
        transaction_id=transaction.id,
        customer_id=transaction.customer_id,
        status="draft",
        subtotal=subtotal - vat,
        vat_amount=vat,
        total_amount=subtotal,
        currency=transaction.currency or "TRY",
        due_date=datetime.utcnow() + timedelta(days=7),
        pdf_status="not_generated",
        send_status="not_sent",
    )
    db.add(invoice)
    db.flush()
    item = db_mod.InvoiceItem(
        invoice_id=invoice.id,
        description=f"Focus Astrology - {transaction.product_type or transaction.service_type or 'Service'}",
        quantity=Decimal("1.00"),
        unit_price=subtotal - vat,
        vat_rate=Decimal("20.00"),
        line_total=subtotal,
    )
    db.add(item)
    transaction.invoice_status = "draft"
    db.flush()
    return invoice


def invoice_for_transaction(db, transaction_id: int):
    return (
        db.query(db_mod.Invoice)
        .filter(db_mod.Invoice.transaction_id == transaction_id, db_mod.Invoice.status != "cancelled")
        .order_by(db_mod.Invoice.created_at.desc())
        .first()
    )


def invoice_readiness(db, transaction) -> dict:
    invoice = invoice_for_transaction(db, transaction.id)
    if invoice:
        if invoice.status == "draft":
            return {"status": "draft_exists", "label": "Draft exists", "missing_fields": [], "reason": "", "invoice": invoice, "can_create": False}
        return {"status": "issued_already", "label": f"{invoice.status.title()} invoice", "missing_fields": [], "reason": "", "invoice": invoice, "can_create": False}

    if transaction.payment_status != "paid":
        return {
            "status": "blocked",
            "label": "Not invoiceable",
            "missing_fields": [],
            "reason": f"Payment status is {transaction.payment_status}.",
            "invoice": None,
            "can_create": False,
        }

    customer = transaction.customer
    missing = []
    if not customer:
        missing.extend(["customer"])
    else:
        customer_type = (customer.customer_type or "individual").strip().lower()
        if not customer.email or customer.email.endswith("@example.local"):
            missing.append("email")
        if not customer.billing_address:
            missing.append("billing address")
        if customer_type == "company":
            if not customer.company_name:
                missing.append("company name")
            if not customer.tax_id:
                missing.append("company tax number")
            if not customer.tax_office:
                missing.append("company tax office")
        else:
            if not customer.name:
                missing.append("full name")
            if not (customer.identity_number or customer.tax_id):
                missing.append("individual identity number")

    if missing:
        return {
            "status": "missing_info",
            "label": "Missing billing info",
            "missing_fields": missing,
            "reason": ", ".join(missing),
            "invoice": None,
            "can_create": False,
        }

    return {"status": "ready", "label": "Ready", "missing_fields": [], "reason": "", "invoice": None, "can_create": True}


def validate_customer_billing_payload(form_data) -> tuple[dict, list[str]]:
    customer_type = str(form_data.get("customer_type", "individual") or "individual").strip().lower()
    if customer_type not in {"individual", "company"}:
        customer_type = "individual"
    payload = {
        "customer_type": customer_type,
        "name": str(form_data.get("full_name", "") or "").strip(),
        "email": str(form_data.get("email", "") or "").strip().lower(),
        "identity_number": str(form_data.get("identity_number", "") or "").strip(),
        "company_name": str(form_data.get("company_name", "") or "").strip(),
        "tax_id": str(form_data.get("tax_number", "") or form_data.get("tax_id", "") or "").strip(),
        "tax_office": str(form_data.get("tax_office", "") or "").strip(),
        "billing_address": str(form_data.get("address_line", "") or form_data.get("billing_address", "") or "").strip(),
        "city": str(form_data.get("city", "") or "").strip(),
        "country": str(form_data.get("country", "") or "TR").strip() or "TR",
    }
    errors = []
    if "@" not in payload["email"]:
        errors.append("Valid email is required.")
    if not payload["billing_address"]:
        errors.append("Billing address is required.")
    if customer_type == "company":
        if not payload["company_name"]:
            errors.append("Company name is required.")
        if not payload["tax_id"]:
            errors.append("Company tax number/VKN is required.")
        if not payload["tax_office"]:
            errors.append("Tax office is required.")
    else:
        if not payload["name"]:
            errors.append("Full name is required.")
        if not payload["identity_number"]:
            errors.append("Identity number/TCKN is required.")
    return payload, errors


def update_customer_billing_info(db, transaction_id: int, form_data):
    transaction = db.query(db_mod.Transaction).filter(db_mod.Transaction.id == transaction_id).first()
    if not transaction:
        raise ValueError("Transaction not found")
    payload, errors = validate_customer_billing_payload(form_data)
    if errors:
        return {"ok": False, "errors": errors, "transaction": transaction, "readiness": invoice_readiness(db, transaction)}

    customer = transaction.customer
    if not customer:
        customer = db_mod.Customer(email=payload["email"])
        db.add(customer)
        db.flush()
        transaction.customer_id = customer.id
        transaction.customer = customer
    customer.customer_type = payload["customer_type"]
    customer.email = payload["email"]
    customer.name = payload["name"] or payload["company_name"]
    customer.identity_number = payload["identity_number"]
    customer.company_name = payload["company_name"]
    customer.tax_id = payload["tax_id"] or payload["identity_number"]
    customer.tax_office = payload["tax_office"]
    customer.billing_address = payload["billing_address"]
    customer.city = payload["city"]
    customer.country = payload["country"]
    db.flush()
    return {"ok": True, "errors": [], "transaction": transaction, "customer": customer, "readiness": invoice_readiness(db, transaction)}


def invoice_queue_rows(db, *, tab="", start_date=None, end_date=None, q="", product_type=""):
    query = db.query(db_mod.Transaction).order_by(db_mod.Transaction.paid_at.desc().nullslast(), db_mod.Transaction.created_at.desc())
    if start_date:
        query = query.filter(db_mod.Transaction.paid_at >= start_date)
    if end_date:
        query = query.filter(db_mod.Transaction.paid_at <= end_date)
    if product_type:
        query = query.filter(db_mod.Transaction.product_type == product_type)
    if q:
        like = f"%{q.strip().lower()}%"
        query = query.outerjoin(db_mod.Customer, db_mod.Transaction.customer_id == db_mod.Customer.id).filter(
            (db_mod.Customer.email.ilike(like)) | (db_mod.Customer.name.ilike(like))
        )

    rows = []
    summary = {"total": 0, "ready": 0, "missing_info": 0, "draft_exists": 0, "blocked": 0, "issued_already": 0}
    for transaction in query.all():
        readiness = invoice_readiness(db, transaction)
        status = readiness["status"]
        summary["total"] += 1
        summary[status] = summary.get(status, 0) + 1
        if tab and tab != "all" and status != tab:
            continue
        rows.append({"transaction": transaction, "readiness": readiness, "invoice": readiness.get("invoice")})
    return rows, summary


def create_drafts_for_ready_transactions(db, transaction_ids):
    created = []
    skipped = []
    for transaction_id in transaction_ids:
        transaction = db.query(db_mod.Transaction).filter(db_mod.Transaction.id == int(transaction_id)).first()
        if not transaction:
            skipped.append((transaction_id, "missing transaction"))
            continue
        readiness = invoice_readiness(db, transaction)
        if not readiness["can_create"]:
            skipped.append((transaction_id, readiness["label"]))
            continue
        created.append(create_draft_invoice(db, transaction.id))
    db.flush()
    return {"created": created, "skipped": skipped}


def issue_invoice(db, invoice_id: int):
    invoice = db.query(db_mod.Invoice).filter(db_mod.Invoice.id == invoice_id).first()
    if not invoice:
        raise ValueError("Invoice not found")
    if not invoice.invoice_number:
        invoice.invoice_number = next_invoice_number(db)
    invoice.status = "issued"
    invoice.issue_date = invoice.issue_date or datetime.utcnow()
    if invoice.transaction:
        invoice.transaction.invoice_status = "issued"
    db.flush()
    return invoice


def send_invoice(db, invoice_id: int):
    invoice = issue_invoice(db, invoice_id)
    invoice.status = "sent"
    invoice.sent_at = datetime.utcnow()
    invoice.send_status = "sent"
    invoice.sent_to_email = invoice.sent_to_email or (invoice.customer.email if invoice.customer else None)
    invoice.send_error_message = None
    invoice.last_send_attempt_at = datetime.utcnow()
    if invoice.transaction:
        invoice.transaction.invoice_status = "sent"
    db.flush()
    return invoice


def cancel_invoice(db, invoice_id: int):
    invoice = db.query(db_mod.Invoice).filter(db_mod.Invoice.id == invoice_id).first()
    if not invoice:
        raise ValueError("Invoice not found")
    invoice.status = "cancelled"
    invoice.cancelled_at = datetime.utcnow()
    invoice.send_status = "not_sent" if invoice.send_status != "sent" else invoice.send_status
    if invoice.transaction:
        invoice.transaction.invoice_status = "uninvoiced"
    db.flush()
    return invoice


def _invoice_pdf_html(invoice) -> str:
    customer_email = invoice.customer.email if invoice.customer else "-"
    customer_name = invoice.customer.company_name or invoice.customer.name if invoice.customer else "-"
    return f"""
    <!doctype html><html><head><meta charset="utf-8">
    <style>
      body {{ font-family: Georgia, serif; color:#111827; padding:42px; }}
      .brand {{ letter-spacing:.14em; text-transform:uppercase; color:#9a7a24; font-size:12px; }}
      h1 {{ font-weight:400; margin:18px 0 28px; }}
      table {{ width:100%; border-collapse:collapse; margin-top:20px; }}
      td, th {{ padding:10px 0; border-bottom:1px solid #e5e7eb; text-align:left; }}
      .muted {{ color:#6b7280; font-size:12px; line-height:1.6; margin-top:28px; }}
    </style></head><body>
      <div class="brand">Focus Astrology</div>
      <h1>Invoice {invoice.invoice_number or invoice.id}</h1>
      <p><strong>Customer:</strong> {customer_name or "-"} &lt;{customer_email}&gt;</p>
      <p><strong>Status:</strong> {invoice.status}</p>
      <table>
        <tr><th>Description</th><th>Total</th></tr>
        <tr><td>Focus Astrology service</td><td>{invoice.total_amount} {invoice.currency or "TRY"}</td></tr>
      </table>
      <p class="muted">Operational document only. Official e-invoice integration is not implemented. Confirm official invoice/tax requirements with your accountant.</p>
    </body></html>
    """


def generate_invoice_pdf(invoice, base_dir="."):
    invoice.pdf_status = "generating"
    invoice.pdf_error_message = None
    output_dir = Path(base_dir) / "static" / "accounting" / "invoices"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"invoice-{invoice.id}.pdf"
    try:
        WeasyHTML(string=_invoice_pdf_html(invoice), base_url=str(base_dir)).write_pdf(str(output_path))
        invoice.pdf_path = str(output_path).replace("\\", "/")
        invoice.pdf_status = "ready"
        invoice.pdf_generated_at = datetime.utcnow()
        invoice.pdf_error_message = None
        return invoice.pdf_path
    except Exception as exc:
        invoice.pdf_status = "failed"
        invoice.pdf_error_message = str(exc)[:500]
        raise


def send_invoice_email(invoice, send_email_func):
    invoice.last_send_attempt_at = datetime.utcnow()
    if invoice.status not in {"issued", "sent"}:
        invoice.send_status = "failed"
        invoice.send_error_message = "Invoice must be issued before sending."
        return False
    if invoice.pdf_status != "ready" or not invoice.pdf_path or not os.path.exists(invoice.pdf_path):
        invoice.send_status = "failed"
        invoice.send_error_message = "PDF is not ready or attachment is missing."
        return False
    to_email = invoice.customer.email if invoice.customer else ""
    if not to_email or "@" not in to_email:
        invoice.send_status = "failed"
        invoice.send_error_message = "Missing customer email."
        return False
    html = f"<p>Hello,</p><p>Your Focus Astrology invoice is attached.</p><p>Invoice: {invoice.invoice_number or invoice.id}</p>"
    ok = bool(send_email_func(
        to_email=to_email,
        subject=f"Focus Astrology Invoice {invoice.invoice_number or invoice.id}",
        html_body=html,
        plain_body=f"Your Focus Astrology invoice {invoice.invoice_number or invoice.id} is attached.",
        attachment_path=invoice.pdf_path,
        attachment_filename=f"FocusAstrology_Invoice_{invoice.invoice_number or invoice.id}.pdf",
    ))
    invoice.sent_to_email = to_email
    if ok:
        invoice.send_status = "sent"
        invoice.send_error_message = None
        invoice.sent_at = datetime.utcnow()
        invoice.status = "sent"
        if invoice.transaction:
            invoice.transaction.invoice_status = "sent"
    else:
        invoice.send_status = "failed"
        invoice.send_error_message = "SMTP/email provider failed."
    return ok


def invoice_last_error(invoice) -> str:
    return invoice.send_error_message or invoice.pdf_error_message or ""


def is_awaiting_send(invoice) -> bool:
    return invoice.status in {"issued", "sent"} and invoice.pdf_status == "ready" and invoice.send_status in {"not_sent", "failed"}
