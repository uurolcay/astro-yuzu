from __future__ import annotations

from datetime import datetime, timedelta

import database as db_mod


def _ensure_reminder(db, reminder_type, title, detail="", related_type=None, related_id=None, due_date=None):
    query = db.query(db_mod.Reminder).filter(
        db_mod.Reminder.reminder_type == reminder_type,
        db_mod.Reminder.status == "open",
    )
    if related_type and related_id:
        query = query.filter(db_mod.Reminder.related_type == related_type, db_mod.Reminder.related_id == related_id)
    existing = query.first()
    if existing:
        existing.detail = detail or existing.detail
        existing.due_date = due_date or existing.due_date
        return existing
    reminder = db_mod.Reminder(
        reminder_type=reminder_type,
        title=title,
        detail=detail,
        related_type=related_type,
        related_id=related_id,
        due_date=due_date,
    )
    db.add(reminder)
    return reminder


def create_operational_reminders(db):
    created = []
    pending_invoices = db.query(db_mod.Invoice).filter(db_mod.Invoice.status.in_(["draft", "issued"])).all()
    for invoice in pending_invoices:
        created.append(_ensure_reminder(db, "pending_invoice", "Pending invoice action", f"Invoice #{invoice.invoice_number or invoice.id} is not sent/closed.", "invoice", invoice.id, invoice.due_date))

    uninvoiced = db.query(db_mod.Transaction).filter(db_mod.Transaction.payment_status == "paid", db_mod.Transaction.invoice_status == "uninvoiced").all()
    for transaction in uninvoiced:
        created.append(_ensure_reminder(db, "uninvoiced_transaction", "Paid transaction needs invoice", f"Transaction #{transaction.id} has no invoice.", "transaction", transaction.id, transaction.paid_at))
        if transaction.customer and not (transaction.customer.tax_id or transaction.customer.billing_address):
            created.append(_ensure_reminder(db, "missing_customer_fields", "Missing customer invoice fields", f"{transaction.customer.email} needs tax/billing details.", "customer", transaction.customer.id))

    now = datetime.utcnow()
    month_close = datetime(now.year, now.month, 28)
    created.append(_ensure_reminder(db, "monthly_close", "Monthly close review", "Review payouts, refunds, invoices, and expenses before month close.", due_date=month_close))
    created.append(_ensure_reminder(db, "tax_deadline", "Tax deadline visibility", "Operational estimate only. Confirm official deadlines with your accountant.", due_date=month_close + timedelta(days=10)))
    db.flush()
    return created
