from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import database as db_mod
from services.accounting.calculations import money


PAID_STATUSES = {"paid", "delivered", "completed", "ready_to_send"}


def commission_for_order(order) -> Decimal:
    # Operational estimate until processor fee reconciliation is imported.
    gross = money(getattr(order, "amount", 0))
    return (gross * Decimal("0.035")).quantize(Decimal("0.01"))


def is_paid_order(order) -> bool:
    return bool(getattr(order, "paid_at", None)) or str(getattr(order, "status", "")).lower() in PAID_STATUSES


def get_or_create_customer(db, *, name: str | None, email: str | None):
    email = (email or "unknown@example.local").strip().lower()
    customer = db.query(db_mod.Customer).filter(db_mod.Customer.email == email).first()
    if customer:
        if name and not customer.name:
            customer.name = name
        return customer
    customer = db_mod.Customer(name=name, email=email)
    db.add(customer)
    db.flush()
    return customer


def sync_transaction_from_order(db, order):
    if not is_paid_order(order):
        return None
    customer = get_or_create_customer(db, name=getattr(order, "customer_name", None), email=getattr(order, "customer_email", None))
    transaction = db.query(db_mod.Transaction).filter(db_mod.Transaction.service_order_id == order.id).first()
    gross = money(getattr(order, "amount", 0))
    refunded = money(getattr(order, "refund_amount", 0))
    commission = commission_for_order(order)
    net = max(gross - refunded - commission, Decimal("0.00"))
    if not transaction:
        transaction = db_mod.Transaction(service_order_id=order.id, customer_id=customer.id)
        db.add(transaction)
    transaction.customer_id = customer.id
    transaction.provider_payment_id = getattr(order, "provider_payment_id", None)
    transaction.product_type = getattr(order, "product_type", None)
    transaction.service_type = getattr(order, "service_type", None)
    transaction.currency = getattr(order, "currency", None) or "TRY"
    transaction.gross_amount = gross
    transaction.refunded_amount = refunded
    transaction.commission_amount = commission
    transaction.net_amount = net
    transaction.payment_status = "refunded" if refunded >= gross and gross > 0 else "paid"
    transaction.paid_at = getattr(order, "paid_at", None) or getattr(order, "payment_verified_at", None) or datetime.utcnow()
    return transaction


def sync_paid_orders(db):
    orders = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.amount.isnot(None)).all()
    synced = []
    for order in orders:
        transaction = sync_transaction_from_order(db, order)
        if transaction:
            synced.append(transaction)
    db.flush()
    return synced


def filtered_transactions(db, *, start_date=None, end_date=None, product_type="", payment_status="", invoice_status=""):
    query = db.query(db_mod.Transaction).order_by(db_mod.Transaction.paid_at.desc().nullslast(), db_mod.Transaction.created_at.desc())
    if start_date:
        query = query.filter(db_mod.Transaction.paid_at >= start_date)
    if end_date:
        query = query.filter(db_mod.Transaction.paid_at <= end_date)
    if product_type:
        query = query.filter(db_mod.Transaction.product_type == product_type)
    if payment_status:
        query = query.filter(db_mod.Transaction.payment_status == payment_status)
    if invoice_status:
        query = query.filter(db_mod.Transaction.invoice_status == invoice_status)
    return query


def uninvoiced_paid_transactions(db):
    return (
        db.query(db_mod.Transaction)
        .filter(db_mod.Transaction.payment_status == "paid", db_mod.Transaction.invoice_status.in_(["uninvoiced", "draft"]))
        .order_by(db_mod.Transaction.paid_at.desc().nullslast())
        .all()
    )
