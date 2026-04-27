from __future__ import annotations

import json
from calendar import monthrange
from datetime import datetime, timedelta
from decimal import Decimal

import database as db_mod
from services.accounting import invoice_service
from services.accounting.calculations import commission_total, gross_sales, money, net_after_commission, net_sales_after_refunds, refunded_total


def month_bounds(year: int, month: int):
    start = datetime(int(year), int(month), 1)
    end = start + timedelta(days=monthrange(int(year), int(month))[1])
    return start, end


def period_key(year: int, month: int) -> str:
    return f"{int(year):04d}-{int(month):02d}"


def get_or_create_period(db, year: int, month: int):
    key = period_key(year, month)
    period = db.query(db_mod.TaxPeriod).filter(db_mod.TaxPeriod.period_key == key).first()
    if period:
        return period
    start, end = month_bounds(year, month)
    period = db_mod.TaxPeriod(period_key=key, start_date=start, end_date=end, status="open")
    db.add(period)
    db.flush()
    return period


def _month_transactions(db, year: int, month: int):
    start, end = month_bounds(year, month)
    return (
        db.query(db_mod.Transaction)
        .filter(db_mod.Transaction.paid_at >= start, db_mod.Transaction.paid_at < end)
        .all()
    )


def _month_invoices(db, year: int, month: int):
    start, end = month_bounds(year, month)
    return (
        db.query(db_mod.Invoice)
        .filter(db_mod.Invoice.created_at >= start, db_mod.Invoice.created_at < end)
        .all()
    )


def _month_expenses(db, year: int, month: int):
    start, end = month_bounds(year, month)
    return (
        db.query(db_mod.Expense)
        .filter(db_mod.Expense.expense_date >= start, db_mod.Expense.expense_date < end)
        .all()
    )


def monthly_summary(db, year: int, month: int):
    start, end = month_bounds(year, month)
    transactions = _month_transactions(db, year, month)
    invoices = _month_invoices(db, year, month)
    expenses = _month_expenses(db, year, month)
    paid_transactions = [t for t in transactions if t.payment_status == "paid"]
    expense_total = sum((money(e.amount) for e in expenses), Decimal("0.00"))
    net_sales = net_sales_after_refunds(transactions)
    retained = net_after_commission(transactions)
    profit_base = max(retained - expense_total, Decimal("0.00"))
    issued_invoices = [i for i in invoices if i.status in {"issued", "sent"}]
    awaiting_send = [i for i in invoices if invoice_service.is_awaiting_send(i)]
    return {
        "year": int(year),
        "month": int(month),
        "period_key": period_key(year, month),
        "start_date": start,
        "end_date": end,
        "gross_revenue": gross_sales(transactions),
        "refunded_amount": refunded_total(transactions),
        "net_sales": net_sales,
        "commission_total": commission_total(transactions),
        "commission_vat_total": Decimal("0.00"),
        "net_retained": retained,
        "issued_invoice_count": len(issued_invoices),
        "uninvoiced_paid_count": len([t for t in paid_transactions if t.invoice_status == "uninvoiced"]),
        "total_expense_amount": expense_total,
        "estimated_vat": (net_sales * Decimal("20") / Decimal("120")).quantize(Decimal("0.01")),
        "estimated_tax": (profit_base * Decimal("0.20")).quantize(Decimal("0.01")),
        "open_reminders_count": db.query(db_mod.Reminder).filter(
            db_mod.Reminder.status == "open",
            db_mod.Reminder.due_date >= start,
            db_mod.Reminder.due_date < end,
        ).count(),
        "awaiting_send_invoice_count": len(awaiting_send),
        "transaction_count": len(transactions),
        "refund_count": len([t for t in transactions if money(t.refunded_amount) > 0 or t.payment_status == "refunded"]),
        "expense_count": len(expenses),
    }


def readiness_checklist(db, year: int, month: int):
    start, end = month_bounds(year, month)
    transactions = _month_transactions(db, year, month)
    invoices = _month_invoices(db, year, month)
    paid_transactions = [t for t in transactions if t.payment_status == "paid"]
    queue_states = [invoice_service.invoice_readiness(db, t) for t in paid_transactions]
    missing_or_blocked = [r for r in queue_states if r["status"] in {"missing_info", "blocked"}]
    uninvoiced = [t for t in paid_transactions if t.invoice_status == "uninvoiced"]
    draft_invoices = [i for i in invoices if i.status == "draft"]
    awaiting_pdf = [i for i in invoices if i.status in {"issued", "sent"} and i.pdf_status != "ready"]
    awaiting_send = [i for i in invoices if invoice_service.is_awaiting_send(i)]
    send_failed = [i for i in invoices if i.send_status == "failed"]
    open_reminders = db.query(db_mod.Reminder).filter(
        db_mod.Reminder.status == "open",
        db_mod.Reminder.due_date >= start,
        db_mod.Reminder.due_date < end,
    ).count()
    under_review = db.query(db_mod.ServiceOrder).filter(
        db_mod.ServiceOrder.created_at >= start,
        db_mod.ServiceOrder.created_at < end,
        db_mod.ServiceOrder.status == "payment_under_review",
    ).count()
    expenses = _month_expenses(db, year, month)
    checks = [
        _check("all_paid_invoiced", "All paid transactions invoiced", len(uninvoiced), "blocker", "Paid transactions still need invoice workflow."),
        _check("missing_billing_info", "Blocked invoice queue items", len(missing_or_blocked), "blocker", "Customer billing information is incomplete."),
        _check("draft_invoices", "Draft invoices unfinished", len(draft_invoices), "warning", "Draft invoices should be issued, cancelled, or documented."),
        _check("awaiting_pdf", "Issued invoices awaiting PDF", len(awaiting_pdf), "warning", "Generate PDFs before sending or closing."),
        _check("awaiting_send", "PDF-ready invoices awaiting send", len(awaiting_send), "warning", "Send ready invoices or note why they are held."),
        _check("send_failed", "Failed invoice sends", len(send_failed), "blocker", "Retry or resolve failed invoice sends."),
        _check("open_reminders", "Open reminders in month", open_reminders, "warning", "Review open operational reminders."),
        _check("payment_under_review", "Payments under review", under_review, "blocker", "Resolve payment review items before clean close."),
        _check("expense_documents", "Expense/document review", 0 if expenses else 1, "warning", "No expenses recorded for this month; confirm this is expected."),
    ]
    return checks


def _check(key: str, label: str, count: int, severity_if_open: str, detail: str):
    if count:
        return {"key": key, "label": label, "count": count, "state": severity_if_open, "detail": detail}
    return {"key": key, "label": label, "count": 0, "state": "complete", "detail": "Complete"}


def can_mark_reviewed(checks, confirmed: bool = False):
    blockers = [c for c in checks if c["state"] == "blocker"]
    warnings = [c for c in checks if c["state"] == "warning"]
    if blockers and not confirmed:
        return False, "Major blockers remain. Confirm review with warnings and add a note to continue."
    if warnings and not confirmed:
        return False, "Warnings remain. Confirm review with warnings and add a note to continue."
    return True, ""


def mark_reviewed(db, year: int, month: int, reviewer: str, note: str = "", confirmed: bool = False):
    summary = monthly_summary(db, year, month)
    checks = readiness_checklist(db, year, month)
    ok, reason = can_mark_reviewed(checks, confirmed=confirmed)
    if not ok:
        return {"ok": False, "reason": reason, "period": get_or_create_period(db, year, month), "checks": checks}
    period = get_or_create_period(db, year, month)
    has_unresolved = any(c["state"] in {"warning", "blocker"} for c in checks)
    period.status = "reviewed_with_warnings" if has_unresolved else "reviewed"
    period.reviewed_at = datetime.utcnow()
    period.reviewed_by = reviewer
    period.notes = note.strip() if note else None
    period.estimated_vat = summary["estimated_vat"]
    period.estimated_tax = summary["estimated_tax"]
    period.snapshot_json = json.dumps({"summary": summary, "checks": checks}, default=str, ensure_ascii=False)
    return {"ok": True, "period": period, "checks": checks, "summary": summary}


def activity_highlights(db, year: int, month: int):
    start, end = month_bounds(year, month)
    transactions = _month_transactions(db, year, month)
    product_rows = {}
    for t in transactions:
        key = t.product_type or t.service_type or "unknown"
        row = product_rows.setdefault(key, {"product_type": key, "count": 0, "gross": Decimal("0.00"), "net": Decimal("0.00")})
        row["count"] += 1
        row["gross"] += money(t.gross_amount)
        row["net"] += money(t.net_amount)
    large_expenses = (
        db.query(db_mod.Expense)
        .filter(db_mod.Expense.expense_date >= start, db_mod.Expense.expense_date < end)
        .order_by(db_mod.Expense.amount.desc())
        .limit(5)
        .all()
    )
    return {
        "top_products": sorted(product_rows.values(), key=lambda r: r["gross"], reverse=True)[:5],
        "consultation_completion_count": db.query(db_mod.ServiceOrder).filter(
            db_mod.ServiceOrder.service_type == "consultation",
            db_mod.ServiceOrder.completed_at >= start,
            db_mod.ServiceOrder.completed_at < end,
        ).count(),
        "report_delivery_count": db.query(db_mod.ServiceOrder).filter(
            db_mod.ServiceOrder.service_type == "report",
            db_mod.ServiceOrder.delivered_at >= start,
            db_mod.ServiceOrder.delivered_at < end,
        ).count(),
        "large_expenses": large_expenses,
    }
