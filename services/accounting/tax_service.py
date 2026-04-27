from __future__ import annotations

from decimal import Decimal

import database as db_mod
from services.accounting.calculations import commission_total, estimated_tax, estimated_vat, gross_sales, money, net_after_commission, net_sales_after_refunds, refunded_total


def tax_overview(db, transactions=None, *, vat_rate=20, income_tax_rate=20):
    transactions = transactions if transactions is not None else db.query(db_mod.Transaction).all()
    expenses = db.query(db_mod.Expense).all()
    expense_total = sum((money(e.amount) for e in expenses), Decimal("0.00"))
    net_sales = net_sales_after_refunds(transactions)
    retained = net_after_commission(transactions)
    profit_base = max(retained - expense_total, Decimal("0.00"))
    return {
        "gross_sales": gross_sales(transactions),
        "refunded_amount": refunded_total(transactions),
        "commission_totals": commission_total(transactions),
        "net_sales": net_sales,
        "net_retained": retained,
        "expenses": expense_total,
        "estimated_profit_base": profit_base,
        "estimated_vat": estimated_vat(net_sales, vat_rate),
        "estimated_tax": estimated_tax(profit_base, income_tax_rate),
        "vat_rate": vat_rate,
        "income_tax_rate": income_tax_rate,
    }
