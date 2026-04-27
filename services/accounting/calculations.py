from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


MONEY = Decimal("0.01")


def money(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0.00")
    try:
        return Decimal(str(value)).quantize(MONEY, rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")


def gross_sales(transactions) -> Decimal:
    return sum((money(getattr(t, "gross_amount", 0)) for t in transactions), Decimal("0.00")).quantize(MONEY)


def refunded_total(transactions) -> Decimal:
    return sum((money(getattr(t, "refunded_amount", 0)) for t in transactions), Decimal("0.00")).quantize(MONEY)


def commission_total(transactions) -> Decimal:
    return sum((money(getattr(t, "commission_amount", 0)) for t in transactions), Decimal("0.00")).quantize(MONEY)


def net_sales_after_refunds(transactions) -> Decimal:
    return max(gross_sales(transactions) - refunded_total(transactions), Decimal("0.00")).quantize(MONEY)


def net_after_commission(transactions) -> Decimal:
    return max(net_sales_after_refunds(transactions) - commission_total(transactions), Decimal("0.00")).quantize(MONEY)


def estimated_vat(net_sales, vat_rate_percent=20) -> Decimal:
    rate = money(vat_rate_percent) / Decimal("100")
    return (money(net_sales) * rate / (Decimal("1.00") + rate)).quantize(MONEY)


def estimated_tax(profit_base, tax_rate_percent=20) -> Decimal:
    return (max(money(profit_base), Decimal("0.00")) * money(tax_rate_percent) / Decimal("100")).quantize(MONEY)


def invoice_coverage_ratio(total_paid_count: int, invoiced_count: int) -> Decimal:
    if not total_paid_count:
        return Decimal("0.00")
    return (Decimal(invoiced_count) * Decimal("100") / Decimal(total_paid_count)).quantize(MONEY)


def collection_ratio(total_amount, collected_amount) -> Decimal:
    total = money(total_amount)
    if total <= 0:
        return Decimal("0.00")
    return (money(collected_amount) * Decimal("100") / total).quantize(MONEY)
