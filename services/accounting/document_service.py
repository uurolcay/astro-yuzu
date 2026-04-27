from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import database as db_mod
from services.accounting import month_close_service


def _safe_exists(base_dir, path: str | None) -> bool:
    if not path:
        return False
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = Path(base_dir) / path
    return candidate.exists() and candidate.is_file()


def _extension(path: str | None, fallback: str = "other") -> str:
    suffix = Path(path or "").suffix.lower().lstrip(".")
    return suffix or fallback


def _month_key(value) -> str:
    if not value:
        return ""
    return f"{value.year:04d}-{value.month:02d}"


def _row(**kwargs):
    defaults = {
        "document_type": "",
        "title": "",
        "related_month": "",
        "customer": "",
        "invoice_id": None,
        "invoice_number": "",
        "transaction_id": None,
        "expense_id": None,
        "tax_period_id": None,
        "created_at": None,
        "file_status": "missing",
        "file_format": "other",
        "path": "",
        "download_url": "",
        "detail_url": "",
        "related_url": "",
        "source": "system",
        "error": "",
    }
    defaults.update(kwargs)
    return defaults


def archive_documents(db, *, base_dir=".", year=None, month=None, document_type="", status="", q="", start_date=None, end_date=None, expense_only=False, month_close_only=False):
    rows = []
    rows.extend(_invoice_pdf_rows(db, base_dir))
    rows.extend(_expense_rows(db, base_dir))
    rows.extend(_accounting_document_rows(db, base_dir))
    rows.extend(_monthly_export_rows(db, year=year, month=month))

    filtered = []
    q = (q or "").strip().lower()
    wanted_month = f"{int(year):04d}-{int(month):02d}" if year and month else ""
    for row in rows:
        if wanted_month and row["related_month"] != wanted_month:
            continue
        if document_type and row["document_type"] != document_type:
            continue
        if status and row["file_status"] != status:
            continue
        if expense_only and row["document_type"] != "expense_receipt":
            continue
        if month_close_only and row["document_type"] not in {"monthly_export", "monthly_zip", "finance_report"}:
            continue
        if start_date and row["created_at"] and row["created_at"] < start_date:
            continue
        if end_date and row["created_at"] and row["created_at"] > end_date:
            continue
        if q:
            haystack = " ".join(str(row.get(key) or "") for key in ["title", "customer", "invoice_number", "transaction_id", "expense_id", "related_month"]).lower()
            if q not in haystack:
                continue
        filtered.append(row)

    filtered.sort(key=lambda r: r["created_at"] or datetime.min, reverse=True)
    return filtered, archive_summary(filtered)


def archive_summary(rows):
    return {
        "total_documents": len(rows),
        "invoice_pdfs": len([r for r in rows if r["document_type"] == "invoice_pdf"]),
        "monthly_exports": len([r for r in rows if r["document_type"] in {"monthly_export", "monthly_zip"}]),
        "expense_receipts": len([r for r in rows if r["document_type"] == "expense_receipt" and r["file_status"] == "ready"]),
        "missing_files": len([r for r in rows if r["file_status"] == "missing"]),
        "failed_files": len([r for r in rows if r["file_status"] == "failed"]),
    }


def _invoice_pdf_rows(db, base_dir):
    rows = []
    for invoice in db.query(db_mod.Invoice).order_by(db_mod.Invoice.created_at.desc()).all():
        exists = _safe_exists(base_dir, invoice.pdf_path)
        if invoice.pdf_status == "failed":
            file_status = "failed"
        elif invoice.pdf_status == "ready" and exists:
            file_status = "ready"
        elif invoice.pdf_status == "generating":
            file_status = "generating"
        else:
            file_status = "missing"
        rows.append(_row(
            document_type="invoice_pdf",
            title=f"Invoice PDF {invoice.invoice_number or invoice.id}",
            related_month=_month_key(invoice.issue_date or invoice.created_at),
            customer=getattr(invoice.customer, "email", "") or getattr(invoice.customer, "name", ""),
            invoice_id=invoice.id,
            invoice_number=invoice.invoice_number or "",
            transaction_id=invoice.transaction_id,
            created_at=invoice.pdf_generated_at or invoice.created_at,
            file_status=file_status,
            file_format="pdf",
            path=invoice.pdf_path or "",
            download_url=f"/admin/accounting/invoices/{invoice.id}/download" if file_status == "ready" else "",
            detail_url=f"/admin/accounting/invoices/{invoice.id}",
            related_url=f"/admin/accounting/invoices/{invoice.id}",
            error=invoice.pdf_error_message or "",
        ))
    return rows


def _expense_rows(db, base_dir):
    rows = []
    for expense in db.query(db_mod.Expense).order_by(db_mod.Expense.expense_date.desc()).all():
        exists = _safe_exists(base_dir, expense.receipt_path)
        rows.append(_row(
            document_type="expense_receipt",
            title=f"Expense Receipt #{expense.id} · {expense.supplier or 'Unknown supplier'}",
            related_month=_month_key(expense.expense_date),
            expense_id=expense.id,
            created_at=expense.created_at or expense.expense_date,
            file_status="ready" if exists else "missing",
            file_format=_extension(expense.receipt_path, "receipt"),
            path=expense.receipt_path or "",
            download_url=f"/admin/accounting/documents/download?path={expense.receipt_path}" if exists else "",
            detail_url="/admin/accounting/expenses",
            related_url="/admin/accounting/expenses",
            source="uploaded" if expense.receipt_path else "missing",
        ))
    return rows


def _accounting_document_rows(db, base_dir):
    rows = []
    for doc in db.query(db_mod.AccountingDocument).order_by(db_mod.AccountingDocument.created_at.desc()).all():
        exists = _safe_exists(base_dir, doc.file_path)
        related_url = ""
        if doc.related_type == "invoice" and doc.related_id:
            related_url = f"/admin/accounting/invoices/{doc.related_id}"
        elif doc.related_type == "transaction" and doc.related_id:
            related_url = f"/admin/accounting/transactions/{doc.related_id}"
        elif doc.related_type == "expense":
            related_url = "/admin/accounting/expenses"
        elif doc.related_type == "tax_period":
            related_url = "/admin/accounting/month-close"
        rows.append(_row(
            document_type=doc.document_type or "accounting_document",
            title=doc.file_name or Path(doc.file_path).name,
            related_month=_month_key(doc.created_at),
            created_at=doc.created_at,
            file_status="ready" if exists else "missing",
            file_format=_extension(doc.file_path),
            path=doc.file_path,
            download_url=f"/admin/accounting/documents/download?path={doc.file_path}" if exists else "",
            detail_url=related_url,
            related_url=related_url,
            source="uploaded",
        ))
    return rows


def _monthly_export_rows(db, *, year=None, month=None):
    periods = []
    if year and month:
        periods = [month_close_service.get_or_create_period(db, int(year), int(month))]
    else:
        periods = db.query(db_mod.TaxPeriod).order_by(db_mod.TaxPeriod.start_date.desc()).limit(18).all()
        if not periods:
            now = datetime.utcnow()
            periods = [month_close_service.get_or_create_period(db, now.year, now.month)]
    rows = []
    for period in periods:
        y, m = period.start_date.year, period.start_date.month
        rows.append(_row(
            document_type="monthly_zip",
            title=f"Monthly Finance ZIP {period.period_key}",
            related_month=period.period_key,
            tax_period_id=period.id,
            created_at=period.reviewed_at or period.created_at,
            file_status="ready",
            file_format="zip",
            download_url=f"/admin/accounting/exports/monthly.zip?year={y}&month={m}",
            detail_url=f"/admin/accounting/month-close?year={y}&month={m}",
            related_url=f"/admin/accounting/month-close?year={y}&month={m}",
            source="generated",
        ))
    return rows
