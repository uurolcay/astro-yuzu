from __future__ import annotations

from pathlib import Path


def parse_pdf(file_path):
    diagnostics = parse_pdf_with_diagnostics(file_path)
    return list(diagnostics.get("blocks") or [])


def parse_pdf_with_diagnostics(file_path, *, start_page=1, max_pages=None):
    path = Path(file_path)
    try:
        start_page = max(1, int(start_page or 1))
    except (TypeError, ValueError):
        start_page = 1
    try:
        max_pages = int(max_pages) if max_pages is not None else None
    except (TypeError, ValueError):
        max_pages = None
    if max_pages is not None and max_pages <= 0:
        max_pages = None
    empty = {
        "blocks": [],
        "page_blocks": [],
        "page_count": 0,
        "block_count": 0,
        "range_start_page": start_page,
        "range_end_page": start_page - 1,
        "has_more_pages": False,
        "preview": "",
        "parser_used": "none",
        "error": None,
    }
    if not path.exists():
        empty["error"] = "file_not_found"
        return empty

    readers = []
    try:
        from pypdf import PdfReader as PyPdfReader  # type: ignore

        readers.append(("pypdf", PyPdfReader))
    except Exception:
        pass
    try:
        from PyPDF2 import PdfReader as PyPdf2Reader  # type: ignore

        readers.append(("PyPDF2", PyPdf2Reader))
    except Exception:
        pass

    if not readers:
        empty["error"] = "no_pdf_parser_available"
        return empty

    last_error = None
    for parser_name, reader_cls in readers:
        try:
            reader = reader_cls(str(path))
            blocks = []
            page_blocks = []
            pages = list(getattr(reader, "pages", []) or [])
            page_count = len(pages)
            start_index = max(start_page - 1, 0)
            stop_index = page_count if max_pages is None else min(start_index + max_pages, page_count)
            selected_pages = pages[start_index:stop_index]
            for index, page in enumerate(selected_pages, start=start_index + 1):
                text = page.extract_text() or ""
                text = "\n".join(line.rstrip() for line in text.splitlines())
                cleaned = text.strip()
                if cleaned:
                    blocks.append(cleaned)
                    page_blocks.append({"page": index, "text": cleaned})
            preview = "\n\n".join(blocks)[:300]
            return {
                "blocks": blocks,
                "page_blocks": page_blocks,
                "page_count": page_count,
                "block_count": len(blocks),
                "range_start_page": start_page,
                "range_end_page": stop_index,
                "has_more_pages": stop_index < page_count,
                "preview": preview,
                "parser_used": parser_name,
                "error": None if blocks else "empty_text_extraction",
            }
        except Exception as exc:  # pragma: no cover - defensive guard
            last_error = f"{parser_name}:{exc}"
            continue

    empty["error"] = last_error or "parser_failed"
    return empty
