from __future__ import annotations

from pathlib import Path


def parse_pdf(file_path):
    diagnostics = parse_pdf_with_diagnostics(file_path)
    return list(diagnostics.get("blocks") or [])


def parse_pdf_with_diagnostics(file_path):
    path = Path(file_path)
    empty = {
        "blocks": [],
        "page_blocks": [],
        "page_count": 0,
        "block_count": 0,
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
            for index, page in enumerate(pages, start=1):
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
                "page_count": len(pages),
                "block_count": len(blocks),
                "preview": preview,
                "parser_used": parser_name,
                "error": None if blocks else "empty_text_extraction",
            }
        except Exception as exc:  # pragma: no cover - defensive guard
            last_error = f"{parser_name}:{exc}"
            continue

    empty["error"] = last_error or "parser_failed"
    return empty
