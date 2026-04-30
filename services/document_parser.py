from pathlib import Path


def parse_pdf(file_path):
    path = Path(file_path)
    if not path.exists():
        return []

    readers = []
    try:
        from pypdf import PdfReader  # type: ignore

        readers.append(PdfReader)
    except Exception:
        pass
    try:
        from PyPDF2 import PdfReader  # type: ignore

        readers.append(PdfReader)
    except Exception:
        pass

    for reader_cls in readers:
        try:
            reader = reader_cls(str(path))
            blocks = []
            for page in getattr(reader, "pages", []) or []:
                text = page.extract_text() or ""
                text = "\n".join(line.rstrip() for line in text.splitlines())
                if text.strip():
                    blocks.append(text.strip())
            if blocks:
                return blocks
        except Exception:
            continue
    return []
