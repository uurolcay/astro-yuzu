import re

from services import knowledge_coverage_service as coverage_svc


NOISE_KEYWORDS = {
    "contents",
    "table of contents",
    "bolumler",
    "bolum",
    "icindekiler",
    "index",
    "copyright",
    "translated by",
    "tesekkur",
    "teşekkür",
    "kapak",
}


def _block_text(block):
    if isinstance(block, dict):
        return str(block.get("text") or "").strip()
    return str(block or "").strip()


def _block_page(block, default_page):
    if isinstance(block, dict):
        try:
            return int(block.get("page") or default_page)
        except Exception:
            return int(default_page)
    return int(default_page)


def _split_heading_and_paragraphs(block):
    block = _block_text(block)
    if not block:
        return []
    parts = [part.strip() for part in re.split(r"\n\s*\n", block) if part.strip()]
    if len(parts) == 1:
        parts = [part.strip() for part in block.split("\n") if part.strip()]
    return parts


def _is_heading(text):
    compact = str(text or "").strip()
    if not compact:
        return False
    if compact.endswith((".", "!", "?", ";", ":")):
        return False
    if len(compact) <= 80 and compact == compact.upper():
        return True
    return bool(
        re.match(r"^[A-Z0-9Ä°IŞĞÜÖÇ][A-Za-z0-9Ä°IŞĞÜÖÇıışğüöç\s./-]{0,80}$", compact)
    ) and len(compact.split()) <= 8


def _line_looks_like_toc_entry(line):
    compact = str(line or "").strip()
    if not compact:
        return False
    if re.match(r"^[\divxlcdm.\-\s]+$", compact, flags=re.IGNORECASE):
        return True
    if re.match(r"^[A-Za-zÄ°IŞĞÜÖÇıışğüöç\s./-]{2,80}\s+\.{0,8}\s*\d{1,4}$", compact):
        return True
    return False


def analyze_noise(text):
    lowered = str(text or "").strip().lower()
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if not lines:
        return {
            "is_toc": False,
            "is_index": False,
            "noise_score": 0.0,
        }
    keyword_hits = sum(1 for keyword in NOISE_KEYWORDS if keyword in lowered)
    toc_entry_count = sum(1 for line in lines if _line_looks_like_toc_entry(line))
    numeric_or_short_count = sum(
        1 for line in lines if len(line) <= 24 or re.match(r"^[\divxlcdm.\-\s]+$", line, flags=re.IGNORECASE)
    )
    ratio_short = numeric_or_short_count / max(len(lines), 1)
    ratio_toc = toc_entry_count / max(len(lines), 1)
    score = 0.0
    if keyword_hits:
        score += 0.45
    if toc_entry_count >= 2:
        score += 0.3
    if ratio_short >= 0.6:
        score += 0.25
    if len(lines) >= 4 and ratio_toc >= 0.5:
        score += 0.3
    is_toc = any(keyword in lowered for keyword in ("contents", "table of contents", "icindekiler", "bolumler")) or (
        toc_entry_count >= 3 and ratio_short >= 0.5
    )
    is_index = any(keyword in lowered for keyword in ("index", "copyright", "translated by", "tesekkur", "teşekkür", "kapak"))
    return {
        "is_toc": bool(is_toc),
        "is_index": bool(is_index),
        "noise_score": round(min(score, 1.0), 2),
    }


def _should_skip_chunk(title, text, analysis):
    lowered_title = str(title or "").strip().lower()
    lowered_text = str(text or "").strip().lower()
    if analysis["is_toc"] or analysis["is_index"] or analysis["noise_score"] >= 0.7:
        return True
    if any(marker in lowered_title for marker in ("contents", "bölümler", "bolumler", "index")):
        return True
    if any(marker in lowered_text for marker in ("table of contents", "contents", "icindekiler")) and analysis["noise_score"] >= 0.5:
        return True
    return False


def classify_chunk(text):
    entities = coverage_svc.extract_entities_from_text(text)
    entity = entities[0] if entities else ""
    lowered = str(text or "").lower()
    if any(item in entities for item in coverage_svc.ASTRO_ENTITY_TAXONOMY["nakshatras"]):
        category = "nakshatra"
    elif any(item in entities for item in coverage_svc.ASTRO_ENTITY_TAXONOMY["dasha_planets"]) or "dasha" in lowered:
        category = "dasha"
    elif any(item in entities for item in coverage_svc.ASTRO_ENTITY_TAXONOMY["houses"]):
        category = "house"
    elif any(item in entities for item in coverage_svc.ASTRO_ENTITY_TAXONOMY["planets"]):
        category = "planet"
    else:
        category = "general"

    if any(keyword in lowered for keyword in ("timing", "zamanlama", "transit", "dasha", "mahadasha", "antardasha")):
        topic = "timing"
    elif any(keyword in lowered for keyword in ("psychology", "psychological", "duygusal", "emotion", "icgoru", "içgörü")):
        topic = "psychology"
    elif any(keyword in lowered for keyword in ("behavior", "davran", "response", "tepki")):
        topic = "behavior"
    else:
        topic = "general"
    return {
        "category": category,
        "entity": entity,
        "topic": topic,
        "coverage_entities": entities,
    }


def chunk_text_blocks(blocks):
    meaningful_chunks = []
    current_heading = ""
    current_body = []
    current_start_page = None
    current_end_page = None

    def flush():
        nonlocal current_heading, current_body, current_start_page, current_end_page
        text = "\n\n".join(part for part in current_body if str(part or "").strip()).strip()
        if not text:
            return
        title = current_heading or text.splitlines()[0][:90]
        analysis = analyze_noise(text)
        if _should_skip_chunk(title, text, analysis):
            return
        classification = classify_chunk(text)
        meaningful_chunks.append(
            {
                "title": title,
                "text": text,
                "category": classification["category"],
                "entity": classification["entity"],
                "topic": classification["topic"],
                "coverage_entities": classification["coverage_entities"],
                "source_page_start": current_start_page,
                "source_page_end": current_end_page,
                "is_toc": analysis["is_toc"],
                "is_index": analysis["is_index"],
                "noise_score": analysis["noise_score"],
            }
        )

    for block_index, block in enumerate(blocks or [], start=1):
        page_number = _block_page(block, block_index)
        for part in _split_heading_and_paragraphs(block):
            if _is_heading(part):
                if current_body:
                    flush()
                    current_body = []
                    current_start_page = None
                    current_end_page = None
                current_heading = part
                continue
            if current_start_page is None:
                current_start_page = page_number
            current_end_page = page_number
            current_body.append(part)
            if len("\n\n".join(current_body)) >= 900:
                flush()
                current_body = []
                current_heading = ""
                current_start_page = None
                current_end_page = None
    if current_body:
        flush()
    return meaningful_chunks
