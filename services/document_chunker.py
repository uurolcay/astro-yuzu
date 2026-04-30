import re

from services import knowledge_coverage_service as coverage_svc


def _split_heading_and_paragraphs(block):
    block = str(block or "").strip()
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
    return bool(re.match(r"^[A-Z0-9İIŞĞÜÖÇ][A-Za-z0-9İIŞĞÜÖÇışğüöç\s.-]{0,80}$", compact)) and len(compact.split()) <= 6


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

    def flush():
        text = "\n\n".join(part for part in current_body if str(part or "").strip()).strip()
        if not text:
            return
        title = current_heading or text.splitlines()[0][:90]
        classification = classify_chunk(text)
        meaningful_chunks.append(
            {
                "title": title,
                "text": text,
                "category": classification["category"],
                "entity": classification["entity"],
                "topic": classification["topic"],
                "coverage_entities": classification["coverage_entities"],
            }
        )

    for block in blocks or []:
        for part in _split_heading_and_paragraphs(block):
            if _is_heading(part):
                if current_body:
                    flush()
                    current_body = []
                current_heading = part
                continue
            current_body.append(part)
            if len("\n\n".join(current_body)) >= 900:
                flush()
                current_body = []
                current_heading = ""
    if current_body:
        flush()
    return meaningful_chunks
