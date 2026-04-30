import re


_NAKSHATRA_ALIASES = {
    "ashwini": {"aswini", "ashwini", "asvini"},
    "bharani": {"bharani", "bharni"},
    "krittika": {"krittika", "kritika", "kritikaa", "kritika", "kritikaa", "kritikah", "krtika", "kritika", "kriitika", "kritika", "kri̇tika", "kri̇ti̇ka", "kritika", "kri̇tika", "kritika", "kri̇ti̇ka", "kritika", "kritika", "kritika", "kri̇ti̇ka", "kritika", "kritika", "krİtİka", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kritika", "kri̇tika", "kri̇tika", "krıtıka", "krİtİka", "kritika", "krittika"},
    "rohini": {"rohini"},
    "mrigashira": {"mrigashira", "mrigasir", "mrigashir", "mrigasira", "mrigasirsha", "mrigasirsa", "mrigasira", "mrigasir", "mrigaşir", "mrigasir"},
    "ardra": {"ardra", "ardraa"},
    "punarvasu": {"punarvasu"},
    "pushya": {"pushya", "pushya"},
    "ashlesha": {"ashlesha", "aslesha", "aşlesha", "ashlesa"},
    "magha": {"magha"},
    "purva_phalguni": {"purva phalguni", "purvaphalguni", "pubba"},
    "uttara_phalguni": {"uttara phalguni", "uttaraphalguni"},
    "hasta": {"hasta"},
    "chitra": {"chitra", "citra"},
    "swati": {"swati", "svati"},
    "vishakha": {"vishakha", "visakha"},
    "anuradha": {"anuradha", "anurada"},
    "jyeshtha": {"jyeshtha", "jyestha"},
    "mula": {"mula", "moola"},
    "purva_ashadha": {"purva ashadha", "purvashadha", "purvaashadha"},
    "uttara_ashadha": {"uttara ashadha", "uttarashadha", "uttaraashadha"},
    "shravana": {"shravana", "sravana"},
    "dhanishta": {"dhanishta", "dhanista"},
    "shatabhisha": {"shatabhisha", "satabhisha", "şatabhişa", "shatabisa", "satabhisa"},
    "purva_bhadrapada": {"purva bhadrapada", "purvabhadrapada"},
    "uttara_bhadrapada": {"uttara bhadrapada", "uttarabhadrapada"},
    "revati": {"revati"},
}

_METADATA_PATTERNS = {
    "zodiac_range": [r"zodiac range[:\s-]+([^\n]+)", r"range[:\s-]+([^\n]+)"],
    "ruler": [r"ruler[:\s-]+([^\n]+)"],
    "deity": [r"deity[:\s-]+([^\n]+)"],
    "symbol": [r"symbol[:\s-]+([^\n]+)"],
    "gana": [r"gana[:\s-]+([^\n]+)"],
    "quality": [r"quality[:\s-]+([^\n]+)"],
    "caste": [r"caste[:\s-]+([^\n]+)"],
    "animal": [r"animal[:\s-]+([^\n]+)", r"yoni[:\s-]+([^\n]+)"],
    "bird": [r"bird[:\s-]+([^\n]+)"],
    "tree": [r"tree[:\s-]+([^\n]+)"],
    "sounds": [r"sounds?[:\s-]+([^\n]+)", r"syllables?[:\s-]+([^\n]+)"],
    "yoga_tara": [r"yoga tara[:\s-]+([^\n]+)"],
    "vargottama": [r"vargottama[:\s-]+([^\n]+)"],
    "pushkara_navamsa": [r"pushkara navamsa[:\s-]+([^\n]+)"],
    "pushkara_bhaga": [r"pushkara bhaga[:\s-]+([^\n]+)"],
}


def _clean(value):
    return str(value or "").strip()


def normalize_nakshatra_name(value):
    text = _clean(value).lower()
    if not text:
        return ""
    replacements = str.maketrans({
        "ş": "s", "Ş": "s",
        "ı": "i", "İ": "i",
        "ğ": "g", "Ğ": "g",
        "ü": "u", "Ü": "u",
        "ö": "o", "Ö": "o",
        "ç": "c", "Ç": "c",
    })
    normalized = text.translate(replacements)
    normalized = normalized.replace("-", " ").replace("/", " ")
    normalized = re.sub(r"[^a-z0-9\s]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    for canonical, aliases in _NAKSHATRA_ALIASES.items():
        if normalized == canonical.replace("_", " ") or normalized in aliases:
            return canonical
    return normalized.replace(" ", "_")


def detect_nakshatra_sections(text_blocks):
    sections = []
    current = None
    for index, block in enumerate(text_blocks or [], start=1):
        block_text = _clean(block)
        if not block_text:
            continue
        lines = [line.strip() for line in block_text.splitlines() if line.strip()]
        detected = ""
        for line in lines[:6]:
            candidate = normalize_nakshatra_name(line)
            if candidate in _NAKSHATRA_ALIASES:
                detected = candidate
                break
        if detected:
            if current:
                current["end_page"] = index - 1 if index > current["start_page"] else current["start_page"]
                sections.append(current)
            current = {
                "nakshatra": detected,
                "start_page": index,
                "end_page": index,
                "raw_text": block_text,
            }
        elif current:
            current["end_page"] = index
            current["raw_text"] = f"{current['raw_text']}\n\n{block_text}".strip()
    if current:
        sections.append(current)
    return sections


def extract_nakshatra_metadata(section_text):
    text = _clean(section_text)
    metadata = {key: "" for key in [
        "zodiac_range", "ruler", "deity", "symbol", "gana", "quality", "caste",
        "animal", "bird", "tree", "sounds", "yoga_tara", "padas", "vargottama",
        "pushkara_navamsa", "pushkara_bhaga",
    ]}
    for key, patterns in _METADATA_PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                metadata[key] = _clean(match.group(1))
                break
    pada_match = re.findall(r"(?:pada|quarter)\s*[1-4][^\n]*", text, flags=re.IGNORECASE)
    if pada_match:
        metadata["padas"] = " | ".join(_clean(item) for item in pada_match)
    return metadata


def classify_nakshatra_paragraph(paragraph):
    text = _clean(paragraph).lower()
    if not text:
        return "unknown"
    if any(keyword in text for keyword in ("ruler:", "deity:", "symbol:", "gana:", "animal:", "tree:", "bird:", "sounds:", "range:")):
        return "metadata"
    if any(keyword in text for keyword in ("animal", "yoni", "instinct", "mate pattern")):
        return "animal_logic"
    if any(keyword in text for keyword in ("deity", "myth", "mythology", "divine")):
        return "deity_mythology"
    if any(keyword in text for keyword in ("symbol", "emblem")):
        return "symbol_logic"
    if any(keyword in text for keyword in ("element", "fire", "earth", "air", "water")):
        return "element_logic"
    if any(keyword in text for keyword in ("psychology", "inner world", "emotion", "mindset", "temperament")):
        return "psychology"
    if any(keyword in text for keyword in ("behavior", "response", "habit", "react", "pattern")):
        return "behavior_pattern"
    if any(keyword in text for keyword in ("relationship", "partner", "bond", "marriage", "attachment")):
        return "relationship_pattern"
    if any(keyword in text for keyword in ("career", "work", "profession", "vocation")):
        return "career_pattern"
    if any(keyword in text for keyword in ("health", "wellbeing", "medical", "body")):
        return "health_safe_notes"
    if any(keyword in text for keyword in ("timing", "dasha", "transit", "activation", "period")):
        return "timing_quality"
    if any(keyword in text for keyword in ("remedy", "mantra", "upaya")):
        return "remedy"
    if any(keyword in text for keyword in ("observation", "anecdote", "sometimes", "in some cases", "may be seen")):
        return "observation_notes"
    if any(keyword in text for keyword in ("pada", "quarter")):
        return "pada_logic"
    if any(keyword in text for keyword in ("death", "illness", "sexual", "child", "pregnancy", "marriage", "divorce")):
        return "caution_sensitive"
    return "unknown"


def _sensitivity_for_text(text):
    lowered = _clean(text).lower()
    if any(keyword in lowered for keyword in ("death", "sexual", "pregnancy", "child", "marriage", "divorce", "illness", "disease")):
        return "high"
    if any(keyword in lowered for keyword in ("health", "partner", "relationship", "family")):
        return "moderate"
    return "low"


def build_suggested_nakshatra_chunks(section):
    raw_text = _clean((section or {}).get("raw_text"))
    if not raw_text:
        return []
    nakshatra = normalize_nakshatra_name((section or {}).get("nakshatra"))
    metadata = extract_nakshatra_metadata(raw_text)
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", raw_text) if part.strip()]
    source_title = _clean((section or {}).get("source_title")) or "Imported Nakshatra Source"
    chunks = []
    for paragraph in paragraphs:
        paragraph_type = classify_nakshatra_paragraph(paragraph)
        if paragraph_type == "metadata":
            continue
        confidence_level = "high"
        if paragraph_type == "observation_notes":
            confidence_level = "low"
        elif paragraph_type in {"caution_sensitive", "health_safe_notes"}:
            confidence_level = "medium"
        sensitivity_level = _sensitivity_for_text(paragraph)
        chunks.append(
            {
                "title": f"{nakshatra.replace('_', ' ').title()} — {paragraph_type.replace('_', ' ').title()}",
                "category": "nakshatra",
                "entity_type": "nakshatra",
                "primary_entity": nakshatra,
                "secondary_entity": paragraph_type,
                "coverage_entities": [nakshatra, paragraph_type],
                "source_type": "classical_text",
                "source_title": source_title,
                "source_reference": f"pages {section.get('start_page')}-{section.get('end_page')}",
                "classical_view": paragraph,
                "modern_synthesis": f"{nakshatra.replace('_', ' ').title()} themes can be translated into a calm, non-deterministic interpretive frame for modern clients.",
                "interpretation_logic": f"Use this material when {nakshatra.replace('_', ' ')} is active in natal emphasis, dasha activation, or transit contact.",
                "strong_condition": f"Stronger when {nakshatra.replace('_', ' ')} is tied to Moon, Lagna, ruling planets, or active dasha layers.",
                "weak_condition": "Weaker when the chart has no clear activation or the statement depends on anecdotal observation only.",
                "risk_pattern": metadata.get("animal") or "May distort into reactive patterning if handled too literally.",
                "opportunity_pattern": metadata.get("symbol") or "Can become a source-aware insight when grounded in chart context.",
                "dasha_activation": f"Review when the native enters {metadata.get('ruler') or nakshatra} timing or related dasha periods.",
                "transit_activation": f"Useful when transits trigger {nakshatra.replace('_', ' ')} planets or the ruler of this nakshatra.",
                "safe_language_notes": "Avoid fate-heavy certainty. Present as pattern, tendency, or quality that may become more visible under activation.",
                "what_not_to_say": "Do not present this as a guaranteed life outcome or irreversible destiny.",
                "premium_synthesis_sentence": f"{nakshatra.replace('_', ' ').title()} can be framed as a nuanced quality pattern that becomes clearest when chart activation and lived context support it.",
                "tags": [nakshatra, paragraph_type, metadata.get("ruler") or ""],
                "confidence_level": confidence_level,
                "sensitivity_level": sensitivity_level,
                "review_required": True,
            }
        )
    return chunks


def extract_nakshatra_knowledge_from_document(text_blocks, source_title):
    sections = detect_nakshatra_sections(text_blocks or [])
    results = []
    for section in sections:
        enriched = dict(section)
        enriched["source_title"] = source_title
        enriched["metadata"] = extract_nakshatra_metadata(section.get("raw_text"))
        enriched["suggested_chunks"] = build_suggested_nakshatra_chunks(enriched)
        results.append(enriched)
    return {
        "sections": results,
        "section_count": len(results),
        "chunk_count": sum(len(section.get("suggested_chunks") or []) for section in results),
    }
