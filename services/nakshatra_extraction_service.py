from __future__ import annotations

import re
import unicodedata

from services import document_chunker


NAKSHATRA_ALIASES = {
    "ashwini": {"ashwini", "aswini", "asvini", "aşvini", "ashwini̇"},
    "bharani": {"bharani", "bharanİ", "barani", "भारानी"},
    "krittika": {"krittika", "kritika", "krİtİka", "kritika", "kritika"},
    "rohini": {"rohini"},
    "mrigashira": {"mrigashira", "mrigasira", "mrigasir", "mrigaşir"},
    "ardra": {"ardra"},
    "punarvasu": {"punarvasu"},
    "pushya": {"pushya"},
    "ashlesha": {"ashlesha", "ashlesa", "aşlesha"},
    "magha": {"magha"},
    "purva_phalguni": {"purva phalguni", "purvaphalguni", "pubba"},
    "uttara_phalguni": {"uttara phalguni", "uttaraphalguni"},
    "hasta": {"hasta"},
    "chitra": {"chitra", "citra"},
    "swati": {"swati", "svati"},
    "vishakha": {"vishakha", "visakha", "vişahha"},
    "anuradha": {"anuradha", "anurada"},
    "jyeshtha": {"jyeshtha", "jyestha"},
    "mula": {"mula", "moola", "moola/mula"},
    "purva_ashadha": {"purva ashadha", "purvashadha"},
    "uttara_ashadha": {"uttara ashadha", "uttarashadha"},
    "shravana": {"shravana", "sravana", "şravana"},
    "dhanishta": {"dhanishta", "dhanista"},
    "shatabhisha": {"shatabhisha", "satabhisha", "şatabhişa", "şatabhisha"},
    "purva_bhadrapada": {"purva bhadrapada", "purvabhadrapada"},
    "uttara_bhadrapada": {"uttara bhadrapada", "uttarabhadrapada"},
    "revati": {"revati"},
}

METADATA_HINTS = [
    "zodiac range",
    "burc araligi",
    "burç aralığı",
    "ruler",
    "gezegene hukmetme",
    "gezegene hükmetme",
    "deity",
    "tanri",
    "tanrı",
    "symbol",
    "sembol",
    "quality",
    "kalite",
    "pada",
    "padalar",
    "animal",
    "yoni",
]

METADATA_PATTERNS = {
    "zodiac_range": [r"zodiac range[:\s-]+([^\n]+)", r"burc araligi[:\s-]+([^\n]+)", r"burç aralığı[:\s-]+([^\n]+)"],
    "ruler": [r"ruler[:\s-]+([^\n]+)", r"gezegene hukmetme[:\s-]+([^\n]+)", r"gezegene hükmetme[:\s-]+([^\n]+)"],
    "deity": [r"deity[:\s-]+([^\n]+)", r"tanri[:\s-]+([^\n]+)", r"tanrı[:\s-]+([^\n]+)"],
    "symbol": [r"symbol[:\s-]+([^\n]+)", r"sembol[:\s-]+([^\n]+)"],
    "gana": [r"gana[:\s-]+([^\n]+)"],
    "quality": [r"quality[:\s-]+([^\n]+)", r"kalite[:\s-]+([^\n]+)"],
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


def _ascii_fold(text):
    folded = unicodedata.normalize("NFKD", str(text or ""))
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = folded.lower().replace("/", " ").replace("-", " ")
    folded = re.sub(r"\s+", " ", folded)
    return folded.strip()


def normalize_nakshatra_name(value):
    text = _ascii_fold(value)
    if not text:
        return ""
    for canonical, aliases in NAKSHATRA_ALIASES.items():
        if text == canonical.replace("_", " ") or text in {_ascii_fold(alias) for alias in aliases}:
            return canonical
    return text.replace(" ", "_")


def _page_entries(text_blocks):
    entries = []
    for idx, block in enumerate(text_blocks or [], start=1):
        if isinstance(block, dict):
            text = _clean(block.get("text"))
            page = int(block.get("page") or idx)
        else:
            text = _clean(block)
            page = idx
        if text:
            entries.append({"page": page, "text": text})
    return entries


def _candidate_heading(lines):
    for line in lines[:10]:
        candidate = normalize_nakshatra_name(line)
        if candidate in NAKSHATRA_ALIASES:
            return candidate
    preview = _ascii_fold(" ".join(lines[:3])[:100])
    for canonical, aliases in NAKSHATRA_ALIASES.items():
        variants = {canonical.replace("_", " "), *{_ascii_fold(alias) for alias in aliases}}
        if any(variant and preview.startswith(variant) for variant in variants):
            return canonical
    return ""


def _has_long_paragraph(text):
    return any(len(part.strip()) >= 180 for part in re.split(r"\n\s*\n", str(text or "")) if part.strip())


def _section_supported(entries, start_index):
    window = "\n\n".join(item["text"] for item in entries[start_index : start_index + 3])
    lowered = _ascii_fold(window)
    if not lowered:
        return False
    if document_chunker.analyze_noise(window)["noise_score"] >= 0.7:
        return False
    if any(hint in lowered for hint in METADATA_HINTS):
        return True
    if _has_long_paragraph(window):
        return True
    if any(keyword in lowered for keyword in ("animal", "yoni", "deity", "myth", "symbol", "sembol")):
        return True
    return False


def detect_nakshatra_sections(text_blocks):
    entries = _page_entries(text_blocks)
    sections = []
    current = None
    for index, entry in enumerate(entries):
        text = entry["text"]
        noise = document_chunker.analyze_noise(text)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        detected = _candidate_heading(lines)
        if detected and not noise["is_toc"] and _section_supported(entries, index):
            if current:
                current["end_page"] = entries[index - 1]["page"] if index > current["entry_index"] else current["start_page"]
                current.pop("entry_index", None)
                sections.append(current)
            current = {
                "nakshatra": detected,
                "start_page": entry["page"],
                "end_page": entry["page"],
                "raw_text": text,
                "entry_index": index,
            }
            continue
        if current:
            current["end_page"] = entry["page"]
            current["raw_text"] = f"{current['raw_text']}\n\n{text}".strip()
    if current:
        current.pop("entry_index", None)
        sections.append(current)
    if sections:
        return sections
    return _soft_split_nakshatra_sections(entries)


def _soft_split_nakshatra_sections(entries):
    full_text = "\n\n".join(entry["text"] for entry in entries)
    if not full_text:
        return []
    hits = []
    for entry_index, entry in enumerate(entries):
        if document_chunker.analyze_noise(entry["text"])["is_toc"]:
            continue
        normalized_text = _ascii_fold(entry["text"])
        entry_hits = []
        for canonical, aliases in NAKSHATRA_ALIASES.items():
            variants = {canonical.replace("_", " "), *{_ascii_fold(alias) for alias in aliases}}
            matched_variant = next((variant for variant in variants if variant and variant in normalized_text), "")
            if matched_variant and (
                _section_supported(entries, entry_index)
                or (len(entry["text"]) >= 80 and len(normalized_text.split(matched_variant, 1)[-1].strip()) >= 10)
            ):
                entry_hits.append((normalized_text.find(matched_variant), canonical))
        if entry_hits:
            for position, canonical in sorted(entry_hits, key=lambda value: value[0]):
                hits.append((entry_index, position, canonical))
    sections = []
    seen = set()
    for hit_index, (entry_index, position, nakshatra) in enumerate(hits):
        if (entry_index, position, nakshatra) in seen:
            continue
        seen.add((entry_index, position, nakshatra))
        next_hit = hits[hit_index + 1] if hit_index + 1 < len(hits) else None
        start_entry = entries[entry_index]
        if next_hit and next_hit[0] == entry_index:
            next_position = next_hit[1]
            raw_text = start_entry["text"][position:next_position].strip()
            end_page = start_entry["page"]
        else:
            end_entry_index = len(entries)
            if next_hit:
                end_entry_index = next_hit[0]
            raw_text = "\n\n".join(entry["text"] for entry in entries[entry_index:end_entry_index]).strip()
            end_page = entries[end_entry_index - 1]["page"] if end_entry_index > entry_index else start_entry["page"]
        if raw_text:
            sections.append(
                {
                    "nakshatra": nakshatra,
                    "start_page": start_entry["page"],
                    "end_page": end_page,
                    "raw_text": raw_text,
                }
            )
    return sections


def extract_nakshatra_metadata(section_text):
    text = _clean(section_text)
    metadata = {key: "" for key in [
        "zodiac_range", "ruler", "deity", "symbol", "gana", "quality", "caste",
        "animal", "bird", "tree", "sounds", "yoga_tara", "padas",
        "vargottama", "pushkara_navamsa", "pushkara_bhaga",
    ]}
    for key, patterns in METADATA_PATTERNS.items():
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
    text = _ascii_fold(paragraph)
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
    if any(keyword in text for keyword in ("death", "illness", "sexual", "child", "pregnancy", "marriage")):
        return "caution_sensitive"
    return "unknown"


def _sensitivity_for_text(text):
    lowered = _ascii_fold(text)
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
                "title": f"{nakshatra.replace('_', ' ').title()} - {paragraph_type.replace('_', ' ').title()}",
                "category": "nakshatra",
                "entity_type": "nakshatra",
                "primary_entity": nakshatra,
                "secondary_entity": paragraph_type,
                "coverage_entities": [nakshatra, paragraph_type],
                "source_type": "classical_text",
                "source_title": source_title,
                "source_reference": f"pages {section.get('start_page')}-{section.get('end_page')}",
                "source_page_start": section.get("start_page"),
                "source_page_end": section.get("end_page"),
                "classical_view": paragraph,
                "modern_synthesis": f"{nakshatra.replace('_', ' ').title()} temalari modern bir danisan deneyimi icin sakin ve kosullu bir dil ile sentezlenebilir.",
                "interpretation_logic": f"Bu icerigi {nakshatra.replace('_', ' ')} natal vurguda, dasha aktivasyonunda veya transit temasinda kullanin.",
                "strong_condition": f"Moon, Lagna veya aktif dasha katmanlari {nakshatra.replace('_', ' ')} ile baglandiginda daha guclu kullanilir.",
                "weak_condition": "Haritada net aktivasyon yoksa veya ifade yalnizca gozlemsel/anekdotal ise daha yumusak kullanilmalidir.",
                "risk_pattern": metadata.get("animal") or "Asiri literal okunursa tepkisel bir oruntuye kayabilir.",
                "opportunity_pattern": metadata.get("symbol") or "Chart baglamina oturdugunda derin ve kaynakli bir icgoru uretebilir.",
                "dasha_activation": f"{metadata.get('ruler') or nakshatra} yonetimiyle ilgili dasha donemlerinde tekrar gozden gecirin.",
                "transit_activation": f"Transitler {nakshatra.replace('_', ' ')} ile bagli gezegenleri tetiklediginde kullanislidir.",
                "safe_language_notes": "Kesin olur dili kullanma. Bunu bir oruntu, egilim veya aktiflesebilecek kalite olarak sun.",
                "what_not_to_say": "Bunu garantili kader, kesin evlilik, kesin hastalik veya geri donulmez sonuc gibi sunma.",
                "premium_synthesis_sentence": f"{nakshatra.replace('_', ' ').title()} daha cok, aktivasyon desteklendikce belirginlesen ince bir kalite olarak okunmalidir.",
                "tags": [tag for tag in [nakshatra, paragraph_type, metadata.get('ruler') or ""] if tag],
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
