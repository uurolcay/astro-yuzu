import json
import re
from collections import Counter

import database as db_mod


ASTRO_ENTITY_TAXONOMY = {
    "planets": [
        "sun", "moon", "mars", "mercury", "jupiter",
        "venus", "saturn", "rahu", "ketu",
    ],
    "houses": [
        "1st_house", "2nd_house", "3rd_house", "4th_house",
        "5th_house", "6th_house", "7th_house", "8th_house",
        "9th_house", "10th_house", "11th_house", "12th_house",
    ],
    "nakshatras": [
        "ashwini", "bharani", "krittika", "rohini", "mrigashirsha",
        "ardra", "punarvasu", "pushya", "ashlesha", "magha",
        "purva_phalguni", "uttara_phalguni", "hasta", "chitra",
        "swati", "vishakha", "anuradha", "jyeshtha", "mula",
        "purva_ashadha", "uttara_ashadha", "shravana", "dhanishta",
        "shatabhisha", "purva_bhadrapada", "uttara_bhadrapada", "revati",
    ],
    "dasha_planets": [
        "ketu_dasha", "venus_dasha", "sun_dasha", "moon_dasha",
        "mars_dasha", "rahu_dasha", "jupiter_dasha",
        "saturn_dasha", "mercury_dasha",
    ],
    "yogas": [
        "raj_yoga", "dhan_yoga", "vipareeta_raj_yoga",
        "sunapha_yoga", "anapha_yoga", "dhurdhara_yoga",
        "neecha_bhanga_yoga", "gaja_kesari_yoga",
    ],
    "concepts": [
        "atmakaraka", "amatyakaraka", "lagna", "ascendant",
        "navamsa", "dasamsa", "transit", "antardasha",
        "mahadasha", "karaka", "drishti",
    ],
    "report_domains": [
        "career", "relationship", "health_safe_language",
        "money", "family", "parent_child", "spiritual_growth",
    ],
}

_HOUSE_ORDINALS = {
    1: ("1st", "first", "birinci"),
    2: ("2nd", "second", "ikinci"),
    3: ("3rd", "third", "ucuncu", "üçüncü"),
    4: ("4th", "fourth", "dorduncu", "dördüncü"),
    5: ("5th", "fifth", "besinci", "beşinci"),
    6: ("6th", "sixth", "altinci", "altıncı"),
    7: ("7th", "seventh", "yedinci"),
    8: ("8th", "eighth", "sekizinci"),
    9: ("9th", "ninth", "dokuzuncu"),
    10: ("10th", "tenth", "onuncu"),
    11: ("11th", "eleventh", "onbirinci"),
    12: ("12th", "twelfth", "onikinci"),
}


def _house_variants(number: int):
    raw = _HOUSE_ORDINALS.get(number) or ()
    ordinal = raw[0] if len(raw) > 0 else f"{number}th"
    english_word = raw[1] if len(raw) > 1 else ordinal
    turkish_words = raw[2:] if len(raw) > 2 else ()
    return ordinal, english_word, turkish_words


def _loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def _normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("’", "'").replace("'", "")
    text = text.replace("-", "_").replace(" ", "_")
    text = re.sub(r"[^\w._]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"_+", "_", text).strip("_.")
    return text


def _dedupe_sorted(values):
    return sorted({str(value) for value in values if str(value or "").strip()})


def _house_entity(number) -> str | None:
    try:
        value = int(number)
    except Exception:
        return None
    if 1 <= value <= 12:
        suffix = "th"
        if value == 1:
            suffix = "st"
        elif value == 2:
            suffix = "nd"
        elif value == 3:
            suffix = "rd"
        return f"{value}{suffix}_house"
    return None


def _readable_variants(entity: str) -> set[str]:
    variants = {entity, entity.replace("_", " ")}
    if entity.endswith("_house"):
        try:
            number = int(entity.split("_", 1)[0][:-2])
        except Exception:
            number = None
        if number in _HOUSE_ORDINALS:
            ordinal, english_word, turkish_words = _house_variants(number)
            variants.update(
                {
                    f"{ordinal.lower()} house",
                    f"{english_word} house",
                    f"house {number}",
                    f"{number}. ev",
                }
            )
            variants.update(f"{word} ev" for word in turkish_words)
    if entity.endswith("_dasha"):
        planet = entity[:-6]
        variants.update(
            {
                f"{planet} dasha",
                f"{planet} mahadasha",
                f"{planet} antardasha",
            }
        )
    if entity.startswith("pada_"):
        number = entity.split("_", 1)[1]
        variants.update({f"pada {number}", f"{number}rd pada", f"{number}. pada"})
    return {variant.lower() for variant in variants if variant}


def normalize_entity(text: str) -> str:
    """
    Normalize entity strings.
    """
    raw = str(text or "").strip()
    if not raw:
        return ""
    lowered = raw.lower().strip()
    lowered = re.sub(r"[^\w\s.-]+", " ", lowered, flags=re.UNICODE)
    lowered = re.sub(r"\s+", " ", lowered).strip()

    house_match = re.search(r"\b([1-9]|1[0-2])(?:st|nd|rd|th)?\s*house\b", lowered)
    if house_match:
        return _house_entity(house_match.group(1)) or ""
    house_match = re.search(r"\bhouse\s*([1-9]|1[0-2])\b", lowered)
    if house_match:
        return _house_entity(house_match.group(1)) or ""
    house_match = re.search(r"\b([1-9]|1[0-2])\.\s*ev\b", lowered)
    if house_match:
        return _house_entity(house_match.group(1)) or ""
    for number, variants in _HOUSE_ORDINALS.items():
        if any(f"{word} ev" in lowered for word in variants[2:]):
            return _house_entity(number) or ""

    dasha_match = re.search(
        r"\b(sun|moon|mars|mercury|jupiter|venus|saturn|rahu|ketu)\s+(?:maha)?antardasha\b",
        lowered,
    ) or re.search(
        r"\b(sun|moon|mars|mercury|jupiter|venus|saturn|rahu|ketu)\s+(?:mahadasha|dasha)\b",
        lowered,
    )
    if dasha_match:
        return f"{dasha_match.group(1)}_dasha"

    normalized = lowered.replace("-", "_").replace(" ", "_")
    normalized = re.sub(r"[^\w.]+", "_", normalized, flags=re.UNICODE)
    normalized = re.sub(r"_+", "_", normalized).strip("_.")
    return normalized


def extract_entities_from_text(text: str) -> list[str]:
    """
    Extract known astrology entities from free text.
    """
    try:
        source = str(text or "")
        if not source.strip():
            return []
        lowered = source.lower()
        found = set()

        for category_entities in ASTRO_ENTITY_TAXONOMY.values():
            for entity in category_entities:
                for variant in _readable_variants(entity):
                    if variant and variant in lowered:
                        found.add(entity)
                        break

        for number in range(1, 13):
            entity = _house_entity(number)
            if not entity:
                continue
            ordinal, english_word, turkish_words = _house_variants(number)
            patterns = [
                rf"\b{number}(?:st|nd|rd|th)?\s*house\b",
                rf"\bhouse\s*{number}\b",
                rf"\b{number}\.\s*ev\b",
                rf"\b{english_word}\s+house\b",
            ]
            patterns.extend(rf"\b{word}\s+ev\b" for word in turkish_words)
            if any(re.search(pattern, lowered) for pattern in patterns):
                found.add(entity)

        for planet in ASTRO_ENTITY_TAXONOMY["planets"]:
            if re.search(rf"\b{planet}\s+(?:dasha|mahadasha|antardasha)\b", lowered):
                found.add(f"{planet}_dasha")

        pada_matchers = [
            re.finditer(r"\bpada\s*([1-4])\b", lowered),
            re.finditer(r"\b([1-4])(?:st|nd|rd|th)\s+pada\b", lowered),
            re.finditer(r"\b([1-4])\.\s*pada\b", lowered),
        ]
        for matcher_group in pada_matchers:
            for match in matcher_group:
                found.add(f"pada_{match.group(1)}")

        return sorted(found)
    except Exception:
        return []


def extract_entities_from_chart_data(chart_data: dict) -> list[str]:
    """
    Extract expected entities from structured chart data.
    """
    try:
        if not isinstance(chart_data, dict) or not chart_data:
            return []
        found = set()
        for planet in chart_data.get("planets") or []:
            if not isinstance(planet, dict):
                continue
            name = normalize_entity(planet.get("name") or planet.get("planet"))
            if name:
                found.add(name)
            house = _house_entity(planet.get("house"))
            if house:
                found.add(house)
            nakshatra = normalize_entity(planet.get("nakshatra"))
            if nakshatra:
                found.add(nakshatra)
            pada = normalize_entity(f"pada {planet.get('pada')}") if planet.get("pada") else ""
            if pada:
                found.add(pada)

        ascendant = chart_data.get("ascendant") or {}
        if isinstance(ascendant, dict):
            nakshatra = normalize_entity(ascendant.get("nakshatra"))
            if nakshatra:
                found.add(nakshatra)

        active_dasha = chart_data.get("active_dasha") or {}
        dasha_planet = (
            chart_data.get("dasha_planet")
            or (active_dasha.get("planet") if isinstance(active_dasha, dict) else None)
            or chart_data.get("mahadasha")
            or chart_data.get("antardasha")
        )
        normalized_dasha = normalize_entity(f"{dasha_planet} dasha") if dasha_planet else ""
        if normalized_dasha:
            found.add(normalized_dasha)

        karakas = chart_data.get("karakas") or {}
        if isinstance(karakas, dict):
            for key in ("atmakaraka", "amatyakaraka"):
                planet = normalize_entity(karakas.get(key))
                if planet:
                    found.add(planet)
            if karakas.get("atmakaraka"):
                found.add("atmakaraka")
            if karakas.get("amatyakaraka"):
                found.add("amatyakaraka")

        for yoga in chart_data.get("yogas") or []:
            if isinstance(yoga, dict):
                candidate = yoga.get("name") or yoga.get("key") or yoga.get("label")
            else:
                candidate = yoga
            normalized = normalize_entity(candidate)
            if normalized:
                found.add(normalized)

        for signal in chart_data.get("dominant_signals") or []:
            if not isinstance(signal, dict):
                continue
            for field in ("key", "domain", "planet", "label"):
                normalized = normalize_entity(signal.get(field))
                if normalized:
                    found.add(normalized)

        report_type = normalize_entity(chart_data.get("report_type"))
        if report_type:
            found.add(report_type)

        return sorted(found)
    except Exception:
        return []


def count_chunks_for_entity(db, entity: str) -> int:
    """
    Count KnowledgeChunk rows that cover the entity.
    """
    try:
        normalized = normalize_entity(entity)
        if not normalized:
            return 0
        count = 0
        variants = _readable_variants(normalized)
        chunks = db.query(db_mod.KnowledgeChunk).all()
        for chunk in chunks:
            matched = False
            for candidate in _loads(getattr(chunk, "coverage_entities_json", None), []):
                if normalize_entity(candidate) == normalized:
                    matched = True
                    break
            if not matched:
                chunk_text = str(getattr(chunk, "chunk_text", "") or "").lower()
                if any(variant in chunk_text for variant in variants):
                    matched = True
            if not matched and getattr(chunk, "knowledge_item", None):
                for candidate in _loads(getattr(chunk.knowledge_item, "coverage_entities_json", None), []):
                    if normalize_entity(candidate) == normalized:
                        matched = True
                        break
            if matched:
                count += 1
        return count
    except Exception:
        return 0


def _coverage_level(chunk_count: int) -> tuple[str, int]:
    if chunk_count <= 0:
        return "missing", 0
    if chunk_count <= 2:
        return "weak", 25
    if chunk_count <= 5:
        return "moderate", 60
    return "strong", 100


def compute_knowledge_coverage(db, *, max_chunks=None) -> dict:
    """
    Compute coverage across ASTRO_ENTITY_TAXONOMY.
    """
    empty = {
        "by_category": {},
        "summary": {
            "total_entities": 0,
            "missing_count": 0,
            "weak_count": 0,
            "moderate_count": 0,
            "strong_count": 0,
            "overall_pct": 0.0,
        },
        "missing_entities": [],
        "weak_entities": [],
    }
    try:
        try:
            max_chunks = int(max_chunks) if max_chunks is not None else None
        except (TypeError, ValueError):
            max_chunks = None
        chunk_query = db.query(db_mod.KnowledgeChunk).order_by(db_mod.KnowledgeChunk.created_at.desc())
        if max_chunks and max_chunks > 0:
            chunk_query = chunk_query.limit(max_chunks)
        chunks = chunk_query.all()
        entity_counts = Counter()
        for chunk in chunks:
            chunk_entities = set()
            for candidate in _loads(getattr(chunk, "coverage_entities_json", None), []):
                normalized = normalize_entity(candidate)
                if normalized:
                    chunk_entities.add(normalized)
            knowledge_item = getattr(chunk, "knowledge_item", None)
            if knowledge_item is not None:
                for candidate in _loads(getattr(knowledge_item, "coverage_entities_json", None), []):
                    normalized = normalize_entity(candidate)
                    if normalized:
                        chunk_entities.add(normalized)
            chunk_text = str(getattr(chunk, "chunk_text", "") or "").lower()
            for category_entities in ASTRO_ENTITY_TAXONOMY.values():
                for entity in category_entities:
                    normalized_entity = normalize_entity(entity)
                    if normalized_entity in chunk_entities:
                        continue
                    if any(variant in chunk_text for variant in _readable_variants(normalized_entity)):
                        chunk_entities.add(normalized_entity)
            for entity in chunk_entities:
                entity_counts[entity] += 1

        by_category = {}
        all_pcts = []
        missing_entities = []
        weak_entities = []
        summary_counts = Counter()
        total_entities = 0
        for category, entities in ASTRO_ENTITY_TAXONOMY.items():
            category_rows = {}
            for entity in entities:
                chunk_count = entity_counts[normalize_entity(entity)]
                level, pct = _coverage_level(chunk_count)
                category_rows[entity] = {
                    "chunk_count": chunk_count,
                    "level": level,
                    "pct": pct,
                }
                total_entities += 1
                all_pcts.append(pct)
                summary_counts[level] += 1
                if level == "missing":
                    missing_entities.append(entity)
                elif level == "weak":
                    weak_entities.append(entity)
            by_category[category] = category_rows
        return {
            "by_category": by_category,
            "summary": {
                "total_entities": total_entities,
                "missing_count": summary_counts["missing"],
                "weak_count": summary_counts["weak"],
                "moderate_count": summary_counts["moderate"],
                "strong_count": summary_counts["strong"],
                "overall_pct": round(sum(all_pcts) / len(all_pcts), 2) if all_pcts else 0.0,
            },
            "missing_entities": sorted(missing_entities),
            "weak_entities": sorted(weak_entities),
        }
    except Exception:
        return empty


def detect_unused_knowledge(used_chunk_ids: list[int], output_text: str, db) -> list[dict]:
    """
    Determine whether retrieved chunks appeared in the final output.
    """
    try:
        if not used_chunk_ids:
            return []
        output_entities = set(extract_entities_from_text(output_text))
        unused = []
        chunks = db.query(db_mod.KnowledgeChunk).filter(db_mod.KnowledgeChunk.id.in_(used_chunk_ids)).all()
        for chunk in chunks:
            entities = set(extract_entities_from_text(getattr(chunk, "chunk_text", "")))
            entities.update(normalize_entity(entity) for entity in _loads(getattr(chunk, "coverage_entities_json", None), []))
            entities = {entity for entity in entities if entity}
            if entities and output_entities.intersection(entities):
                continue
            unused.append(
                {
                    "chunk_id": chunk.id,
                    "chunk_text_preview": str(chunk.chunk_text or "")[:160],
                    "entities": sorted(entities),
                    "reason": "no entity from this chunk found in output",
                }
            )
        return unused
    except Exception:
        return []


def _extract_chart_payload(interpretation) -> dict:
    for field in ("input_payload_json", "chart_data_json", "payload_json", "raw_payload_json"):
        raw = getattr(interpretation, field, None)
        if not raw:
            continue
        payload = _loads(raw, {})
        if not isinstance(payload, dict):
            continue
        if isinstance(payload.get("natal_data"), dict):
            chart_data = dict(payload.get("natal_data") or {})
            for extra_key in ("active_dasha", "dasha_planet", "mahadasha", "antardasha", "karakas", "yogas", "dominant_signals", "report_type"):
                if payload.get(extra_key) is not None and chart_data.get(extra_key) is None:
                    chart_data[extra_key] = payload.get(extra_key)
            return chart_data
        return payload
    return {}


def get_knowledge_trace_for_interpretation(db, interpretation) -> dict:
    """
    Build full knowledge trace for one InternalInterpretation.
    """
    empty = {
        "used_chunks": [],
        "used_entity_set": [],
        "chart_expected_entities": [],
        "output_entities": [],
        "missing_from_knowledge": [],
        "retrieved_but_weak": [],
        "not_in_output": [],
        "unused_chunks": [],
        "gap_types": {},
    }
    try:
        used_chunk_ids = _loads(getattr(interpretation, "used_chunk_ids_json", None), [])
        if not isinstance(used_chunk_ids, list):
            used_chunk_ids = []
        used_chunk_ids = [int(chunk_id) for chunk_id in used_chunk_ids if str(chunk_id).isdigit()]
        output_text = str(getattr(interpretation, "interpretation_text", "") or "")
        output_entities = extract_entities_from_text(output_text)
        chart_data = _extract_chart_payload(interpretation)
        chart_expected_entities = extract_entities_from_chart_data(chart_data)

        used_chunks = []
        used_entity_set = set()
        if used_chunk_ids:
            chunks = db.query(db_mod.KnowledgeChunk).filter(db_mod.KnowledgeChunk.id.in_(used_chunk_ids)).all()
            chunk_map = {chunk.id: chunk for chunk in chunks}
            for chunk_id in used_chunk_ids:
                chunk = chunk_map.get(chunk_id)
                if not chunk:
                    continue
                chunk_entities = set(extract_entities_from_text(getattr(chunk, "chunk_text", "")))
                chunk_entities.update(normalize_entity(entity) for entity in _loads(getattr(chunk, "coverage_entities_json", None), []))
                chunk_entities = {entity for entity in chunk_entities if entity}
                used_entity_set.update(chunk_entities)
                counts = [count_chunks_for_entity(db, entity) for entity in chunk_entities] or [0]
                level, _pct = _coverage_level(max(counts))
                used_chunks.append(
                    {
                        "chunk_id": chunk.id,
                        "chunk_text_preview": str(chunk.chunk_text or "")[:180],
                        "knowledge_item_title": chunk.knowledge_item.title if chunk.knowledge_item else "Knowledge",
                        "entities": sorted(chunk_entities),
                        "coverage_level": level if level else "unknown",
                    }
                )

        gap_types = {}
        missing_from_knowledge = []
        retrieved_but_weak = []
        for entity in chart_expected_entities:
            chunk_count = count_chunks_for_entity(db, entity)
            if chunk_count == 0:
                gap_types[entity] = "missing_knowledge"
                missing_from_knowledge.append(entity)
            elif chunk_count <= 2:
                gap_types[entity] = "low_depth"
                retrieved_but_weak.append(entity)

        not_in_output = []
        output_entity_set = set(output_entities)
        for entity in sorted(used_entity_set):
            if entity not in output_entity_set:
                gap_types[entity] = "unused_knowledge"
                not_in_output.append(entity)

        return {
            "used_chunks": used_chunks,
            "used_entity_set": sorted(used_entity_set),
            "chart_expected_entities": chart_expected_entities,
            "output_entities": output_entities,
            "missing_from_knowledge": sorted(missing_from_knowledge),
            "retrieved_but_weak": sorted(retrieved_but_weak),
            "not_in_output": sorted(not_in_output),
            "unused_chunks": detect_unused_knowledge(used_chunk_ids, output_text, db),
            "gap_types": gap_types,
        }
    except Exception:
        return empty


def build_suggested_training_tasks_from_trace(trace: dict) -> list[dict]:
    """
    Convert trace gaps into admin-readable suggested training tasks.
    """
    suggestions = []
    for entity, gap_type in (trace or {}).get("gap_types", {}).items():
        if gap_type == "missing_knowledge":
            suggestions.append(
                {
                    "priority": "high",
                    "title": f"Add source material for {entity}",
                    "entity": entity,
                    "reason": gap_type,
                }
            )
        elif gap_type == "low_depth":
            suggestions.append(
                {
                    "priority": "medium",
                    "title": f"Deepen interpretation material for {entity}",
                    "entity": entity,
                    "reason": gap_type,
                }
            )
        elif gap_type == "unused_knowledge":
            suggestions.append(
                {
                    "priority": "low",
                    "title": f"Review prompt usage for {entity}",
                    "entity": entity,
                    "reason": gap_type,
                }
            )
    return suggestions
