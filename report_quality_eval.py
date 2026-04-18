import math
import re
from collections import Counter
from itertools import combinations


SECTION_RE = re.compile(r"^#{2,4}\s+(.+?)\s*$", re.MULTILINE)
SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
WORD_RE = re.compile(r"[A-Za-zÇĞİÖŞÜçğıöşü']+", re.UNICODE)

STOPWORDS = {
    "the", "and", "for", "that", "with", "this", "from", "your", "you", "are", "but", "not", "into", "what",
    "bir", "ve", "bu", "ile", "icin", "için", "olarak", "daha", "olan", "degil", "değil", "gereken", "kadar",
}

LANGUAGE_MARKERS = {
    "tr": {
        "english": {
            "this period", "this recommendation", "chart-based guidance", "you are choosing",
            "your chart", "journey", "growth", "transformation",
        }
    },
    "en": {
        "turkish": {
            "bu dönem", "danışmanlık", "harita", "zamanlama", "fırsat", "riskler", "yönlendirme",
            "kariyer", "ilişkiler", "karar kalitesi",
        }
    },
}

OVERUSED_KEYWORDS = {
    "tr": ["enerji", "dönüşüm", "yolculuk", "tema", "bu dönem"],
    "en": ["energy", "growth", "transformation", "journey", "theme", "this period"],
}

ADVISORY_KEYWORDS = {
    "tr": [
        "öncelik", "karar", "zamanlama", "konumlanma", "tempo", "denge", "netlik", "strateji",
        "yapılandır", "sınır", "odak", "seçim", "baskı", "fırsat", "kaçın", "ilerle",
    ],
    "en": [
        "priority", "decision", "timing", "positioning", "pacing", "leverage", "clarity", "strategy",
        "structure", "boundary", "focus", "choice", "pressure", "opportunity", "avoid", "move",
    ],
}

GENERIC_AI_PATTERNS = {
    "it is important to", "it is essential to", "this journey", "embrace the energy",
    "bu süreçte önemli olan", "enerjiyi kucakla", "kendini keşfetme yolculuğu",
}


GOLDEN_SAMPLES = [
    {
        "id": "tr_career_high_timing",
        "structured_payload": {
            "language": "tr",
            "full_name": "Aylin",
            "psychological_themes": {"primary": "kariyerde görünürlük ve sorumluluk"},
            "life_area_analysis": {"career": {"score": 9}, "money": {"score": 7}},
            "narrative_analysis": {"dominant": "mesleki konumlanma"},
            "timing_data": {"peak": {"label": "Mayis ortasi", "intensity": "high"}},
            "interpretation_context": {"confidence_level": "high", "primary_focus": "career"},
        },
        "expected_output_characteristics": {
            "dominant_themes": ["career", "visibility", "responsibility"],
            "life_areas": ["career", "money"],
            "timing_intensity": "high",
            "language": "tr",
        },
    },
    {
        "id": "tr_relationship_moderate_timing",
        "structured_payload": {
            "language": "tr",
            "full_name": "Deniz",
            "psychological_themes": {"primary": "ilişkide sınır ve yakınlık dengesi"},
            "life_area_analysis": {"relationships": {"score": 8}, "identity": {"score": 6}},
            "narrative_analysis": {"dominant": "duygusal netlik"},
            "timing_data": {"opportunity": {"label": "Haziran baslangici", "intensity": "moderate"}},
            "interpretation_context": {"confidence_level": "moderate", "primary_focus": "relationships"},
        },
        "expected_output_characteristics": {
            "dominant_themes": ["relationship", "boundaries", "closeness"],
            "life_areas": ["relationships", "identity"],
            "timing_intensity": "moderate",
            "language": "tr",
        },
    },
    {
        "id": "en_identity_direction_low_timing",
        "structured_payload": {
            "language": "en",
            "full_name": "Mira",
            "psychological_themes": {"primary": "identity reset and self-direction"},
            "life_area_analysis": {"identity": {"score": 9}, "home": {"score": 5}},
            "narrative_analysis": {"dominant": "choosing a clearer direction"},
            "timing_data": {"integration": {"label": "late summer", "intensity": "low"}},
            "interpretation_context": {"confidence_level": "moderate", "primary_focus": "identity"},
        },
        "expected_output_characteristics": {
            "dominant_themes": ["identity", "direction", "self-definition"],
            "life_areas": ["identity", "home"],
            "timing_intensity": "low",
            "language": "en",
        },
    },
    {
        "id": "tr_parent_child_support",
        "structured_payload": {
            "language": "tr",
            "report_type": "parent_child",
            "psychological_themes": {"primary": "çocuğun temposunu doğru okuma"},
            "life_area_analysis": {"family": {"score": 9}, "communication": {"score": 8}},
            "narrative_analysis": {"dominant": "ebeveyn-çocuk iletişim dengesi"},
            "timing_data": {"pressure": {"label": "okul donemi baslangici", "intensity": "moderate"}},
            "interpretation_context": {"confidence_level": "high", "primary_focus": "family"},
        },
        "expected_output_characteristics": {
            "dominant_themes": ["family", "communication", "support"],
            "life_areas": ["family", "communication"],
            "timing_intensity": "moderate",
            "language": "tr",
        },
    },
    {
        "id": "en_career_money_pressure",
        "structured_payload": {
            "language": "en",
            "psychological_themes": {"primary": "professional pressure and financial pacing"},
            "life_area_analysis": {"career": {"score": 8}, "money": {"score": 8}},
            "narrative_analysis": {"dominant": "responsible expansion"},
            "timing_data": {"pressure": {"label": "Q3", "intensity": "high"}},
            "interpretation_context": {"confidence_level": "high", "primary_focus": "career"},
        },
        "expected_output_characteristics": {
            "dominant_themes": ["career", "money", "pacing"],
            "life_areas": ["career", "money"],
            "timing_intensity": "high",
            "language": "en",
        },
    },
    {
        "id": "tr_life_direction_transition",
        "structured_payload": {
            "language": "tr",
            "psychological_themes": {"primary": "yaşam yönünü sadeleştirme"},
            "life_area_analysis": {"direction": {"score": 9}, "relationships": {"score": 5}, "career": {"score": 6}},
            "narrative_analysis": {"dominant": "çoklu konuları tek önceliğe indirme"},
            "timing_data": {"build_up": {"label": "ilkbahar", "intensity": "moderate"}, "peak": {"label": "yaz baslangici"}},
            "interpretation_context": {"confidence_level": "moderate", "primary_focus": "direction"},
        },
        "expected_output_characteristics": {
            "dominant_themes": ["direction", "priority", "transition"],
            "life_areas": ["direction", "career", "relationships"],
            "timing_intensity": "moderate",
            "language": "tr",
        },
    },
]


def _normalize_language(language):
    normalized = str(language or "tr").strip().lower()
    return normalized if normalized in {"tr", "en"} else "tr"


def _sentences(text):
    return [part.strip() for part in SENTENCE_RE.split(text or "") if len(part.strip()) > 20]


def _tokens(text):
    return [
        token.lower()
        for token in WORD_RE.findall(text or "")
        if len(token) > 2 and token.lower() not in STOPWORDS
    ]


def _split_sections(text):
    matches = list(SECTION_RE.finditer(text or ""))
    if not matches:
        return {"body": text or ""}
    sections = {}
    for index, match in enumerate(matches):
        title = match.group(1).strip()
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        sections[title] = text[start:end].strip()
    return sections


def _ngrams(tokens, size=4):
    return [" ".join(tokens[index:index + size]) for index in range(0, max(len(tokens) - size + 1, 0))]


def _duplicate_sentence_ratio(sentences):
    if not sentences:
        return 0.0
    normalized = [re.sub(r"\s+", " ", sentence.lower()) for sentence in sentences]
    counts = Counter(normalized)
    duplicates = sum(count - 1 for count in counts.values() if count > 1)
    return duplicates / max(len(normalized), 1)


def _repeated_phrase_density(tokens):
    if len(tokens) < 16:
        return 0.0
    grams = _ngrams(tokens, 4)
    counts = Counter(grams)
    repeated = sum(count - 1 for count in counts.values() if count > 1)
    return repeated / max(len(grams), 1)


def _jaccard(left, right):
    left_set = set(_tokens(left))
    right_set = set(_tokens(right))
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _section_overlap_score(sections):
    bodies = [body for body in sections.values() if body.strip()]
    if len(bodies) < 2:
        return 0.0
    similarities = [_jaccard(left, right) for left, right in combinations(bodies, 2)]
    return max(similarities) if similarities else 0.0


def _keyword_overuse_score(text, language):
    lowered = (text or "").lower()
    words = max(len(_tokens(text)), 1)
    hits = 0
    for keyword in OVERUSED_KEYWORDS[_normalize_language(language)]:
        hits += lowered.count(keyword)
    return min(hits / max(words / 120, 1), 1.0)


def _language_quality_score(text, language, flags):
    lowered = (text or "").lower()
    language = _normalize_language(language)
    marker_key = "english" if language == "tr" else "turkish"
    leaked = [marker for marker in LANGUAGE_MARKERS[language][marker_key] if marker in lowered]
    if leaked:
        flags.append(f"mixed_language_markers:{','.join(leaked[:5])}")
    return max(0.0, 1.0 - (0.18 * len(leaked)))


def _advisory_tone_score(text, language, flags):
    lowered = (text or "").lower()
    tokens = _tokens(text)
    advisory_hits = sum(lowered.count(keyword) for keyword in ADVISORY_KEYWORDS[_normalize_language(language)])
    generic_hits = sum(1 for pattern in GENERIC_AI_PATTERNS if pattern in lowered)
    if generic_hits:
        flags.append(f"generic_ai_patterns:{generic_hits}")
    density = advisory_hits / max(math.sqrt(len(tokens)), 1)
    return max(0.0, min(1.0, density - (generic_hits * 0.12)))


def evaluate_report_output(report_text, language):
    """Return lightweight report-quality metrics; lower repetition/overlap is better, higher tone/language is better."""
    language = _normalize_language(language)
    flags = []
    text = report_text or ""
    sentences = _sentences(text)
    tokens = _tokens(text)
    sections = _split_sections(text)

    duplicate_sentence_ratio = _duplicate_sentence_ratio(sentences)
    repeated_phrase_density = _repeated_phrase_density(tokens)
    keyword_overuse = _keyword_overuse_score(text, language)
    repetition_score = min(1.0, (duplicate_sentence_ratio * 0.45) + (repeated_phrase_density * 0.35) + (keyword_overuse * 0.20))

    section_overlap = _section_overlap_score(sections)
    theme_distinctness = max(0.0, 1.0 - section_overlap - (repetition_score * 0.25))
    advisory_tone_score = _advisory_tone_score(text, language, flags)
    language_quality = _language_quality_score(text, language, flags)

    if repetition_score > 0.22:
        flags.append("high_repetition")
    if section_overlap > 0.42:
        flags.append("high_section_overlap")
    if theme_distinctness < 0.55:
        flags.append("weak_theme_distinctness")
    if advisory_tone_score < 0.25:
        flags.append("weak_advisory_tone")
    if language_quality < 0.9:
        flags.append("language_leakage")

    return {
        "repetition_score": round(repetition_score, 4),
        "section_overlap": round(section_overlap, 4),
        "theme_distinctness": round(theme_distinctness, 4),
        "advisory_tone_score": round(advisory_tone_score, 4),
        "language_quality": round(language_quality, 4),
        "flags": flags,
    }


def evaluate_golden_sample_outputs(outputs_by_sample_id):
    results = {}
    for sample in GOLDEN_SAMPLES:
        report_text = outputs_by_sample_id.get(sample["id"], "")
        results[sample["id"]] = evaluate_report_output(report_text, sample["expected_output_characteristics"]["language"])
    return results
