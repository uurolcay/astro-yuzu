import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from copy import deepcopy
from typing import Any

from dotenv import load_dotenv
from google import genai

try:
    from services.report_structure_v3 import build_report_structure_v3
except Exception:  # pragma: no cover
    build_report_structure_v3 = None


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
VALID_LANGUAGES = {"tr", "en"}
SIGNAL_TR_LOOKUP = {
    "synchronizes and performs": "senkronize olur ve icra eder",
    "Choose value over comfort.": "Konfor yerine degeri sec.",
    "Mars drives urgency.": "Mars aciliyeti artirir.",
    "Pressure increases.": "Baski artar.",
    "Invite questions and dialogue so the child can process by interacting, not only by listening.": "Cocugun yalnizca dinleyerek degil, etkilesime girerek islemesi icin soru ve diyalog alani acin.",
    "Child core emotional pattern": "Cocugun temel duygusal oruntusu",
    "Parent-child relationship dynamic": "Ebeveyn-cocuk iliski dinamigi",
    "Parenting guidance that lands best": "En etkili ebeveynlik yaklasimi",
    "Lead with calm, specific communication": "Sakin ve net iletisimle ilerleyin",
    "Current phase": "Mevcut donem",
    "Current Phase": "Mevcut donem",
    "School and routine cycles": "Okul ve rutin donguleri",
    "Moments of stress": "Stres anlari",
    "Growth-supportive rhythm": "Gelisimi destekleyen ritim",
    "Pressure-sensitive periods": "Baskiya hassas donemler",
    "Mars period emphasis": "Mars donemi vurgusu",
    "Support learning through the child's natural pace": "Ogrenmeyi cocugun dogal temposuyla destekleyin",
    "Reduce pressure before correcting behavior": "Davranisi duzeltmeden once baskiyi azaltin",
    "The relationship strengthens when support stays calm, specific, and repeatable.": "Destek sakin, net ve tekrarlanabilir kaldiginda iliski guclenir.",
    "Use short, calm, repeatable language and check for emotional readiness before teaching.": "Kisa, sakin ve tekrarlanabilir bir dil kullanin; ogretmeden once duygusal hazir olusu kontrol edin.",
    "Emotional flow is easier here because both charts process feelings with a similar tempo.": "Her iki harita da duygulari benzer bir tempoda isledigi icin duygusal akis burada daha kolaydir.",
    "Emotional tone may differ: one chart processes quickly while the other needs more time and softness.": "Duygusal ton farki olabilir: bir harita hizli islerken digeri daha fazla zaman ve yumusaklik ister.",
    "Communication can feel naturally smooth when ideas are spoken through in real time.": "Fikirler anlik olarak konusuldugunda iletisim daha dogal ve akici hissedilebilir.",
    "Misunderstandings are more likely when the parent explains quickly but the child needs slower repetition or reassurance.": "Ebeveyn hizli anlattiginda ama cocuk daha yavas tekrar ya da guvenceye ihtiyac duydugunda yanlis anlasilmalar artabilir.",
    "The parent can help most by combining consistency, emotional safety, and a clear rhythm.": "Ebeveyn en cok; tutarliligi, duygusal guveni ve net bir ritmi birlestirerek destek olabilir.",
    "The child thrives when encouragement is consistent, emotionally safe, and tied to realistic pacing.": "Cesaretlendirme duzenli, duygusal olarak guvenli ve gercekci bir tempoya bagli oldugunda cocuk daha iyi gelisir.",
    "The relationship strengthens when correction becomes guidance and timing is treated with patience.": "Duzeltme rehberlige donustugunde ve zamanlama sabirla ele alindiginda iliski guclenir.",
    "Confidence grows through calm repetition and trust-building support.": "Sakin tekrar ve guven insa eden destekle ozguven buyur.",
    "The chart is readable, but no dominant interpretation signal is strong enough to rank as a premium lead insight yet.": "Harita okunabilir durumda, ancak henuz premium seviyede ana icgoru olacak kadar guclu bir baskin yorum sinyali yok.",
    "No dominant premium signal is concentrated in this area right now.": "Bu alanda su anda yeterince guclu bir baskin premium sinyal yogunlasmiyor.",
    "Move with structure, not urgency. The chart rewards disciplined sequencing over reactive decisions.": "Aceleyle degil, yapiyla ilerleyin. Harita tepkisel kararlar yerine disiplinli siralamayi odullendirir.",
    "Use the active opening deliberately. The chart favors timely action when the opportunity is already visible.": "Aktif acilimi bilincli kullanin. Firsat zaten gorunur oldugunda harita zamaninda atilan adimi destekler.",
    "Follow the strongest repeating theme, not the loudest short-term distraction.": "En yuksek sesli kisa vadeli dikkat dagiticiyi degil, en guclu tekrar eden temayi izleyin.",
    "No severe concentration of risk signals stands above the rest. The main risk is dilution rather than collapse.": "Digerlerinin belirgin ustune cikan siddetli bir risk yogunlugu yok. Ana risk cokusu degil, dagilmayi isaret ediyor.",
    "emotional pattern, mental rhythm, and security need": "duygusal örüntü, zihinsel ritim ve güvenlik ihtiyacı",
    "identity style, body-life approach, and first response to life": "kimlik tarzı, beden-yaşam yaklaşımı ve hayata ilk tepki",
    "purpose expression, visibility, and authority tone": "amaç ifadesi, görünürlük ve otorite tonu",
    "communication style and learning style": "iletişim tarzı ve öğrenme biçimi",
    "affection style and relationship comfort pattern": "sevgi tarzı ve ilişki konfor örüntüsü",
    "action style and conflict style": "eylem tarzı ve çatışma biçimi",
    "belief style and guidance-wisdom style": "inanç tarzı ve rehberlik-bilgelik biçimi",
    "discipline style and pressure response": "disiplin tarzı ve baskıya tepki",
    "desire pattern, obsession vector, and unconventional growth": "arzu örüntüsü, takıntı vektörü ve alışılmadık büyüme",
    "detachment pattern, karmic release, and spiritualization": "kopukluk örüntüsü, karmik serbest bırakma ve ruhsallaşma",
    "identity and embodied direction": "kimlik ve bedensel yön",
    "value, resources, and speech karma": "değer, kaynaklar ve söz karması",
    "effort, courage, and self-generated initiative": "çaba, cesaret ve öz-girişim",
    "emotional roots, home, and inner stability": "duygusal kökler, yuva ve iç istikrar",
    "creativity, intelligence, children, and merit": "yaratıcılık, zekâ, çocuklar ve erdem",
    "friction, discipline, labor, and karmic correction": "sürtüşme, disiplin, emek ve karmik düzeltme",
    "partnership, mirroring, and relational contracts": "ortaklık, yansıma ve ilişki sözleşmeleri",
    "upheaval, hidden karma, and deep transformation": "alt üst oluş, gizli karma ve derin dönüşüm",
    "belief, dharma, mentors, and meaning": "inanç, dharma, mentorlar ve anlam",
    "career, duty, visibility, and public consequence": "kariyer, görev, görünürlük ve kamusal sonuç",
    "gains, networks, and desire fulfillment": "kazanımlar, ağlar ve arzu gerçekleşimi",
    "release, loss, surrender, and spiritual closure": "bırakma, kayıp, teslimiyet ve ruhsal kapanış",
    "an unclassified karmic life area": "sınıflandırılmamış karmik yaşam alanı",
}


def _localize_known_english_text(value: Any, language: str) -> Any:
    text = str(value or "")
    if language != "tr" or not text:
        return value
    if text in SIGNAL_TR_LOOKUP:
        return SIGNAL_TR_LOOKUP[text]
    if text.startswith("The chart is currently led by "):
        body = text.replace("The chart is currently led by ", "", 1).rstrip(".")
        body = body.replace("money and nodes", "finans ve Ay Dugumleri")
        body = body.replace("money", "finans")
        body = body.replace("nodes", "Ay Dugumleri")
        body = body.replace("Timing and money", "zamanlama ve finans")
        body = body.replace("timing and money", "zamanlama ve finans")
        return f"Haritada su anda {body} temasi one cikiyor."
    if text.startswith("The chart is currently "):
        body = text.replace("The chart is currently ", "", 1).rstrip(".")
        body = body.replace("money and nodes", "finans ve Ay Dugumleri")
        body = body.replace("money", "finans")
        body = body.replace("nodes", "Ay Dugumleri")
        return f"Haritada su anda {body} vurgusu one cikiyor."
    if " shows a clear inner pattern that responds strongly to emotional tone and pacing. " in text and " grows best when guidance stays calm, specific, and emotionally attuned." in text:
        child_name, tail = text.split(" shows a clear inner pattern that responds strongly to emotional tone and pacing. ", 1)
        parent_name = tail.replace("The relationship with ", "", 1).replace(" grows best when guidance stays calm, specific, and emotionally attuned.", "")
        return (
            f"{child_name} duygusal ton ve tempoya guclu tepki veren belirgin bir ic oruntu gosteriyor. "
            f"{parent_name} ile iliski, rehberlik sakin, net ve duygusal olarak uyumlu kaldiginda en saglikli sekilde gelisiyor."
        )
    if " period emphasis" in text:
        return text.replace(" period emphasis", " donemi vurgusu")
    replacements = {
        "money": "finans",
        "wealth": "kaynak ve kazanc",
        "career": "kariyer",
        "growth": "buyume",
        "timing": "zamanlama",
        "nodes": "Ay Dugumleri",
        "relationship": "iliski",
        "parent-child": "ebeveyn-cocuk",
    }
    localized = text
    for source, target in replacements.items():
        localized = localized.replace(source, target)
    return localized


def _localize_nested_payload_strings(value: Any, language: str) -> Any:
    if language != "tr":
        return value
    if isinstance(value, dict):
        return {key: _localize_nested_payload_strings(item, language) for key, item in value.items()}
    if isinstance(value, list):
        return [_localize_nested_payload_strings(item, language) for item in value]
    if isinstance(value, str):
        return _localize_known_english_text(value, language)
    return value


def _rebuild_nakshatra_explanation_tr(signal: dict) -> str | None:
    """
    Rebuild a Turkish explanation string for a nakshatra signal
    by translating each component field via SIGNAL_TR_LOOKUP
    and assembling a Turkish sentence pattern.

    Returns None if required fields are missing.
    """
    profile = signal.get("nakshatra_profile")
    if not isinstance(profile, dict):
        return None
    planet = signal.get("planet") or ""
    nakshatra = profile.get("nakshatra") or str(signal.get("label", "")).split(" - ")[-1]
    focus_raw = signal.get("domain") or ""

    core_action = SIGNAL_TR_LOOKUP.get(
        profile.get("core_action", ""), profile.get("core_action", "")
    )
    dependency = SIGNAL_TR_LOOKUP.get(
        profile.get("dependency", ""), profile.get("dependency", "")
    )
    output = SIGNAL_TR_LOOKUP.get(
        profile.get("output", ""), profile.get("output", "")
    )
    risk = SIGNAL_TR_LOOKUP.get(
        profile.get("risk_pattern", ""), profile.get("risk_pattern", "")
    )
    evolution = SIGNAL_TR_LOOKUP.get(
        profile.get("evolution_path", ""), profile.get("evolution_path", "")
    )
    focus = SIGNAL_TR_LOOKUP.get(focus_raw, focus_raw)

    if not core_action:
        return None

    return (
        f"{planet} – {nakshatra}: {focus}. "
        f"Temel eylem: {core_action}; bağımlılık: {dependency}; "
        f"çıktı: {output}; risk: {risk}; evrim yolu: {evolution}."
    )


def _rebuild_atmakaraka_explanation_tr(signal: dict) -> str | None:
    planet = signal.get("planet") or ""
    desire = SIGNAL_TR_LOOKUP.get(
        signal.get("desire_pattern", ""), signal.get("desire_pattern", "")
    )
    lesson = SIGNAL_TR_LOOKUP.get(
        signal.get("soul_lesson", ""), signal.get("soul_lesson", "")
    )
    house = SIGNAL_TR_LOOKUP.get(
        signal.get("house_domain", ""), signal.get("house_domain", "")
    )
    if not planet or not desire:
        return None
    return (
        f"{planet} Atmakaraka'dır; haritanın ruh yönü {desire} üzerinden şekillenir. "
        f"Temel ders {lesson} — özellikle {house} alanında."
    )


class AIConfigurationError(RuntimeError):
    pass


class AIServiceError(RuntimeError):
    pass


@dataclass(frozen=True)
class AgentResult:
    name: str
    output: str
    model: str
    generated_at: str


def _safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def _normalize_language(language: Any) -> str:
    normalized = str(language or "tr").strip().lower()
    return normalized if normalized in VALID_LANGUAGES else "tr"


def _merge_payload(data: dict | None = None, **kwargs) -> dict:
    payload = data.copy() if isinstance(data, dict) else {}
    payload.update(kwargs)
    return payload


def _get_model_name() -> str:
    return os.getenv("GEMINI_MODEL", DEFAULT_MODEL)


def _get_model_client():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise AIConfigurationError("GEMINI_API_KEY environment variable bulunamadi. Lutfen .env dosyasini kontrol edin.")
    return genai.Client(api_key=api_key)


def localize_signal_explanations(
    astro_signal_context: dict,
    language: str,
    *,
    client=None,
    model: str | None = None,
) -> dict:
    if language != "tr" or not isinstance(astro_signal_context, dict):
        return astro_signal_context

    localized = deepcopy(astro_signal_context)

    def translate(container: dict, field: str) -> None:
        if not isinstance(container, dict):
            return
        value = container.get(field)
        if isinstance(value, str):
            container[field] = _localize_known_english_text(value, language)

    def walk_list(items: Any) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            walk_dict(item)

    def walk_dict(container: Any) -> None:
        if not isinstance(container, dict):
            return
        for field in [
            "explanation", "soul_lesson", "desire_pattern", "risk_pattern",
            "evolution_path", "core_action", "dependency", "output",
            "psychological_pattern", "behavioral_pattern", "interaction_style",
            "summary", "base_condition", "house_domain",
        ]:
            translate(container, field)

    walk_list(localized.get("dominant_signals"))
    walk_list(localized.get("risk_signals"))
    walk_list(localized.get("opportunity_signals"))

    nakshatra = localized.get("nakshatra_signals") or {}
    walk_dict(nakshatra.get("moon_nakshatra"))
    walk_dict(nakshatra.get("lagna_nakshatra"))
    walk_list(nakshatra.get("planet_signals"))
    walk_list(nakshatra.get("planetary_nakshatra_signals"))

    atmakaraka = localized.get("atmakaraka_signals") or {}
    walk_list(atmakaraka.get("signals"))
    for field in ("soul_lesson", "desire_pattern", "risk_pattern", "evolution_path", "house_domain"):
        translate(atmakaraka, field)

    yoga = localized.get("yoga_signals") or {}
    walk_list(yoga.get("detected_yogas"))

    def rebuild_nakshatra_signals(items: Any) -> None:
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("nakshatra_profile"):
                rebuilt = _rebuild_nakshatra_explanation_tr(item)
                if rebuilt:
                    item["explanation"] = rebuilt
                    item["summary"] = rebuilt

    rebuild_nakshatra_signals(localized.get("dominant_signals"))
    rebuild_nakshatra_signals(localized.get("risk_signals"))
    rebuild_nakshatra_signals(localized.get("opportunity_signals"))

    moon = nakshatra.get("moon_nakshatra")
    lagna = nakshatra.get("lagna_nakshatra")
    if isinstance(moon, dict) and moon.get("nakshatra_profile"):
        rebuilt = _rebuild_nakshatra_explanation_tr(moon)
        if rebuilt:
            moon["explanation"] = rebuilt
            moon["summary"] = rebuilt
    if isinstance(lagna, dict) and lagna.get("nakshatra_profile"):
        rebuilt = _rebuild_nakshatra_explanation_tr(lagna)
        if rebuilt:
            lagna["explanation"] = rebuilt
            lagna["summary"] = rebuilt
    for item in (nakshatra.get("planet_signals") or []):
        if isinstance(item, dict) and item.get("nakshatra_profile"):
            rebuilt = _rebuild_nakshatra_explanation_tr(item)
            if rebuilt:
                item["explanation"] = rebuilt
                item["summary"] = rebuilt
    for item in (nakshatra.get("planetary_nakshatra_signals") or []):
        if isinstance(item, dict) and item.get("nakshatra_profile"):
            rebuilt = _rebuild_nakshatra_explanation_tr(item)
            if rebuilt:
                item["explanation"] = rebuilt
                item["summary"] = rebuilt

    atmakaraka_signals = localized.get("atmakaraka_signals") or {}
    for sig in (atmakaraka_signals.get("signals") or []):
        if isinstance(sig, dict):
            rebuilt = _rebuild_atmakaraka_explanation_tr(sig)
            if rebuilt:
                sig["explanation"] = rebuilt
                sig["summary"] = rebuilt

    return localized


def build_structured_payload(data: dict | None = None, **kwargs) -> dict:
    payload = _merge_payload(data, **kwargs)
    interpretation_context = payload.get("interpretation_context") or {}
    language = _normalize_language(payload.get("language") or interpretation_context.get("language"))
    if language == "tr" and interpretation_context:
        interpretation_context = _localize_nested_payload_strings(interpretation_context, language)
    report_variant = (
        payload.get("workspace_report_type")
        or payload.get("report_order_type")
        or interpretation_context.get("report_type")
        or payload.get("report_type")
        or "premium"
    )
    natal_data = payload.get("natal_data") or {}
    dasha_data = payload.get("dasha_data") or {}
    navamsa_data = payload.get("navamsa_data") or {}
    transit_data = payload.get("transit_data") or {}
    eclipse_data = payload.get("eclipse_data") or {}
    fullmoon_data = payload.get("fullmoon_data") or {}
    lunation_data = payload.get("lunation_data") or payload.get("lunations") or {}
    timing_data = payload.get("timing_data") or {}
    astro_signal_context = payload.get("astro_signal_context") or {}
    if language == "tr" and astro_signal_context:
        try:
            astro_signal_context = localize_signal_explanations(
                astro_signal_context,
                language,
                client=None,
                model=None,
            )
        except Exception:
            pass
    psychological_themes = payload.get("psychological_themes") or {}
    life_area_analysis = payload.get("life_area_analysis") or {}
    narrative_analysis = payload.get("narrative_analysis") or {}
    if language == "tr":
        psychological_themes = _localize_nested_payload_strings(psychological_themes, language)
        life_area_analysis = _localize_nested_payload_strings(life_area_analysis, language)
        narrative_analysis = _localize_nested_payload_strings(narrative_analysis, language)
        timing_data = _localize_nested_payload_strings(timing_data, language)
    debug_signals = bool(payload.get("debug_signals"))
    report_structure_v3 = {}
    if build_report_structure_v3 and astro_signal_context:
        try:
            report_structure_v3 = build_report_structure_v3(
                astro_signal_context,
                report_variant,
                language,
            )
        except Exception:
            report_structure_v3 = {}

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "language": language,
        "debug_signals": debug_signals,
        "client": {
            "full_name": payload.get("full_name") or payload.get("client_name") or "",
            "birth_date": payload.get("birth_date") or "",
            "birth_time": payload.get("birth_time") or "",
            "birth_city": payload.get("birth_city") or payload.get("normalized_birth_place") or "",
        },
        "natal_data": natal_data,
        "natal_planets": natal_data.get("planets", []),
        "lagna": natal_data.get("ascendant", {}),
        "karakas": natal_data.get("karakas", {}),
        "dasha_data": dasha_data,
        "navamsa_data": navamsa_data,
        "transit_data": transit_data,
        "eclipse_data": eclipse_data,
        "fullmoon_data": fullmoon_data,
        "lunation_data": lunation_data,
        "timing_data": timing_data,
        "timing_intelligence": timing_data,
        "astro_signal_context": astro_signal_context,
        "report_structure_v3": report_structure_v3,
        "psychological_themes": psychological_themes,
        "life_area_analysis": life_area_analysis,
        "narrative_analysis": narrative_analysis,
        "interpretation_context": interpretation_context,
        "report_type": payload.get("report_type") or interpretation_context.get("report_type") or "premium",
        "report_variant": report_variant,
        "signal_debug_block": _build_signal_debug_block(astro_signal_context) if debug_signals else "",
    }


def _report_variant_instruction(structured_payload: dict) -> str:
    variant = str(structured_payload.get("report_variant") or structured_payload.get("report_type") or "premium").strip().lower()
    if variant == "career":
        return """
REPORT ROUTING:
- This is a career-focused reading.
- Prioritize vocation, professional direction, work rhythm, visibility, credibility, earning pattern, and career decisions.
- Do not let relationship or generic life-theme material dominate unless it materially affects career decisions.
"""
    if variant == "annual_transit":
        return """
REPORT ROUTING:
- This is an annual transit reading.
- Prioritize timing, upcoming windows, transit pressure, openings, turning points, and what decisions belong to which period.
- Do not turn this into a generic natal personality report.
"""
    if variant == "birth_chart_karma":
        return """
REPORT ROUTING:
- This is a birth chart and karma reading.
- Prioritize natal structure, karmic repetition, enduring strengths, recurring challenge patterns, and life-direction themes.
"""
    if variant == "parent_child":
        return """
REPORT ROUTING:
- This is a parent-child guidance reading.
- Prioritize child temperament, emotional needs, learning pattern, parent-child dynamic, supportive communication, and parenting guidance.
- If parent_child_interaction_signals are present, prioritize interaction_patterns, emotional_dynamics, discipline_dynamics, communication_dynamics, attachment_dynamics, and karmic_dynamics over static profile description.
- Explain trigger-response loops, misunderstandings, and long-term relational patterns.
- Avoid generic compatibility language.
- Do not rewrite this as a single-person natal or karma report.
"""
    return """
REPORT ROUTING:
- Use the selected report variant in the payload to decide emphasis.
- Do not collapse specialized report types into a generic premium report voice.
"""


def _language_instruction(language: str) -> str:
    if language == "en":
        return """
LANGUAGE QUALITY:
- Write in polished premium English.
- Be intelligent but readable, emotionally aware but not sentimental.
- Avoid therapy-speak, cliche mystical language, and inflated transformational buzzwords.
- Keep the tone direct but not cold, refined but not ornate.
"""
    return """
DIL KALITESI:
- Dogal, rafine ve akici Turkce yaz.
- Robotik veya literal ceviri gibi duyulan cumlelerden kacin.
- "Bu donem", "enerji", "tema" gibi kelimeleri gereksiz tekrar etme.
- Duygusal farkindalik olsun ama sentimental ya da abartili olmasin.
- Spirituel okuryazarlik hissi ver, teatral mistik dil kullanma.
- Asla Ingilizce cumle birakma.
- Ham sinyal anahtarlarini, dahili etiketleri veya Ingilizce action/timing label'larini koruma.
- Girdi Ingilizce bir signal/action/timing etiketi iceriyorsa, raporda kullanmadan once bunu dogal Turkceye cevir.
- Sinyal aciklamalari zaten Turkceye cevrilmis olarak gelecek.
- Yine de Ingilizce bir aciklama gorursen onu da dogal Turkceye cevir, oldugu gibi rapora yansitma.
"""


def _source_rules() -> str:
    return """
SOURCE AND ASTROLOGY DISCIPLINE:
- Use ONLY the provided structured payload and prior agent outputs.
- Deterministic astrology layers are the source of truth; you interpret them, you do not act as the astrology engine.
- Use astro_signal_context first whenever it is present. Treat dominant_signals, risk_signals, opportunity_signals, nakshatra_signals, yoga_signals, and chart_relationships as the primary structured reasoning layer before generic astrology description.
- If report_structure_v3 is present, use it as the section blueprint. Expand its sections; do not invent a different report architecture.
- Do not invent placements, houses, aspects, dates, dashas, transits, eclipses, lunations, events, or outcomes.
- Do not invent signals, combinations, or timing certainty that are not in astro_signal_context or other provided structured layers.
- Explain dominant signal combinations when they are present instead of falling back to generic textbook astrology language.
- Mention risk only if risk_signals are present. Mention opportunity only if opportunity_signals are present.
- If yoga data exists, describe yoga as potential, pattern, or capacity - never as guaranteed fate.
- If dasha activation is missing or weak, do not claim timing certainty.
- If parent_child_interaction_signals exist, use them before generic parent/child descriptions. Explain loops and misunderstandings, not just trait summaries.
- Do not restate raw data unless it is needed to explain meaning.
- Prefer interpretation over description.
- Prefer prioritization over exhaustive listing.
- Focus on the most active signals, not every possible factor.
- Frame astrology as tendency, timing, developmental pressure, and opportunity, never as guaranteed fate.
- If a field is weak, missing, or ambiguous, say so gently and interpret at a higher level.
- Do not give definitive medical, legal, or financial advice.
"""


def _editorial_rules() -> str:
    return """
PREMIUM EDITORIAL RULES:
- Premium, calm, clear, grounded, and human.
- Short paragraphs; every paragraph must add new value.
- No filler opening sentences.
- No repeated bullets in paraphrased form.
- Avoid repetition between sections and between agents.
- Avoid vague spiritual filler, melodrama, fear, fatalism, and manipulative mystical tone.
- Avoid generic "journey" language unless clearly justified by the payload.
- Be specific to the provided signal hierarchy without fabricating details.
"""


def _premium_advisory_writing_rules(language: str) -> str:
    if language == "en":
        vocabulary = "dynamic, direction, timing, pressure, opportunity, priority, decision quality, positioning, pacing, leverage"
        avoid = "growth, transformation, journey"
    else:
        vocabulary = "dinamik, yön, zamanlama, baskı, fırsat, öncelik, karar kalitesi, konumlanma, tempo, denge"
        avoid = "enerji, dönüşüm, yolculuk"
    return f"""
PREMIUM ADVISORY WRITING RULES:
- Write like decision intelligence: an expert translating chart signals into practical priorities.
- Prefer decision relevance over abstract description.
- Use premium strategic vocabulary when natural: {vocabulary}.
- Avoid repeated use of: {avoid}, unless the payload clearly requires it.
- Avoid "AI voice" patterns: balanced-but-empty sentences, repeated reassurance, generic transitions, and symmetrical paragraphs that say the same thing.
- Do not write like an explainer article; write like a calm strategic advisor.
- Keep the document printable: elegant headings, coherent flow across pages, not too fragmented, not too verbose.
"""


def _signal_debug_block(astro_signal_context: Any) -> str:
    if not isinstance(astro_signal_context, dict) or not astro_signal_context:
        return ""
    return _safe_json(
        {
            "dominant_signals": astro_signal_context.get("dominant_signals") or [],
            "risk_signals": astro_signal_context.get("risk_signals") or [],
            "opportunity_signals": astro_signal_context.get("opportunity_signals") or [],
            "detected_yogas": (
                (astro_signal_context.get("yoga_signals") or {}).get("detected_yogas")
                or astro_signal_context.get("detected_yogas")
                or []
            ),
            "moon_nakshatra_signal": (astro_signal_context.get("nakshatra_signals") or {}).get("moon_nakshatra") or {},
            "lagna_nakshatra_signal": (astro_signal_context.get("nakshatra_signals") or {}).get("lagna_nakshatra") or {},
        }
    )


def _build_signal_debug_block(astro_signal_context: Any) -> str:
    block = _signal_debug_block(astro_signal_context)
    if not block:
        return ""
    return f"""
SIGNAL DEBUG BLOCK:
{block}
"""


def build_insight_prompt(structured_payload: dict) -> str:
    language = structured_payload["language"]
    return f"""
You are the Insight Agent in a premium Vedic astrology report pipeline.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}
{_report_variant_instruction(structured_payload)}

Your scope:
- Start from astro_signal_context when present. Use dominant_signals as the primary ranking layer before broader interpretation_context summaries.
- If report_variant is parent_child and astro_signal_context.parent_child_interaction_signals exists, prioritize those interaction patterns before static descriptions of either person.
- Prioritize psychological_themes, life_area_analysis, narrative_analysis, and interpretation_context.
- Identify the top 3 active themes in clear hierarchy: primary, secondary, tertiary.
- Explain why these are the dominant themes NOW, using importance, confidence, stress/growth, tone flags, anchors, or narrative compression when present.
- Name the dominant life areas and explain their relationship to the active themes.
- Avoid generic natal explanations unless the payload strongly supports them.
- Include a short restraint note, "what this does NOT necessarily mean", when confidence is low, mixed, or limited.
- Do not list every possible factor; select the signals that actually carry the report.

Return concise markdown only with these labels:
- Primary theme
- Secondary theme
- Tertiary theme
- Dominant life areas
- Restraint note, only if needed

STRUCTURED PAYLOAD:
{_safe_json(structured_payload)}
{structured_payload.get("signal_debug_block") or ""}
"""


def build_timing_prompt(structured_payload: dict, insight_output: str | None = None) -> str:
    language = structured_payload["language"]
    return f"""
You are the Timing Agent in a premium Vedic astrology report pipeline.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}
{_report_variant_instruction(structured_payload)}

Your scope:
- Prioritize timing_intelligence / timing_data first.
- If astro_signal_context contains dasha_activation_signals, use them carefully to frame timing. If they are missing, say timing confidence is limited rather than inventing certainty.
- Use dasha_data, transit_data, eclipse_data, fullmoon_data, and lunation_data only as timing support.
- Relate timing explicitly to the themes already identified by the Insight Agent.
- Do not merely list dates or windows; explain the kind of build-up, pressure, opportunity, peak, or integration each window represents.
- Distinguish build-up, peak, and integration phases only when supported by the payload.
- Do not invent dates, certainty, or deterministic outcomes.
- If timing data is sparse, state the limitation and keep the timing guidance higher-level.

Return concise markdown only. Avoid repeating Insight Agent language verbatim.

INSIGHT AGENT OUTPUT:
{insight_output or "Not available. Use only the structured payload and avoid unsupported theme claims."}

STRUCTURED PAYLOAD:
{_safe_json(structured_payload)}
{structured_payload.get("signal_debug_block") or ""}
"""


def build_guidance_prompt(structured_payload: dict, insight_output: str, timing_output: str) -> str:
    language = structured_payload["language"]
    return f"""
You are the Guidance Agent in a premium Vedic astrology report pipeline.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}
{_report_variant_instruction(structured_payload)}

Your scope:
- Use the structured payload plus Insight Agent and Timing Agent outputs.
- Use astro_signal_context to keep guidance anchored in dominant signal combinations instead of generic counseling language.
- For parent_child, use parent_child_interaction_signals to recommend concrete parent actions tied to specific loops and misunderstandings.
- Transform insight + timing into practical direction.
- Sound strategic, mature, and useful; avoid self-help fluff.
- Separate guidance into:
  1. What to lean into
  2. What to slow down
  3. What to structure more carefully
- Include what to avoid without sounding alarmist.
- Include what to build without vague motivational language.
- Avoid therapy language and diagnostic phrasing.
- Avoid definitive health, legal, or financial advice.
- Do not repeat the Insight or Timing outputs; translate them into action posture.

Return concise markdown only.

STRUCTURED PAYLOAD:
{_safe_json(structured_payload)}

INSIGHT AGENT OUTPUT:
{insight_output}

TIMING AGENT OUTPUT:
{timing_output}
{structured_payload.get("signal_debug_block") or ""}
"""


def _composer_sections(language: str) -> list[str]:
    if language == "en":
        return [
            "Identity Layer",
            "Core Drivers",
            "Dominant Signals",
            "Interaction Layer",
            "Risk & Opportunity Map",
            "Timing Engine",
            "Action Engine",
            "Strategic Summary",
        ]
    return [
        "Temel Kimlik",
        "Ana Yönlendiriciler",
        "Baskın Sinyaller",
        "Dinamikler ve Etkileşimler",
        "Risk ve Fırsatlar",
        "Zamanlama Analizi",
        "Önerilen Aksiyonlar",
        "Stratejik Özet",
    ]


def build_composer_prompt(
    structured_payload: dict,
    insight_output: str,
    timing_output: str,
    guidance_output: str,
) -> str:
    language = structured_payload["language"]
    sections = "\n".join(f"### {section}" for section in _composer_sections(language))
    legacy_sections = "\n".join(
        [
            "### MAIN DIRECTION AND LIFE THEME",
            "### WHY IT MATTERS",
            "### RUHSAL YON ve HAYATIN ANA TEMASI",
            "### NEDEN ONEMLI",
        ]
    )
    return f"""
You are the Composer Agent for a premium Vedic astrology SaaS report.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}
{_premium_advisory_writing_rules(language)}
{_report_variant_instruction(structured_payload)}

Create the final premium report draft by combining inputs in this strict priority order:
1. structured deterministic payload as source truth
2. astro_signal_context as the primary structured reasoning layer when present
3. Insight Agent conclusions
4. Timing Agent conclusions
5. Guidance Agent conclusions

Composer role:
- You are NOT a re-analyst.
- You are a premium report writer and synthesizer.
- When report_structure_v3 is present, follow its section order and emphasis rather than older narrative section conventions.
- Preserve the core priorities from Insight, Timing, and Guidance.
- Do not invent a new narrative.
- Do not introduce new main themes.
- Do not overwrite or dilute the timing emphasis.
- Do not add astrology details that were not established upstream.
- Use astro_signal_context before generic astrology text.
- For parent_child, use parent_child_interaction_signals before generic compatibility language.
- Do not invent signals.
- Explain dominant signal combinations when they are present.
- For parent_child, explain trigger-response loops, misunderstandings, and support patterns when they are present.
- Mention risk only if risk_signals are present.
- Mention opportunity only if opportunity_signals are present.
- If yoga appears in astro_signal_context, describe it as potential or pattern, never guaranteed fate.
- If dasha activation is missing, do not claim timing certainty.
- Maintain section differentiation so sections do not sound the same.
- If a conclusion is strong but grounded, keep it clear rather than weakening it into generic reassurance.

Mandatory writing rules:
- Do not restate the same idea across sections.
- Do not use generic spiritual filler.
- Every section must add distinct value.
- Each theme must have a different functional angle.
- Avoid repeating the same causal sentence pattern.
- Use short, high-value paragraphs.
- If a point already appeared in Insight, do not restate it verbatim; synthesize and move the reader forward.
- If a timing point already appeared in Timing, compress it and explain what kind of decision belongs there.
- Avoid overusing "this period" / "bu dönem"; vary sentence structure and only use it when needed.
- If language == tr, write fully natural Turkish with no literal-translation stiffness.
- If language == en, write polished premium English, concise and human.

Section function differentiation:
- Identity Layer / Temel Kimlik: define the core identity axis from lagna, moon, atmakaraka, and dominant nakshatra when present.
- Core Drivers / Ana Yönlendiriciler: explain the main engines pushing the chart, especially atmakaraka and strongest yogas.
- Dominant Signals / Baskın Sinyaller: explain the strongest signals and why they rank highest.
- Interaction Layer / Dinamikler ve Etkileşimler: explain internal frictions, synergies, or parent-child loops when present.
- Risk & Opportunity Map / Risk ve Fırsatlar: explain what can go wrong and what opens if handled well.
- Timing Engine / Zamanlama Analizi: explain why now, using dasha and transit delivery logic.
- Action Engine / Önerilen Aksiyonlar: translate signals and timing into practical action.
- Strategic Summary / Stratejik Özet: compress the whole reading into a clear decision frame.
- MAIN THEME / ANA TEMA: define the dominant dynamic and the main life direction signal.
- WHY IT MATTERS / NEDEN ONEMLI: explain why the signal matters in practical life and decision quality.
- No two sections should sound interchangeable.

Theme-writing discipline:
- For each top theme, include: theme label, why it is dominant now, where it shows up most clearly, what decision quality it affects, what opportunity it opens, and what mistake it can create.
- Do NOT let multiple themes reuse the same explanation template.
- Each theme must feel distinct in life area, behavioral implication, and risk pattern.

PDF/client delivery discipline:
- The final draft is used in PDF; make it printable, coherent across pages, and suitable for client delivery.
- Use elegant section flow rather than scattered fragments.
- Avoid technical output-wrapper language.
- Do not become verbose or poetic for its own sake.

Mandatory markdown sections, in this exact order:
{sections}

Legacy compatibility section anchors:
{legacy_sections}

Return only the report draft.

STRUCTURED PAYLOAD:
{_safe_json(structured_payload)}

INSIGHT AGENT OUTPUT:
{insight_output}

TIMING AGENT OUTPUT:
{timing_output}

GUIDANCE AGENT OUTPUT:
{guidance_output}
{structured_payload.get("signal_debug_block") or ""}
"""


def build_safety_prompt(structured_payload: dict, composer_draft: str) -> str:
    language = structured_payload["language"]
    return f"""
You are the Safety Agent for a premium Vedic astrology report.
{_language_instruction(language)}
{_source_rules()}
{_report_variant_instruction(structured_payload)}

Final-pass the draft below as a minimal-edit compliance and restraint layer.
Perform the smallest possible edits needed for safety and restraint.
Preserve strong advisory tone while softening only unsafe certainty.

Detect and soften:
- fatalistic certainty
- fear-based wording
- fabricated specifics
- manipulative mystical tone
- definitive medical, legal, or financial advice
- overly absolute claims

Preserve:
- headings
- section order
- core meaning
- premium calm tone
- clear strategic advice and decision-oriented language

Do NOT:
- introduce new meaning
- introduce new examples
- rewrite the report from scratch
- change headings
- substantially change structure
- add generic disclaimers unless necessary to soften a risky claim
- flatten strong advisory language into weak generic wording

Return only the revised final report. Do not add commentary.

STRUCTURED PAYLOAD FOR FACT CHECKING:
{_safe_json(structured_payload)}
{structured_payload.get("signal_debug_block") or ""}

COMPOSER DRAFT:
{composer_draft}
"""


def call_model(prompt: str, *, client=None, model: str | None = None) -> str:
    model_name = model or _get_model_name()
    model_client = client or _get_model_client()
    try:
        response = model_client.models.generate_content(model=model_name, contents=prompt)
        text = getattr(response, "text", "") or ""
        if not text.strip():
            raise AIServiceError("AI modeli bos yanit dondu.")
        return text.strip()
    except Exception as exc:
        if isinstance(exc, (AIConfigurationError, AIServiceError)):
            raise
        raise AIServiceError(f"AI yorum servisine baglanilamadi: {str(exc)}") from exc


def _run_agent(name: str, prompt: str, *, client=None, model: str | None = None) -> AgentResult:
    model_name = model or _get_model_name()
    output = call_model(prompt, client=client, model=model_name)
    return AgentResult(
        name=name,
        output=output,
        model=model_name,
        generated_at=datetime.utcnow().isoformat() + "Z",
    )


def run_insight_agent(structured_payload: dict, *, client=None, model: str | None = None) -> AgentResult:
    return _run_agent("insight", build_insight_prompt(structured_payload), client=client, model=model)


def run_timing_agent(
    structured_payload: dict,
    insight_output: str | None = None,
    *,
    client=None,
    model: str | None = None,
) -> AgentResult:
    return _run_agent("timing", build_timing_prompt(structured_payload, insight_output), client=client, model=model)


def run_guidance_agent(
    structured_payload: dict,
    insight_output: str,
    timing_output: str,
    *,
    client=None,
    model: str | None = None,
) -> AgentResult:
    prompt = build_guidance_prompt(structured_payload, insight_output, timing_output)
    return _run_agent("guidance", prompt, client=client, model=model)


def run_composer_agent(
    structured_payload: dict,
    insight_output: str,
    timing_output: str,
    guidance_output: str,
    *,
    client=None,
    model: str | None = None,
) -> AgentResult:
    prompt = build_composer_prompt(structured_payload, insight_output, timing_output, guidance_output)
    return _run_agent("composer_draft", prompt, client=client, model=model)


def run_safety_agent(
    structured_payload: dict,
    composer_draft: str,
    *,
    client=None,
    model: str | None = None,
) -> AgentResult:
    return _run_agent("safety_final", build_safety_prompt(structured_payload, composer_draft), client=client, model=model)


def run_agent_pipeline(data: dict | None = None, **kwargs) -> dict:
    structured_payload = build_structured_payload(data, **kwargs)
    client = _get_model_client()
    model = _get_model_name()

    insight = run_insight_agent(structured_payload, client=client, model=model)
    timing = run_timing_agent(structured_payload, insight.output, client=client, model=model)
    guidance = run_guidance_agent(structured_payload, insight.output, timing.output, client=client, model=model)
    composer = run_composer_agent(
        structured_payload,
        insight.output,
        timing.output,
        guidance.output,
        client=client,
        model=model,
    )
    safety = run_safety_agent(structured_payload, composer.output, client=client, model=model)

    return {
        "final_text": safety.output,
        "structured_payload": structured_payload,
        "agents": {
            "insight": insight.output,
            "timing": timing.output,
            "guidance": guidance.output,
            "composer_draft": composer.output,
            "safety_final": safety.output,
        },
        "metadata": {
            "model": model,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "pipeline": ["insight", "timing", "guidance", "composer_draft", "safety_final"],
        },
    }


def generate_interpretation(data: dict | None = None, **kwargs) -> str:
    return run_agent_pipeline(data, **kwargs)["final_text"]
