import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google import genai


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
VALID_LANGUAGES = {"tr", "en"}


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


def build_structured_payload(data: dict | None = None, **kwargs) -> dict:
    payload = _merge_payload(data, **kwargs)
    interpretation_context = payload.get("interpretation_context") or {}
    language = _normalize_language(payload.get("language") or interpretation_context.get("language"))
    natal_data = payload.get("natal_data") or {}
    dasha_data = payload.get("dasha_data") or {}
    navamsa_data = payload.get("navamsa_data") or {}
    transit_data = payload.get("transit_data") or {}
    eclipse_data = payload.get("eclipse_data") or {}
    fullmoon_data = payload.get("fullmoon_data") or {}
    lunation_data = payload.get("lunation_data") or payload.get("lunations") or {}
    timing_data = payload.get("timing_data") or {}

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "language": language,
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
        "psychological_themes": payload.get("psychological_themes") or {},
        "life_area_analysis": payload.get("life_area_analysis") or {},
        "narrative_analysis": payload.get("narrative_analysis") or {},
        "interpretation_context": interpretation_context,
        "report_type": payload.get("report_type") or interpretation_context.get("report_type") or "premium",
    }


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
"""


def _source_rules() -> str:
    return """
SOURCE AND ASTROLOGY DISCIPLINE:
- Use ONLY the provided structured payload and prior agent outputs.
- Deterministic astrology layers are the source of truth; you interpret them, you do not act as the astrology engine.
- Do not invent placements, houses, aspects, dates, dashas, transits, eclipses, lunations, events, or outcomes.
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


def build_insight_prompt(structured_payload: dict) -> str:
    language = structured_payload["language"]
    return f"""
You are the Insight Agent in a premium Vedic astrology report pipeline.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}

Your scope:
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
"""


def build_timing_prompt(structured_payload: dict, insight_output: str | None = None) -> str:
    language = structured_payload["language"]
    return f"""
You are the Timing Agent in a premium Vedic astrology report pipeline.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}

Your scope:
- Prioritize timing_intelligence / timing_data first.
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
"""


def build_guidance_prompt(structured_payload: dict, insight_output: str, timing_output: str) -> str:
    language = structured_payload["language"]
    return f"""
You are the Guidance Agent in a premium Vedic astrology report pipeline.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}

Your scope:
- Use the structured payload plus Insight Agent and Timing Agent outputs.
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
"""


def _composer_sections(language: str) -> list[str]:
    if language == "en":
        return [
            "MAIN DIRECTION AND LIFE THEME",
            "KARMIC DYNAMICS",
            "CAREER AND MONEY",
            "RELATIONSHIPS",
            "TIMING - WHEN WHAT MATTERS",
            "RISKS",
            "OPPORTUNITIES",
            "STRATEGIC GUIDANCE",
        ]
    return [
        "RUHSAL YON ve HAYATIN ANA TEMASI",
        "KARMIK DINAMIKLER",
        "KARIYER ve PARA",
        "ILISKILER",
        "ZAMANLAMA - NE ZAMAN NE ONEMLI",
        "RISKLER",
        "FIRSATLAR",
        "STRATEJIK YONLENDIRME",
    ]


def build_composer_prompt(
    structured_payload: dict,
    insight_output: str,
    timing_output: str,
    guidance_output: str,
) -> str:
    language = structured_payload["language"]
    sections = "\n".join(f"### {section}" for section in _composer_sections(language))
    return f"""
You are the Composer Agent for a premium Vedic astrology SaaS report.
{_language_instruction(language)}
{_source_rules()}
{_editorial_rules()}
{_premium_advisory_writing_rules(language)}

Create the final premium report draft by combining inputs in this strict priority order:
1. structured deterministic payload as source truth
2. Insight Agent conclusions
3. Timing Agent conclusions
4. Guidance Agent conclusions

Composer role:
- You are NOT a re-analyst.
- You are a premium report writer and synthesizer.
- Preserve the core priorities from Insight, Timing, and Guidance.
- Do not invent a new narrative.
- Do not introduce new main themes.
- Do not overwrite or dilute the timing emphasis.
- Do not add astrology details that were not established upstream.
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
- MAIN THEME / ANA TEMA: define the dominant dynamic.
- WHY IT MATTERS / NEDEN ONEMLI: explain why this affects the person now.
- IMPACT AREA / ETKI ALANI: show where it appears in life.
- OPPORTUNITY / FIRSAT: explain what becomes possible if handled well.
- RISK / DIKKAT: explain what goes wrong if mismanaged.
- ACTION / ONERILEN YON: clarify what behavior, prioritization, or pacing is needed.
- TIMING / ZAMANLAMA: explain when this matters most and which decisions belong there.
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

Return only the report draft.

STRUCTURED PAYLOAD:
{_safe_json(structured_payload)}

INSIGHT AGENT OUTPUT:
{insight_output}

TIMING AGENT OUTPUT:
{timing_output}

GUIDANCE AGENT OUTPUT:
{guidance_output}
"""


def build_safety_prompt(structured_payload: dict, composer_draft: str) -> str:
    language = structured_payload["language"]
    return f"""
You are the Safety Agent for a premium Vedic astrology report.
{_language_instruction(language)}
{_source_rules()}

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
