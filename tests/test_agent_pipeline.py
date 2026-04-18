import unittest
from unittest.mock import patch

import agent_pipeline
import ai_interpreter


class _FakeResponse:
    def __init__(self, text):
        self.text = text


class _FakeModels:
    def __init__(self):
        self.prompts = []

    def generate_content(self, model, contents):
        self.prompts.append(contents)
        if "You are the Insight Agent" in contents:
            return _FakeResponse("Insight output")
        if "You are the Timing Agent" in contents:
            return _FakeResponse("Timing output")
        if "You are the Guidance Agent" in contents:
            return _FakeResponse("Guidance output")
        if "You are the Composer Agent" in contents:
            return _FakeResponse("Composer draft")
        if "You are the Safety Agent" in contents:
            return _FakeResponse("Safety final")
        return _FakeResponse("Fallback")


class _FakeClient:
    def __init__(self):
        self.models = _FakeModels()


class AgentPipelineTests(unittest.TestCase):
    def test_pipeline_returns_final_text_and_intermediate_outputs(self):
        fake_client = _FakeClient()
        payload = {
            "language": "en",
            "natal_data": {"planets": [{"name": "Sun"}]},
            "interpretation_context": {"confidence_level": "moderate"},
            "psychological_themes": {"theme": "direction"},
            "life_area_analysis": {"career": {"score": 8}},
            "narrative_analysis": {"dominant": "career"},
            "timing_data": {"peak": {"label": "Spring"}},
        }
        with patch.object(agent_pipeline, "_get_model_client", return_value=fake_client):
            result = agent_pipeline.run_agent_pipeline(payload)

        self.assertEqual(result["final_text"], "Safety final")
        self.assertEqual(result["agents"]["insight"], "Insight output")
        self.assertEqual(result["agents"]["timing"], "Timing output")
        self.assertEqual(result["agents"]["guidance"], "Guidance output")
        self.assertEqual(result["agents"]["composer_draft"], "Composer draft")
        self.assertEqual(result["agents"]["safety_final"], "Safety final")
        self.assertEqual(result["structured_payload"]["language"], "en")
        self.assertEqual(len(fake_client.models.prompts), 5)
        self.assertIn("INSIGHT AGENT OUTPUT", fake_client.models.prompts[1])
        self.assertIn("Insight output", fake_client.models.prompts[1])

    def test_generate_interpretation_preserves_string_return_contract(self):
        with patch.object(agent_pipeline, "run_agent_pipeline", return_value={"final_text": "Final report"}):
            self.assertEqual(agent_pipeline.generate_interpretation({"language": "tr"}), "Final report")

    def test_ai_interpreter_wrapper_reexports_pipeline_contract(self):
        self.assertIs(ai_interpreter.AIConfigurationError, agent_pipeline.AIConfigurationError)
        self.assertIs(ai_interpreter.AIServiceError, agent_pipeline.AIServiceError)
        self.assertIs(ai_interpreter.run_agent_pipeline, agent_pipeline.run_agent_pipeline)
        self.assertIs(ai_interpreter.generate_interpretation, agent_pipeline.generate_interpretation)

    def test_invalid_language_defaults_to_turkish(self):
        structured = agent_pipeline.build_structured_payload({"language": "de"})
        self.assertEqual(structured["language"], "tr")

    def test_timing_prompt_uses_insight_output_when_available(self):
        structured = agent_pipeline.build_structured_payload({"language": "en", "timing_data": {"peak": "May"}})
        prompt = agent_pipeline.build_timing_prompt(structured, "Primary theme: career direction")
        self.assertIn("INSIGHT AGENT OUTPUT", prompt)
        self.assertIn("Primary theme: career direction", prompt)
        self.assertIn("Relate timing explicitly to the themes already identified by the Insight Agent", prompt)

    def test_composer_prompt_preserves_prior_agent_priorities(self):
        structured = agent_pipeline.build_structured_payload({"language": "en"})
        prompt = agent_pipeline.build_composer_prompt(
            structured,
            "Primary theme: career direction",
            "Timing: build-up then peak",
            "Guidance: structure carefully",
        )
        self.assertIn("You are NOT a re-analyst", prompt)
        self.assertIn("structured deterministic payload as source truth", prompt)
        self.assertIn("Preserve the core priorities from Insight, Timing, and Guidance", prompt)
        self.assertIn("Do not invent a new narrative", prompt)
        self.assertIn("Do not overwrite or dilute the timing emphasis", prompt)

    def test_composer_prompt_avoids_repeated_section_logic(self):
        structured = agent_pipeline.build_structured_payload({"language": "en"})
        prompt = agent_pipeline.build_composer_prompt(
            structured,
            "Primary theme: career direction",
            "Timing: build-up then peak",
            "Guidance: structure carefully",
        )
        self.assertIn("Do not restate the same idea across sections", prompt)
        self.assertIn("Avoid repeating the same causal sentence pattern", prompt)
        self.assertIn("If a point already appeared in Insight, do not restate it verbatim", prompt)
        self.assertIn("If a timing point already appeared in Timing, compress it", prompt)
        self.assertIn('Avoid overusing "this period" / "bu dönem"', prompt)

    def test_composer_prompt_preserves_section_differentiation(self):
        structured = agent_pipeline.build_structured_payload({"language": "en"})
        prompt = agent_pipeline.build_composer_prompt(
            structured,
            "Primary theme: career direction",
            "Timing: build-up then peak",
            "Guidance: structure carefully",
        )
        self.assertIn("Section function differentiation", prompt)
        self.assertIn("MAIN THEME / ANA TEMA: define the dominant dynamic", prompt)
        self.assertIn("WHY IT MATTERS / NEDEN ONEMLI", prompt)
        self.assertIn("No two sections should sound interchangeable", prompt)
        self.assertIn("Each theme must feel distinct in life area, behavioral implication, and risk pattern", prompt)

    def test_composer_prompt_turkish_output_rules_remain_intact(self):
        structured = agent_pipeline.build_structured_payload({"language": "tr"})
        prompt = agent_pipeline.build_composer_prompt(
            structured,
            "Birincil tema: kariyer yönü",
            "Zamanlama: birikim ve zirve",
            "Yönlendirme: dikkatli yapılandır",
        )
        self.assertIn("Dogal, rafine ve akici Turkce yaz", prompt)
        self.assertIn("If language == tr, write fully natural Turkish", prompt)
        self.assertIn("dinamik, yön, zamanlama, baskı, fırsat", prompt)
        self.assertIn("enerji, dönüşüm, yolculuk", prompt)
        self.assertIn("### RUHSAL YON ve HAYATIN ANA TEMASI", prompt)

    def test_composer_prompt_english_output_rules_remain_intact(self):
        structured = agent_pipeline.build_structured_payload({"language": "en"})
        prompt = agent_pipeline.build_composer_prompt(
            structured,
            "Primary theme: career direction",
            "Timing: build-up then peak",
            "Guidance: structure carefully",
        )
        self.assertIn("Write in polished premium English", prompt)
        self.assertIn("If language == en, write polished premium English", prompt)
        self.assertIn("dynamic, direction, timing, pressure, opportunity", prompt)
        self.assertIn("growth, transformation, journey", prompt)
        self.assertIn("### MAIN DIRECTION AND LIFE THEME", prompt)

    def test_safety_prompt_is_minimal_edit_oriented(self):
        structured = agent_pipeline.build_structured_payload({"language": "tr"})
        prompt = agent_pipeline.build_safety_prompt(structured, "### BASLIK\nKesin olacak.")
        self.assertIn("Perform the smallest possible edits needed for safety and restraint", prompt)
        self.assertIn("Do NOT:", prompt)
        self.assertIn("rewrite the report from scratch", prompt)
        self.assertIn("change headings", prompt)

    def test_safety_prompt_does_not_over_flatten_advisory_tone(self):
        structured = agent_pipeline.build_structured_payload({"language": "en"})
        prompt = agent_pipeline.build_safety_prompt(structured, "### MAIN DIRECTION\nPrioritize timing discipline.")
        self.assertIn("Preserve strong advisory tone while softening only unsafe certainty", prompt)
        self.assertIn("clear strategic advice and decision-oriented language", prompt)
        self.assertIn("flatten strong advisory language into weak generic wording", prompt)


if __name__ == "__main__":
    unittest.main()
