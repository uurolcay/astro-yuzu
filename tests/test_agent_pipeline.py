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
        self.assertIn("You are a synthesizer, not a new analyst", prompt)
        self.assertIn("Preserve the core priorities from Insight, Timing, and Guidance", prompt)
        self.assertIn("Do not invent a new narrative", prompt)
        self.assertIn("Do not overwrite or dilute the timing emphasis", prompt)

    def test_safety_prompt_is_minimal_edit_oriented(self):
        structured = agent_pipeline.build_structured_payload({"language": "tr"})
        prompt = agent_pipeline.build_safety_prompt(structured, "### BASLIK\nKesin olacak.")
        self.assertIn("Perform the smallest possible edits needed for safety and restraint", prompt)
        self.assertIn("Do NOT:", prompt)
        self.assertIn("rewrite the report from scratch", prompt)
        self.assertIn("change headings", prompt)


if __name__ == "__main__":
    unittest.main()
