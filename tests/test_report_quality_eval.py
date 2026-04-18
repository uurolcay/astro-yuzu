import os
import unittest
from unittest.mock import patch

import agent_pipeline
from report_quality_eval import GOLDEN_SAMPLES, evaluate_golden_sample_outputs, evaluate_report_output


GOOD_TR_REPORT = """
### RUHSAL YON ve HAYATIN ANA TEMASI
Ana dinamik, dağınık seçenekleri tek bir öncelik hattına indirmek. Harita dili burada kişiye her kapıyı aynı anda açmayı değil, karar kalitesini yükselten bir yön seçmeyi öneriyor.

### KARMIK DINAMIKLER
Tekrarlayan örüntü, sorumluluk alırken kendi temposunu kaybetme eğilimi. Bu kez mesele daha fazla yük taşımak değil, hangi yükün gerçekten kendi konumlanmasına hizmet ettiğini ayırt etmek.

### KARIYER ve PARA
Mesleki alanda görünürlük artarken para tarafı ölçülü bir büyüme ister. En doğru hamle, hızlı genişleme yerine değeri net olan işi güçlendirmek ve kaynak kullanımını disipline etmektir.

### ILISKILER
İlişkiler karar alanını yumuşatabilir, fakat kişinin yönünü başkasının ritmine göre değiştirmesi dengeyi bozar. Açık sınırlar yakınlığı azaltmaz; daha sağlıklı bir temas zemini kurar.

### ZAMANLAMA - NE ZAMAN NE ONEMLI
Yaklaşan yoğunluk, hazırlık ve zirve arasında ayrım yapmayı gerektirir. Hazırlık aşamasında plan sadeleşmeli, zirveye yakın günlerde ise görünür kararlar daha net ifade edilmelidir.

### RISKLER
En büyük risk, baskı arttığında eski otomatik tepkiyle her talebe evet demek. Bu dağılım hem odağı zayıflatır hem de doğru fırsatı yanlış tempoda tüketir.

### FIRSATLAR
Fırsat, daha seçici bir konumlanma ile gelir. Kişi kendi önceliğini açık tuttuğunda hem mesleki güven hem de ilişkilerde açıklık güçlenir.

### STRATEJIK YONLENDIRME
Önümüzdeki adım, kararları iki ölçüte göre süzmek: Bu seçim yönümü güçlendiriyor mu, temposu sürdürülebilir mi? Cevap net değilse beklemek pasiflik değil, stratejik hazırlıktır.
"""


GOOD_EN_REPORT = """
### MAIN DIRECTION AND LIFE THEME
The dominant dynamic is cleaner self-definition. The chart points less toward constant movement and more toward choosing a direction that improves decision quality.

### KARMIC DYNAMICS
The repeating pattern is overextending before the structure is ready. The advisory emphasis is to separate meaningful responsibility from inherited pressure.

### CAREER AND MONEY
Career decisions benefit from sharper positioning. Money improves when the client protects pacing, prices effort realistically, and avoids expanding before the core offer is stable.

### RELATIONSHIPS
Relationships matter because they test boundaries and timing. The useful move is not withdrawal, but clearer agreements about attention, support, and emotional availability.

### TIMING - WHEN WHAT MATTERS
The build-up phase is for preparation and narrowing options. The peak window is better used for visible choices, direct conversations, and commitments that have already been structured.

### RISKS
The main risk is mistaking pressure for urgency. If every signal is treated as equally important, the client may spend energy on movement that does not create leverage.

### OPPORTUNITIES
The opportunity is deliberate positioning. By choosing fewer priorities, the client gains more authority, steadier pacing, and better use of the supportive windows.

### STRATEGIC GUIDANCE
Prioritize the decision that clarifies direction first. Then set a practical sequence: stabilize the base, communicate the boundary, and act when timing supports visibility.
"""


BAD_REPETITIVE_REPORT = """
### MAIN DIRECTION AND LIFE THEME
This period is about transformation and growth. This period is about transformation and growth. This journey is important.

### KARMIC DYNAMICS
This period is about transformation and growth. This period is about transformation and growth. This journey is important.

### CAREER AND MONEY
This period is about transformation and growth. This period is about transformation and growth. This journey is important.

### TIMING - WHEN WHAT MATTERS
This period is about transformation and growth. This period is about transformation and growth. This journey is important.
"""


class _FakeResponse:
    def __init__(self, text):
        self.text = text


class _QualityFakeModels:
    def generate_content(self, model, contents):
        if "You are the Insight Agent" in contents:
            return _FakeResponse("Primary theme: direction\nSecondary theme: pacing\nTertiary theme: relationship boundaries")
        if "You are the Timing Agent" in contents:
            return _FakeResponse("Timing: build-up, peak, and integration require different decisions.")
        if "You are the Guidance Agent" in contents:
            return _FakeResponse("Guidance: prioritize structure, slow reactive choices, and protect pacing.")
        if "You are the Composer Agent" in contents:
            return _FakeResponse("Composer draft")
        if "You are the Safety Agent" in contents:
            return _FakeResponse(GOOD_EN_REPORT if '"language": "en"' in contents else GOOD_TR_REPORT)
        return _FakeResponse("")


class _QualityFakeClient:
    def __init__(self):
        self.models = _QualityFakeModels()


class ReportQualityEvalTests(unittest.TestCase):
    def assert_quality_passes(self, metrics):
        self.assertLess(metrics["repetition_score"], 0.22)
        self.assertLess(metrics["section_overlap"], 0.42)
        self.assertGreater(metrics["theme_distinctness"], 0.55)
        self.assertGreater(metrics["advisory_tone_score"], 0.25)
        self.assertGreaterEqual(metrics["language_quality"], 0.9)

    def test_golden_sample_set_is_representative(self):
        self.assertGreaterEqual(len(GOLDEN_SAMPLES), 5)
        languages = {sample["expected_output_characteristics"]["language"] for sample in GOLDEN_SAMPLES}
        focuses = {sample["structured_payload"]["interpretation_context"]["primary_focus"] for sample in GOLDEN_SAMPLES}
        intensities = {sample["expected_output_characteristics"]["timing_intensity"] for sample in GOLDEN_SAMPLES}

        self.assertEqual(languages, {"tr", "en"})
        self.assertTrue({"career", "relationships", "identity", "family", "direction"}.issubset(focuses))
        self.assertTrue({"high", "moderate", "low"}.issubset(intensities))

    def test_evaluator_scores_premium_turkish_report_above_thresholds(self):
        metrics = evaluate_report_output(GOOD_TR_REPORT, "tr")
        self.assert_quality_passes(metrics)
        self.assertNotIn("language_leakage", metrics["flags"])

    def test_evaluator_scores_premium_english_report_above_thresholds(self):
        metrics = evaluate_report_output(GOOD_EN_REPORT, "en")
        self.assert_quality_passes(metrics)
        self.assertNotIn("language_leakage", metrics["flags"])

    def test_evaluator_flags_repetitive_template_like_output(self):
        metrics = evaluate_report_output(BAD_REPETITIVE_REPORT, "en")
        self.assertGreater(metrics["repetition_score"], 0.22)
        self.assertGreater(metrics["section_overlap"], 0.42)
        self.assertIn("high_repetition", metrics["flags"])
        self.assertIn("high_section_overlap", metrics["flags"])

    def test_golden_sample_outputs_can_be_evaluated_as_a_batch(self):
        outputs = {
            sample["id"]: GOOD_EN_REPORT if sample["expected_output_characteristics"]["language"] == "en" else GOOD_TR_REPORT
            for sample in GOLDEN_SAMPLES
        }
        results = evaluate_golden_sample_outputs(outputs)

        self.assertEqual(set(results), {sample["id"] for sample in GOLDEN_SAMPLES})
        for metrics in results.values():
            self.assert_quality_passes(metrics)

    def test_pipeline_outputs_for_golden_samples_can_be_quality_checked(self):
        with patch.object(agent_pipeline, "_get_model_client", return_value=_QualityFakeClient()):
            for sample in GOLDEN_SAMPLES:
                result = agent_pipeline.run_agent_pipeline(sample["structured_payload"])
                metrics = evaluate_report_output(
                    result["final_text"],
                    sample["expected_output_characteristics"]["language"],
                )
                self.assert_quality_passes(metrics)

    @unittest.skipUnless(os.getenv("RUN_LIVE_AI_QUALITY_EVAL") == "1", "Set RUN_LIVE_AI_QUALITY_EVAL=1 to run live model quality evaluation.")
    def test_live_pipeline_golden_samples_meet_quality_thresholds(self):
        weak = []
        for sample in GOLDEN_SAMPLES:
            result = agent_pipeline.run_agent_pipeline(sample["structured_payload"])
            metrics = evaluate_report_output(result["final_text"], sample["expected_output_characteristics"]["language"])
            if not (
                metrics["repetition_score"] < 0.22
                and metrics["section_overlap"] < 0.42
                and metrics["theme_distinctness"] > 0.55
                and metrics["advisory_tone_score"] > 0.25
                and metrics["language_quality"] >= 0.9
            ):
                weak.append((sample["id"], metrics))
        self.assertEqual(weak, [])


if __name__ == "__main__":
    unittest.main()
