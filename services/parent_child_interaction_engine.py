from __future__ import annotations

from copy import deepcopy


UNIVERSAL_PATTERNS = {
    "pressure",
    "control",
    "sensitivity",
    "emotional absorption",
    "withdrawal",
    "expansion",
    "guidance",
    "rebellion",
    "detachment",
    "harmony",
    "analysis",
}

PLANET_PATTERN_MAP = {
    "Sun": {"control", "guidance"},
    "Moon": {"sensitivity", "emotional absorption"},
    "Mars": {"pressure", "rebellion"},
    "Mercury": {"analysis"},
    "Jupiter": {"guidance", "expansion"},
    "Venus": {"harmony", "sensitivity"},
    "Saturn": {"control", "withdrawal"},
    "Rahu": {"expansion", "rebellion"},
    "Ketu": {"detachment", "withdrawal"},
}

CATEGORY_PATTERN_MAP = {
    "emotional": {"sensitivity", "emotional absorption"},
    "emotional_needs": {"sensitivity"},
    "relationship": {"harmony"},
    "support_strategy": {"harmony"},
    "communication": {"analysis"},
    "learning_style": {"analysis"},
    "discipline": {"pressure"},
    "discipline_response": {"rebellion", "withdrawal"},
    "risk": {"pressure"},
    "authority": {"control"},
    "karmic": {"detachment"},
    "life_direction": {"guidance", "expansion"},
    "parent_child_friction": {"pressure", "rebellion"},
}

KEYWORD_PATTERN_MAP = {
    "pressure": {"pressure"},
    "discipline": {"control"},
    "restriction": {"control"},
    "withdrawal": {"withdrawal"},
    "detachment": {"detachment"},
    "sensitive": {"sensitivity"},
    "emotional": {"emotional absorption", "sensitivity"},
    "bond": {"harmony"},
    "harmony": {"harmony"},
    "guidance": {"guidance"},
    "growth": {"expansion"},
    "analysis": {"analysis"},
    "communication": {"analysis"},
    "rebellion": {"rebellion"},
    "instability": {"rebellion", "expansion"},
}

PATTERN_DOMAIN_MAP = {
    "pressure": {"discipline", "behavioral"},
    "control": {"discipline", "attachment"},
    "sensitivity": {"emotional", "attachment"},
    "emotional absorption": {"emotional", "attachment"},
    "withdrawal": {"emotional", "attachment", "communication"},
    "expansion": {"behavioral", "communication"},
    "guidance": {"communication", "discipline", "attachment"},
    "rebellion": {"discipline", "behavioral"},
    "detachment": {"attachment", "karmic"},
    "harmony": {"attachment", "emotional", "communication"},
    "analysis": {"communication", "discipline"},
}

NEGATIVE_INTERACTIONS = {
    ("pressure", "sensitivity"): {
        "trigger": "direct pressure",
        "child_response": "emotional withdrawal",
        "misinterpretation": "the parent reads overwhelm as disobedience",
        "loop": "pressure -> withdrawal -> more pressure",
        "outcome": "communication breakdown and reduced felt safety",
        "category": "emotional_dynamics",
    },
    ("control", "rebellion"): {
        "trigger": "tight correction and control",
        "child_response": "pushback, argument, or covert resistance",
        "misinterpretation": "the parent reads autonomy-seeking as disrespect",
        "loop": "control -> rebellion -> stronger control",
        "outcome": "conflict escalation and authority fatigue",
        "category": "discipline_dynamics",
    },
    ("control", "withdrawal"): {
        "trigger": "heavy structure under stress",
        "child_response": "silent retreat or compliance without openness",
        "misinterpretation": "the parent mistakes silence for understanding",
        "loop": "control -> withdrawal -> less emotional access",
        "outcome": "attachment strain and reduced honesty",
        "category": "attachment_dynamics",
    },
    ("analysis", "sensitivity"): {
        "trigger": "over-explaining before emotional settling",
        "child_response": "feeling corrected rather than understood",
        "misinterpretation": "the parent assumes more explanation will solve the feeling",
        "loop": "analysis -> emotional overload -> more explaining",
        "outcome": "communication fatigue",
        "category": "communication_dynamics",
    },
    ("detachment", "sensitivity"): {
        "trigger": "low-emotion or distanced response",
        "child_response": "clinging, worry, or deeper hurt",
        "misinterpretation": "the parent sees need for closeness as overreaction",
        "loop": "distance -> protest -> more distance",
        "outcome": "attachment insecurity",
        "category": "attachment_dynamics",
    },
}

POSITIVE_INTERACTIONS = {
    ("harmony", "sensitivity"): {
        "trigger": "calm attunement and soft pacing",
        "child_response": "emotional openness and faster regulation",
        "misinterpretation": "none when the pace stays gentle",
        "loop": "attunement -> safety -> trust",
        "outcome": "felt security, trust, and easier repair",
        "category": "emotional_dynamics",
    },
    ("guidance", "analysis"): {
        "trigger": "clear explanation with patient structure",
        "child_response": "receptive thinking and better cooperation",
        "misinterpretation": "low when the parent explains without shaming",
        "loop": "guidance -> understanding -> growth",
        "outcome": "learning support and calmer decision-making",
        "category": "communication_dynamics",
    },
    ("guidance", "sensitivity"): {
        "trigger": "gentle guidance after emotional settling",
        "child_response": "receptive trust",
        "misinterpretation": "low when correction follows connection",
        "loop": "connection -> guidance -> growth",
        "outcome": "secure learning and emotional cooperation",
        "category": "support_patterns",
    },
    ("harmony", "harmony"): {
        "trigger": "warm bonding and relational steadiness",
        "child_response": "easy closeness and trust",
        "misinterpretation": "minimal unless conflict is avoided too long",
        "loop": "bonding -> trust -> easier repair",
        "outcome": "strong attachment and felt belonging",
        "category": "attachment_dynamics",
    },
}

ACTION_MAP = {
    ("pressure", "sensitivity"): {
        "do": "slow communication and reduce emotional temperature before correction",
        "avoid": "sudden pressure or repeated demands during overwhelm",
        "reason": "the child processes pressure as emotional overload before instruction lands",
    },
    ("control", "rebellion"): {
        "do": "offer bounded choices with clear reasons",
        "avoid": "control escalation, public confrontation, or power struggles",
        "reason": "the child protects autonomy by resisting when control feels too tight",
    },
    ("harmony", "sensitivity"): {
        "do": "lead with calm mirroring and reassurance",
        "avoid": "assuming the child can regulate alone under stress",
        "reason": "emotional safety increases openness and makes guidance easier to receive",
    },
    ("analysis", "sensitivity"): {
        "do": "name the feeling first, then explain the step",
        "avoid": "too much explanation before the child feels understood",
        "reason": "emotional regulation needs to happen before mental processing becomes useful",
    },
}

INTENSITY_SCORE = {"low": 1, "medium": 2, "high": 3, "very_high": 4}
CONFIDENCE_SCORE = {"low": 1, "medium": 2, "high": 3}

TR_TEXT = {
    "Parent-child interaction modeling is only available for parent_child report type.": "Ebeveyn-çocuk etkileşim modellemesi yalnızca parent_child rapor türü için kullanılabilir.",
    "No stable parent-child interaction pattern could be derived from the currently available signals.": "Mevcut sinyallerden güvenli biçimde tutarlı bir ebeveyn-çocuk etkileşim örüntüsü çıkarılamadı.",
    "direct pressure": "doğrudan baskı",
    "emotional withdrawal": "duygusal geri çekilme",
    "the parent reads overwhelm as disobedience": "ebeveyn bunalmayı söz dinlememe olarak okuyabilir",
    "pressure -> withdrawal -> more pressure": "baskı -> geri çekilme -> daha fazla baskı",
    "communication breakdown and reduced felt safety": "iletişimde kopukluk ve azalan güven hissi",
    "tight correction and control": "sıkı düzeltme ve kontrol",
    "pushback, argument, or covert resistance": "itiraz, tartışma ya da örtük direnç",
    "the parent reads autonomy-seeking as disrespect": "ebeveyn özerklik arayışını saygısızlık olarak okuyabilir",
    "control -> rebellion -> stronger control": "kontrol -> isyan -> daha güçlü kontrol",
    "conflict escalation and authority fatigue": "çatışmanın tırmanması ve otorite yorgunluğu",
    "calm attunement and soft pacing": "sakin uyumlanma ve yumuşak tempo",
    "emotional openness and faster regulation": "duygusal açıklık ve daha hızlı düzenlenme",
    "none when the pace stays gentle": "tempo yumuşak kaldığında belirgin bir yanlış okuma oluşmaz",
    "attunement -> safety -> trust": "uyumlanma -> güvenlik -> güven",
    "felt security, trust, and easier repair": "güven hissi, güven ilişkisi ve daha kolay onarım",
    "clear explanation with patient structure": "sabırlı bir yapı içinde net açıklama",
    "receptive thinking and better cooperation": "alıcılığı yüksek düşünme ve daha iyi iş birliği",
    "low when the parent explains without shaming": "ebeveyn utandırmadan açıkladığında düşüktür",
    "guidance -> understanding -> growth": "rehberlik -> anlama -> büyüme",
    "learning support and calmer decision-making": "öğrenme desteği ve daha sakin karar alma",
    "gentle guidance after emotional settling": "duygusal olarak yatıştıktan sonra nazik rehberlik",
    "receptive trust": "alıcı güven",
    "low when correction follows connection": "düzeltme temas ve bağdan sonra geldiğinde düşüktür",
    "connection -> guidance -> growth": "bağ kurma -> rehberlik -> büyüme",
    "secure learning and emotional cooperation": "güvenli öğrenme ve duygusal iş birliği",
    "warm bonding and relational steadiness": "sıcak bağ kurma ve ilişkisel istikrar",
    "easy closeness and trust": "kolay yakınlık ve güven",
    "minimal unless conflict is avoided too long": "çatışma çok uzun ertelenmedikçe düşüktür",
    "bonding -> trust -> easier repair": "bağ kurma -> güven -> daha kolay onarım",
    "strong attachment and felt belonging": "güçlü bağlanma ve aidiyet hissi",
    "slow communication and reduce emotional temperature before correction": "düzeltmeden önce iletişimi yavaşlatın ve duygusal harareti düşürün",
    "sudden pressure or repeated demands during overwhelm": "bunalmışken ani baskı ya da tekrarlayan talepler",
    "the child processes pressure as emotional overload before instruction lands": "çocuk yönergeyi almadan önce baskıyı duygusal aşırı yük olarak işler",
    "offer bounded choices with clear reasons": "net gerekçelerle sınırları belli seçenekler sunun",
    "control escalation, public confrontation, or power struggles": "kontrolü tırmandırmak, herkesin önünde yüzleştirmek ya da güç savaşına girmek",
    "the child protects autonomy by resisting when control feels too tight": "kontrol fazla sıkı geldiğinde çocuk özerkliğini dirençle korur",
    "lead with calm mirroring and reassurance": "sakin yansıtma ve güven verme ile başlayın",
    "assuming the child can regulate alone under stress": "çocuğun stres altında kendi kendine düzenlenebileceğini varsaymak",
    "emotional safety increases openness and makes guidance easier to receive": "duygusal güven açıklığı artırır ve rehberliğin alınmasını kolaylaştırır",
    "name the feeling first, then explain the step": "önce duyguyu adlandırın, sonra adımı açıklayın",
    "too much explanation before the child feels understood": "çocuk anlaşıldığını hissetmeden önce fazla açıklama yapmak",
    "emotional regulation needs to happen before mental processing becomes useful": "zihinsel işlemenin faydalı olabilmesi için önce duygusal düzenlenme gerekir",
    "repeat the conditions that create safety and receptivity": "güven ve alıcılık oluşturan koşulları tekrar edin",
    "switching suddenly from connection to pressure": "bağ kurmaktan bir anda baskıya geçmek",
    "the interaction already shows a workable support pattern that strengthens trust": "etkileşim zaten güveni güçlendiren uygulanabilir bir destek örüntüsü gösteriyor",
    "The interaction already shows a workable support pattern that strengthens trust.": "Etkileşim zaten güveni güçlendiren uygulanabilir bir destek örüntüsü gösteriyor.",
    "Insufficient parent-child signal overlap to model interaction patterns safely.": "Etkileşim örüntülerini güvenli biçimde modellemek için yeterli ebeveyn-çocuk sinyal örtüşmesi yok.",
    "No stable parent-child interaction pattern could be derived from the currently available signal overlap.": "Mevcut sinyal örtüşmesinden güvenli biçimde tutarlı bir ebeveyn-çocuk etkileşim örüntüsü çıkarılamadı.",
    "Parent Atmakaraka context was not available for karmic interaction modeling.": "Karmik etkileşim modellemesi için ebeveyn Atmakaraka bağlamı mevcut değildi.",
    "Child dominant signals were too sparse for deeper loop modeling.": "Daha derin döngü modellemesi için çocuğun baskın sinyalleri fazla sınırlı kaldı.",
}


def build_parent_child_interaction_bundle(parent_astro_signal_context, child_astro_signal_context, report_type="parent_child", language="en"):
    if str(report_type or "parent_child").strip().lower() != "parent_child":
        result = {
            "source": "parent_child_interaction_engine",
            "interaction_patterns": [],
            "emotional_dynamics": [],
            "discipline_dynamics": [],
            "communication_dynamics": [],
            "attachment_dynamics": [],
            "karmic_dynamics": [],
            "risk_loops": [],
            "support_patterns": [],
            "recommended_parent_actions": [],
            "confidence_notes": ["Parent-child interaction modeling is only available for parent_child report type."],
        }
        return _localize_bundle(result, language)

    parent_patterns = _extract_behavior_patterns(parent_astro_signal_context, role="parent")
    child_patterns = _extract_behavior_patterns(child_astro_signal_context, role="child")
    if not parent_patterns or not child_patterns:
        result = {
            "source": "parent_child_interaction_engine",
            "interaction_patterns": [],
            "emotional_dynamics": [],
            "discipline_dynamics": [],
            "communication_dynamics": [],
            "attachment_dynamics": [],
            "karmic_dynamics": [],
            "risk_loops": [],
            "support_patterns": [],
            "recommended_parent_actions": [],
            "confidence_notes": ["No stable parent-child interaction pattern could be derived from the currently available signals."],
        }
        return _localize_bundle(result, language)

    bundle = {
        "source": "parent_child_interaction_engine",
        "interaction_patterns": [],
        "emotional_dynamics": [],
        "discipline_dynamics": [],
        "communication_dynamics": [],
        "attachment_dynamics": [],
        "karmic_dynamics": [],
        "risk_loops": [],
        "support_patterns": [],
        "recommended_parent_actions": [],
        "confidence_notes": [],
    }

    seen_pairs = set()
    for parent_pattern in parent_patterns:
        for child_pattern in child_patterns:
            pair_key = (parent_pattern["pattern_type"], child_pattern["pattern_type"])
            if pair_key in seen_pairs:
                continue
            interaction = _build_interaction(parent_pattern, child_pattern)
            if not interaction:
                continue
            seen_pairs.add(pair_key)
            bundle["interaction_patterns"].append(interaction)
            category = interaction.get("category")
            if category in bundle:
                bundle[category].append(interaction)
            if interaction.get("kind") == "risk":
                bundle["risk_loops"].append(
                    {
                        "loop": interaction["loop"],
                        "outcome": interaction["outcome"],
                        "trigger_pair": f"{parent_pattern['pattern_type']} -> {child_pattern['pattern_type']}",
                        "intensity": interaction["intensity"],
                    }
                )
            elif interaction.get("kind") == "support":
                bundle["support_patterns"].append(
                    {
                        "pattern": interaction["loop"],
                        "outcome": interaction["outcome"],
                        "trigger_pair": f"{parent_pattern['pattern_type']} -> {child_pattern['pattern_type']}",
                        "intensity": interaction["intensity"],
                    }
                )
            action = _recommended_action(parent_pattern, child_pattern, interaction)
            if action:
                bundle["recommended_parent_actions"].append(action)

    karmic_dynamic = _build_karmic_dynamic(parent_astro_signal_context, child_patterns)
    if karmic_dynamic:
        bundle["karmic_dynamics"].append(karmic_dynamic)
        bundle["interaction_patterns"].append(karmic_dynamic)

    bundle["recommended_parent_actions"] = _dedupe_actions(bundle["recommended_parent_actions"])
    bundle["confidence_notes"].extend(_confidence_notes(parent_astro_signal_context, child_astro_signal_context, bundle))
    return _localize_bundle(bundle, language)


def _localize_bundle(bundle, language):
    if language != "tr":
        return bundle
    localized = deepcopy(bundle)
    for key in (
        "interaction_patterns",
        "emotional_dynamics",
        "discipline_dynamics",
        "communication_dynamics",
        "attachment_dynamics",
        "karmic_dynamics",
    ):
        rows = localized.get(key) or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            for field in ("trigger", "child_response", "misinterpretation", "loop", "outcome", "description"):
                if isinstance(row.get(field), str):
                    row[field] = TR_TEXT.get(row[field], row[field])
    for key in ("risk_loops", "support_patterns"):
        rows = localized.get(key) or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            for field in ("pattern", "loop", "outcome"):
                if isinstance(row.get(field), str):
                    row[field] = TR_TEXT.get(row[field], row[field])
    for action in localized.get("recommended_parent_actions") or []:
        if not isinstance(action, dict):
            continue
        for field in ("do", "avoid", "reason"):
            if isinstance(action.get(field), str):
                action[field] = TR_TEXT.get(action[field], action[field])
    localized["confidence_notes"] = [TR_TEXT.get(note, note) for note in localized.get("confidence_notes") or []]
    return localized


def _extract_behavior_patterns(signal_context, *, role):
    context = signal_context or {}
    signals = []
    signals.extend(list(context.get("dominant_signals") or []))
    signals.extend(list(context.get("risk_signals") or []))
    signals.extend(list(((context.get("nakshatra_signals") or {}).get("signals") or [])))
    signals.extend(list(((context.get("atmakaraka_signals") or {}).get("signals") or [])))

    patterns = {}
    for signal in signals:
        for pattern_type in _signal_patterns(signal):
            domains = _signal_domains(signal, pattern_type)
            key = f"{role}:{pattern_type}"
            strength = _normalize_strength(signal.get("strength"))
            confidence = _normalize_confidence(signal)
            if key not in patterns or INTENSITY_SCORE[strength] > INTENSITY_SCORE[patterns[key]["strength"]]:
                patterns[key] = {
                    "role": role,
                    "pattern_type": pattern_type,
                    "domains": sorted(domains),
                    "strength": strength,
                    "confidence": confidence,
                    "source_key": signal.get("key"),
                    "source_label": signal.get("label"),
                    "planet": signal.get("planet"),
                    "tone": signal.get("tone"),
                }
    return list(patterns.values())


def _signal_patterns(signal):
    patterns = set()
    planet = str(signal.get("planet") or "").strip()
    patterns |= PLANET_PATTERN_MAP.get(planet, set())
    for category in signal.get("categories") or []:
        patterns |= CATEGORY_PATTERN_MAP.get(str(category or "").strip().lower(), set())
    blob = " ".join(
        [
            str(signal.get("label") or ""),
            str(signal.get("explanation") or ""),
            " ".join(signal.get("keywords") or []),
        ]
    ).lower()
    for keyword, mapped in KEYWORD_PATTERN_MAP.items():
        if keyword in blob:
            patterns |= mapped
    if signal.get("source") == "atmakaraka":
        patterns |= {"guidance"} if signal.get("tone") != "risk" else {"control"}
    return sorted(pattern for pattern in patterns if pattern in UNIVERSAL_PATTERNS)


def _signal_domains(signal, pattern_type):
    domains = set(PATTERN_DOMAIN_MAP.get(pattern_type, {"behavioral"}))
    categories = {str(item or "").strip().lower() for item in signal.get("categories") or []}
    if categories & {"emotional", "emotional_needs"}:
        domains.add("emotional")
    if categories & {"communication", "learning_style"}:
        domains.add("communication")
    if categories & {"discipline", "discipline_response", "authority", "parent_child_friction"}:
        domains.add("discipline")
    if categories & {"relationship", "support_strategy"}:
        domains.add("attachment")
    if categories & {"karmic", "life_direction"}:
        domains.add("karmic")
    return domains


def _build_interaction(parent_pattern, child_pattern):
    pair = (parent_pattern["pattern_type"], child_pattern["pattern_type"])
    if pair in NEGATIVE_INTERACTIONS:
        base = NEGATIVE_INTERACTIONS[pair]
        return _interaction_row(parent_pattern, child_pattern, base, kind="risk")
    if pair in POSITIVE_INTERACTIONS:
        base = POSITIVE_INTERACTIONS[pair]
        category = base["category"]
        if category == "support_patterns":
            category = _primary_category(parent_pattern, child_pattern)
        return _interaction_row(parent_pattern, child_pattern, {**base, "category": category}, kind="support")

    if parent_pattern["pattern_type"] == "guidance" and child_pattern["pattern_type"] in {"sensitivity", "analysis", "harmony"}:
        return _interaction_row(
            parent_pattern,
            child_pattern,
            {
                "trigger": "measured guidance",
                "child_response": "receptive engagement",
                "misinterpretation": "low when the parent stays paced and respectful",
                "loop": "guidance -> receptivity -> growth",
                "outcome": "supportive development",
                "category": _primary_category(parent_pattern, child_pattern),
            },
            kind="support",
        )
    if parent_pattern["pattern_type"] in {"pressure", "control"} and child_pattern["pattern_type"] in {"sensitivity", "withdrawal", "rebellion", "detachment"}:
        return _interaction_row(
            parent_pattern,
            child_pattern,
            {
                "trigger": f"{parent_pattern['pattern_type']} under stress",
                "child_response": f"{child_pattern['pattern_type']} response",
                "misinterpretation": "each side reads protection as resistance",
                "loop": f"{parent_pattern['pattern_type']} -> {child_pattern['pattern_type']} -> escalation",
                "outcome": "recurring friction and reduced trust",
                "category": _primary_category(parent_pattern, child_pattern),
            },
            kind="risk",
        )
    return None


def _interaction_row(parent_pattern, child_pattern, base, *, kind):
    category = base.get("category") or _primary_category(parent_pattern, child_pattern)
    confidence = "high" if CONFIDENCE_SCORE[parent_pattern["confidence"]] >= 2 and CONFIDENCE_SCORE[child_pattern["confidence"]] >= 2 else "medium"
    return {
        "type": "interaction_pattern",
        "category": category,
        "kind": kind,
        "parent_pattern": parent_pattern["pattern_type"],
        "child_pattern": child_pattern["pattern_type"],
        "trigger": base["trigger"],
        "child_response": base["child_response"],
        "misinterpretation": base["misinterpretation"],
        "loop": base["loop"],
        "outcome": base["outcome"],
        "intensity": _combine_intensity(parent_pattern["strength"], child_pattern["strength"]),
        "confidence": confidence,
        "source_keys": [parent_pattern.get("source_key"), child_pattern.get("source_key")],
    }


def _primary_category(parent_pattern, child_pattern):
    shared = set(parent_pattern["domains"]) & set(child_pattern["domains"])
    for preferred in ("emotional", "discipline", "communication", "attachment", "karmic"):
        if preferred in shared:
            return f"{preferred}_dynamics"
    for preferred in ("emotional", "discipline", "communication", "attachment", "karmic"):
        if preferred in set(parent_pattern["domains"]) | set(child_pattern["domains"]):
            return f"{preferred}_dynamics"
    return "communication_dynamics"


def _recommended_action(parent_pattern, child_pattern, interaction):
    action = ACTION_MAP.get((parent_pattern["pattern_type"], child_pattern["pattern_type"]))
    if not action and interaction.get("kind") == "support":
        action = {
            "do": "repeat the conditions that create safety and receptivity",
            "avoid": "switching suddenly from connection to pressure",
            "reason": "the interaction already shows a workable support pattern that strengthens trust",
        }
    if not action:
        return None
    return {
        **action,
        "based_on": f"{parent_pattern['pattern_type']} -> {child_pattern['pattern_type']}",
        "category": interaction.get("category"),
    }


def _build_karmic_dynamic(parent_context, child_patterns):
    atmakaraka = (parent_context or {}).get("atmakaraka_signals") or {}
    planet = atmakaraka.get("atmakaraka_planet")
    if not planet:
        return None
    parent_signal = ((atmakaraka.get("signals") or [{}])[0]) if isinstance(atmakaraka.get("signals"), list) else {}
    parent_patterns = _signal_patterns(parent_signal)
    if not parent_patterns:
        parent_patterns = list(PLANET_PATTERN_MAP.get(planet, set()))
    child_types = {row["pattern_type"] for row in child_patterns}
    conflict_pairs = {
        ("control", "sensitivity"),
        ("control", "rebellion"),
        ("guidance", "detachment"),
        ("pressure", "sensitivity"),
    }
    conflicts = [(pp, cp) for pp in parent_patterns for cp in child_types if (pp, cp) in conflict_pairs]
    if not conflicts:
        return None
    affliction_level = str(atmakaraka.get("affliction_level") or "low").lower()
    intensity = "high" if affliction_level in {"moderate", "high"} or atmakaraka.get("karmic_intensity") == "very_high" else "medium"
    pp, cp = conflicts[0]
    return {
        "type": "karmic_dynamic",
        "category": "karmic_dynamics",
        "kind": "risk",
        "description": (
            f"{planet} Atmakaraka can make the parent try to resolve life through {pp}, while the child currently receives that energy as {cp}. "
            f"This can turn guidance into pressure around lessons the child still needs to experience in their own rhythm."
        ),
        "intensity": intensity,
        "confidence": "medium" if affliction_level == "low" else "high",
    }


def _confidence_notes(parent_context, child_context, bundle):
    notes = []
    if not bundle["interaction_patterns"]:
        notes.append("No stable parent-child interaction pattern could be derived from the currently available signal overlap.")
    if not (parent_context or {}).get("atmakaraka_signals"):
        notes.append("Parent Atmakaraka context was not available for karmic interaction modeling.")
    if not (child_context or {}).get("dominant_signals"):
        notes.append("Child dominant signals were too sparse for deeper loop modeling.")
    return list(dict.fromkeys(notes))


def _dedupe_actions(actions):
    deduped = {}
    for action in actions or []:
        key = (action.get("do"), action.get("avoid"))
        deduped[key] = action
    return list(deduped.values())


def _normalize_strength(value):
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in INTENSITY_SCORE:
            return lowered
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        numeric = 0.0
    if numeric >= 4.5:
        return "very_high"
    if numeric >= 3.5:
        return "high"
    if numeric >= 2.2:
        return "medium"
    return "low"


def _normalize_confidence(signal):
    confidence = str(signal.get("confidence") or "").strip().lower()
    if confidence in CONFIDENCE_SCORE:
        return confidence
    if signal.get("source") == "atmakaraka":
        return "high"
    return "medium"


def _combine_intensity(parent_strength, child_strength):
    score = max(INTENSITY_SCORE.get(parent_strength, 1), INTENSITY_SCORE.get(child_strength, 1))
    for label, label_score in sorted(INTENSITY_SCORE.items(), key=lambda item: item[1], reverse=True):
        if score >= label_score:
            return label
    return "medium"
