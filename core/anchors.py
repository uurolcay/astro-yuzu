"""Top interpretation anchors for prompt guidance and narrative compression."""

from __future__ import annotations

from collections import defaultdict


ANCHOR_TITLE_LOOKUP = {
    ("growth", "inner_state"): "Inner expansion under invisible pressure",
    ("career", "money"): "Career ambition with material consequences",
    ("relationships", "inner_state"): "Emotional independence in relationships",
    ("growth", "career"): "Growth path with public consequence",
    ("money", "inner_state"): "Security rebuilding through restraint",
}

ANCHOR_TYPE_LOOKUP = {
    "career": "career_pressure_axis",
    "relationships": "relationship_pattern",
    "growth": "growth_path",
    "inner_state": "karmic_lesson",
    "money": "core_life_theme",
}

OPPORTUNITY_HINTS = {
    "growth": "Maturity, meaningful expansion, and stronger long-range vision.",
    "career": "Strategic positioning, earned credibility, and visible progress.",
    "relationships": "Cleaner standards, better reciprocity, and emotional clarity.",
    "inner_state": "Inner steadiness, self-awareness, and better energetic boundaries.",
    "money": "Better prioritization, cleaner value decisions, and resource discipline.",
}

RISK_HINTS = {
    "growth": "Inflation, drift, or chasing meaning without grounded follow-through.",
    "career": "Pressure fatigue, over-control, or mistaking delay for failure.",
    "relationships": "Mixed signals, over-accommodation, or avoidable emotional repetition.",
    "inner_state": "Withdrawal, overload, or losing clarity through internal noise.",
    "money": "Leakage, reactive decisions, or comfort spending under pressure.",
}


def build_interpretation_anchors(
    prioritized_signals,
    domain_scores,
    premium_interpretation=None,
    personalization=None,
):
    premium_interpretation = premium_interpretation or {}
    personalization = personalization or {}
    grouped = _group_related_signals(prioritized_signals, domain_scores)
    selected_groups = _select_anchor_groups(grouped)

    anchors = []
    confidence_notes = []
    for rank, group in enumerate(selected_groups, start=1):
        anchor = _build_anchor(rank, group, premium_interpretation, personalization)
        anchors.append(anchor)
        confidence_notes.append(_build_confidence_note(anchor, group))

    while len(anchors) < 3:
        fallback_rank = len(anchors) + 1
        anchors.append(_fallback_anchor(fallback_rank, prioritized_signals, premium_interpretation))
        confidence_notes.append(f"Anchor {fallback_rank} created as a fallback to preserve a stable 3-anchor scaffold.")

    return assemble_anchor_payload(anchors[:3], confidence_notes[:3])


def assemble_anchor_payload(top_anchors, confidence_notes):
    return {
        "top_anchors": top_anchors[:3],
        "narrative_backbone": _build_narrative_backbone(top_anchors[:3]),
        "anchor_prompt_block": _build_anchor_prompt_block(top_anchors[:3]),
        "confidence_notes": confidence_notes[:3],
    }


def _group_related_signals(prioritized_signals, domain_scores):
    groups = {}
    for signal in prioritized_signals:
        domains = signal.get("domains") or _infer_domains(signal, domain_scores)
        primary_domain = domains[0] if domains else "growth"
        secondary_domain = domains[1] if len(domains) > 1 else None
        key = (primary_domain, secondary_domain, signal.get("planet"))
        group = groups.setdefault(
            key,
            {
                "primary_domain": primary_domain,
                "secondary_domain": secondary_domain,
                "signals": [],
                "score": 0.0,
                "tags": [],
            },
        )
        if len(group["signals"]) < 3:
            group["signals"].append(signal)
        group["score"] += float(signal.get("score", 0.0))
        for tag in signal.get("tags", []):
            if tag not in group["tags"]:
                group["tags"].append(tag)
    return sorted(groups.values(), key=lambda item: item["score"], reverse=True)


def _select_anchor_groups(grouped):
    selected = []
    used_domains = set()
    used_titles = set()
    for group in grouped:
        title = _generate_anchor_title(group)
        domains = [group["primary_domain"]]
        if group.get("secondary_domain"):
            domains.append(group["secondary_domain"])
        overlap = len(set(domains) & used_domains)
        if overlap >= len(domains) and len(selected) >= 2:
            continue
        if title in used_titles:
            continue
        selected.append(group)
        used_domains.update(domains)
        used_titles.add(title)
        if len(selected) == 3:
            break
    return selected


def _build_anchor(rank, group, premium_interpretation, personalization):
    title = _generate_anchor_title(group)
    primary_domain = group["primary_domain"]
    secondary_domain = group.get("secondary_domain")
    domains = [domain for domain in [primary_domain, secondary_domain] if domain]
    summary = _generate_summary(group, premium_interpretation)
    return {
        "rank": rank,
        "title": title,
        "anchor_type": ANCHOR_TYPE_LOOKUP.get(primary_domain, "core_life_theme"),
        "summary": summary,
        "why_it_matters": _generate_why_it_matters(domains),
        "domains": domains,
        "supporting_signals": [
            {
                "type": signal.get("type"),
                "planet": signal.get("planet"),
                "house": signal.get("house"),
                "score": signal.get("score"),
            }
            for signal in group["signals"][:3]
        ],
        "opportunity": OPPORTUNITY_HINTS.get(primary_domain, OPPORTUNITY_HINTS["growth"]),
        "risk": RISK_HINTS.get(primary_domain, RISK_HINTS["growth"]),
        "prompt_anchor": _generate_prompt_anchor(group, title, personalization),
    }


def _generate_anchor_title(group):
    key = (group["primary_domain"], group.get("secondary_domain"))
    if key in ANCHOR_TITLE_LOOKUP:
        return ANCHOR_TITLE_LOOKUP[key]
    lead_tag = group["tags"][0].replace("_", " ") if group["tags"] else group["primary_domain"].replace("_", " ")
    return f"{lead_tag.title()} shaping {group['primary_domain'].replace('_', ' ')}"


def _generate_summary(group, premium_interpretation):
    primary = group["primary_domain"].replace("_", " ")
    secondary = (group.get("secondary_domain") or "growth").replace("_", " ")
    premium_summary = premium_interpretation.get("summary", "")
    if premium_summary:
        return f"{premium_summary.split('. ')[0]}. This cluster lands most strongly across {primary} and {secondary}."
    return f"This cluster concentrates the chart's strongest weight across {primary} and {secondary}."


def _generate_why_it_matters(domains):
    domain_text = ", ".join(domain.replace("_", " ") for domain in domains)
    return f"This anchor shapes decision quality, emotional orientation, and timing across {domain_text}."


def _generate_prompt_anchor(group, title, personalization):
    emphasis = ", ".join(group["tags"][:3]) if group["tags"] else group["primary_domain"]
    memory_note = ""
    if personalization.get("dominant_patterns"):
        memory_note = f" Tie it back to the user's recurring pattern around {personalization['dominant_patterns'][0]}."
    return f"Emphasize {title.lower()} through {emphasis}.{memory_note}".strip()


def _build_confidence_note(anchor, group):
    return (
        f"Anchor {anchor['rank']} supported by {len(group['signals'])} converging signals across "
        f"{', '.join(anchor['domains'])}, with combined score {round(group['score'], 2)}."
    )


def _build_narrative_backbone(anchors):
    if not anchors:
        return "No stable narrative backbone could be formed from the current signal set."
    return " -> ".join(anchor["title"] for anchor in anchors)


def _build_anchor_prompt_block(anchors):
    lines = ["Primary chart anchors:"]
    for anchor in anchors:
        lines.append(f"{anchor['rank']}. {anchor['title']} — {anchor['prompt_anchor']}")
    return "\n".join(lines)


def _fallback_anchor(rank, prioritized_signals, premium_interpretation):
    lead = prioritized_signals[min(rank - 1, len(prioritized_signals) - 1)] if prioritized_signals else {}
    title = f"Supporting emphasis {rank} around {lead.get('planet', 'chart pattern')}"
    return {
        "rank": rank,
        "title": title,
        "anchor_type": "core_life_theme",
        "summary": premium_interpretation.get("summary") or "A smaller but still useful supporting theme remains active.",
        "why_it_matters": "This anchor preserves narrative completeness when the chart compresses into fewer dominant clusters.",
        "domains": ["growth"],
        "supporting_signals": [
            {
                "type": lead.get("type"),
                "planet": lead.get("planet"),
                "house": lead.get("house"),
                "score": lead.get("score"),
            }
        ] if lead else [],
        "opportunity": OPPORTUNITY_HINTS["growth"],
        "risk": RISK_HINTS["growth"],
        "prompt_anchor": f"Emphasize {title.lower()} without repeating the main theme.",
    }


def _infer_domains(signal, domain_scores):
    explicit = signal.get("domains")
    if explicit:
        return explicit
    ranked_domains = sorted(domain_scores.items(), key=lambda item: item[1], reverse=True)
    if ranked_domains:
        return [ranked_domains[0][0]]
    return ["growth"]
