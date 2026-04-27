import copy
from collections import defaultdict

import database as db_mod


DEFAULT_RULE_SET_NAME = "Default Astro AI Behavior"

IMMUTABLE_GROUNDING_RULES = [
    "Use only computed or explicitly provided astrology data.",
    "Do not assume planetary positions.",
    "If data is missing, say so clearly.",
    "Swiss Ephemeris-backed calculations are the authority for chart facts.",
]

DEFAULT_RULES = [
    {
        "code": "truth_no_generic_claims",
        "category": "Truth & Grounding Rules",
        "section": "Safety & Truth",
        "title": "Avoid generic astrology statements",
        "content": "Avoid generic astrology statements unless they are tied to chart-specific computed signals.",
        "sort_order": 10,
    },
    {
        "code": "truth_tie_to_signals",
        "category": "Truth & Grounding Rules",
        "section": "Safety & Truth",
        "title": "Tie claims to signals",
        "content": "Tie interpretations to natal, transit, dasha, timing, parent-child, or recommendation signals present in the payload.",
        "sort_order": 20,
    },
    {
        "code": "quality_premium_synthesis",
        "category": "Interpretation Quality Rules",
        "section": "Analysis Style",
        "title": "Premium synthesis",
        "content": "Prefer intelligent synthesis over listing placements; explain why the strongest signals matter together.",
        "sort_order": 30,
    },
    {
        "code": "quality_depth_control",
        "category": "Interpretation Quality Rules",
        "section": "Depth Controls",
        "title": "Depth control",
        "content": "Go deep when the context supports it, but do not over-explain weak or missing signals.",
        "sort_order": 40,
    },
    {
        "code": "conversation_consistency",
        "category": "Conversation Rules",
        "section": "Conversation Behavior",
        "title": "Maintain continuity",
        "content": "Maintain consistency across follow-up answers and use recent messages as continuity context.",
        "sort_order": 50,
    },
    {
        "code": "conversation_no_repetition",
        "category": "Conversation Rules",
        "section": "Conversation Behavior",
        "title": "Avoid repetition",
        "content": "Avoid repeating the same interpretation unless the admin asks for a recap or rewrite.",
        "sort_order": 60,
    },
    {
        "code": "style_premium_calm",
        "category": "Style Rules",
        "section": "Tone Controls",
        "title": "Premium calm tone",
        "content": "Default to premium, calm, intelligent language with no sensationalism.",
        "sort_order": 70,
    },
    {
        "code": "workspace_internal_expert",
        "category": "Admin Workspace Rules",
        "section": "Focus Controls",
        "title": "Internal expert mode",
        "content": "Treat Admin Astro Workspace output as an expert-use internal reading unless asked to rewrite for a client.",
        "sort_order": 80,
    },
]

UI_SECTIONS = [
    "Safety & Truth",
    "Analysis Style",
    "Depth Controls",
    "Focus Controls",
    "Conversation Behavior",
    "Tone Controls",
]


def ensure_default_rule_set(db, *, admin_user=None):
    rule_set = db.query(db_mod.AiBehaviorRuleSet).filter(db_mod.AiBehaviorRuleSet.name == DEFAULT_RULE_SET_NAME).first()
    if not rule_set:
        rule_set = db_mod.AiBehaviorRuleSet(
            name=DEFAULT_RULE_SET_NAME,
            description="Internal behavior controls for Admin Astro Workspace and Astro Chat.",
            is_active=True,
            created_by_user_id=getattr(admin_user, "id", None),
        )
        db.add(rule_set)
        db.flush()
    for item in DEFAULT_RULES:
        existing = db.query(db_mod.AiBehaviorRule).filter(db_mod.AiBehaviorRule.code == item["code"]).first()
        if existing:
            continue
        db.add(
            db_mod.AiBehaviorRule(
                rule_set=rule_set,
                category=item["category"],
                section=item["section"],
                code=item["code"],
                title=item["title"],
                content=item["content"],
                is_enabled=True,
                is_editable=True,
                sort_order=item["sort_order"],
            )
        )
    return rule_set


def get_active_rule_set(db):
    rule_set = db.query(db_mod.AiBehaviorRuleSet).filter(db_mod.AiBehaviorRuleSet.is_active.is_(True)).order_by(db_mod.AiBehaviorRuleSet.updated_at.desc()).first()
    return rule_set or ensure_default_rule_set(db)


def load_active_rules(db):
    rule_set = get_active_rule_set(db)
    return (
        db.query(db_mod.AiBehaviorRule)
        .filter(db_mod.AiBehaviorRule.rule_set_id == rule_set.id, db_mod.AiBehaviorRule.is_enabled.is_(True))
        .order_by(db_mod.AiBehaviorRule.sort_order.asc(), db_mod.AiBehaviorRule.id.asc())
        .all()
    )


def group_rules_by_category(rules):
    grouped = defaultdict(list)
    for rule in rules or []:
        grouped[rule.category].append(rule)
    return dict(grouped)


def group_rules_by_section(rules):
    grouped = {section: [] for section in UI_SECTIONS}
    for rule in rules or []:
        grouped.setdefault(rule.section or "Analysis Style", []).append(rule)
    return grouped


def build_prompt_rule_blocks(db=None, *, active_rules=None, runtime_overrides=None):
    rules = list(active_rules if active_rules is not None else (load_active_rules(db) if db is not None else []))
    editable_rules = [
        {
            "category": rule.category,
            "section": rule.section,
            "code": rule.code,
            "title": rule.title,
            "content": rule.content,
        }
        for rule in rules
    ]
    if runtime_overrides:
        for item in runtime_overrides:
            if str(item or "").strip():
                editable_rules.append(
                    {
                        "category": "Runtime Overrides",
                        "section": "Runtime",
                        "code": "runtime_override",
                        "title": "Runtime override",
                        "content": str(item).strip(),
                    }
                )
    grouped = defaultdict(list)
    for item in editable_rules:
        grouped[item["category"]].append(item["content"])
    lines = ["Immutable grounding rules:"]
    lines.extend(f"- {rule}" for rule in IMMUTABLE_GROUNDING_RULES)
    if editable_rules:
        lines.append("Active admin behavior rules:")
        for item in editable_rules:
            lines.append(f"- [{item['category']}] {item['content']}")
    return {
        "immutable_rules": list(IMMUTABLE_GROUNDING_RULES),
        "active_rules": editable_rules,
        "grouped_rules": dict(grouped),
        "prompt_block": "\n".join(lines),
    }


def inject_rules_into_payload(payload, *, db=None, active_rules=None, runtime_overrides=None, task_instruction=None):
    enriched = copy.deepcopy(payload or {})
    rule_block = build_prompt_rule_blocks(db, active_rules=active_rules, runtime_overrides=runtime_overrides)
    if task_instruction:
        rule_block["task_instruction"] = task_instruction
    enriched["ai_behavior_rules"] = rule_block
    return enriched


def update_rules_from_form(db, form):
    ensure_default_rule_set(db)
    rules = db.query(db_mod.AiBehaviorRule).order_by(db_mod.AiBehaviorRule.sort_order.asc(), db_mod.AiBehaviorRule.id.asc()).all()
    for rule in rules:
        if not rule.is_editable:
            continue
        enabled_key = f"rule_enabled_{rule.id}"
        content_key = f"rule_content_{rule.id}"
        rule.is_enabled = form.get(enabled_key) in {"1", "true", "on", "yes"}
        next_content = str(form.get(content_key) or "").strip()
        if next_content:
            rule.content = next_content
    return rules
