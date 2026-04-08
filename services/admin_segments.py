"""Read-only admin segmentation, playbook, and export helpers."""

from datetime import datetime, timedelta

import database as db_mod

SEGMENT_GROUPS = {
    "activation": ["NEW_SIGNUPS", "NO_REPORT_USERS", "FIRST_REPORT_USERS", "RETURNING_USERS"],
    "upsell": ["FREE_UPSELL_CANDIDATES", "BASIC_TO_PREMIUM_CANDIDATES", "PREMIUM_TO_ELITE_CANDIDATES"],
    "retention": ["CHURN_RISK_USERS", "INACTIVE_PAID_USERS"],
    "high_value": ["HIGH_VALUE_USERS"],
}
SEGMENT_PLAYBOOKS = {
    "NEW_SIGNUPS": {
        "segment_group": "activation",
        "description": "New accounts that need a strong first-use path.",
        "business_goal": "move new signups into first report activation",
        "recommended_channel": "email",
        "recommended_message_type": "onboarding",
        "recommended_offer_type": "feature_discovery",
        "priority": "medium",
        "urgency": "medium",
        "campaign_tag": "onboarding_new_signup",
        "suggested_cta": "Create your first report",
        "notes_for_admin": "These users are early in the journey and need clarity on the first value moment.",
        "campaign_name": "New Signup Onboarding",
        "campaign_objective": "Activate new users into first report generation",
        "secondary_cta": "See how the platform works",
        "suggested_tone": "clear, reassuring, guided",
        "key_value_angle": "Show the first concrete value moment as early as possible.",
        "caution_note": "Do not overwhelm new users with too many features at once.",
        "recommended_followup_window": "2-3 days",
        "export_ready_tag": "email_onboarding_new_signup",
        "subject_line_angle": "welcome + first-value clarity",
        "opening_angle": "start with what the first report unlocks",
        "message_focus": "easy activation and first-use confidence",
        "cta_focus": "generate the first report",
    },
    "NO_REPORT_USERS": {
        "segment_group": "activation",
        "description": "Signed up but never activated into report generation.",
        "business_goal": "convert signups into activated users",
        "recommended_channel": "email",
        "recommended_message_type": "activation",
        "recommended_offer_type": "feature_discovery",
        "priority": "high",
        "urgency": "medium",
        "campaign_tag": "activation_no_report",
        "suggested_cta": "Generate your first Vedic report",
        "notes_for_admin": "Best used for removing friction and clarifying what the user gets from the first report.",
        "campaign_name": "Signup Without Activation",
        "campaign_objective": "Convert idle signups into first-report users",
        "secondary_cta": "Review what the report includes",
        "suggested_tone": "helpful, low-friction, confidence-building",
        "key_value_angle": "Reduce hesitation by making the first report outcome concrete.",
        "caution_note": "Avoid sounding like a generic reminder with no product value.",
        "recommended_followup_window": "3-5 days",
        "export_ready_tag": "email_activation_no_report",
        "subject_line_angle": "first insight waiting",
        "opening_angle": "you signed up but have not used the core value yet",
        "message_focus": "activation and first report value",
        "cta_focus": "start the first report",
    },
    "FIRST_REPORT_USERS": {
        "segment_group": "activation",
        "description": "Activated once but have not formed a repeat habit yet.",
        "business_goal": "drive second-use activation",
        "recommended_channel": "email",
        "recommended_message_type": "activation",
        "recommended_offer_type": "value_reminder",
        "priority": "high",
        "urgency": "medium",
        "campaign_tag": "activation_second_use",
        "suggested_cta": "See what changed in your current timing",
        "notes_for_admin": "Second-use conversion is usually a strong predictor of retention.",
        "campaign_name": "Second Use Activation",
        "campaign_objective": "Turn first-report users into repeat users",
        "secondary_cta": "Open your previous result again",
        "suggested_tone": "timely, useful, lightly nudging",
        "key_value_angle": "The next report shows evolving timing, not just static profile data.",
        "caution_note": "Do not repeat generic onboarding language; speak to returning value.",
        "recommended_followup_window": "4-6 days",
        "export_ready_tag": "email_second_use_activation",
        "subject_line_angle": "what changed since your last reading",
        "opening_angle": "you already saw the first layer, now see the next one",
        "message_focus": "ongoing value and fresh timing",
        "cta_focus": "return for the second report",
    },
    "RETURNING_USERS": {
        "segment_group": "activation",
        "description": "Users already showing retained behavior.",
        "business_goal": "deepen habit and expand usage depth",
        "recommended_channel": "email",
        "recommended_message_type": "retention",
        "recommended_offer_type": "value_reminder",
        "priority": "medium",
        "urgency": "low",
        "campaign_tag": "retention_returning_users",
        "suggested_cta": "Explore your next timing window",
        "notes_for_admin": "These users are proving repeat value and may be ready for deeper product positioning.",
        "campaign_name": "Returning User Deepening",
        "campaign_objective": "Increase usage depth among retained users",
        "secondary_cta": "Review your strongest life-area themes",
        "suggested_tone": "confident, useful, non-pushy",
        "key_value_angle": "Returning users respond well to deeper timing and interpretation value.",
        "caution_note": "Do not over-sell if the main need is habit reinforcement.",
        "recommended_followup_window": "7-10 days",
        "export_ready_tag": "crm_returning_user_depth",
        "subject_line_angle": "next timing layer",
        "opening_angle": "you already return because the product is useful",
        "message_focus": "deeper interpretation and continuity",
        "cta_focus": "open the next timing insight",
    },
    "FREE_UPSELL_CANDIDATES": {
        "segment_group": "upsell",
        "description": "Free users with repeated usage already showing intent.",
        "business_goal": "convert active free users into paid",
        "recommended_channel": "email",
        "recommended_message_type": "upsell",
        "recommended_offer_type": "premium_unlock",
        "priority": "high",
        "urgency": "medium",
        "campaign_tag": "upsell_free_active",
        "suggested_cta": "Unlock premium report insights",
        "notes_for_admin": "These users already show repeated product usage and are strong monetization candidates.",
        "campaign_name": "Free Active Users Upsell",
        "campaign_objective": "Convert active free users into premium subscribers",
        "secondary_cta": "See what your full report includes",
        "suggested_tone": "clear, value-led, non-pushy",
        "key_value_angle": "They already use the product repeatedly; premium adds depth and timing clarity.",
        "caution_note": "Do not sound overly salesy or vague.",
        "recommended_followup_window": "3-5 days",
        "export_ready_tag": "email_upsell_free_active",
        "subject_line_angle": "timely insight + unlocked value",
        "opening_angle": "you already use this regularly",
        "message_focus": "premium depth and timing clarity",
        "cta_focus": "upgrade to unlock the full reading",
    },
    "BASIC_TO_PREMIUM_CANDIDATES": {
        "segment_group": "upsell",
        "description": "Basic users whose usage suggests they may need more depth.",
        "business_goal": "upgrade basic users into premium",
        "recommended_channel": "email",
        "recommended_message_type": "upsell",
        "recommended_offer_type": "premium_value_framing",
        "priority": "high",
        "urgency": "medium",
        "campaign_tag": "upsell_basic_to_premium",
        "suggested_cta": "Unlock full timing intelligence",
        "notes_for_admin": "Position premium as deeper context, better timing visibility, and more strategic guidance.",
        "campaign_name": "Basic To Premium Expansion",
        "campaign_objective": "Move engaged basic users into premium depth",
        "secondary_cta": "Compare premium report depth",
        "suggested_tone": "strategic, specific, value-forward",
        "key_value_angle": "Premium helps frequent users make better timing-based decisions.",
        "caution_note": "Avoid generic plan comparison language with no real value framing.",
        "recommended_followup_window": "5-7 days",
        "export_ready_tag": "email_upsell_basic_premium",
        "subject_line_angle": "more depth for frequent users",
        "opening_angle": "you are already using the platform consistently",
        "message_focus": "timing intelligence and richer interpretation",
        "cta_focus": "upgrade into premium depth",
    },
    "PREMIUM_TO_ELITE_CANDIDATES": {
        "segment_group": "upsell",
        "description": "Premium users with strong engagement fit for elite positioning.",
        "business_goal": "identify top premium users for elite expansion",
        "recommended_channel": "support_outreach",
        "recommended_message_type": "upsell",
        "recommended_offer_type": "elite_positioning",
        "priority": "medium",
        "urgency": "low",
        "campaign_tag": "upsell_premium_to_elite",
        "suggested_cta": "Access elite-level strategic guidance",
        "notes_for_admin": "Use more consultative positioning here instead of generic discount language.",
        "campaign_name": "Premium To Elite Positioning",
        "campaign_objective": "Qualify heavily engaged premium users for elite expansion",
        "secondary_cta": "Review elite guidance benefits",
        "suggested_tone": "consultative, premium, selective",
        "key_value_angle": "Elite should feel like a strategic step-up, not a simple upsell.",
        "caution_note": "Avoid discount framing; keep it premium and intentional.",
        "recommended_followup_window": "7-10 days",
        "export_ready_tag": "support_elite_positioning",
        "subject_line_angle": "next-tier strategic guidance",
        "opening_angle": "you are already extracting strong value from premium",
        "message_focus": "high-touch depth and strategic support",
        "cta_focus": "explore elite access",
    },
    "CHURN_RISK_USERS": {
        "segment_group": "billing",
        "description": "Users with billing or subscription risk signals.",
        "business_goal": "recover at-risk revenue and prevent churn",
        "recommended_channel": "email",
        "recommended_message_type": "billing_recovery",
        "recommended_offer_type": "support_recovery",
        "priority": "critical",
        "urgency": "high",
        "campaign_tag": "billing_recovery_risk",
        "suggested_cta": "Update billing and restore premium access",
        "notes_for_admin": "This segment is sensitive; support-oriented messaging usually works better than hard upsell language.",
        "campaign_name": "Billing Recovery Risk",
        "campaign_objective": "Recover at-risk revenue before churn finalizes",
        "secondary_cta": "Get billing support",
        "suggested_tone": "supportive, calm, practical",
        "key_value_angle": "Restore access and reduce friction before value perception drops.",
        "caution_note": "Do not overstate the problem or imply more billing issues than actually exist.",
        "recommended_followup_window": "1-3 days",
        "export_ready_tag": "email_billing_recovery_risk",
        "subject_line_angle": "restore access quickly",
        "opening_angle": "there is a solvable account issue to fix",
        "message_focus": "billing recovery and support help",
        "cta_focus": "restore access safely",
    },
    "INACTIVE_PAID_USERS": {
        "segment_group": "retention",
        "description": "Paying users who have gone quiet recently.",
        "business_goal": "re-engage paying users before churn",
        "recommended_channel": "email",
        "recommended_message_type": "re_engagement",
        "recommended_offer_type": "value_reminder",
        "priority": "critical",
        "urgency": "high",
        "campaign_tag": "retention_inactive_paid",
        "suggested_cta": "See your new personalized transit window",
        "notes_for_admin": "Focus on reminding them why the paid plan is worth revisiting now.",
        "campaign_name": "Paid User Re-Engagement",
        "campaign_objective": "Reactivate paying users before churn risk rises",
        "secondary_cta": "Open your personalized report",
        "suggested_tone": "supportive, confidence-building",
        "key_value_angle": "They already have access; the value is waiting for them now.",
        "caution_note": "Avoid implying billing problems unless they actually exist.",
        "recommended_followup_window": "5-7 days",
        "export_ready_tag": "email_reengage_paid",
        "subject_line_angle": "new personalized timing available",
        "opening_angle": "you already have access to current value",
        "message_focus": "re-engagement and timely relevance",
        "cta_focus": "return to the report experience",
    },
    "HIGH_VALUE_USERS": {
        "segment_group": "high_value",
        "description": "High-usage paying users worth protecting closely.",
        "business_goal": "retain and deepen value for top users",
        "recommended_channel": "support_outreach",
        "recommended_message_type": "VIP_retention",
        "recommended_offer_type": "value_reminder",
        "priority": "high",
        "urgency": "medium",
        "campaign_tag": "vip_high_value",
        "suggested_cta": "Review your next premium insight cycle",
        "notes_for_admin": "These users are the strongest proof of product value and deserve careful retention handling.",
        "campaign_name": "High Value User Protection",
        "campaign_objective": "Protect high-value paying users and deepen loyalty",
        "secondary_cta": "Review your premium roadmap",
        "suggested_tone": "premium, appreciative, intelligent",
        "key_value_angle": "These users respond to being understood, not mass messaging.",
        "caution_note": "Avoid generic promotional language for this segment.",
        "recommended_followup_window": "10-14 days",
        "export_ready_tag": "support_vip_retention",
        "subject_line_angle": "continued premium value",
        "opening_angle": "acknowledge their deep engagement",
        "message_focus": "VIP retention and relationship depth",
        "cta_focus": "keep them close to the product",
    },
}


def get_user_report_stats(db):
    stats = {}
    for report in db.query(db_mod.GeneratedReport).order_by(db_mod.GeneratedReport.created_at.asc()).all():
        if not report.user_id:
            continue
        item = stats.setdefault(
            report.user_id,
            {
                "report_count": 0,
                "first_report_at": None,
                "last_report_at": None,
                "report_dates": set(),
            },
        )
        item["report_count"] += 1
        if report.created_at:
            if item["first_report_at"] is None:
                item["first_report_at"] = report.created_at
            item["last_report_at"] = report.created_at
            item["report_dates"].add(report.created_at.date())
    return stats


def get_user_last_activity_map(db):
    return {
        user_id: data.get("last_report_at")
        for user_id, data in get_user_report_stats(db).items()
    }


def get_user_email_signal_map(db):
    signal_map = {}
    for log in db.query(db_mod.EmailLog).order_by(db_mod.EmailLog.created_at.desc()).all():
        key = log.user_id or str(log.recipient_email or "").strip().lower()
        if not key:
            continue
        signal_map.setdefault(key, []).append(log)
    return signal_map


def get_segment_playbook(segment_name):
    segment_key = str(segment_name or "").strip().upper()
    playbook = dict(SEGMENT_PLAYBOOKS.get(segment_key, {}))
    if not playbook:
        playbook = {
            "segment_group": "ops",
            "description": "Operational segment",
            "business_goal": "review segment manually",
            "recommended_channel": "email",
            "recommended_message_type": "retention",
            "recommended_offer_type": "value_reminder",
            "priority": "medium",
            "urgency": "low",
            "campaign_tag": "ops_review",
            "suggested_cta": "Review user segment",
            "notes_for_admin": "No specific playbook metadata defined yet.",
        }
    playbook["segment_name"] = segment_key
    return playbook


def build_segment_row(user, report_stats, email_signals, segment_reason):
    first_report_at = report_stats.get("first_report_at")
    last_report_at = report_stats.get("last_report_at")
    return {
        "user_id": user.id,
        "email": user.email,
        "plan_code": user.plan_code,
        "created_at": user.created_at.strftime("%Y-%m-%d %H:%M") if user.created_at else None,
        "report_count": report_stats.get("report_count", 0),
        "first_report_at": first_report_at.strftime("%Y-%m-%d %H:%M") if first_report_at else None,
        "last_report_at": last_report_at.strftime("%Y-%m-%d %H:%M") if last_report_at else None,
        "subscription_status": getattr(user, "subscription_status", None),
        "segment_reason": segment_reason,
        "email_signals": [log.email_type for log in email_signals[:3]],
    }


def enrich_segment_rows_with_playbook(segment_name, rows):
    playbook = get_segment_playbook(segment_name)
    enriched_rows = []
    for row in rows:
        enriched = dict(row)
        enriched.update({
            "segment_group": playbook["segment_group"],
            "business_goal": playbook["business_goal"],
            "recommended_channel": playbook["recommended_channel"],
            "recommended_message_type": playbook["recommended_message_type"],
            "recommended_offer_type": playbook["recommended_offer_type"],
            "priority": playbook["priority"],
            "urgency": playbook["urgency"],
            "campaign_tag": playbook["campaign_tag"],
            "suggested_cta": playbook["suggested_cta"],
            "notes_for_admin": playbook["notes_for_admin"],
            "playbook_hint": f"{playbook['recommended_message_type']} via {playbook['recommended_channel']}",
        })
        enriched_rows.append(enriched)
    return enriched_rows


def get_segment_display_meta(segment_name, rows):
    playbook = get_segment_playbook(segment_name)
    meta = dict(playbook)
    meta["count"] = len(rows or [])
    meta["campaign_summary"] = (
        f"{playbook['recommended_message_type'].replace('_', ' ')} via "
        f"{playbook['recommended_channel'].replace('_', ' ')} with "
        f"{playbook['recommended_offer_type'].replace('_', ' ')}"
    )
    return meta


def get_campaign_brief(segment_name, rows):
    playbook = get_segment_playbook(segment_name)
    return {
        "campaign_name": playbook.get("campaign_name"),
        "campaign_objective": playbook.get("campaign_objective"),
        "target_segment": segment_name,
        "target_count": len(rows or []),
        "priority": playbook.get("priority"),
        "urgency": playbook.get("urgency"),
        "recommended_channel": playbook.get("recommended_channel"),
        "recommended_message_type": playbook.get("recommended_message_type"),
        "recommended_offer_type": playbook.get("recommended_offer_type"),
        "primary_cta": playbook.get("suggested_cta"),
        "secondary_cta": playbook.get("secondary_cta"),
        "suggested_tone": playbook.get("suggested_tone"),
        "key_value_angle": playbook.get("key_value_angle"),
        "caution_note": playbook.get("caution_note"),
        "recommended_followup_window": playbook.get("recommended_followup_window"),
        "export_ready_tag": playbook.get("export_ready_tag"),
        "subject_line_angle": playbook.get("subject_line_angle"),
        "opening_angle": playbook.get("opening_angle"),
        "message_focus": playbook.get("message_focus"),
        "cta_focus": playbook.get("cta_focus"),
    }


def generate_lifecycle_segments(db):
    now = datetime.utcnow()
    recent_signup_cutoff = now - timedelta(days=7)
    inactive_cutoff = now - timedelta(days=30)
    risky_statuses = {"past_due", "unpaid", "canceled", "cancelled", "incomplete", "incomplete_expired"}

    users = db.query(db_mod.AppUser).order_by(db_mod.AppUser.created_at.desc()).all()
    report_stats_map = get_user_report_stats(db)
    email_signal_map = get_user_email_signal_map(db)

    paid_usage_counts = sorted(
        [
            report_stats_map.get(user.id, {}).get("report_count", 0)
            for user in users
            if user.plan_code != "free"
        ],
        reverse=True,
    )
    high_value_threshold = max(5, paid_usage_counts[min(len(paid_usage_counts), 5) - 1]) if paid_usage_counts else 5

    segments = {name: [] for names in SEGMENT_GROUPS.values() for name in names}

    for user in users:
        report_stats = report_stats_map.get(user.id, {"report_count": 0, "report_dates": set(), "first_report_at": None, "last_report_at": None})
        report_count = report_stats.get("report_count", 0)
        distinct_report_dates = len(report_stats.get("report_dates", set()))
        last_report_at = report_stats.get("last_report_at")
        email_signals = email_signal_map.get(user.id, []) + email_signal_map.get(str(user.email or "").strip().lower(), [])
        signal_types = {signal.email_type for signal in email_signals}

        if user.created_at and user.created_at >= recent_signup_cutoff:
            segments["NEW_SIGNUPS"].append(build_segment_row(user, report_stats, email_signals, "Signed up in the last 7 days"))
        if report_count == 0:
            segments["NO_REPORT_USERS"].append(build_segment_row(user, report_stats, email_signals, "No generated reports yet"))
        if report_count == 1:
            segments["FIRST_REPORT_USERS"].append(build_segment_row(user, report_stats, email_signals, "Only one report generated after signup"))
        if report_count >= 2 and distinct_report_dates >= 2:
            segments["RETURNING_USERS"].append(build_segment_row(user, report_stats, email_signals, f"Returning user with {report_count} reports across multiple dates"))
        elif report_count >= 2:
            segments["RETURNING_USERS"].append(build_segment_row(user, report_stats, email_signals, f"Returning user with {report_count} reports"))

        if user.plan_code == "free" and report_count >= 3:
            segments["FREE_UPSELL_CANDIDATES"].append(build_segment_row(user, report_stats, email_signals, f"Free plan with {report_count} reports"))
        if user.plan_code == "basic" and report_count >= 5:
            segments["BASIC_TO_PREMIUM_CANDIDATES"].append(build_segment_row(user, report_stats, email_signals, f"Basic plan with {report_count} reports"))
        if user.plan_code == "premium" and report_count >= 8:
            segments["PREMIUM_TO_ELITE_CANDIDATES"].append(build_segment_row(user, report_stats, email_signals, f"Premium plan with {report_count} reports"))

        if signal_types.intersection({"payment_failed", "cancellation", "payment_recovery"}) or str(getattr(user, "subscription_status", "")).strip().lower() in risky_statuses:
            reason_bits = []
            if signal_types.intersection({"payment_failed", "cancellation", "payment_recovery"}):
                reason_bits.append("Billing warning signal found in EmailLog")
            if str(getattr(user, "subscription_status", "")).strip().lower() in risky_statuses:
                reason_bits.append(f"Subscription status is {user.subscription_status}")
            segments["CHURN_RISK_USERS"].append(build_segment_row(user, report_stats, email_signals, "; ".join(reason_bits)))

        if user.plan_code != "free" and ((last_report_at is None) or (last_report_at < inactive_cutoff)):
            if last_report_at is None:
                reason = "Paid user with no report activity yet"
            else:
                inactive_days = max((now - last_report_at).days, 0)
                reason = f"Paid user inactive for {inactive_days} days"
            segments["INACTIVE_PAID_USERS"].append(build_segment_row(user, report_stats, email_signals, reason))

        if user.plan_code != "free" and report_count >= high_value_threshold:
            segments["HIGH_VALUE_USERS"].append(build_segment_row(user, report_stats, email_signals, f"Paid user in top usage bucket with {report_count} reports"))

    for segment_name, rows in segments.items():
        rows.sort(key=lambda item: (-item["report_count"], item["email"]))
        if segment_name in {"NEW_SIGNUPS"}:
            rows.sort(key=lambda item: item["created_at"] or "", reverse=True)

    summary = {segment_name: len(rows) for segment_name, rows in segments.items()}
    return {
        "segments": segments,
        "summary": summary,
        "groups": SEGMENT_GROUPS,
    }


def generate_campaign_ready_segments(db):
    segment_context = generate_lifecycle_segments(db)
    enriched_segments = {}
    segment_meta = {}
    campaign_briefs = {}
    for segment_name, rows in segment_context["segments"].items():
        enriched_rows = enrich_segment_rows_with_playbook(segment_name, rows)
        enriched_segments[segment_name] = enriched_rows
        segment_meta[segment_name] = get_segment_display_meta(segment_name, enriched_rows)
        campaign_briefs[segment_name] = get_campaign_brief(segment_name, enriched_rows)
    return {
        "segments": enriched_segments,
        "summary": segment_context["summary"],
        "groups": segment_context["groups"],
        "segment_meta": segment_meta,
        "campaign_briefs": campaign_briefs,
    }


def _resolve_segment_filters(segment, group, priority=""):
    normalized_segment = str(segment or "").strip().upper()
    normalized_group = str(group or "").strip().lower()
    normalized_priority = str(priority or "").strip().lower()
    all_segments = {name for names in SEGMENT_GROUPS.values() for name in names}
    if normalized_segment and normalized_segment not in all_segments:
        return None, None, None, f"Unknown segment: {segment}"
    if normalized_group and normalized_group not in SEGMENT_GROUPS:
        return None, None, None, f"Unknown group: {group}"
    if normalized_priority and normalized_priority not in {"low", "medium", "high", "critical"}:
        return None, None, None, f"Unknown priority: {priority}"
    if normalized_segment:
        return [normalized_segment], normalized_group, normalized_priority, None
    if normalized_group:
        return list(SEGMENT_GROUPS[normalized_group]), normalized_group, normalized_priority, None
    return list(all_segments), normalized_group, normalized_priority, None


def _segment_export_rows(segments, selected_segment_names):
    rows = []
    for segment_name in selected_segment_names:
        for row in segments.get(segment_name, []):
            export_row = dict(row)
            export_row["segment_name"] = segment_name
            export_row.pop("email_signals", None)
            export_row.pop("playbook_hint", None)
            rows.append(export_row)
    return rows


def build_export_columns_for_view(view_name):
    view = str(view_name or "crm").strip().lower() or "crm"
    columns = {
        "crm": [
            "segment_name", "segment_group", "user_id", "email", "plan_code", "created_at", "report_count",
            "first_report_at", "last_report_at", "subscription_status", "segment_reason", "business_goal",
            "recommended_channel", "recommended_message_type", "recommended_offer_type", "priority", "urgency",
            "campaign_tag", "suggested_cta", "notes_for_admin", "campaign_name", "campaign_objective",
            "secondary_cta", "suggested_tone", "key_value_angle", "caution_note", "recommended_followup_window",
            "export_ready_tag", "subject_line_angle", "opening_angle", "message_focus", "cta_focus",
        ],
        "email": [
            "segment_name", "email", "plan_code", "report_count", "segment_reason", "campaign_name",
            "recommended_channel", "recommended_message_type", "priority", "urgency", "campaign_tag",
            "suggested_cta", "secondary_cta", "suggested_tone", "key_value_angle", "subject_line_angle",
            "opening_angle", "message_focus", "cta_focus", "recommended_followup_window", "export_ready_tag",
        ],
        "support": [
            "segment_name", "user_id", "email", "plan_code", "subscription_status", "report_count",
            "last_report_at", "segment_reason", "recommended_channel", "priority", "urgency", "campaign_tag",
            "suggested_cta", "notes_for_admin", "caution_note", "recommended_followup_window",
        ],
        "minimal": [
            "segment_name", "user_id", "email", "plan_code", "segment_reason", "campaign_tag", "export_ready_tag",
        ],
    }
    return view, columns.get(view)


def build_campaign_export_row(row, campaign_brief):
    export_row = dict(row)
    export_row.update(campaign_brief or {})
    return export_row


    return dict(row or {})


def _serialize_campaign_brief(brief):
    return dict(brief or {})


def _apply_segment_context_filters(segment_context, segment, group, priority, channel="", message_type=""):
    selected_segment_names, selected_group, selected_priority, error_message = _resolve_segment_filters(segment, group, priority)
    if error_message:
        return None, None, None, None, error_message

    normalized_channel = str(channel or "").strip().lower()
    normalized_message_type = str(message_type or "").strip().lower()

    if selected_priority:
        selected_segment_names = [
            name for name in selected_segment_names
            if segment_context["segment_meta"].get(name, {}).get("priority") == selected_priority
        ]
    if normalized_channel:
        selected_segment_names = [
            name for name in selected_segment_names
            if str(segment_context["segment_meta"].get(name, {}).get("recommended_channel", "")).strip().lower() == normalized_channel
        ]
    if normalized_message_type:
        selected_segment_names = [
            name for name in selected_segment_names
            if str(segment_context["segment_meta"].get(name, {}).get("recommended_message_type", "")).strip().lower() == normalized_message_type
        ]

    filtered_segments = {name: segment_context["segments"].get(name, []) for name in selected_segment_names}
    filtered_meta = {name: segment_context["segment_meta"].get(name, {}) for name in selected_segment_names}
    filtered_briefs = {name: segment_context["campaign_briefs"].get(name, {}) for name in selected_segment_names}
    return selected_segment_names, filtered_segments, filtered_meta, filtered_briefs, None


def _serialize_segment_row(row):
    return dict(row or {})


def _serialize_campaign_brief(brief):
    return dict(brief or {})


def build_admin_segments_api_payload(db, segment="", group="", priority="", channel="", message_type="", limit=None):
    """Build a stable, read-only campaign segments payload for admin API consumers."""
    segment_context = generate_campaign_ready_segments(db)
    selected_segment_names, filtered_segments, filtered_meta, filtered_briefs, error_message = _apply_segment_context_filters(
        segment_context, segment, group, priority, channel, message_type
    )
    if error_message:
        return None, error_message

    segment_items = []
    total_available_rows = 0
    total_rows = 0
    for name in selected_segment_names:
        available_rows = filtered_segments.get(name, [])
        total_available_rows += len(available_rows)
        rows = available_rows
        if limit is not None:
            rows = rows[:limit]
        serialized_rows = [_serialize_segment_row(row) for row in rows]
        total_rows += len(rows)
        segment_meta = dict(filtered_meta.get(name, {}) or {})
        segment_meta.setdefault("row_count", len(available_rows))
        segment_meta.setdefault("rows_returned_count", len(serialized_rows))
        segment_items.append({
            "segment_name": name,
            "meta": segment_meta,
            "campaign_brief": _serialize_campaign_brief(filtered_briefs.get(name, {})),
            "count": filtered_meta.get(name, {}).get("count", len(available_rows)),
            "row_count": len(available_rows),
            "rows_returned_count": len(serialized_rows),
            "rows": serialized_rows,
        })

    return {
        "filters": {
            "segment": segment or None,
            "group": group or None,
            "priority": priority or None,
            "channel": channel or None,
            "message_type": message_type or None,
            "limit": limit,
        },
        "limit_behavior": "per_segment" if limit is not None else "unbounded",
        "summary": {
            "total_segments": len(segment_items),
            "total_rows": total_rows,
            "segment_count": len(segment_items),
            "rows_returned_count": total_rows,
            "rows_available_count": total_available_rows,
        },
        "segments": segment_items,
        "meta": {
            "notes": [
                "Filters are echoed back exactly as applied to the read-only segment view.",
                "Segment `count` and `row_count` describe total rows available before any per-segment limit is applied.",
            ],
        },
    }, None

