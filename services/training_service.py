import json

import database as db_mod


def _loads(value, default):
    try:
        return json.loads(value or "null")
    except Exception:
        return default


def generate_training_tasks_from_gaps(db, *, created_by_user_id=None, status="open"):
    gaps = (
        db.query(db_mod.KnowledgeGap)
        .filter(db_mod.KnowledgeGap.status == status)
        .order_by(db_mod.KnowledgeGap.created_at.desc())
        .all()
    )
    created = []
    for gap in gaps:
        existing = (
            db.query(db_mod.TrainingTask)
            .filter(db_mod.TrainingTask.knowledge_gap_id == gap.id, db_mod.TrainingTask.status.in_(("open", "in_progress")))
            .first()
        )
        if existing:
            continue
        missing_entities = _loads(gap.missing_entities_json, [])
        task = db_mod.TrainingTask(
            knowledge_gap=gap,
            task_type="knowledge_gap",
            title=f"Fill knowledge gap for {', '.join(missing_entities[:3]) or 'chart coverage'}",
            description="Create or update knowledge items that explicitly cover the missing chart entities and their applied interpretation logic.",
            priority="high" if len(missing_entities) >= 3 else "medium",
            status="open",
            payload_json=json.dumps(
                {
                    "knowledge_gap_id": gap.id,
                    "missing_entities": missing_entities,
                    "missing_topics": _loads(gap.missing_topics_json, []),
                },
                ensure_ascii=False,
            ),
            created_by_user_id=created_by_user_id,
        )
        db.add(task)
        created.append(task)
    db.flush()
    return created
