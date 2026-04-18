import logging

import database as db_mod
from celery_app import celery_app


logger = logging.getLogger(__name__)
TRANSIENT_EXCEPTIONS = (ConnectionError, TimeoutError)


def _load_order(db, order_id):
    order = db.query(db_mod.ServiceOrder).filter(db_mod.ServiceOrder.id == int(order_id)).first()
    if not order:
        raise ValueError(f"ServiceOrder not found: {order_id}")
    return order


@celery_app.task(bind=True, autoretry_for=TRANSIENT_EXCEPTIONS, retry_backoff=True, retry_kwargs={"max_retries": 3})
def generate_ai_draft_task(self, order_id):
    logger.info("Task start task=generate_ai_draft_task order_id=%s", order_id)
    db = db_mod.SessionLocal()
    try:
        import app

        order = _load_order(db, order_id)
        result = app.generate_ai_draft_for_order(db, order)
        logger.info("Task success task=generate_ai_draft_task order_id=%s result=%s", order_id, result.get("status"))
        return result
    except TRANSIENT_EXCEPTIONS:
        logger.warning("Task retry task=generate_ai_draft_task order_id=%s", order_id)
        raise
    except Exception:
        logger.exception("Task failure task=generate_ai_draft_task order_id=%s", order_id)
        raise
    finally:
        db.close()


@celery_app.task(bind=True, autoretry_for=TRANSIENT_EXCEPTIONS, retry_backoff=True, retry_kwargs={"max_retries": 3})
def generate_pdf_task(self, order_id, deliver_after=False):
    logger.info("Task start task=generate_pdf_task order_id=%s", order_id)
    db = db_mod.SessionLocal()
    try:
        import app

        order = _load_order(db, order_id)
        result = app.generate_pdf_for_order(db, order)
        if deliver_after and (result.get("status") in {"ready", "completed"} or result.get("reason") == "already_ready"):
            from email_tasks import deliver_final_report_task

            delivery = deliver_final_report_task.delay(order_id)
            result["delivery_task_id"] = delivery.id
        logger.info("Task success task=generate_pdf_task order_id=%s result=%s", order_id, result.get("status"))
        return result
    except TRANSIENT_EXCEPTIONS:
        logger.warning("Task retry task=generate_pdf_task order_id=%s", order_id)
        raise
    except Exception:
        logger.exception("Task failure task=generate_pdf_task order_id=%s", order_id)
        raise
    finally:
        db.close()
