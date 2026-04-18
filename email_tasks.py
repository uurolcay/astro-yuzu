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
def send_admin_notification_email_task(self, order_id):
    logger.info("Task start task=send_admin_notification_email_task order_id=%s", order_id)
    db = db_mod.SessionLocal()
    try:
        import app

        order = _load_order(db, order_id)
        result = app.send_admin_notification_for_order(db, order)
        logger.info("Task success task=send_admin_notification_email_task order_id=%s result=%s", order_id, result.get("status"))
        return result
    except TRANSIENT_EXCEPTIONS:
        logger.warning("Task retry task=send_admin_notification_email_task order_id=%s", order_id)
        raise
    except Exception:
        logger.exception("Task failure task=send_admin_notification_email_task order_id=%s", order_id)
        raise
    finally:
        db.close()


@celery_app.task(bind=True, autoretry_for=TRANSIENT_EXCEPTIONS, retry_backoff=True, retry_kwargs={"max_retries": 3})
def send_customer_confirmation_email_task(self, order_id):
    logger.info("Task start task=send_customer_confirmation_email_task order_id=%s", order_id)
    db = db_mod.SessionLocal()
    try:
        import app

        order = _load_order(db, order_id)
        result = app.send_customer_confirmation_for_order(db, order)
        logger.info("Task success task=send_customer_confirmation_email_task order_id=%s result=%s", order_id, result.get("status"))
        return result
    except TRANSIENT_EXCEPTIONS:
        logger.warning("Task retry task=send_customer_confirmation_email_task order_id=%s", order_id)
        raise
    except Exception:
        logger.exception("Task failure task=send_customer_confirmation_email_task order_id=%s", order_id)
        raise
    finally:
        db.close()


@celery_app.task(bind=True, autoretry_for=TRANSIENT_EXCEPTIONS, retry_backoff=True, retry_kwargs={"max_retries": 3})
def deliver_final_report_task(self, order_id):
    logger.info("Task start task=deliver_final_report_task order_id=%s", order_id)
    db = db_mod.SessionLocal()
    try:
        import app

        order = _load_order(db, order_id)
        result = app.deliver_final_report_for_order(db, order, actor="celery")
        logger.info("Task success task=deliver_final_report_task order_id=%s result=%s", order_id, result.get("status"))
        return result
    except TRANSIENT_EXCEPTIONS:
        logger.warning("Task retry task=deliver_final_report_task order_id=%s", order_id)
        raise
    except Exception:
        logger.exception("Task failure task=deliver_final_report_task order_id=%s", order_id)
        raise
    finally:
        db.close()

