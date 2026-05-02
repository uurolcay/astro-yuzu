import os
import sys
from types import SimpleNamespace


def _env_flag(name, default=False):
    raw = str(os.getenv(name, "true" if default else "false")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", REDIS_URL)
RUNNING_UNDER_PYTEST = "pytest" in sys.modules
TASK_ALWAYS_EAGER = _env_flag("CELERY_TASK_ALWAYS_EAGER", default=RUNNING_UNDER_PYTEST)


try:
    from celery import Celery
except Exception:  # pragma: no cover - exercised when local deps are not installed.
    Celery = None


class _EagerResult:
    def __init__(self, value=None):
        self.id = "eager-local"
        self.result = value

    def get(self, timeout=None):
        return self.result


class _EagerTask:
    def __init__(self, func, bind=False):
        self.func = func
        self.bind = bind
        self.__name__ = getattr(func, "__name__", "task")
        self.name = self.__name__

    def __call__(self, *args, **kwargs):
        if self.bind:
            return self.func(self, *args, **kwargs)
        return self.func(*args, **kwargs)

    def delay(self, *args, **kwargs):
        return self.apply_async(args=args, kwargs=kwargs)

    def apply_async(self, args=None, kwargs=None, **_options):
        return _EagerResult(self(*(args or ()), **(kwargs or {})))


class _EagerCelery:
    """Small local fallback so tests/dev can run before Celery is installed."""

    conf = SimpleNamespace(task_always_eager=True)

    def task(self, *task_args, **task_kwargs):
        def decorator(func):
            return _EagerTask(func, bind=bool(task_kwargs.get("bind")))

        if task_args and callable(task_args[0]) and not task_kwargs:
            return decorator(task_args[0])
        return decorator


if Celery is None:
    celery_app = _EagerCelery()
else:
    celery_app = Celery(
        "astro_yuzu",
        broker=CELERY_BROKER_URL,
        backend=CELERY_RESULT_BACKEND,
        include=["report_tasks", "email_tasks"],
    )
    celery_app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone=os.getenv("CELERY_TIMEZONE", "Europe/Istanbul"),
        enable_utc=True,
        task_track_started=True,
        task_time_limit=int(os.getenv("CELERY_TASK_TIME_LIMIT", "600")),
        task_soft_time_limit=int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "540")),
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_always_eager=TASK_ALWAYS_EAGER,
        task_eager_propagates=True,
    )
