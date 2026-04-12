import os

from celery import Celery


celery_app = Celery(
    "hiregraph",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1"),
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
)

celery_app.autodiscover_tasks(["app.worker"])

import app.worker.tasks  # noqa: E402,F401

app = celery_app
