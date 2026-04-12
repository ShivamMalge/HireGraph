import logging
from uuid import UUID

from sqlmodel import Session

from ..db import engine
from ..services.processing import process_raw_ingestion
from .celery_app import celery_app


logger = logging.getLogger(__name__)


@celery_app.task(
    name="process_pipeline_task",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def process_pipeline_task(self, raw_ingestion_id: str) -> dict:
    print("TASK STARTED", raw_ingestion_id, flush=True)
    pipeline_id = self.request.id or raw_ingestion_id
    logger.info("[%s] [INGEST] Worker received task for raw ingestion %s", pipeline_id, raw_ingestion_id)

    try:
        with Session(engine) as session:
            result = process_raw_ingestion(
                UUID(raw_ingestion_id),
                session,
                pipeline_id=pipeline_id,
            )
            return {
                "message": result["message"],
                "raw_ingestion_id": raw_ingestion_id,
                "job_posting_id": str(result["job_posting"].job_posting_id),
                "job_id": str(result["job"].job_id) if result["job"] is not None else None,
                "skills": [skill.name for skill in result["skills"]],
            }
    except Exception as exc:
        logger.exception("[%s] [ERROR] Pipeline failed for raw ingestion %s", pipeline_id, raw_ingestion_id)
        raise exc
