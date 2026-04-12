import json
import logging
import re
from uuid import UUID

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from sqlmodel import Session, select

from .db import create_db_and_tables, engine, get_session, refresh_all_materialized_views
from .models.job import Job, JobRead
from .models.job_posting import JobPosting, JobPostingRead
from .models.job_skill import JobSkill
from .models.raw_job import RawJobIngestion, RawJobIngestionCreate
from .models.skill import Skill, SkillRead
from .services.analytics import (
    get_company_analytics,
    get_filtered_job_analytics,
    get_filtered_skill_analytics,
    get_job_title_analytics,
    get_location_analytics,
    get_skill_demand_analytics,
    get_skill_gap,
    get_top_skills,
    get_trend_analytics,
)
from .worker.celery_app import celery_app

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SKILL_KEYWORDS = ["python", "fastapi", "sql", "docker", "aws", "ml", "react", "node"]

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(title="HireGraph Backend", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class EnqueueTaskResponse(BaseModel):
    task_id: str
    status: str


class ProcessRawResponse(BaseModel):
    status: str


class TopSkillResponse(BaseModel):
    skill: str
    count: int


class SkillGapRequest(BaseModel):
    role: str
    skills: list[str]


class SkillGapResponse(BaseModel):
    skill: str
    demand: int


class SkillDemandAnalyticsResponse(BaseModel):
    skill_name: str
    job_count: int


class JobTitleAnalyticsResponse(BaseModel):
    canonical_title: str
    job_count: int


class LocationAnalyticsResponse(BaseModel):
    canonical_location: str
    job_count: int


class CompanyAnalyticsResponse(BaseModel):
    company_id: str
    job_count: int


class AnalyticsEnvelope(BaseModel):
    data: list[dict]
    filters: dict


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: dict | None = None


def _extract_section(raw_payload: str, field_name: str) -> str | None:
    pattern = rf"(?im)^{re.escape(field_name)}:\s*(.+)$"
    match = re.search(pattern, raw_payload or "")
    if match:
        value = match.group(1).strip()
        return value or None
    return None


def _extract_description(raw_payload: str) -> str | None:
    match = re.search(r"(?is)Description:\s*(.+)$", raw_payload or "")
    if match:
        value = match.group(1).strip()
        return value or None
    return None


def _parse_raw_payload(raw_payload: str) -> dict[str, str | None]:
    payload = raw_payload or ""
    return {
        "title": _extract_section(payload, "Title"),
        "company": _extract_section(payload, "Company"),
        "location": _extract_section(payload, "Location"),
        "description": _extract_description(payload),
    }


def _extract_skills(description: str | None) -> list[str]:
    lowered = (description or "").lower()
    return [skill for skill in SKILL_KEYWORDS if re.search(rf"\b{re.escape(skill)}\b", lowered)]


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "HireGraph backend is running"}


@app.post("/raw-jobs", response_model=RawJobIngestion)
def create_raw_job(
    raw_job: RawJobIngestionCreate, session: Session = Depends(get_session)
) -> RawJobIngestion:
    logger.info("[INGEST] Received raw ingestion for locator %s", raw_job.source_record_locator)
    db_raw_job = RawJobIngestion.model_validate(raw_job)
    session.add(db_raw_job)
    session.commit()
    session.refresh(db_raw_job)
    return db_raw_job


@app.get("/raw-jobs", response_model=list[RawJobIngestion])
def list_raw_jobs(session: Session = Depends(get_session)) -> list[RawJobIngestion]:
    statement = (
        select(RawJobIngestion)
        .order_by(RawJobIngestion.ingested_at.desc())
        .limit(10)
    )
    return list(session.exec(statement))


@app.post("/process-raw/{raw_ingestion_id}", response_model=ProcessRawResponse)
def process_raw_job(
    raw_ingestion_id: UUID,
) -> dict[str, str]:
    try:
        with engine.begin() as connection:
            raw_job = connection.execute(
                text(
                    """
                    SELECT id, raw_payload, ingestion_status
                    FROM raw_jobs
                    WHERE id = :raw_ingestion_id
                    """
                ),
                {"raw_ingestion_id": str(raw_ingestion_id)},
            ).mappings().first()

            if raw_job is None:
                raise HTTPException(status_code=404, detail="Raw job not found")

            existing_processed = connection.execute(
                text(
                    """
                    SELECT id
                    FROM processed_jobs
                    WHERE raw_job_id = :raw_ingestion_id
                    """
                ),
                {"raw_ingestion_id": str(raw_ingestion_id)},
            ).first()
            if existing_processed is not None:
                connection.execute(
                    text(
                        """
                        UPDATE raw_jobs
                        SET ingestion_status = 'processed'
                        WHERE id = :raw_ingestion_id
                        """
                    ),
                    {"raw_ingestion_id": str(raw_ingestion_id)},
                )
                return {"status": "processed"}

            parsed = _parse_raw_payload(raw_job["raw_payload"] or "")
            skills = _extract_skills(parsed["description"])

            connection.execute(
                text(
                    """
                    INSERT INTO processed_jobs (
                        raw_job_id,
                        title,
                        company,
                        location,
                        skills,
                        experience_level,
                        role_type,
                        score
                    )
                    VALUES (
                        :raw_job_id,
                        :title,
                        :company,
                        :location,
                        CAST(:skills AS JSONB),
                        :experience_level,
                        :role_type,
                        :score
                    )
                    """
                ),
                {
                    "raw_job_id": str(raw_ingestion_id),
                    "title": parsed["title"],
                    "company": parsed["company"],
                    "location": parsed["location"],
                    "skills": json.dumps(skills),
                    "experience_level": None,
                    "role_type": None,
                    "score": None,
                },
            )

            connection.execute(
                text(
                    """
                    UPDATE raw_jobs
                    SET ingestion_status = 'processed'
                    WHERE id = :raw_ingestion_id
                    """
                ),
                {"raw_ingestion_id": str(raw_ingestion_id)},
            )

        return {"status": "processed"}
    except HTTPException:
        raise
    except Exception:
        logger.exception("[PROCESS] Failed to process raw job %s", raw_ingestion_id)
        raise HTTPException(status_code=500, detail="Failed to process raw job")


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task_status(task_id: str) -> TaskStatusResponse:
    task_result = celery_app.AsyncResult(task_id)

    status_map = {
        "PENDING": "pending",
        "RECEIVED": "running",
        "STARTED": "started",
        "RETRY": "running",
        "SUCCESS": "success",
        "FAILURE": "failed",
    }
    normalized_status = status_map.get(task_result.status, task_result.status.lower())

    result = task_result.result if task_result.successful() else None
    if task_result.failed():
        result = {"error": str(task_result.result)}

    return TaskStatusResponse(task_id=task_id, status=normalized_status, result=result)


@app.get("/debug/pipeline/{raw_ingestion_id}")
def debug_pipeline(raw_ingestion_id: UUID, session: Session = Depends(get_session)) -> dict:
    raw_job = session.get(RawJobIngestion, raw_ingestion_id)
    if raw_job is None:
        raise HTTPException(status_code=404, detail="Raw job ingestion not found")

    job_posting = session.exec(
        select(JobPosting).where(JobPosting.raw_ingestion_id == raw_ingestion_id)
    ).first()
    job = session.get(Job, job_posting.job_id) if job_posting and job_posting.job_id else None

    skills: list[SkillRead] = []
    if job is not None:
        skill_statement = (
            select(Skill)
            .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
            .where(JobSkill.job_id == job.job_id)
            .order_by(Skill.name.asc())
        )
        skills = [SkillRead.model_validate(skill) for skill in session.exec(skill_statement)]

    return {
        "raw_job": raw_job.model_dump(),
        "job_posting": JobPostingRead.model_validate(job_posting).model_dump()
        if job_posting
        else None,
        "job": JobRead.model_validate(job).model_dump() if job else None,
        "skills": [skill.model_dump() for skill in skills],
    }


@app.get("/job-postings", response_model=list[JobPostingRead])
def list_job_postings(session: Session = Depends(get_session)) -> list[JobPostingRead]:
    statement = select(JobPosting).order_by(JobPosting.parsed_at.desc()).limit(10)
    return list(session.exec(statement))


@app.get("/jobs", response_model=list[JobRead])
def list_jobs(session: Session = Depends(get_session)) -> list[JobRead]:
    statement = select(Job).order_by(Job.job_last_seen_at.desc()).limit(10)
    return list(session.exec(statement))


@app.get("/skills", response_model=list[SkillRead])
def list_skills(session: Session = Depends(get_session)) -> list[SkillRead]:
    statement = select(Skill).order_by(Skill.name.asc()).limit(100)
    return list(session.exec(statement))


@app.get("/jobs/{job_id}/skills", response_model=list[SkillRead])
def list_job_skills(job_id: UUID, session: Session = Depends(get_session)) -> list[SkillRead]:
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    statement = (
        select(Skill)
        .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
        .where(JobSkill.job_id == job_id)
        .order_by(Skill.name.asc())
    )
    return list(session.exec(statement))


@app.get("/analytics/top-skills", response_model=list[TopSkillResponse])
def analytics_top_skills(
    role: str, limit: int = 10, session: Session = Depends(get_session)
) -> list[TopSkillResponse]:
    return [TopSkillResponse(**item) for item in get_top_skills(session, role, limit)]


@app.post("/analytics/skill-gap", response_model=list[SkillGapResponse])
def analytics_skill_gap(
    payload: SkillGapRequest, session: Session = Depends(get_session)
) -> list[SkillGapResponse]:
    return [
        SkillGapResponse(**item)
        for item in get_skill_gap(session, payload.skills, payload.role)
    ]


@app.get("/analytics/skills", response_model=AnalyticsEnvelope)
def analytics_skills(
    location: str | None = None,
    limit: int = 10,
    refresh: bool = False,
    session: Session = Depends(get_session),
) -> AnalyticsEnvelope:
    if refresh:
        refresh_all_materialized_views()
    data = get_filtered_skill_analytics(session, location=location, limit=limit)
    return AnalyticsEnvelope(data=data, filters={"location": location, "limit": limit})


@app.get("/analytics/jobs", response_model=AnalyticsEnvelope)
def analytics_jobs(
    skill: str | None = None,
    location: str | None = None,
    limit: int = 10,
    refresh: bool = False,
    session: Session = Depends(get_session),
) -> AnalyticsEnvelope:
    if refresh:
        refresh_all_materialized_views()
    data = get_filtered_job_analytics(session, skill=skill, location=location, limit=limit)
    return AnalyticsEnvelope(
        data=data,
        filters={"skill": skill, "location": location, "limit": limit},
    )


@app.get("/analytics/locations", response_model=list[LocationAnalyticsResponse])
def analytics_locations(
    limit: int = 100, refresh: bool = False, session: Session = Depends(get_session)
) -> list[LocationAnalyticsResponse]:
    if refresh:
        refresh_all_materialized_views()
    return [
        LocationAnalyticsResponse(**item)
        for item in get_location_analytics(session, limit)
    ]


@app.get("/analytics/companies", response_model=list[CompanyAnalyticsResponse])
def analytics_companies(
    limit: int = 100, refresh: bool = False, session: Session = Depends(get_session)
) -> list[CompanyAnalyticsResponse]:
    if refresh:
        refresh_all_materialized_views()
    return [
        CompanyAnalyticsResponse(**item)
        for item in get_company_analytics(session, limit)
    ]


@app.get("/analytics/trends", response_model=AnalyticsEnvelope)
def analytics_trends(
    time_range: str = "30d",
    refresh: bool = False,
    session: Session = Depends(get_session),
) -> AnalyticsEnvelope:
    if time_range not in {"7d", "30d"}:
        raise HTTPException(status_code=400, detail="time_range must be one of: 7d, 30d")
    if refresh:
        refresh_all_materialized_views()
    data = get_trend_analytics(session, time_range=time_range)
    return AnalyticsEnvelope(data=data, filters={"time_range": time_range})
