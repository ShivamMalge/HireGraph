import logging
from uuid import UUID

from sqlmodel import Session, func, select

from ..models.job import Job, JobRead
from ..models.job_posting import JobPosting, JobPostingRead
from ..models.job_skill import JobSkill
from ..models.raw_job import RawJobIngestion
from ..models.skill import Skill
from ..models.skill import SkillRead
from .canonicalizer import canonicalize_posting
from .parser import parse_raw_job
from .skill_extractor import enrich_job_with_skills


logger = logging.getLogger(__name__)


def _log(pipeline_id: str, stage: str, message: str) -> None:
    logger.info("[%s] [%s] %s", pipeline_id, stage, message)


def _validate_pipeline_integrity(
    raw_ingestion_id: UUID,
    job_posting: JobPosting,
    job: Job,
    session: Session,
    pipeline_id: str,
) -> None:
    posting_count = session.exec(
        select(func.count()).select_from(JobPosting).where(
            JobPosting.raw_ingestion_id == raw_ingestion_id
        )
    ).one()
    if posting_count != 1:
        raise ValueError("Raw job must produce exactly one JobPosting")

    if job_posting.job_id is None or job_posting.job_id != job.job_id:
        raise ValueError("JobPosting must link to exactly one Job")

    duplicate_job_skills = session.exec(
        select(func.count())
        .select_from(JobSkill)
        .where(JobSkill.job_id == job.job_id)
    ).one()
    distinct_job_skills = session.exec(
        select(func.count(func.distinct(JobSkill.skill_id)))
        .select_from(JobSkill)
        .where(JobSkill.job_id == job.job_id)
    ).one()
    if duplicate_job_skills != distinct_job_skills:
        raise ValueError("Duplicate JobSkill entries detected")

    _log(pipeline_id, "INGEST", f"Integrity checks passed for raw ingestion {raw_ingestion_id}")


def process_raw_ingestion(raw_ingestion_id: UUID, session: Session, pipeline_id: str = "sync") -> dict:
    _log(pipeline_id, "INGEST", f"Starting raw ingestion processing {raw_ingestion_id}")

    raw_job = session.get(RawJobIngestion, raw_ingestion_id)
    if raw_job is None:
        raise ValueError("Raw job ingestion not found")

    existing_posting = session.exec(
        select(JobPosting).where(JobPosting.raw_ingestion_id == raw_ingestion_id)
    ).first()
    if existing_posting is not None:
        _log(
            pipeline_id,
            "INGEST",
            f"Processing skipped because posting already exists for {raw_ingestion_id}",
        )

        existing_job = session.get(Job, existing_posting.job_id) if existing_posting.job_id else None
        existing_skills = []
        if existing_job is not None:
            skill_statement = (
                select(Skill)
                .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
                .where(JobSkill.job_id == existing_job.job_id)
                .order_by(Skill.name.asc())
            )
            existing_skills = [
                SkillRead.model_validate(skill) for skill in session.exec(skill_statement)
            ]

        return {
            "job_posting": JobPostingRead.model_validate(existing_posting),
            "job": JobRead.model_validate(existing_job) if existing_job is not None else None,
            "skills": existing_skills,
            "message": "already_processed",
        }

    _log(pipeline_id, "PARSE", f"Parsing raw ingestion {raw_ingestion_id}")
    parsed_job = parse_raw_job(raw_job)
    if parsed_job.parse_status == "failed":
        logger.warning("[%s] [PARSE] Failed to parse raw ingestion %s", pipeline_id, raw_ingestion_id)
        raise ValueError("Raw payload is empty and cannot be parsed")

    job_posting = JobPosting.model_validate(parsed_job)
    session.add(job_posting)
    session.flush()

    _log(pipeline_id, "DEDUP", f"Canonicalizing posting {job_posting.job_posting_id}")
    canonical_job = canonicalize_posting(job_posting, session)

    _log(pipeline_id, "SKILL", f"Extracting skills for posting {job_posting.job_posting_id}")
    extracted_skills = enrich_job_with_skills(job_posting, canonical_job, session)
    _log(
        pipeline_id,
        "SKILL",
        f"Stored {len(extracted_skills)} skills for job {canonical_job.job_id}",
    )
    _validate_pipeline_integrity(raw_ingestion_id, job_posting, canonical_job, session, pipeline_id)

    session.commit()
    session.refresh(job_posting)
    session.refresh(canonical_job)

    _log(pipeline_id, "INGEST", f"Completed raw ingestion processing {raw_ingestion_id}")

    return {
        "job_posting": JobPostingRead.model_validate(job_posting),
        "job": JobRead.model_validate(canonical_job),
        "skills": [SkillRead.model_validate(skill) for skill in extracted_skills],
        "message": "processed",
    }
