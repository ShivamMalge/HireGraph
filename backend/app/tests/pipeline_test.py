from __future__ import annotations

from datetime import datetime, timezone

from sqlmodel import Session, select

from app.db import engine
from app.models.job import Job
from app.models.job_posting import JobPosting
from app.models.job_skill import JobSkill
from app.models.raw_job import RawJobIngestion
from app.models.skill import Skill
from app.services.processing import process_raw_ingestion


def main() -> None:
    sample_payload = "\n".join(
        [
            "Backend Engineer",
            "Company: HireGraph",
            "Location: Remote",
            "We use Python, SQL, AWS and React to build hiring intelligence products.",
        ]
    )

    with Session(engine) as session:
        raw_job = RawJobIngestion(
            observed_at=datetime.now(timezone.utc),
            fetched_at=datetime.now(timezone.utc),
            source_id="test-suite",
            source_record_locator="pipeline-test://raw-job",
            payload_format="text",
            raw_payload=sample_payload,
            ingestion_status="success",
        )
        session.add(raw_job)
        session.commit()
        session.refresh(raw_job)

        print("Inserted RawJobIngestion:", raw_job.raw_ingestion_id)

        result = process_raw_ingestion(raw_job.raw_ingestion_id, session)

        job_posting = session.exec(
            select(JobPosting).where(JobPosting.raw_ingestion_id == raw_job.raw_ingestion_id)
        ).first()
        job = session.get(Job, job_posting.job_id) if job_posting and job_posting.job_id else None
        skills = session.exec(
            select(Skill)
            .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
            .where(JobSkill.job_id == job.job_id if job else None)
            .order_by(Skill.name.asc())
        ).all() if job else []

        print("Processing Result:", result["message"])
        print("JobPosting Created:", bool(job_posting))
        print("Canonical Job Created:", bool(job))
        print("Skills Extracted:", [skill.name for skill in skills])

        assert job_posting is not None, "Expected one JobPosting to be created"
        assert job is not None, "Expected one canonical Job to be created"
        assert job_posting.job_id == job.job_id, "JobPosting should link to one Job"
        assert len(skills) > 0, "Expected at least one skill to be extracted"

        print("Pipeline test passed.")


if __name__ == "__main__":
    main()
