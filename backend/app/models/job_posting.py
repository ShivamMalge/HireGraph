from datetime import datetime
from typing import List
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

from .raw_job import utc_now


class JobPostingBase(SQLModel):
    posting_observed_at: datetime
    posted_at: datetime | None = None
    title_raw: str = Field(index=True)
    description_raw: str
    company_name_raw: str = Field(index=True)
    location_raw: str
    employment_type_raw: str | None = None
    salary_raw: str | None = None
    posting_status: str
    parse_status: str
    parse_version: str
    field_completeness: float


class JobPostingCreate(JobPostingBase):
    raw_ingestion_id: UUID


class JobPostingRead(JobPostingBase):
    job_posting_id: UUID
    raw_ingestion_id: UUID
    job_id: UUID | None = None
    parsed_at: datetime


class JobPosting(JobPostingBase, table=True):
    __tablename__ = "job_postings"

    job_posting_id: UUID = Field(default_factory=uuid4, primary_key=True)
    raw_ingestion_id: UUID = Field(
        foreign_key="raw_job_ingestions.raw_ingestion_id", index=True, unique=True
    )
    job_id: UUID | None = Field(default=None, foreign_key="jobs.job_id", index=True)
    parsed_at: datetime = Field(default_factory=utc_now)

    raw_ingestion: "RawJobIngestion" = Relationship(back_populates="job_posting")
    job: "Job" = Relationship(back_populates="job_postings")
    skill_mentions: List["SkillMention"] = Relationship(back_populates="job_posting")
