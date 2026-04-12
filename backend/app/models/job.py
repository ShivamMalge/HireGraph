from typing import List
from datetime import datetime
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

from .raw_job import utc_now


class JobBase(SQLModel):
    canonical_title: str = Field(index=True)
    company_id: UUID | None = None
    canonical_location: str
    job_status: str
    job_confidence: float
    canonicalization_version: str


class JobRead(JobBase):
    job_id: UUID
    job_first_seen_at: datetime
    job_last_seen_at: datetime
    canonicalized_at: datetime


class Job(JobBase, table=True):
    __tablename__ = "jobs"

    job_id: UUID = Field(default_factory=uuid4, primary_key=True)
    job_first_seen_at: datetime = Field(default_factory=utc_now)
    job_last_seen_at: datetime = Field(default_factory=utc_now)
    canonicalized_at: datetime = Field(default_factory=utc_now)

    job_postings: List["JobPosting"] = Relationship(back_populates="job")
    job_skills: List["JobSkill"] = Relationship(back_populates="job")
