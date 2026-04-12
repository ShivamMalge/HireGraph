from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Relationship, SQLModel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RawJobIngestionBase(SQLModel):
    observed_at: datetime
    fetched_at: datetime
    source_id: str
    source_record_locator: str
    payload_format: str
    raw_payload: str
    ingestion_status: str
    http_metadata: dict[str, Any] | None = Field(
        default=None, sa_column=Column(JSONB, nullable=True)
    )
    run_id: str | None = None


class RawJobIngestionCreate(RawJobIngestionBase):
    pass


class RawJobIngestion(RawJobIngestionBase, table=True):
    __tablename__ = "raw_job_ingestions"

    raw_ingestion_id: UUID = Field(default_factory=uuid4, primary_key=True)
    ingested_at: datetime = Field(default_factory=utc_now)
    job_posting: "JobPosting" = Relationship(back_populates="raw_ingestion")
