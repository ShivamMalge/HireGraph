from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel


class SkillMentionBase(SQLModel):
    raw_text: str
    confidence: float


class SkillMentionRead(SkillMentionBase):
    id: UUID
    job_posting_id: UUID


class SkillMention(SkillMentionBase, table=True):
    __tablename__ = "skill_mentions"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    job_posting_id: UUID = Field(foreign_key="job_postings.job_posting_id", index=True)

    job_posting: "JobPosting" = Relationship(back_populates="skill_mentions")
