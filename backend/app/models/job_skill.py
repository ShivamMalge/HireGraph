from uuid import UUID

from sqlmodel import Field, Relationship, SQLModel


class JobSkill(SQLModel, table=True):
    __tablename__ = "job_skills"

    job_id: UUID = Field(foreign_key="jobs.job_id", primary_key=True, index=True)
    skill_id: UUID = Field(foreign_key="skills.skill_id", primary_key=True, index=True)

    job: "Job" = Relationship(back_populates="job_skills")
    skill: "Skill" = Relationship(back_populates="job_skills")
