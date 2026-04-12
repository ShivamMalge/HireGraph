from typing import List
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel


class SkillBase(SQLModel):
    name: str = Field(index=True, unique=True)
    slug: str = Field(unique=True)
    category: str


class SkillRead(SkillBase):
    skill_id: UUID


class Skill(SkillBase, table=True):
    __tablename__ = "skills"

    skill_id: UUID = Field(default_factory=uuid4, primary_key=True)

    job_skills: List["JobSkill"] = Relationship(back_populates="skill")
