import logging
import re

from sqlmodel import Session, select

from ..models.job import Job
from ..models.job_posting import JobPosting
from ..models.job_skill import JobSkill
from ..models.skill import Skill
from ..models.skill_mention import SkillMention

logger = logging.getLogger(__name__)


TECH_SKILLS = [
    "python",
    "java",
    "c++",
    "javascript",
    "typescript",
    "sql",
    "postgres",
    "mongodb",
    "docker",
    "kubernetes",
    "aws",
    "gcp",
    "azure",
    "fastapi",
    "django",
    "flask",
    "react",
    "node",
    "next.js",
    "machine learning",
    "deep learning",
]

SOFT_SKILLS = [
    "communication",
    "leadership",
    "teamwork",
    "problem solving",
    "critical thinking",
]

SKILL_CATEGORIES: dict[str, str] = {
    "python": "language",
    "java": "language",
    "c++": "language",
    "javascript": "language",
    "typescript": "language",
    "sql": "language",
    "postgres": "database",
    "mongodb": "database",
    "docker": "tool",
    "kubernetes": "tool",
    "aws": "cloud",
    "gcp": "cloud",
    "azure": "cloud",
    "fastapi": "framework",
    "django": "framework",
    "flask": "framework",
    "react": "framework",
    "node": "framework",
    "next.js": "framework",
    "machine learning": "domain",
    "deep learning": "domain",
    "communication": "soft-skill",
    "leadership": "soft-skill",
    "teamwork": "soft-skill",
    "problem solving": "soft-skill",
    "critical thinking": "soft-skill",
}

SKILL_PATTERNS: dict[str, tuple[str, ...]] = {
    "c++": (r"\bc\+\+\b",),
    "next.js": (r"\bnext\.?js\b",),
    "node": (r"\bnode(?:\.js)?\b",),
    "postgres": (r"\bpostgres(?:ql)?\b",),
    "machine learning": (r"\bmachine learning\b", r"\bml\b"),
    "deep learning": (r"\bdeep learning\b", r"\bdl\b"),
    "problem solving": (r"\bproblem[- ]solving\b",),
    "critical thinking": (r"\bcritical thinking\b",),
}

NOISE_WORDS = {"job", "role", "team", "experience", "work"}


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _patterns_for_skill(skill: str) -> tuple[str, ...]:
    if skill in SKILL_PATTERNS:
        return SKILL_PATTERNS[skill]
    return (rf"\b{re.escape(skill)}\b",)


def extract_skills(text: str) -> list[str]:
    lowered_text = (text or "").lower()
    extracted: list[str] = []

    for skill in [*TECH_SKILLS, *SOFT_SKILLS]:
        if skill in NOISE_WORDS:
            continue
        for pattern in _patterns_for_skill(skill):
            if re.search(pattern, lowered_text) and skill not in extracted:
                extracted.append(skill)
                break

    return extracted


def normalize_skill(skill: str, session: Session) -> Skill:
    normalized_name = skill.lower().strip()
    slug = _slugify(normalized_name)

    existing_skill = session.exec(select(Skill).where(Skill.slug == slug)).first()
    if existing_skill is not None:
        return existing_skill

    canonical_skill = Skill(
        name=normalized_name,
        slug=slug,
        category=SKILL_CATEGORIES.get(normalized_name, "general"),
    )
    session.add(canonical_skill)
    session.flush()
    return canonical_skill


def enrich_job_with_skills(posting: JobPosting, job: Job, session: Session) -> list[Skill]:
    source_text = "\n".join(
        [
            posting.title_raw or "",
            posting.description_raw or "",
        ]
    ).strip()
    extracted_skills = extract_skills(source_text)
    mapped_skills: list[Skill] = []
    logger.info("[SKILL] Extracted skills: %s", extracted_skills)

    for skill_name in extracted_skills:
        existing_mention = session.exec(
            select(SkillMention).where(
                SkillMention.job_posting_id == posting.job_posting_id,
                SkillMention.raw_text == skill_name,
            )
        ).first()
        if existing_mention is None:
            session.add(
                SkillMention(
                    job_posting_id=posting.job_posting_id,
                    raw_text=skill_name,
                    confidence=1.0,
                )
            )

        canonical_skill = normalize_skill(skill_name, session)

        existing_job_skill = session.exec(
            select(JobSkill).where(
                JobSkill.job_id == job.job_id,
                JobSkill.skill_id == canonical_skill.skill_id,
            )
        ).first()
        if existing_job_skill is None:
            session.add(JobSkill(job_id=job.job_id, skill_id=canonical_skill.skill_id))
            logger.info(
                "[SKILL] Linked job %s to skill %s",
                job.job_id,
                canonical_skill.name,
            )
        else:
            logger.info(
                "[SKILL] Skipped duplicate job-skill link for job %s and skill %s",
                job.job_id,
                canonical_skill.name,
            )

        mapped_skills.append(canonical_skill)

    return mapped_skills
