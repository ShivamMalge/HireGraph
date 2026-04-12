import os

from sqlalchemy import text
from sqlmodel import SQLModel, Session, create_engine

from .models import Job, JobPosting, JobSkill, RawJobIngestion, Skill, SkillMention


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@db:5432/hiregraph"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


SKILL_DEMAND_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS skill_demand_mv AS
SELECT
    skills.name AS skill_name,
    COUNT(job_skills.job_id) AS job_count
FROM job_skills
JOIN skills ON skills.skill_id = job_skills.skill_id
GROUP BY skills.name
"""

JOB_TITLE_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS job_title_mv AS
SELECT
    canonical_title,
    COUNT(job_id) AS job_count
FROM jobs
GROUP BY canonical_title
"""

LOCATION_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS location_mv AS
SELECT
    canonical_location,
    COUNT(job_id) AS job_count
FROM jobs
GROUP BY canonical_location
"""

COMPANY_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS company_mv AS
SELECT
    company_id,
    COUNT(job_id) AS job_count
FROM jobs
WHERE company_id IS NOT NULL
GROUP BY company_id
"""

SKILL_LOCATION_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS skill_location_mv AS
SELECT
    skills.name AS skill_name,
    jobs.canonical_location AS canonical_location,
    COUNT(job_skills.job_id) AS job_count
FROM job_skills
JOIN skills ON skills.skill_id = job_skills.skill_id
JOIN jobs ON jobs.job_id = job_skills.job_id
GROUP BY skills.name, jobs.canonical_location
"""

JOB_FILTER_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS job_filter_mv AS
SELECT
    jobs.job_id,
    jobs.canonical_title,
    jobs.canonical_location,
    jobs.job_first_seen_at::date AS seen_date,
    COALESCE(string_agg(DISTINCT skills.name, ','), '') AS skills
FROM jobs
LEFT JOIN job_skills ON job_skills.job_id = jobs.job_id
LEFT JOIN skills ON skills.skill_id = job_skills.skill_id
GROUP BY jobs.job_id, jobs.canonical_title, jobs.canonical_location, jobs.job_first_seen_at::date
"""

TRENDS_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS trends_mv AS
SELECT
    jobs.job_first_seen_at::date AS trend_date,
    COUNT(jobs.job_id) AS job_count
FROM jobs
GROUP BY jobs.job_first_seen_at::date
"""

MV_INDEX_SQL = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_skill_demand_mv_skill_name ON skill_demand_mv (skill_name)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_job_title_mv_canonical_title ON job_title_mv (canonical_title)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_location_mv_canonical_location ON location_mv (canonical_location)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_mv_company_id ON company_mv (company_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_skill_location_mv_skill_location ON skill_location_mv (skill_name, canonical_location)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_job_filter_mv_job_id ON job_filter_mv (job_id)",
    "CREATE INDEX IF NOT EXISTS idx_job_filter_mv_location ON job_filter_mv (canonical_location)",
    "CREATE INDEX IF NOT EXISTS idx_job_filter_mv_title ON job_filter_mv (canonical_title)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_trends_mv_trend_date ON trends_mv (trend_date)",
]


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)
    create_materialized_views()


def create_materialized_views() -> None:
    with engine.begin() as connection:
        connection.execute(text(SKILL_DEMAND_MV_SQL))
        connection.execute(text(JOB_TITLE_MV_SQL))
        connection.execute(text(LOCATION_MV_SQL))
        connection.execute(text(COMPANY_MV_SQL))
        connection.execute(text(SKILL_LOCATION_MV_SQL))
        connection.execute(text(JOB_FILTER_MV_SQL))
        connection.execute(text(TRENDS_MV_SQL))
        for statement in MV_INDEX_SQL:
            connection.execute(text(statement))


def refresh_all_materialized_views() -> None:
    with engine.begin() as connection:
        connection.execute(text("REFRESH MATERIALIZED VIEW skill_demand_mv"))
        connection.execute(text("REFRESH MATERIALIZED VIEW job_title_mv"))
        connection.execute(text("REFRESH MATERIALIZED VIEW location_mv"))
        connection.execute(text("REFRESH MATERIALIZED VIEW company_mv"))
        connection.execute(text("REFRESH MATERIALIZED VIEW skill_location_mv"))
        connection.execute(text("REFRESH MATERIALIZED VIEW job_filter_mv"))
        connection.execute(text("REFRESH MATERIALIZED VIEW trends_mv"))


def get_session():
    with Session(engine) as session:
        yield session
