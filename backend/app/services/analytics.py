from sqlalchemy import func, text
from sqlmodel import Session, select

from ..models.job import Job
from ..models.job_skill import JobSkill
from ..models.skill import Skill


def get_top_skills(
    session: Session, role_filter: str, limit: int = 10
) -> list[dict[str, int | str]]:
    normalized_limit = max(1, min(limit, 100))

    statement = (
        select(Skill.name, func.count(JobSkill.skill_id).label("job_count"))
        .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
        .join(Job, Job.job_id == JobSkill.job_id)
        .where(Job.canonical_title.ilike(f"%{role_filter}%"))
        .group_by(Skill.name)
        .order_by(func.count(JobSkill.skill_id).desc(), Skill.name.asc())
        .limit(normalized_limit)
    )

    return [
        {"skill": row.name, "count": row.job_count}
        for row in session.exec(statement).all()
    ]


def get_skill_gap(
    session: Session, user_skills: list[str], role: str
) -> list[dict[str, int | str]]:
    normalized_user_skills = {skill.strip().lower() for skill in user_skills if skill.strip()}
    top_skills = get_top_skills(session=session, role_filter=role, limit=50)

    return [
        {"skill": skill_entry["skill"], "demand": skill_entry["count"]}
        for skill_entry in top_skills
        if str(skill_entry["skill"]).lower() not in normalized_user_skills
    ]


def get_skill_demand_analytics(session: Session, limit: int = 100) -> list[dict[str, int | str]]:
    statement = text(
        """
        SELECT skill_name, job_count
        FROM skill_demand_mv
        ORDER BY job_count DESC, skill_name ASC
        LIMIT :limit
        """
    )
    rows = session.exec(statement, {"limit": max(1, min(limit, 500))}).mappings().all()
    return [{"skill_name": row["skill_name"], "job_count": row["job_count"]} for row in rows]


def get_job_title_analytics(session: Session, limit: int = 100) -> list[dict[str, int | str]]:
    statement = text(
        """
        SELECT canonical_title, job_count
        FROM job_title_mv
        ORDER BY job_count DESC, canonical_title ASC
        LIMIT :limit
        """
    )
    rows = session.exec(statement, {"limit": max(1, min(limit, 500))}).mappings().all()
    return [
        {"canonical_title": row["canonical_title"], "job_count": row["job_count"]}
        for row in rows
    ]


def get_location_analytics(session: Session, limit: int = 100) -> list[dict[str, int | str]]:
    statement = text(
        """
        SELECT canonical_location, job_count
        FROM location_mv
        ORDER BY job_count DESC, canonical_location ASC
        LIMIT :limit
        """
    )
    rows = session.exec(statement, {"limit": max(1, min(limit, 500))}).mappings().all()
    return [
        {"canonical_location": row["canonical_location"], "job_count": row["job_count"]}
        for row in rows
    ]


def get_company_analytics(session: Session, limit: int = 100) -> list[dict[str, int | str]]:
    statement = text(
        """
        SELECT company_id, job_count
        FROM company_mv
        ORDER BY job_count DESC, company_id ASC
        LIMIT :limit
        """
    )
    rows = session.exec(statement, {"limit": max(1, min(limit, 500))}).mappings().all()
    return [{"company_id": str(row["company_id"]), "job_count": row["job_count"]} for row in rows]


def get_filtered_skill_analytics(
    session: Session, location: str | None = None, limit: int = 10
) -> list[dict[str, int | str]]:
    normalized_limit = max(1, min(limit, 100))

    if location:
        statement = text(
            """
            SELECT skill_name, SUM(job_count) AS job_count
            FROM skill_location_mv
            WHERE canonical_location ILIKE :location
            GROUP BY skill_name
            ORDER BY job_count DESC, skill_name ASC
            LIMIT :limit
            """
        )
        params = {"location": f"%{location}%", "limit": normalized_limit}
    else:
        statement = text(
            """
            SELECT skill_name, job_count
            FROM skill_demand_mv
            ORDER BY job_count DESC, skill_name ASC
            LIMIT :limit
            """
        )
        params = {"limit": normalized_limit}

    rows = session.exec(statement, params).mappings().all()
    return [{"skill_name": row["skill_name"], "job_count": row["job_count"]} for row in rows]


def get_filtered_job_analytics(
    session,
    skill: str | None = None,
    location: str | None = None,
    limit: int = 10,
) -> list[dict[str, int | str]]:
    normalized_limit = max(1, min(limit, 100))
    conditions: list[str] = []
    params: dict[str, int | str] = {"limit": normalized_limit}

    if skill:
        conditions.append("skills ILIKE :skill")
        params["skill"] = f"%{skill.lower()}%"
    if location:
        conditions.append("canonical_location ILIKE :location")
        params["location"] = f"%{location}%"

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    statement = text(
        f"""
        SELECT canonical_title, COUNT(job_id) AS job_count
        FROM job_filter_mv
        {where_clause}
        GROUP BY canonical_title
        ORDER BY job_count DESC, canonical_title ASC
        LIMIT :limit
        """
    )
    rows = session.exec(statement, params).mappings().all()
    return [
        {"canonical_title": row["canonical_title"], "job_count": row["job_count"]}
        for row in rows
    ]


def get_trend_analytics(session: Session, time_range: str = "30d") -> list[dict[str, int | str]]:
    days = 7 if time_range == "7d" else 30
    statement = text(
        """
        SELECT trend_date, job_count
        FROM trends_mv
        WHERE trend_date >= CURRENT_DATE - CAST(:days AS INTEGER)
        ORDER BY trend_date DESC
        """
    )
    rows = session.exec(statement, {"days": days}).mappings().all()
    return [{"date": str(row["trend_date"]), "job_count": row["job_count"]} for row in rows]
