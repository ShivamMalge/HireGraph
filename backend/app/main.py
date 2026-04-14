import json
import logging
import re
import time
from itertools import combinations
from uuid import UUID

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from sqlmodel import Session, select

from .db import create_db_and_tables, engine, get_session, refresh_all_materialized_views
from .models.job import Job, JobRead
from .models.job_posting import JobPosting, JobPostingRead
from .models.job_skill import JobSkill
from .models.raw_job import RawJobIngestion, RawJobIngestionCreate
from .models.skill import Skill, SkillRead
from .services.analytics import (
    get_company_analytics,
    get_filtered_job_analytics,
    get_filtered_skill_analytics,
    get_job_title_analytics,
    get_location_analytics,
    get_skill_demand_analytics,
    get_skill_gap,
    get_top_skills,
    get_trend_analytics,
)
from .worker.celery_app import celery_app

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SKILL_PATTERNS = {
    "python": [r"\bpython\b", r"\bpy\b"],
    "fastapi": [r"\bfastapi\b"],
    "sql": [r"\bsql\b"],
    "docker": [r"\bdocker\b"],
    "aws": [r"\baws\b", r"\bamazon web services\b"],
    "ml": [r"\bml\b", r"\bmachine learning\b"],
    "react": [r"\breact(?:\.js|js)?\b"],
    "node": [r"\bnode(?:\.js|js)?\b"],
}

NORMALIZATION_MAP = {
    "react.js": "react",
    "reactjs": "react",
    "node.js": "node",
    "nodejs": "node",
    "py": "python",
}

SKILL_WEIGHTS = {
    "python": 3,
    "aws": 3,
    "docker": 2,
    "sql": 2,
    "fastapi": 2,
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(title="HireGraph Backend", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class EnqueueTaskResponse(BaseModel):
    task_id: str
    status: str


class ProcessRawResponse(BaseModel):
    status: str


class RecommendJobsRequest(BaseModel):
    skills: list[str]


class RecommendJobResponse(BaseModel):
    job_id: str
    title: str | None = None
    company: str | None = None
    score: float


class DiagnosticsSkillExtractionResponse(BaseModel):
    skills: list[str]
    frequency_correct: bool


class DiagnosticsScoringResponse(BaseModel):
    expected_score: int
    actual_score: int
    match: bool


class DiagnosticsGraphResponse(BaseModel):
    graph_valid: bool


class DiagnosticsRecommendationResponse(BaseModel):
    recommendation_valid: bool


class DiagnosticsPerformanceResponse(BaseModel):
    latency_ms: float
    performance_ok: bool


class DiagnosticsReport(BaseModel):
    pipeline_integrity: str
    skill_extraction: DiagnosticsSkillExtractionResponse
    scoring: DiagnosticsScoringResponse
    graph: DiagnosticsGraphResponse
    recommendation: DiagnosticsRecommendationResponse
    edge_cases: str
    performance: DiagnosticsPerformanceResponse
    overall_status: str


class TopSkillResponse(BaseModel):
    skill: str
    count: int


class SkillGapRequest(BaseModel):
    role: str
    skills: list[str]


class SkillGapResponse(BaseModel):
    skill: str
    demand: int


class SkillDemandAnalyticsResponse(BaseModel):
    skill_name: str
    job_count: int


class JobTitleAnalyticsResponse(BaseModel):
    canonical_title: str
    job_count: int


class LocationAnalyticsResponse(BaseModel):
    canonical_location: str
    job_count: int


class CompanyAnalyticsResponse(BaseModel):
    company_id: str
    job_count: int


class AnalyticsEnvelope(BaseModel):
    data: list[dict]
    filters: dict


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: dict | None = None


def _extract_section(raw_payload: str, field_name: str) -> str | None:
    pattern = rf"(?im)^{re.escape(field_name)}:\s*(.+)$"
    match = re.search(pattern, raw_payload or "")
    if match:
        value = match.group(1).strip()
        return value or None
    return None


def _extract_description(raw_payload: str) -> str | None:
    match = re.search(r"(?is)Description:\s*(.+)$", raw_payload or "")
    if match:
        value = match.group(1).strip()
        return value or None
    return None


def _parse_raw_payload(raw_payload: str) -> dict[str, str | None]:
    payload = raw_payload or ""
    return {
        "title": _extract_section(payload, "Title"),
        "company": _extract_section(payload, "Company"),
        "location": _extract_section(payload, "Location"),
        "description": _extract_description(payload),
    }


def normalize_skill(skill: str) -> str:
    normalized = (skill or "").strip().lower()
    return NORMALIZATION_MAP.get(normalized, normalized)


def _extract_skills(description: str | None) -> dict[str, int]:
    lowered = (description or "").lower()
    if not lowered.strip():
        return {}

    skills: dict[str, int] = {}

    for skill, patterns in SKILL_PATTERNS.items():
        normalized_skill = normalize_skill(skill)
        max_count = 0

        for pattern in patterns:
            matches = re.findall(pattern, lowered)
            if matches:
                max_count = max(max_count, len(matches))

        if max_count > 0:
            skills[normalized_skill] = skills.get(normalized_skill, 0) + max_count

    return skills


def _extract_experience(description: str | None) -> str | None:
    text = (description or "").lower()
    if not text.strip():
        return None

    if re.search(r"\bintern\b", text):
        return "intern"
    if re.search(r"\bjunior\b|\bentry\b", text):
        return "junior"
    if re.search(r"\bsenior\b|\blead\b|\b[5-9]\+\b|\b\d{2,}\+\b", text):
        return "senior"
    return "mid"


def _classify_role(title: str | None, description: str | None) -> str:
    text = f"{title or ''} {description or ''}".lower()
    if "backend" in text:
        return "backend"
    if "frontend" in text:
        return "frontend"
    if "data" in text:
        return "data"
    if "ml" in text or "machine learning" in text:
        return "ml"
    return "general"


def _compute_score(skills: dict[str, int], role: str, experience: str | None) -> int:
    score = sum(SKILL_WEIGHTS.get(skill, 1) * frequency for skill, frequency in skills.items())
    if role == "backend":
        score += 2
    if experience == "senior":
        score += 3
    return int(score)


def generate_skill_pairs(skills: list[str]) -> list[tuple[str, str]]:
    unique_skills = sorted(set(skills))
    if len(unique_skills) < 2:
        return []
    return list(combinations(unique_skills, 2))


def _normalize_user_skills(skills: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for skill in skills:
        normalized_skill = normalize_skill(skill)
        if normalized_skill and normalized_skill not in seen:
            seen.add(normalized_skill)
            normalized.append(normalized_skill)

    return normalized


def _job_skills_to_vector(job_skills: object) -> dict[str, int]:
    if not isinstance(job_skills, list):
        return {}

    vector: dict[str, int] = {}
    for skill in job_skills:
        normalized_skill = normalize_skill(str(skill))
        if normalized_skill:
            vector[normalized_skill] = vector.get(normalized_skill, 0) + 1
    return vector


def compute_similarity(job_skills: dict[str, int], user_skills: list[str]) -> float:
    if not job_skills or not user_skills:
        return 0.0

    user_skill_set = set(user_skills)
    overlap_score = sum(
        SKILL_WEIGHTS.get(skill, 0) * frequency
        for skill, frequency in job_skills.items()
        if skill in user_skill_set
    )
    total_possible_score = sum(
        SKILL_WEIGHTS.get(skill, 0) * frequency for skill, frequency in job_skills.items()
    )
    return float(overlap_score / (total_possible_score + 1e-6))


def _build_graph_bonus_map(user_skills: list[str], graph_rows: list[dict]) -> dict[str, float]:
    bonus_map: dict[str, float] = {}
    user_skill_set = set(user_skills)

    for row in graph_rows:
        skill_a = row["skill_a"]
        skill_b = row["skill_b"]
        count = int(row["co_occurrence_count"] or 0)
        if count <= 0:
            continue

        if skill_a in user_skill_set and skill_b not in user_skill_set:
            bonus_map[skill_b] = max(bonus_map.get(skill_b, 0.0), min(count / 10.0, 0.2))
        if skill_b in user_skill_set and skill_a not in user_skill_set:
            bonus_map[skill_a] = max(bonus_map.get(skill_a, 0.0), min(count / 10.0, 0.2))

    return bonus_map


def _compute_graph_bonus(
    job_skills: dict[str, int],
    user_skills: list[str],
    graph_bonus_map: dict[str, float],
) -> float:
    if not job_skills or not user_skills:
        return 0.0

    user_skill_set = set(user_skills)
    bonus = 0.0
    for skill in job_skills:
        if skill in user_skill_set:
            continue
        bonus += graph_bonus_map.get(skill, 0.0)
    return min(bonus, 0.5)


def _fetch_recommendation_jobs(connection, normalized_user_skills: list[str]) -> list[dict]:
    if not normalized_user_skills:
        return []

    return list(
        connection.execute(
            text(
                """
                SELECT id, title, company, skills, score
                FROM processed_jobs
                WHERE skills IS NOT NULL
                  AND jsonb_array_length(skills) > 0
                  AND skills ?| string_to_array(:user_skills_csv, ',')
                ORDER BY score DESC NULLS LAST, created_at DESC
                LIMIT 50
                """
            ),
            {"user_skills_csv": ",".join(normalized_user_skills)},
        ).mappings().all()
    )


def get_recommendations(session: Session, skills: list[str], limit: int = 5) -> list[dict]:
    if not skills:
        return []

    rows = session.execute(
        text("SELECT * FROM processed_jobs ORDER BY score DESC LIMIT 50")
    ).mappings().all()

    results: list[dict] = []

    for row in rows:
        job_skills = row.get("skills") or []
        if isinstance(job_skills, str):
            job_skills = json.loads(job_skills)

        if any(skill in job_skills for skill in skills):
            results.append(dict(row))

    return results[:limit]


def _run_pipeline_integrity_check(connection) -> str:
    processed_jobs_count = connection.execute(text("SELECT COUNT(*) FROM processed_jobs")).scalar() or 0
    return "pass" if processed_jobs_count > 0 else "fail"


def _run_skill_extraction_validation() -> DiagnosticsSkillExtractionResponse:
    test_payload = "Python Python FastAPI AWS Docker"
    extracted = _extract_skills(test_payload)
    expected = {"python": 2, "fastapi": 1, "aws": 1, "docker": 1}
    return DiagnosticsSkillExtractionResponse(
        skills=list(extracted.keys()),
        frequency_correct=extracted == expected,
    )


def _run_scoring_validation() -> DiagnosticsScoringResponse:
    skills_dict = _extract_skills("Python Python FastAPI AWS Docker")
    expected_score = (3 * 2) + (2 * 1) + (3 * 1) + (2 * 1) + 2 + 3
    actual_score = _compute_score(skills_dict, "backend", "senior")
    return DiagnosticsScoringResponse(
        expected_score=expected_score,
        actual_score=actual_score,
        match=expected_score == actual_score,
    )


def _run_graph_consistency_check(connection) -> DiagnosticsGraphResponse:
    duplicate_pairs = connection.execute(
        text(
            """
            SELECT COUNT(*)
            FROM (
                SELECT skill_a, skill_b
                FROM skill_relationships
                GROUP BY skill_a, skill_b
                HAVING COUNT(*) > 1
            ) AS duplicates
            """
        )
    ).scalar() or 0
    invalid_order = connection.execute(
        text("SELECT COUNT(*) FROM skill_relationships WHERE skill_a >= skill_b")
    ).scalar() or 0
    invalid_count = connection.execute(
        text("SELECT COUNT(*) FROM skill_relationships WHERE co_occurrence_count <= 0")
    ).scalar() or 0

    return DiagnosticsGraphResponse(
        graph_valid=duplicate_pairs == 0 and invalid_order == 0 and invalid_count == 0
    )


def _run_recommendation_validation() -> tuple[DiagnosticsRecommendationResponse, float]:
    normalized_user_skills = _normalize_user_skills(["python"])
    start = time.perf_counter()
    with Session(engine) as session:
        recommendations = get_recommendations(session, normalized_user_skills, limit=5)
    latency_ms = (time.perf_counter() - start) * 1000

    recommendation_valid = False

    if recommendations:
        for job in recommendations:
            job_skills = job.get("skills") or []
            if isinstance(job_skills, str):
                job_skills = json.loads(job_skills)

            if any(skill in job_skills for skill in ["python", "fastapi", "docker", "aws"]):
                recommendation_valid = True
                break

    return DiagnosticsRecommendationResponse(recommendation_valid=recommendation_valid), latency_ms


def _run_edge_case_validation() -> str:
    try:
        empty_result = recommend_jobs(RecommendJobsRequest(skills=[]))
        unknown_result = recommend_jobs(RecommendJobsRequest(skills=["unknown-skill"]))
        missing_description_skills = _extract_skills(None)
        if empty_result != []:
            return "fail"
        if not isinstance(unknown_result, list):
            return "fail"
        if missing_description_skills != {}:
            return "fail"
        return "pass"
    except Exception:
        return "fail"


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "HireGraph backend is running"}


@app.post("/raw-jobs", response_model=RawJobIngestion)
def create_raw_job(
    raw_job: RawJobIngestionCreate, session: Session = Depends(get_session)
) -> RawJobIngestion:
    logger.info("[INGEST] Received raw ingestion for locator %s", raw_job.source_record_locator)
    db_raw_job = RawJobIngestion.model_validate(raw_job)
    session.add(db_raw_job)
    session.commit()
    session.refresh(db_raw_job)
    return db_raw_job


@app.get("/raw-jobs", response_model=list[RawJobIngestion])
def list_raw_jobs(session: Session = Depends(get_session)) -> list[RawJobIngestion]:
    statement = (
        select(RawJobIngestion)
        .order_by(RawJobIngestion.ingested_at.desc())
        .limit(10)
    )
    return list(session.exec(statement))


@app.post("/process-raw/{raw_ingestion_id}", response_model=ProcessRawResponse)
def process_raw_job(
    raw_ingestion_id: UUID,
) -> dict[str, str]:
    try:
        with engine.begin() as connection:
            raw_job = connection.execute(
                text(
                    """
                    SELECT id, raw_payload, ingestion_status
                    FROM raw_jobs
                    WHERE id = :raw_ingestion_id
                    """
                ),
                {"raw_ingestion_id": str(raw_ingestion_id)},
            ).mappings().first()

            if raw_job is None:
                raise HTTPException(status_code=404, detail="Raw job not found")

            existing_processed = connection.execute(
                text(
                    """
                    SELECT id
                    FROM processed_jobs
                    WHERE raw_job_id = :raw_ingestion_id
                    """
                ),
                {"raw_ingestion_id": str(raw_ingestion_id)},
            ).first()
            if existing_processed is not None:
                connection.execute(
                    text(
                        """
                        UPDATE raw_jobs
                        SET ingestion_status = 'processed'
                        WHERE id = :raw_ingestion_id
                        """
                    ),
                    {"raw_ingestion_id": str(raw_ingestion_id)},
                )
                return {"status": "processed"}

            parsed = _parse_raw_payload(raw_job["raw_payload"] or "")
            skills_dict = _extract_skills(parsed["description"])
            skills = list(skills_dict.keys())
            experience = _extract_experience(parsed["description"]) or "mid"
            role = _classify_role(parsed["title"], parsed["description"]) or "general"
            score = _compute_score(skills_dict, role, experience)
            skill_pairs = generate_skill_pairs(skills)

            connection.execute(
                text(
                    """
                    INSERT INTO processed_jobs (
                        raw_job_id,
                        title,
                        company,
                        location,
                        skills,
                        experience_level,
                        role_type,
                        score
                    )
                    VALUES (
                        :raw_job_id,
                        :title,
                        :company,
                        :location,
                        CAST(:skills AS JSONB),
                        :experience_level,
                        :role_type,
                        :score
                    )
                    """
                ),
                {
                    "raw_job_id": str(raw_ingestion_id),
                    "title": parsed["title"],
                    "company": parsed["company"],
                    "location": parsed["location"],
                    "skills": json.dumps(skills),
                    "experience_level": experience,
                    "role_type": role,
                    "score": score,
                },
            )

            for skill_a, skill_b in skill_pairs:
                connection.execute(
                    text(
                        """
                        INSERT INTO skill_relationships (
                            skill_a,
                            skill_b,
                            co_occurrence_count
                        )
                        VALUES (
                            :skill_a,
                            :skill_b,
                            1
                        )
                        ON CONFLICT (skill_a, skill_b)
                        DO UPDATE SET
                            co_occurrence_count = skill_relationships.co_occurrence_count + 1
                        """
                    ),
                    {
                        "skill_a": skill_a,
                        "skill_b": skill_b,
                    },
                )

            connection.execute(
                text(
                    """
                    UPDATE raw_jobs
                    SET ingestion_status = 'processed'
                    WHERE id = :raw_ingestion_id
                    """
                ),
                {"raw_ingestion_id": str(raw_ingestion_id)},
            )

        return {"status": "processed"}
    except HTTPException:
        raise
    except Exception:
        logger.exception("[PROCESS] Failed to process raw job %s", raw_ingestion_id)
        raise HTTPException(status_code=500, detail="Failed to process raw job")


@app.post("/recommend-jobs", response_model=list[RecommendJobResponse])
def recommend_jobs(payload: RecommendJobsRequest) -> list[RecommendJobResponse]:
    normalized_user_skills = _normalize_user_skills(payload.skills)
    if not normalized_user_skills:
        return []

    try:
        with Session(engine) as session:
            jobs = get_recommendations(session, normalized_user_skills, limit=50)

        recommendations: list[RecommendJobResponse] = []

        for job in jobs:
            job_skills = _job_skills_to_vector(job["skills"])
            if not job_skills:
                continue

            overlap = set(job_skills) & set(normalized_user_skills)
            score = 0.0
            for skill in overlap:
                score += SKILL_WEIGHTS.get(skill, 1)
            logger.info(
                "[RECOMMEND] job_id=%s, skills=%s, score=%.4f, overlap=%s",
                job["id"],
                job_skills,
                score,
                sorted(overlap),
            )

            if score <= 0:
                continue

            recommendations.append(
                RecommendJobResponse(
                    job_id=str(job["id"]),
                    title=job["title"],
                    company=job["company"],
                    score=round(float(score), 4),
                )
            )

        recommendations.sort(key=lambda item: item.score, reverse=True)
        logger.info("[DEBUG_RECOMMEND] %s", [item.model_dump() for item in recommendations[:10]])
        logger.info("[RECOMMEND_RESULTS] %s", [item.model_dump() for item in recommendations[:10]])
        return recommendations[:10]
    except Exception:
        logger.exception("[RECOMMEND] Failed to compute job recommendations")
        raise HTTPException(status_code=500, detail="Failed to recommend jobs")


@app.get("/diagnostics/run", response_model=DiagnosticsReport)
def run_diagnostics() -> DiagnosticsReport:
    try:
        with engine.begin() as connection:
            graph = _run_graph_consistency_check(connection)

        skill_extraction = _run_skill_extraction_validation()
        scoring = _run_scoring_validation()
        recommendation, latency_ms = _run_recommendation_validation()
        edge_cases = _run_edge_case_validation()
        pipeline_integrity = (
            skill_extraction.frequency_correct
            and scoring.match
            and graph.graph_valid
            and recommendation.recommendation_valid
        )
        performance = DiagnosticsPerformanceResponse(
            latency_ms=round(latency_ms, 2),
            performance_ok=latency_ms < 300,
        )

        checks = [
            pipeline_integrity,
            skill_extraction.frequency_correct,
            scoring.match,
            graph.graph_valid,
            recommendation.recommendation_valid,
            edge_cases == "pass",
            performance.performance_ok,
        ]

        return DiagnosticsReport(
            pipeline_integrity="pass" if pipeline_integrity else "fail",
            skill_extraction=skill_extraction,
            scoring=scoring,
            graph=graph,
            recommendation=recommendation,
            edge_cases=edge_cases,
            performance=performance,
            overall_status="pass" if all(checks) else "fail",
        )
    except Exception:
        logger.exception("[DIAGNOSTICS] Failed to run diagnostics")
        raise HTTPException(status_code=500, detail="Failed to run diagnostics")


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task_status(task_id: str) -> TaskStatusResponse:
    task_result = celery_app.AsyncResult(task_id)

    status_map = {
        "PENDING": "pending",
        "RECEIVED": "running",
        "STARTED": "started",
        "RETRY": "running",
        "SUCCESS": "success",
        "FAILURE": "failed",
    }
    normalized_status = status_map.get(task_result.status, task_result.status.lower())

    result = task_result.result if task_result.successful() else None
    if task_result.failed():
        result = {"error": str(task_result.result)}

    return TaskStatusResponse(task_id=task_id, status=normalized_status, result=result)


@app.get("/debug/pipeline/{raw_ingestion_id}")
def debug_pipeline(raw_ingestion_id: UUID, session: Session = Depends(get_session)) -> dict:
    raw_job = session.get(RawJobIngestion, raw_ingestion_id)
    if raw_job is None:
        raise HTTPException(status_code=404, detail="Raw job ingestion not found")

    job_posting = session.exec(
        select(JobPosting).where(JobPosting.raw_ingestion_id == raw_ingestion_id)
    ).first()
    job = session.get(Job, job_posting.job_id) if job_posting and job_posting.job_id else None

    skills: list[SkillRead] = []
    if job is not None:
        skill_statement = (
            select(Skill)
            .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
            .where(JobSkill.job_id == job.job_id)
            .order_by(Skill.name.asc())
        )
        skills = [SkillRead.model_validate(skill) for skill in session.exec(skill_statement)]

    return {
        "raw_job": raw_job.model_dump(),
        "job_posting": JobPostingRead.model_validate(job_posting).model_dump()
        if job_posting
        else None,
        "job": JobRead.model_validate(job).model_dump() if job else None,
        "skills": [skill.model_dump() for skill in skills],
    }


@app.get("/job-postings", response_model=list[JobPostingRead])
def list_job_postings(session: Session = Depends(get_session)) -> list[JobPostingRead]:
    statement = select(JobPosting).order_by(JobPosting.parsed_at.desc()).limit(10)
    return list(session.exec(statement))


@app.get("/jobs", response_model=list[JobRead])
def list_jobs(session: Session = Depends(get_session)) -> list[JobRead]:
    statement = select(Job).order_by(Job.job_last_seen_at.desc()).limit(10)
    return list(session.exec(statement))


@app.get("/skills", response_model=list[SkillRead])
def list_skills(session: Session = Depends(get_session)) -> list[SkillRead]:
    statement = select(Skill).order_by(Skill.name.asc()).limit(100)
    return list(session.exec(statement))


@app.get("/jobs/{job_id}/skills", response_model=list[SkillRead])
def list_job_skills(job_id: UUID, session: Session = Depends(get_session)) -> list[SkillRead]:
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    statement = (
        select(Skill)
        .join(JobSkill, JobSkill.skill_id == Skill.skill_id)
        .where(JobSkill.job_id == job_id)
        .order_by(Skill.name.asc())
    )
    return list(session.exec(statement))


@app.get("/analytics/top-skills", response_model=list[TopSkillResponse])
def analytics_top_skills(
    role: str, limit: int = 10, session: Session = Depends(get_session)
) -> list[TopSkillResponse]:
    return [TopSkillResponse(**item) for item in get_top_skills(session, role, limit)]


@app.post("/analytics/skill-gap", response_model=list[SkillGapResponse])
def analytics_skill_gap(
    payload: SkillGapRequest, session: Session = Depends(get_session)
) -> list[SkillGapResponse]:
    return [
        SkillGapResponse(**item)
        for item in get_skill_gap(session, payload.skills, payload.role)
    ]


@app.get("/analytics/skills", response_model=AnalyticsEnvelope)
def analytics_skills(
    location: str | None = None,
    limit: int = 10,
    refresh: bool = False,
    session: Session = Depends(get_session),
) -> AnalyticsEnvelope:
    if refresh:
        refresh_all_materialized_views()
    data = get_filtered_skill_analytics(session, location=location, limit=limit)
    return AnalyticsEnvelope(data=data, filters={"location": location, "limit": limit})


@app.get("/analytics/jobs", response_model=AnalyticsEnvelope)
def analytics_jobs(
    skill: str | None = None,
    location: str | None = None,
    limit: int = 10,
    refresh: bool = False,
    session: Session = Depends(get_session),
) -> AnalyticsEnvelope:
    if refresh:
        refresh_all_materialized_views()
    data = get_filtered_job_analytics(session, skill=skill, location=location, limit=limit)
    return AnalyticsEnvelope(
        data=data,
        filters={"skill": skill, "location": location, "limit": limit},
    )


@app.get("/analytics/locations", response_model=list[LocationAnalyticsResponse])
def analytics_locations(
    limit: int = 100, refresh: bool = False, session: Session = Depends(get_session)
) -> list[LocationAnalyticsResponse]:
    if refresh:
        refresh_all_materialized_views()
    return [
        LocationAnalyticsResponse(**item)
        for item in get_location_analytics(session, limit)
    ]


@app.get("/analytics/companies", response_model=list[CompanyAnalyticsResponse])
def analytics_companies(
    limit: int = 100, refresh: bool = False, session: Session = Depends(get_session)
) -> list[CompanyAnalyticsResponse]:
    if refresh:
        refresh_all_materialized_views()
    return [
        CompanyAnalyticsResponse(**item)
        for item in get_company_analytics(session, limit)
    ]


@app.get("/analytics/trends", response_model=AnalyticsEnvelope)
def analytics_trends(
    time_range: str = "30d",
    refresh: bool = False,
    session: Session = Depends(get_session),
) -> AnalyticsEnvelope:
    if time_range not in {"7d", "30d"}:
        raise HTTPException(status_code=400, detail="time_range must be one of: 7d, 30d")
    if refresh:
        refresh_all_materialized_views()
    data = get_trend_analytics(session, time_range=time_range)
    return AnalyticsEnvelope(data=data, filters={"time_range": time_range})
