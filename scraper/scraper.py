from __future__ import annotations

import logging
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel, Field


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


class JobRecord(BaseModel):
    title: str
    company: str
    location: str
    description: str
    source: str
    url: str


class ScrapeRequest(BaseModel):
    source: Literal["all", "wellfound", "company"] = "all"
    limit: int = Field(default=5, ge=1, le=20)


class ScrapeResponse(BaseModel):
    jobs: list[JobRecord]


app = FastAPI(title="HireGraph Scraper Service", version="1.0.0")


def fetch_wellfound_jobs(limit: int = 5) -> list[JobRecord]:
    jobs = [
        JobRecord(
            title="Backend Engineer",
            company="Northstar Labs",
            location="Bengaluru, India",
            description=(
                "Build Python and FastAPI services, design PostgreSQL-backed APIs, "
                "and improve Docker-based deployment workflows for a growing data platform."
            ),
            source="wellfound",
            url="https://wellfound.com/jobs/backend-engineer-northstar-labs",
        ),
        JobRecord(
            title="Data Scientist",
            company="BlueOrbit AI",
            location="Hyderabad, India",
            description=(
                "Develop machine learning models, analyze product telemetry, and work "
                "with Python, SQL, and cloud tooling to ship data products."
            ),
            source="wellfound",
            url="https://wellfound.com/jobs/data-scientist-blueorbit-ai",
        ),
        JobRecord(
            title="Frontend Developer",
            company="LumenStack",
            location="Remote",
            description=(
                "Create modern React interfaces, collaborate closely with product and "
                "design, and ship clean dashboard experiences for analytics customers."
            ),
            source="wellfound",
            url="https://wellfound.com/jobs/frontend-developer-lumenstack",
        ),
    ]
    return jobs[:limit]


def fetch_company_jobs(limit: int = 5) -> list[JobRecord]:
    jobs = [
        JobRecord(
            title="Platform Engineer",
            company="Atlas Cloud Systems",
            location="Pune, India",
            description=(
                "Own internal platform tooling, improve Kubernetes operations, and "
                "support scalable backend services across multiple product teams."
            ),
            source="company-careers",
            url="https://careers.atlascloudsystems.com/platform-engineer",
        ),
        JobRecord(
            title="Analytics Engineer",
            company="Crest Data Works",
            location="Chennai, India",
            description=(
                "Model business data, build reliable transformation pipelines, and "
                "deliver trusted analytics datasets for internal decision-making."
            ),
            source="company-careers",
            url="https://jobs.crestdataworks.com/analytics-engineer",
        ),
    ]
    return jobs[:limit]


def fetch_jobs(source: str, limit: int) -> list[JobRecord]:
    if source == "wellfound":
        return fetch_wellfound_jobs(limit=limit)
    if source == "company":
        return fetch_company_jobs(limit=limit)

    jobs = fetch_wellfound_jobs(limit=limit)
    remaining = max(limit - len(jobs), 0)
    if remaining > 0:
        jobs.extend(fetch_company_jobs(limit=remaining))
    return jobs[:limit]


@app.post("/scrape", response_model=ScrapeResponse)
def scrape_jobs(payload: ScrapeRequest) -> ScrapeResponse:
    logger.info("[SCRAPER] Fetching jobs")
    jobs = fetch_jobs(source=payload.source, limit=payload.limit)
    logger.info("[SCRAPER] Returning %s jobs", len(jobs))
    return ScrapeResponse(jobs=jobs)
