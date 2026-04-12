import logging

from ..models.job_posting import JobPostingCreate
from ..models.raw_job import RawJobIngestion


PARSE_VERSION = "v1"
logger = logging.getLogger(__name__)


def parse_raw_job(raw: RawJobIngestion) -> JobPostingCreate:
    payload = (raw.raw_payload or "").strip()

    if not payload:
        logger.warning("[PARSE] Empty payload for raw ingestion %s", raw.raw_ingestion_id)
        return JobPostingCreate(
            raw_ingestion_id=raw.raw_ingestion_id,
            posting_observed_at=raw.observed_at,
            posted_at=None,
            title_raw="",
            description_raw="",
            company_name_raw="",
            location_raw="",
            employment_type_raw=None,
            salary_raw=None,
            posting_status="failed",
            parse_status="failed",
            parse_version=PARSE_VERSION,
            field_completeness=0.0,
        )

    lines = [line.strip() for line in payload.splitlines() if line.strip()]
    title = lines[0] if lines else "Untitled Job"

    company_name = "Unknown Company"
    location = "Unknown Location"

    for line in lines[1:]:
        lower_line = line.lower()
        if lower_line.startswith("company:"):
            company_name = line.split(":", 1)[1].strip() or company_name
        elif lower_line.startswith("location:"):
            location = line.split(":", 1)[1].strip() or location

    populated_fields = [
        title,
        payload,
        company_name if company_name != "Unknown Company" else "",
        location if location != "Unknown Location" else "",
    ]
    field_completeness = sum(bool(value) for value in populated_fields) / 4
    logger.info("[PARSE] Extracted title: %s", title)
    logger.info("[PARSE] Extracted company: %s", company_name)
    logger.info("[PARSE] Extracted location: %s", location)

    return JobPostingCreate(
        raw_ingestion_id=raw.raw_ingestion_id,
        posting_observed_at=raw.observed_at,
        posted_at=None,
        title_raw=title,
        description_raw=payload,
        company_name_raw=company_name,
        location_raw=location,
        employment_type_raw=None,
        salary_raw=None,
        posting_status="active",
        parse_status="success",
        parse_version=PARSE_VERSION,
        field_completeness=field_completeness,
    )
