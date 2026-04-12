import logging

from sqlmodel import Session

from ..models.job import Job
from ..models.job_posting import JobPosting
from ..models.raw_job import utc_now
from .dedup import find_similar_job


CANONICALIZATION_VERSION = "v1"
logger = logging.getLogger(__name__)


def canonicalize_posting(posting: JobPosting, session: Session) -> Job:
    matched_job = find_similar_job(posting, session)

    if matched_job is not None:
        logger.info(
            "[DEDUP] Matched posting %s to existing job %s",
            posting.job_posting_id,
            matched_job.job_id,
        )
        matched_job.job_last_seen_at = max(
            matched_job.job_last_seen_at, posting.posting_observed_at
        )
        matched_job.canonicalized_at = utc_now()
        matched_job.job_status = posting.posting_status
        posting.job_id = matched_job.job_id
        session.add(matched_job)
        session.add(posting)
        return matched_job

    logger.info("[DEDUP] Creating new canonical job for posting %s", posting.job_posting_id)
    new_job = Job(
        canonical_title=posting.title_raw,
        company_id=None,
        canonical_location=posting.location_raw,
        job_status=posting.posting_status,
        job_confidence=1.0,
        canonicalization_version=CANONICALIZATION_VERSION,
        job_first_seen_at=posting.posting_observed_at,
        job_last_seen_at=posting.posting_observed_at,
    )
    session.add(new_job)
    session.flush()

    posting.job_id = new_job.job_id
    session.add(posting)
    return new_job
