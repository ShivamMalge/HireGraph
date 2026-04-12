import logging
import hashlib
import re

from sqlmodel import Session, select

from ..models.job import Job
from ..models.job_posting import JobPosting

logger = logging.getLogger(__name__)


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def compute_simhash(text: str) -> int:
    tokens = _tokenize(text)
    if not tokens:
        return 0

    vector = [0] * 64
    for token in tokens:
        token_hash = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
        for bit in range(64):
            if token_hash & (1 << bit):
                vector[bit] += 1
            else:
                vector[bit] -= 1

    fingerprint = 0
    for bit, value in enumerate(vector):
        if value > 0:
            fingerprint |= 1 << bit
    return fingerprint


def _hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def _posting_text(posting: JobPosting) -> str:
    return f"{posting.title_raw} {posting.location_raw}".strip()


def _job_text(job: Job) -> str:
    return f"{job.canonical_title} {job.canonical_location}".strip()


def find_similar_job(posting: JobPosting, session: Session) -> Job | None:
    posting_text = _posting_text(posting)
    posting_hash = compute_simhash(posting_text)

    if posting_hash == 0:
        logger.info("[DEDUP] Empty similarity signature for posting %s", posting.job_posting_id)
        return None

    statement = select(Job)
    for job in session.exec(statement):
        job_hash = compute_simhash(_job_text(job))
        distance = _hamming_distance(posting_hash, job_hash)
        if distance <= 8:
            logger.info(
                "[DEDUP] Similar job found for posting %s with distance %s",
                posting.job_posting_id,
                distance,
            )
            return job

    logger.info("[DEDUP] No similar job found for posting %s", posting.job_posting_id)
    return None
