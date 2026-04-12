from __future__ import annotations

import logging

from client import send_raw_job, trigger_processing
from wellfound_scraper import scrape_wellfound_jobs


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    jobs = scrape_wellfound_jobs(limit=5)

    for job_payload in jobs:
        logger.info("[SCRAPER] Processing job: %s", job_payload["source_record_locator"])
        raw_ingestion_id = send_raw_job(job_payload)
        task_info = trigger_processing(raw_ingestion_id)
        logger.info(
            "[SCRAPER] Triggered processing for %s with task %s",
            raw_ingestion_id,
            task_info["task_id"],
        )


if __name__ == "__main__":
    main()
