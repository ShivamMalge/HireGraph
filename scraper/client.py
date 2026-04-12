from __future__ import annotations

import logging
import os
from typing import Any

import requests


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

BACKEND_BASE_URL = os.getenv("HIREGRAPH_BACKEND_URL", "http://localhost:8000")


def send_raw_job(data: dict[str, Any]) -> str:
    response = requests.post(
        f"{BACKEND_BASE_URL}/raw-jobs",
        json=data,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    raw_ingestion_id = payload["raw_ingestion_id"]
    logger.info("[SCRAPER] Sent to pipeline %s", raw_ingestion_id)
    return raw_ingestion_id


def trigger_processing(raw_ingestion_id: str) -> dict[str, Any]:
    response = requests.post(
        f"{BACKEND_BASE_URL}/process-raw/{raw_ingestion_id}",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()
