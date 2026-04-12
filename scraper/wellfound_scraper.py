from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
import json
import logging
from pathlib import Path
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import Browser, Page, sync_playwright


WELLFOUND_JOBS_URL = "https://wellfound.com/jobs"
FALLBACK_JOBS_URL = "https://remoteok.com/remote-dev-jobs"
CACHE_PATH = Path(__file__).with_name("processed_urls.json")
logger = logging.getLogger(__name__)


@dataclass
class ScrapedJob:
    job_url: str
    html_content: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_processed_urls() -> set[str]:
    if not CACHE_PATH.exists():
        return set()

    try:
        return set(json.loads(CACHE_PATH.read_text(encoding="utf-8")))
    except Exception:
        return set()


def save_processed_urls(processed_urls: set[str]) -> None:
    CACHE_PATH.write_text(json.dumps(sorted(processed_urls), indent=2), encoding="utf-8")


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _extract_first_text(soup: BeautifulSoup, selectors: list[str]) -> str:
    for selector in selectors:
        element = soup.select_one(selector)
        if element:
            text = _normalize_text(element.get_text(" ", strip=True))
            if text:
                return text
    return ""


def _clean_html_text(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    lines = [_normalize_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def _extract_description(soup: BeautifulSoup) -> str:
    selectors = [
        "main",
        "article",
        "[data-test='JobDescription']",
        "[class*='description']",
        "[class*='job-description']",
    ]
    for selector in selectors:
        section = soup.select_one(selector)
        if section:
            for tag in section(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = _normalize_text(section.get_text("\n", strip=True))
            if text:
                return text
    return _clean_html_text(str(soup))


def _build_clean_text(scraped_job: ScrapedJob) -> str:
    soup = BeautifulSoup(scraped_job.html_content, "html.parser")

    title = _extract_first_text(
        soup,
        ["h1", "[data-test='job-title']", "[class*='title']"],
    ) or "Unknown Title"
    company = _extract_first_text(
        soup,
        [
            "[data-test='company-name']",
            "[class*='company']",
            "a[href*='/company']",
        ],
    ) or "Unknown Company"
    location = _extract_first_text(
        soup,
        [
            "[data-test='job-location']",
            "[class*='location']",
            "span[title]",
        ],
    ) or "Unknown Location"
    description = _extract_description(soup)

    clean_text = "\n".join(
        [
            f"Title: {title}",
            f"Company: {company}",
            f"Location: {location}",
            "",
            "Description:",
            description,
        ]
    ).strip()

    logger.info("[SCRAPER] Extracted clean text")
    return clean_text


def _new_page(browser: Browser) -> Page:
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    )
    page = context.new_page()
    page.set_default_timeout(30000)
    return page


def _extract_listing_urls(page: Page, limit: int) -> list[str]:
    selectors = [
        "a[href*='/jobs/']",
        "a[href*='/jobs']",
    ]
    job_links: list[str] = []
    seen: set[str] = set()

    for selector in selectors:
        anchors = page.locator(selector)
        count = anchors.count()

        for index in range(count):
            href = anchors.nth(index).get_attribute("href")
            if not href:
                continue

            absolute_url = urljoin(WELLFOUND_JOBS_URL, href.strip())
            if "/jobs/" not in absolute_url:
                continue
            if absolute_url in seen:
                logger.info("[SCRAPER] Skipping duplicate URL %s", absolute_url)
                continue

            seen.add(absolute_url)
            job_links.append(absolute_url)
            if len(job_links) >= limit:
                return job_links

    return job_links


def fetch_listing_urls(browser: Browser, limit: int = 10) -> list[str]:
    logger.info("[SCRAPER] Fetching jobs")
    page = _new_page(browser)
    page.goto(WELLFOUND_JOBS_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(3000)
    job_links = _extract_listing_urls(page, limit=limit)
    page.context.close()
    return job_links


def fetch_job_page(browser: Browser, job_url: str) -> ScrapedJob | None:
    logger.info("[SCRAPER] Processing job URL %s", job_url)
    page = _new_page(browser)

    try:
        page.goto(job_url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        html_content = page.content()
        if not html_content.strip():
            logger.warning("[SCRAPER] Empty HTML for %s", job_url)
            return None
        return ScrapedJob(job_url=job_url, html_content=html_content)
    except Exception as exc:
        logger.warning("[SCRAPER] Failed to fetch %s: %s", job_url, exc)
        return None
    finally:
        page.context.close()


def build_raw_job_payload(scraped_job: ScrapedJob) -> dict[str, str]:
    timestamp = _utc_now_iso()
    cleaned_text = _build_clean_text(scraped_job)
    return {
        "source_id": "wellfound",
        "source_record_locator": scraped_job.job_url,
        "payload_format": "text",
        "raw_payload": cleaned_text,
        "ingestion_status": "success",
        "observed_at": timestamp,
        "fetched_at": timestamp,
    }


def _fallback_jobs(limit: int = 5) -> list[dict[str, str]]:
    logger.warning("[SCRAPER] Falling back to RemoteOK")
    response = requests.get(
        FALLBACK_JOBS_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
        timeout=30,
    )
    response.raise_for_status()

    html = response.text
    if not html.strip():
        return []

    jobs: list[dict[str, str]] = []
    chunks = html.split("<tr class=\"job")
    for index, chunk in enumerate(chunks[1 : limit + 1], start=1):
        cleaned_text = _clean_html_text(chunk)
        jobs.append(
            {
                "source_id": "wellfound",
                "source_record_locator": f"{FALLBACK_JOBS_URL}#job-{index}",
                "payload_format": "text",
                "raw_payload": cleaned_text or f"Description:\n{escape(chunk)}",
                "ingestion_status": "success",
                "observed_at": _utc_now_iso(),
                "fetched_at": _utc_now_iso(),
            }
        )
    return jobs


def scrape_wellfound_jobs(limit: int = 10) -> list[dict[str, str]]:
    processed_urls = load_processed_urls()
    payloads: list[dict[str, str]] = []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                for job_url in fetch_listing_urls(browser, limit=limit):
                    if job_url in processed_urls:
                        logger.info("[SCRAPER] Already processed URL %s", job_url)
                        continue

                    scraped_job = fetch_job_page(browser, job_url)
                    if scraped_job is None:
                        continue

                    payloads.append(build_raw_job_payload(scraped_job))
                    processed_urls.add(job_url)
            finally:
                browser.close()
    except Exception as exc:
        logger.warning("[SCRAPER] Wellfound scraping failed: %s", exc)

    save_processed_urls(processed_urls)
    if payloads:
        return payloads[:limit]
    return _fallback_jobs(limit=min(limit, 5))
