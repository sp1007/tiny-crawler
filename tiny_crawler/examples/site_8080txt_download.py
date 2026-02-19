"""
Example: download TXT novels from 8080txt.com in 3 steps.
Pipeline:
  Step 1: entry URL (/category/txt{id}.html) -> find download-page URL
  Step 2: download page -> find direct .txt link (down.8080txt.com or down.txt8080.com)
  Step 3: fetch .txt content and save to:
          data/site_8080txtcom/<category>/<id>_<txt_file_name>.txt
Modes:
  - Default: run pipeline from given step-1 URLs.
  - --crawl-all-pages: while parsing step 1, discovered same-domain URLs are pushed
    back into queue of step 1 (same-step crawling).
Proxy:
  - If proxies.txt exists and has proxies, engine uses proxy rotation.
  - If no proxy file (or empty), crawl continues without proxy.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import threading
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup  # type: ignore

from tiny_crawler import (
    BaseParser,
    CrawlerEngine,
    NextTask,
    ParserResult,
    PipelineStep,
    ProxyManager,
    RawStorage,
    TaskContext,
    load_proxies,
)
from tiny_crawler.utils.logger import setup_logging
from tiny_crawler.utils.loop import quiet_proactor

DEFAULT_START_URL = "https://www.8080txt.com/xuanhuan/txt18374.html"
TARGET_SITE_DIR = "site_8080txtcom"
VALID_DOWNLOAD_HOSTS = {"down.8080txt.com", "down.txt8080.com"}
STEP1_URL_RE = re.compile(r"/(?P<category>[^/]+)/txt(?P<book_id>\d+)\.html$", re.IGNORECASE)
INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
DISCOVERY_SKIP_SUFFIXES = {
    ".7z",
    ".avi",
    ".bmp",
    ".css",
    ".csv",
    ".doc",
    ".docx",
    ".exe",
    ".gif",
    ".gz",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".m3u8",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".rar",
    ".svg",
    ".tar",
    ".tgz",
    ".webp",
    ".xls",
    ".xlsx",
    ".xml",
    ".zip",
}


def sanitize_part(value: object, fallback: str = "unknown") -> str:
    cleaned = INVALID_PATH_CHARS.sub("_", str(value)).strip(" .")
    return cleaned or fallback


def normalize_txt_name(value: object) -> str:
    name = sanitize_part(unquote(str(value)), fallback="unknown.txt")
    if not name.lower().endswith(".txt"):
        name = f"{name}.txt"
    return name


def parse_entry_url(url: str) -> tuple[str, str]:
    match = STEP1_URL_RE.search(urlparse(url).path)
    if not match:
        return "unknown", "unknown"
    return match.group("category"), match.group("book_id")


def normalize_crawl_url(url: str) -> Optional[str]:
    raw = (url or "").strip()
    if not raw:
        return None

    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None

    normalized = parsed._replace(fragment="")
    if not normalized.path:
        normalized = normalized._replace(path="/")
    return normalized.geturl()


def is_entry_url(url: str) -> bool:
    return STEP1_URL_RE.search(urlparse(url).path) is not None


def is_probably_html_page(url: str) -> bool:
    path = urlparse(url).path.lower()
    if not path or path.endswith("/"):
        return True
    suffix = Path(path).suffix.lower()
    return suffix not in DISCOVERY_SKIP_SUFFIXES


class Step1EntryParser(BaseParser):
    """Step 1: parse entry page, optionally discover same-domain URLs in same step."""

    def __init__(self, crawl_all_pages: bool = False, max_pages: int = 2000) -> None:
        self.crawl_all_pages = crawl_all_pages
        self.max_pages = max(1, max_pages)
        self._lock = threading.Lock()
        self._seen_urls: set[str] = set()
        self._domain_counts: dict[str, int] = {}

    def _mark_processed(self, url: str) -> None:
        normalized = normalize_crawl_url(url)
        if not normalized:
            return
        host = urlparse(normalized).netloc.lower()
        with self._lock:
            if normalized in self._seen_urls:
                return
            self._seen_urls.add(normalized)
            self._domain_counts[host] = self._domain_counts.get(host, 0) + 1

    def _accept_discovered(self, url: str) -> bool:
        normalized = normalize_crawl_url(url)
        if not normalized:
            return False

        host = urlparse(normalized).netloc.lower()
        with self._lock:
            if normalized in self._seen_urls:
                return False
            if self._domain_counts.get(host, 0) >= self.max_pages:
                return False
            self._seen_urls.add(normalized)
            self._domain_counts[host] = self._domain_counts.get(host, 0) + 1
        return True

    def _discover_same_step_urls(self, soup: BeautifulSoup, current_url: str) -> list[NextTask]:
        if not self.crawl_all_pages:
            return []

        current_host = urlparse(current_url).netloc.lower()
        same_step_tasks: list[NextTask] = []

        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            normalized = normalize_crawl_url(urljoin(current_url, href))
            if not normalized:
                continue
            parsed = urlparse(normalized)
            if parsed.netloc.lower() != current_host:
                continue
            if not is_probably_html_page(normalized):
                continue
            if not self._accept_discovered(normalized):
                continue
            same_step_tasks.append(NextTask(url=normalized))

        return same_step_tasks

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        self._mark_processed(context.url)
        soup = BeautifulSoup(html, "html.parser")

        same_step_tasks = self._discover_same_step_urls(soup, context.url)

        if not is_entry_url(context.url):
            return ParserResult(same_step_tasks=same_step_tasks)

        category, book_id = parse_entry_url(context.url)
        download_page_url: Optional[str] = None
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            text = a.get_text(" ", strip=True)
            if "\u8fdb\u5165\u5c0f\u8bf4\u4e0b\u8f7d\u5730\u5740" in text or re.search(
                r"/down/txt[^\"']+\.html$", href, re.IGNORECASE
            ):
                download_page_url = urljoin(context.url, href)
                break

        if not download_page_url:
            return ParserResult(same_step_tasks=same_step_tasks)

        return ParserResult(
            next_tasks=[
                NextTask(
                    url=download_page_url,
                    meta={
                        "category": sanitize_part(category),
                        "book_id": sanitize_part(book_id),
                    },
                )
            ],
            same_step_tasks=same_step_tasks,
        )


class Step2DownloadPageParser(BaseParser):
    """Step 2: parse download page and emit direct TXT file URL."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        best_txt_url: Optional[str] = None
        best_txt_name: Optional[str] = None

        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            abs_url = urljoin(context.url, href)
            parsed = urlparse(abs_url)
            if parsed.netloc.lower() not in VALID_DOWNLOAD_HOSTS:
                continue
            href_lower = href.lower()
            abs_lower = abs_url.lower()
            if ".txt" not in href_lower and ".txt" not in abs_lower:
                continue

            raw_name = self._extract_txt_filename(href) or self._extract_txt_filename(abs_url)
            txt_name = normalize_txt_name(raw_name or a.get("download") or "unknown.txt")
            best_txt_url = abs_url
            best_txt_name = txt_name
            break

        if not best_txt_url or not best_txt_name:
            return ParserResult()

        return ParserResult(
            next_tasks=[
                NextTask(
                    url=best_txt_url,
                    meta={
                        "txt_name": best_txt_name,
                    },
                )
            ]
        )

    @staticmethod
    def _extract_txt_filename(url: str) -> str:
        decoded = unquote(url).split("#", 1)[0]
        match = re.search(r"/([^/]+?\.txt)(?:$|[?#])", decoded, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
        else:
            name = decoded.rsplit("/", 1)[-1].strip()
        lower = name.lower()
        if ".txt" in lower and not lower.endswith(".txt"):
            name = name[: lower.find(".txt") + 4]
        return name


class Step3TxtParser(BaseParser):
    """Step 3: keep fetched TXT content for storage."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        return ParserResult(data=html)


def build_output_path(context: TaskContext, data: object) -> Path:
    _ = data
    category = sanitize_part(context.meta.get("category", "unknown"))
    book_id = sanitize_part(context.meta.get("book_id", "unknown"))
    txt_name = normalize_txt_name(context.meta.get("txt_name", "unknown.txt"))
    filename = f"{book_id}_{txt_name}"
    return Path(TARGET_SITE_DIR) / category / filename


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download txt files from 8080txt.com using 3-step crawler pipeline."
    )
    parser.add_argument(
        "start_urls",
        nargs="*",
        default=[DEFAULT_START_URL],
        help="Step-1 seed URLs.",
    )
    parser.add_argument(
        "--crawl-all-pages",
        action="store_true",
        help="Discover same-domain URLs in step 1 and enqueue them back to step 1.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2000,
        help="Maximum discovered pages per domain for step-1 same-step crawling.",
    )
    parser.add_argument("--proxy-file", default="proxies.txt", help="Path to proxy list (http:// or socks://).")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(logging.INFO)
    quiet_proactor()
    logger = logging.getLogger(__name__)

    proxy_manager = ProxyManager()
    if os.path.exists(args.proxy_file):
        proxies = load_proxies(args.proxy_file)
        if proxies:
            proxy_manager.set_proxies(proxies)
            logger.info("Loaded %d proxies from %s", len(proxies), args.proxy_file)
        else:
            logger.warning("Proxy file %s is empty. Continue without proxy.", args.proxy_file)
    else:
        logger.info("Proxy file %s not found. Continue without proxy.", args.proxy_file)

    if args.crawl_all_pages:
        logger.info("Same-step crawl enabled in step 1 (max_pages_per_domain=%d)", max(1, args.max_pages))

    steps = [
        PipelineStep(
            name="entry",
            parser=Step1EntryParser(
                crawl_all_pages=args.crawl_all_pages,
                max_pages=args.max_pages,
            ),
        ),
        PipelineStep(name="download-page", parser=Step2DownloadPageParser()),
        PipelineStep(name="txt", parser=Step3TxtParser()),
    ]

    engine = CrawlerEngine(
        steps=steps,
        workers=args.workers,
        storage_targets=[
            RawStorage(
                base_dir="data",
                suffix=".txt",
                path_builder=build_output_path,
                include_default_subdir=False,
            )
        ],
        proxy_manager=proxy_manager,
        http_timeout=20,
        http_retries=3,
        per_worker_concurrency=10,
        prioritize_next_step=True,
        show_step_progress=True,
    )

    engine.run(args.start_urls)


if __name__ == "__main__":
    main()
