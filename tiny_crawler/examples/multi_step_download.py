"""
Example: 3-step pipeline to discover and download TXT files.
Pipeline:
  Step 1: fetch list page -> collect detail URLs + category for step 2
  Step 2: fetch detail page -> collect TXT file URLs for step 3
  Step 3: fetch TXT content and save as data/<site>/<category>/<file>.txt
Behavior:
  - Step 3 download will use proxy rotation if proxies.txt has proxies.
  - If no proxy (or proxy keeps failing), download falls back to direct.
"""
import os
import logging
import re
from pathlib import Path
from typing import List
from urllib.parse import urljoin, urlparse, unquote

from bs4 import BeautifulSoup  # type: ignore

from tiny_crawler import (
    CrawlerEngine,
    PipelineStep,
    NextTask,
    ParserResult,
    TaskContext,
    BaseParser,
    RawStorage,
    ProxyManager,
    load_proxies,
)
from tiny_crawler.utils.logger import setup_logging
from tiny_crawler.utils.loop import quiet_proactor
from tiny_crawler.utils.misc import safe_filename
from tiny_crawler.http.client import HttpClient


class EbookListParser(BaseParser):
    """Step 1: parse list page and emit detail URLs with category meta."""

    def __init__(self, max_items: int = 8) -> None:
        self.max_items = max_items

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        tasks: List[NextTask] = []
        seen_urls = set()

        # Gutenberg top page groups links by heading + <ol>.
        for heading in soup.select("h2"):
            category = heading.get_text(" ", strip=True)
            if not category:
                continue
            listing = heading.find_next_sibling("ol")
            if not listing:
                continue
            for a in listing.select("li a[href^='/ebooks/']"):
                href = a.get("href")
                if not href:
                    continue
                detail_url = urljoin(context.url, href)
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                tasks.append(NextTask(url=detail_url, meta={"category": category}))
                if len(tasks) >= self.max_items:
                    return ParserResult(next_tasks=tasks)

        # Fallback for pages without the heading/list structure.
        if not tasks:
            for a in soup.select("ol li a[href^='/ebooks/']"):
                href = a.get("href")
                if not href:
                    continue
                detail_url = urljoin(context.url, href)
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                tasks.append(NextTask(url=detail_url, meta={"category": "unknown"}))
                if len(tasks) >= self.max_items:
                    break

        return ParserResult(next_tasks=tasks)


class EbookDetailParser(BaseParser):
    """Step 2: parse detail page and emit TXT file URL."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        txt_candidates: List[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            if ".txt" not in href.lower():
                continue
            txt_candidates.append(urljoin(context.url, href))

        best = self._pick_best_txt_url(txt_candidates)
        if not best:
            return ParserResult()
        return ParserResult(next_urls=[best])

    @staticmethod
    def _pick_best_txt_url(candidates: List[str]) -> str:
        best_url = ""
        best_score = -1
        for url in candidates:
            lower = url.lower()
            score = 1
            if ".txt.utf-8" in lower:
                score = 3
            elif lower.endswith(".txt"):
                score = 2
            if score > best_score:
                best_url = url
                best_score = score
        return best_url


class TextFileParser(BaseParser):
    """Step 3: download TXT via proxy (if available) and return raw text."""

    def __init__(
        self,
        proxy_manager: ProxyManager,
        timeout: float = 8.0,
        retries: int = 2,
        max_proxy_errors: int = 2,
    ) -> None:
        self.proxy_manager = proxy_manager
        self.timeout = timeout
        self.retries = retries
        self.max_proxy_errors = max_proxy_errors

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        _ = html
        client = HttpClient(
            timeout=self.timeout,
            retries=self.retries,
            proxy_manager=self.proxy_manager,
            max_proxy_errors=self.max_proxy_errors,
            proxy_fallback_direct=True,
        )
        try:
            text = await client.fetch(context.url)
            return ParserResult(data=text)
        finally:
            await client.close()


async def skip_fetcher(url: str) -> str:
    """Step 3 parser handles download itself."""
    _ = url
    return ""


INVALID_PATH_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def sanitize_part(value: str, fallback: str = "unknown") -> str:
    part = INVALID_PATH_CHARS.sub("_", value).strip(" .")
    return part or fallback


def build_text_path(context: TaskContext, data: object) -> Path:
    _ = data
    parsed = urlparse(context.url)
    site = sanitize_part(parsed.netloc or "unknown_site")
    category = sanitize_part(str(context.meta.get("category", "unknown")))

    raw_name = Path(unquote(parsed.path)).name
    file_name = sanitize_part(raw_name, fallback="")
    if not file_name:
        file_name = safe_filename(context.url, suffix=".txt")

    return Path(site) / category / file_name


def main() -> None:
    setup_logging(logging.INFO)
    quiet_proactor()
    logger = logging.getLogger(__name__)

    proxy_manager = ProxyManager()
    proxy_file = "proxies.txt"
    if os.path.exists(proxy_file):
        proxies = load_proxies(proxy_file)
        if proxies:
            proxy_manager.set_proxies(proxies)
            logger.info("Loaded %d proxies from %s", len(proxies), proxy_file)
        else:
            logger.warning("Proxy file %s is empty. Continue without proxy.", proxy_file)
    else:
        logger.info("No proxy file found. Continue without proxy.")

    steps = [
        PipelineStep(name="list", parser=EbookListParser(max_items=8)),
        PipelineStep(name="detail", parser=EbookDetailParser()),
        PipelineStep(name="download", parser=TextFileParser(proxy_manager=proxy_manager), fetcher=skip_fetcher),
    ]

    engine = CrawlerEngine(
        steps=steps,
        workers=6,
        storage_targets=[
            RawStorage(
                base_dir="data",
                suffix=".txt",
                path_builder=build_text_path,
                include_default_subdir=False,
            )
        ],
        http_timeout=15,
        http_retries=3,
        per_worker_concurrency=12,
        show_step_progress=True,
    )

    start_urls = ["https://www.gutenberg.org/browse/scores/top"]
    engine.run(start_urls)


if __name__ == "__main__":
    main()
