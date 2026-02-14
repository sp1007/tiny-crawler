"""
Example: 3-step pipeline to discover and download TXT files.
Pipeline:
  Step 1: fetch list page -> collect detail URLs for step 2
  Step 2: fetch detail page -> collect TXT file URLs for step 3
  Step 3: fetch TXT content and store with RawStorage
Behavior:
  - Step 3 download will use proxy rotation if proxies.txt has proxies.
  - If no proxy (or proxy keeps failing), download falls back to direct.
"""
import os
import logging
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup  # type: ignore

from tiny_crawler import (
    CrawlerEngine,
    PipelineStep,
    ParserResult,
    TaskContext,
    BaseParser,
    RawStorage,
    ProxyManager,
    load_proxies,
)
from tiny_crawler.utils.logger import setup_logging
from tiny_crawler.utils.loop import quiet_proactor
from tiny_crawler.http.client import HttpClient


class EbookListParser(BaseParser):
    """Step 1: parse list page and emit detail URLs."""

    def __init__(self, max_items: int = 8) -> None:
        self.max_items = max_items

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        links = soup.select("ol li a[href^='/ebooks/']")
        urls: List[str] = []
        for a in links:
            href = a.get("href")
            if href:
                urls.append(urljoin(context.url, href))
        return ParserResult(next_urls=list(dict.fromkeys(urls))[: self.max_items])


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
    """Step 3: download TXT via proxy (if available) and return text."""

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
        storage_targets=[RawStorage(base_dir="output", force_utf8=True, suffix=".txt")],
        http_timeout=15,
        http_retries=3,
        per_worker_concurrency=12,
        show_step_progress=True,
    )

    start_urls = ["https://www.gutenberg.org/browse/scores/top"]
    engine.run(start_urls)


if __name__ == "__main__":
    main()
