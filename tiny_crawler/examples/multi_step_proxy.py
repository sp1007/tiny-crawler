"""
Example: multi-step crawl with proxy auto-fetch + check.
Behavior:
  - If proxies.txt exists: load & use
  - Else: fetch free proxies -> check alive -> save -> use
  - If none alive: proceed without proxy
Pipeline:
  Step 1: fetch homepage -> collect tag URLs
  Step 2: fetch tag page -> collect author detail URLs
  Step 3: fetch author page -> extract author info -> save to JSONL
"""
import asyncio
import logging
import os
from typing import List

from bs4 import BeautifulSoup  # type: ignore

from tiny_crawler import (
    CrawlerEngine,
    PipelineStep,
    ParserResult,
    TaskContext,
    BaseParser,
    ProxyManager,
    JSONLStorage,
)
from tiny_crawler.proxy import fetch_free_proxies, filter_alive, save_live_proxies, load_proxies
from tiny_crawler.utils.logger import setup_logging
from tiny_crawler.utils.loop import quiet_proactor


class TagListParser(BaseParser):
    """Parse homepage to collect tag URLs."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        tags = soup.select(".tag-item a")
        urls = []
        for a in tags:
            href = a.get("href")
            if not href:
                continue
            if href.startswith("http"):
                urls.append(href)
            else:
                urls.append(f"https://quotes.toscrape.com{href}")
        return ParserResult(next_urls=list(dict.fromkeys(urls)))


class TagPageParser(BaseParser):
    """Parse a tag page to collect author URLs."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        author_links = soup.select(".quote span a")
        urls = []
        for a in author_links:
            href = a.get("href")
            if not href:
                continue
            if href.startswith("http"):
                urls.append(href)
            else:
                urls.append(f"https://quotes.toscrape.com{href}")
        return ParserResult(next_urls=list(dict.fromkeys(urls)))


class AuthorDetailParser(BaseParser):
    """Extract author details from author page."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        name = soup.select_one("h3.author-title")
        born = soup.select_one(".author-born-date")
        location = soup.select_one(".author-born-location")
        description = soup.select_one(".author-description")
        data = {
            "name": name.get_text(strip=True) if name else None,
            "born_date": born.get_text(strip=True) if born else None,
            "born_location": location.get_text(strip=True) if location else None,
            "bio": description.get_text(strip=True) if description else None,
            "source": context.url,
        }
        return ParserResult(data=data)


async def prepare_proxies(proxy_file: str, concurrency: int = 200) -> ProxyManager:
    """Load proxies from file or fetch+check when missing."""
    logger = logging.getLogger(__name__)
    manager = ProxyManager()

    if os.path.exists(proxy_file):
        proxies = load_proxies(proxy_file)
        if proxies:
            logger.info("Loaded %d proxies from %s", len(proxies), proxy_file)
            manager.set_proxies(proxies)
            return manager

    logger.info("No proxy file found. Fetching free proxies...")
    proxies = await fetch_free_proxies()
    if not proxies:
        logger.warning("No proxies fetched. Continue without proxy.")
        return manager

    logger.info("Checking %d proxies...", len(proxies))
    alive = await filter_alive(proxies, concurrency=concurrency)
    if not alive:
        logger.warning("No alive proxies. Continue without proxy.")
        return manager

    save_live_proxies(proxy_file, alive)
    logger.info("Saved %d alive proxies to %s", len(alive), proxy_file)
    manager.set_proxies(alive)
    return manager


async def main() -> None:
    setup_logging(logging.INFO)
    quiet_proactor(asyncio.get_event_loop())

    proxy_manager = await prepare_proxies("proxies.txt", concurrency=300)

    jsonl_storage = JSONLStorage("output/authors.jsonl")

    steps = [
        PipelineStep(name="tags", parser=TagListParser()),
        PipelineStep(name="tag-page", parser=TagPageParser()),
        PipelineStep(name="author", parser=AuthorDetailParser()),
    ]

    engine = CrawlerEngine(
        steps=steps,
        workers=8,
        storage_targets=[jsonl_storage],
        proxy_manager=proxy_manager,
        http_timeout=12,
        http_retries=3,
        per_worker_concurrency=16,
        show_step_progress=True,
    )

    start_urls: List[str] = ["https://quotes.toscrape.com/"]
    engine.run(start_urls)


if __name__ == "__main__":
    asyncio.run(main())
