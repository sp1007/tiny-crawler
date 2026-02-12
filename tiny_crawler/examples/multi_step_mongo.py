"""
Example: multi-step crawl with proxy rotation and MongoDB storage.
Pipeline:
  Step 1: fetch homepage -> collect tag URLs
  Step 2: fetch tag page -> collect author detail URLs
  Step 3: fetch author page -> extract author info -> save to JSONL + Mongo
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
    MongoStorage,
)
from tiny_crawler.proxy.manager import load_proxies
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


def main() -> None:
    setup_logging(logging.INFO)
    quiet_proactor()
    logger = logging.getLogger(__name__)

    # Prepare proxy pool (optional but shown)
    proxy_manager = ProxyManager()
    proxy_file = "proxies.txt"
    if os.path.exists(proxy_file):
        proxy_manager.set_proxies(load_proxies(proxy_file))
    else:
        # Uncomment to fetch & filter live proxies (can take time):
        # asyncio.run(proxy_manager.refresh_from_free_sources())
        # asyncio.run(proxy_manager.prune_dead(concurrency=300))
        pass

    # Storage targets
    jsonl_storage = JSONLStorage("output/authors.jsonl")
    storage_targets = [jsonl_storage]

    mongo_uri = os.environ.get("MONGO_URI")
    mongo_storage = None
    if mongo_uri:
        mongo_storage = MongoStorage(uri=mongo_uri, database="tinycrawler", collection="authors", batch_size=50)
        storage_targets.append(mongo_storage)
    else:
        logger.warning("MONGO_URI not set, skipping MongoStorage")

    steps = [
        PipelineStep(name="tags", parser=TagListParser()),
        PipelineStep(name="tag-page", parser=TagPageParser()),
        PipelineStep(name="author", parser=AuthorDetailParser()),
    ]

    engine = CrawlerEngine(
        steps=steps,
        workers=8,
        storage_targets=storage_targets,
        proxy_manager=proxy_manager,
        http_timeout=12,
        http_retries=3,
        per_worker_concurrency=16,
    )

    start_urls: List[str] = ["https://quotes.toscrape.com/"]
    engine.run(start_urls)

    if mongo_storage:
        mongo_storage.close()


if __name__ == "__main__":
    main()
