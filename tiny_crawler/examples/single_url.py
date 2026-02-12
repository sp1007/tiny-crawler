"""Simple single-URL crawl example."""
import logging

from bs4 import BeautifulSoup  # type: ignore

from tiny_crawler import CrawlerEngine, PipelineStep, ParserResult, TaskContext, BaseParser, JSONLStorage
from tiny_crawler.utils.logger import setup_logging


class TitleParser(BaseParser):
    """Extract page title."""

    async def parse(self, html: str, context: TaskContext) -> ParserResult:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else None
        return ParserResult(data={"url": context.url, "title": title})


def main() -> None:
    setup_logging(logging.INFO)

    steps = [PipelineStep(name="title", parser=TitleParser())]
    engine = CrawlerEngine(steps=steps, workers=4, storage_targets=[JSONLStorage("output/titles.jsonl")])
    engine.run(["https://example.org/"])


if __name__ == "__main__":
    main()
