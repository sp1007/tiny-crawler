"""Base parser interface."""
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tiny_crawler.crawler.pipeline import ParserResult, TaskContext


class BaseParser(ABC):
    """Implement parse to transform HTML into data / next URLs."""

    @abstractmethod
    async def parse(self, html: str, context: "TaskContext") -> "ParserResult":
        ...
