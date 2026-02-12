"""Base protocol for storage targets."""
from abc import ABC, abstractmethod
from typing import Any
from tiny_crawler.crawler.pipeline import TaskContext


class StorageTarget(ABC):
    """A storage target consumes parsed data items."""

    @abstractmethod
    async def write(self, data: Any, context: TaskContext) -> None:
        ...

    def close(self) -> None:
        """Optional cleanup hook."""
        return None
