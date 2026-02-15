"""Pipeline entities: step, result, task context."""
from dataclasses import dataclass, field
from typing import Any, List, Dict, Callable, Optional, Awaitable, Union

from tiny_crawler.parser.base import BaseParser


@dataclass
class NextTask:
    """A next-step task with per-url metadata."""
    url: str
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParserResult:
    """Result returned by parser."""
    data: Any = None
    next_urls: List[str] = field(default_factory=list)
    # Use next_tasks when each emitted URL needs its own metadata.
    next_tasks: List[NextTask] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskContext:
    """Carries contextual information through steps."""
    url: str
    step_index: int
    root_id: str
    meta: Dict[str, Any] = field(default_factory=dict)
    parent_url: Optional[str] = None


class PipelineStep:
    """Single pipeline step composed of a parser and optional fetcher."""

    def __init__(
        self,
        name: str,
        parser: BaseParser,
        fetcher: Optional[Callable[[str], Awaitable[Union[str, bytes]]]] = None,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self.parser = parser
        self.fetcher = fetcher
        self.method = method.upper()
        self.headers = headers or {}
