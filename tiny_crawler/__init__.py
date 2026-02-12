"""
Tiny Crawler package entry point.
Provides convenient imports for public API.
"""
from .crawler.engine import CrawlerEngine
from .crawler.pipeline import PipelineStep, ParserResult, TaskContext
from .parser.base import BaseParser
from .proxy.manager import ProxyManager
from .proxy.fetcher import fetch_free_proxies
from .proxy.checker import check_proxy_alive
from .proxy.manager import save_live_proxies, load_proxies
from .storage.raw import RawStorage
from .storage.html import HtmlStorage
from .storage.json import JSONStorage, JSONLStorage
from .storage.mongo import MongoStorage

__all__ = [
    "CrawlerEngine",
    "PipelineStep",
    "ParserResult",
    "TaskContext",
    "BaseParser",
    "ProxyManager",
    "fetch_free_proxies",
    "check_proxy_alive",
    "save_live_proxies",
    "load_proxies",
    "RawStorage",
    "HtmlStorage",
    "JSONStorage",
    "JSONLStorage",
    "MongoStorage",
]
