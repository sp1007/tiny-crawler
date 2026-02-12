"""Proxy manager with single-connection locks and rotation."""
import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import List, Dict, Optional

from tiny_crawler.proxy import fetcher, checker

logger = logging.getLogger(__name__)


class ProxyLease:
    """Async context manager representing exclusive use of a proxy."""

    def __init__(self, proxy: str, thread_sem: threading.Semaphore, async_sem: asyncio.Semaphore) -> None:
        self.proxy = proxy
        self._thread_sem = thread_sem
        self._async_sem = async_sem

    async def __aenter__(self) -> str:
        return self.proxy

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._thread_sem.release()
        self._async_sem.release()


class ProxyManager:
    """Manage proxy pool, health, and single-connection locks."""

    def __init__(self, proxies: Optional[List[str]] = None) -> None:
        self._proxies: List[str] = []
        self._thread_sems: Dict[str, threading.Semaphore] = {}
        self._loop_sems: Dict[int, Dict[str, asyncio.Semaphore]] = {}
        self._rr_index = 0
        self._lock = threading.Lock()
        if proxies:
            self.set_proxies(proxies)

    @property
    def has_proxies(self) -> bool:
        return bool(self._proxies)

    @property
    def proxies(self) -> List[str]:
        return list(self._proxies)

    def set_proxies(self, proxies: List[str]) -> None:
        """Replace proxy list and reset locks."""
        unique = list(dict.fromkeys(proxies))
        with self._lock:
            self._proxies = unique
            self._thread_sems = {px: threading.Semaphore(1) for px in self._proxies}
            self._loop_sems = {}
            self._rr_index = 0

    @classmethod
    def from_file(cls, path: str) -> "ProxyManager":
        return cls(load_proxies(path))

    async def refresh_from_free_sources(self) -> None:
        """Fetch free proxies and replace current list."""
        self.set_proxies(await fetcher.fetch_free_proxies())

    async def prune_dead(self, concurrency: int = 200) -> None:
        """Remove dead proxies after async health check."""
        alive = await checker.filter_alive(self._proxies, concurrency=concurrency)
        self.set_proxies(alive)
        logger.info("Alive proxies: %d", len(self._proxies))

    async def acquire_proxy(self, timeout: float = 10.0) -> ProxyLease:
        """Round-robin pick and lock a proxy for exclusive use."""
        if not self._proxies:
            raise RuntimeError("Proxy pool empty")
        start = time.monotonic()
        while True:
            proxy = self._next_proxy()
            async_sem = self._get_async_sem(proxy)
            await async_sem.acquire()
            thread_sem = self._thread_sems[proxy]
            if thread_sem.acquire(blocking=False):
                return ProxyLease(proxy, thread_sem, async_sem)
            async_sem.release()
            if timeout and time.monotonic() - start > timeout:
                raise TimeoutError("No free proxy slots")
            await asyncio.sleep(0.02)

    def _next_proxy(self) -> str:
        with self._lock:
            proxy = self._proxies[self._rr_index % len(self._proxies)]
            self._rr_index += 1
            return proxy

    def _get_async_sem(self, proxy: str) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        with self._lock:
            if loop_id not in self._loop_sems:
                self._loop_sems[loop_id] = {px: asyncio.Semaphore(1) for px in self._proxies}
            if proxy not in self._loop_sems[loop_id]:
                self._loop_sems[loop_id][proxy] = asyncio.Semaphore(1)
            return self._loop_sems[loop_id][proxy]

    async def mark_bad(self, proxy: str) -> None:
        """Optional hook: demote or remove bad proxies."""
        logger.debug("Marking proxy as bad: %s", proxy)


def save_live_proxies(path: str, proxies: List[str]) -> None:
    Path(path).write_text("\n".join(proxies), encoding="utf-8")


def load_proxies(path: str) -> List[str]:
    file = Path(path)
    if not file.exists():
        return []
    return [line.strip() for line in file.read_text(encoding="utf-8").splitlines() if line.strip()]
