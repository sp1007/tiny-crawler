"""Async proxy checker with progress bar."""
import asyncio
import logging
from typing import List, Optional
from urllib.parse import urlparse

import aiohttp
from aiohttp_socks import ProxyConnector
from tqdm.asyncio import tqdm

logger = logging.getLogger(__name__)


def _new_http_connector(limit: int = 100) -> aiohttp.TCPConnector:
    """
    Build a connector that aggressively closes sockets to avoid leaked SSL transports.
    """
    return aiohttp.TCPConnector(
        limit=limit,
        force_close=True,
        enable_cleanup_closed=True,
    )


async def check_proxy_alive(
    proxy: str,
    test_url: str = "https://httpbin.org/ip",
    timeout: float = 6.0,
    http_session: Optional[aiohttp.ClientSession] = None,
) -> bool:
    """Return True if proxy can reach test_url."""
    try:
        timeout_cfg = aiohttp.ClientTimeout(total=timeout)
        scheme = urlparse(proxy).scheme.lower()
        if scheme.startswith("socks"):
            connector = ProxyConnector.from_url(
                proxy,
                force_close=True,
                enable_cleanup_closed=True,
            )
            async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
                async with session.get(test_url) as resp:
                    await resp.read()
                    return resp.status == 200
        if http_session is not None:
            async with http_session.get(test_url, proxy=proxy) as resp:
                await resp.read()
                return resp.status == 200
        async with aiohttp.ClientSession(connector=_new_http_connector(limit=1), timeout=timeout_cfg) as session:
            async with session.get(test_url, proxy=proxy) as resp:
                await resp.read()
                return resp.status == 200
    except Exception:
        return False


async def filter_alive(
    proxies: List[str],
    concurrency: int = 200,
    test_url: str = "https://httpbin.org/ip",
    timeout: float = 6.0,
) -> List[str]:
    """Check proxies concurrently and return alive ones."""
    semaphore = asyncio.Semaphore(concurrency)
    alive: List[str] = []

    connector = _new_http_connector(limit=max(1, concurrency))
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as shared_http_session:

        async def worker(px: str) -> None:
            async with semaphore:
                ok = await check_proxy_alive(
                    px,
                    test_url=test_url,
                    timeout=timeout,
                    http_session=shared_http_session,
                )
                if ok:
                    alive.append(px)

        tasks = [asyncio.create_task(worker(px)) for px in proxies]
        try:
            for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="checking proxies"):
                await future
        finally:
            # Ensure no task keeps an open response/session reference.
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    # Give SSL transports a moment to finalize before outer loop shutdown.
    await asyncio.sleep(0.25)
    return alive
