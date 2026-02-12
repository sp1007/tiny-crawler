"""Async proxy checker with progress bar."""
import asyncio
import logging
from typing import List
from urllib.parse import urlparse

import aiohttp
from aiohttp_socks import ProxyConnector
from tqdm.asyncio import tqdm

logger = logging.getLogger(__name__)


async def check_proxy_alive(proxy: str, test_url: str = "https://httpbin.org/ip", timeout: float = 6.0) -> bool:
    """Return True if proxy can reach test_url."""
    try:
        timeout_cfg = aiohttp.ClientTimeout(total=timeout)
        scheme = urlparse(proxy).scheme.lower()
        if scheme.startswith("socks"):
            connector = ProxyConnector.from_url(proxy)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
                async with session.get(test_url) as resp:
                    await resp.read()
                    return resp.status == 200
        async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
            async with session.get(test_url, proxy=proxy) as resp:
                await resp.read()
                return resp.status == 200
    except Exception:
        return False


async def filter_alive(proxies: List[str], concurrency: int = 200) -> List[str]:
    """Check proxies concurrently and return alive ones."""
    semaphore = asyncio.Semaphore(concurrency)
    alive: List[str] = []

    async def worker(px: str) -> None:
        async with semaphore:
            ok = await check_proxy_alive(px)
            if ok:
                alive.append(px)

    tasks = [asyncio.create_task(worker(px)) for px in proxies]
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="checking proxies"):
        await f
    return alive
