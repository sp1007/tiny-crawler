"""Fetch free proxies from public lists (best-effort)."""
import asyncio
import logging
import random
import re
from typing import List
import aiohttp
from bs4 import BeautifulSoup
import requests

from tiny_crawler.utils.user_agents import USER_AGENTS

logger = logging.getLogger(__name__)

# HTTP_SOURCES = [
#     "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
#     "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
# ]
# HTTPS_SOURCES = [
#     "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
# ]
# SOCKS4_SOURCES = [
#     "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
# ]
# SOCKS5_SOURCES = [
#     "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
# ]

PROXY_SOURCES = [
        "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text",
        "https://www.freeproxy.world/?type=&anonymity=&country=CN&page=1",
        "https://www.freeproxy.world/?type=&anonymity=&country=CN&page=2",
        "https://www.freeproxy.world/?type=&anonymity=&country=CN&page=3",
        "https://www.freeproxy.world/?type=&anonymity=&country=CN&page=4",
        "https://proxydb.net/?anonlvl=4&country=CN&offset=0",
        "https://proxydb.net/?anonlvl=4&country=CN&offset=30",
        "https://proxydb.net/?anonlvl=4&country=CN&offset=60",
        "https://proxydb.net/?anonlvl=4&country=CN&offset=90",
        "https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list/socks5.txt",
        "https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list/http.txt",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    ]


# def _load_list(url: str) -> List[str]:
#     try:
#         resp = requests.get(url, timeout=8)
#         resp.raise_for_status()
#         lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
#         return lines
#     except Exception as exc:
#         logger.warning("Proxy source failed %s: %s", url, exc)
#         return []


# def fetch_free_proxies() -> List[str]:
#     """Return a deduplicated list of proxies with scheme prefixes."""
#     proxies = set()
#     for url in HTTP_SOURCES:
#         for line in _load_list(url):
#             proxies.add(f"http://{line}")
#     for url in HTTPS_SOURCES:
#         for line in _load_list(url):
#             proxies.add(f"https://{line}")
#     for url in SOCKS4_SOURCES:
#         for line in _load_list(url):
#             proxies.add(f"socks4://{line}")
#     for url in SOCKS5_SOURCES:
#         for line in _load_list(url):
#             proxies.add(f"socks5://{line}")
#     return list(proxies)

async def fetch_free_proxies() -> list[str]:
    logger.info("Fetching proxies from %s sources...", len(PROXY_SOURCES))
    timeout = aiohttp.ClientTimeout(total=15, connect=15, sock_connect=15, sock_read=15)
    async with aiohttp.ClientSession(timeout=timeout, headers=_default_headers()) as session:
        tasks = [_fetch_from_source(session, source) for source in PROXY_SOURCES]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    all_proxies: list[str] = []
    for result in results:
        if isinstance(result, list):
            all_proxies.extend(result)
    # De-duplicate and drop obvious invalid entries.
    deduped = list(dict.fromkeys(p.strip() for p in all_proxies if p and ":" in p))
    logger.info("Fetched %s unique proxies", len(deduped))
    return deduped

async def _fetch_from_source(session: aiohttp.ClientSession, source: str) -> list[str]:
    try:
        async with session.get(source, ssl=True) as response:
            if response.status != 200:
                logger.debug("Proxy source HTTP %s: %s", response.status, source)
                return []
            content = await response.text(errors="ignore")
            return _parse_proxy_list(content, source)
    except Exception as e:
        logger.debug("Failed to fetch proxies from %s: %s", source, e)
        return []

def _parse_proxy_list(content: str, source: str) -> list[str]:
    proxies: list[str] = []
    if "api.proxyscrape.com" in source:
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            # The API may already include the protocol (e.g. http://ip:port).
            if "://" in line:
                proxies.append(line)
            elif ":" in line:
                proxies.append(f"http://{line}")
        return proxies
    if "cdn.jsdelivr.net/gh/databay-labs" in source:
        for line in content.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            if "socks5" in source:
                proxies.append(f"socks5://{line}")
            else:
                proxies.append(f"http://{line}")
        return proxies
    if "raw.githubusercontent.com" in source:
        for line in content.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            if "socks5.txt" in source:
                proxies.append(f"socks5://{line}")
            if "socks4.txt" in source:
                proxies.append(f"socks4://{line}")
            if "https.txt" in source:
                proxies.append(f"https://{line}")
            else:
                proxies.append(f"http://{line}")
        return proxies
    if "www.freeproxy.world" in source:
        return _parse_freeproxy_world(content)
    if "proxydb.net" in source:
        return _parse_proxydb_net(content)
    # Generic fallback: scrape IP:PORT patterns from HTML/text.
    try:
        soup = BeautifulSoup(content, "html.parser")
        text = soup.get_text(" ", strip=True)
    except Exception:
        text = content
    pattern = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d{2,5}\b")
    for match in pattern.findall(text):
        proxies.append(f"http://{match}")
    return proxies


def _parse_freeproxy_world(content: str) -> list[str]:
    proxies: list[str] = []
    try:
        soup = BeautifulSoup(content, "html.parser")
        for row in soup.select("div.table-container table tbody tr"):
            cols = row.select("td")
            # The site layout can change; this is best-effort.
            if len(cols) < 8:
                continue
            # Skip proxies that are explicitly marked as not alive/valid.
            if cols[6].get_text(strip=True).lower() == "no":
                continue
            ip = cols[0].get_text(strip=True)
            port = cols[1].get_text(strip=True)
            types = cols[5].select("a")
            if not ip or not port or not types:
                continue
            for t in types:
                protocol = t.get_text(strip=True).lower()
                if protocol in {"http", "https", "socks4", "socks5"}:
                    proxies.append(f"{protocol}://{ip}:{port}")
    except Exception as e:
        logger.debug("Failed to parse freeproxy.world: %s", e)
    return proxies


def _parse_proxydb_net(content: str) -> list[str]:
    proxies: list[str] = []
    try:
        soup = BeautifulSoup(content, "html.parser")
        for row in soup.select("div.table-responsive tbody tr"):
            cols = row.select("td")
            if len(cols) < 9:
                continue
            ip = cols[0].get_text(strip=True)
            port_el = cols[1].select_one("a")
            port = port_el.get_text(strip=True) if port_el else cols[1].get_text(strip=True)
            protocol = cols[2].get_text(strip=True).lower()
            if ip and port and protocol in {"http", "https", "socks4", "socks5"}:
                proxies.append(f"{protocol}://{ip}:{port}")
    except Exception as e:
        logger.debug("Failed to parse proxydb.net: %s", e)
    return proxies


def _default_headers() -> dict:
    """Return default headers with a random User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }
