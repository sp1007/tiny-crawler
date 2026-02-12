"""Async HTTP client with proxy rotation, retries, and UA rotation."""
import asyncio
import logging
import random
from typing import Mapping, Optional, Dict, Any, Tuple

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from aiohttp import ClientError

from tiny_crawler.proxy.manager import ProxyManager, ProxyLease
from tiny_crawler.utils.user_agents import USER_AGENTS
from tiny_crawler.utils.backoff import sleep_backoff
from tiny_crawler.http.socks_session import create_socks_session

logger = logging.getLogger(__name__)

try:
    from aiohttp_socks import ProxyError as AiohttpSocksProxyError
except Exception:  # pragma: no cover
    AiohttpSocksProxyError = None

try:
    from python_socks._errors import (
        ProxyError as SocksProxyError,
        ProxyConnectionError,
        ProxyTimeoutError,
        ProxyAuthenticationError,
    )

    SOCKS_EXCEPTIONS: Tuple[type, ...] = (
        SocksProxyError,
        ProxyConnectionError,
        ProxyTimeoutError,
        ProxyAuthenticationError,
    )
except Exception:
    SOCKS_EXCEPTIONS = ()

def build_headers(
    headers: Optional[Mapping[str, str]] = None,
    *,
    user_agent: Optional[str] = None,
) -> dict[str, str]:
    merged: dict[str, str] = {
        "User-Agent": user_agent or random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    if headers:
        # Preserve caller intent (their keys win).
        for k, v in headers.items():
            merged[str(k)] = str(v)
    return merged


class ProxyFetchError(RuntimeError):
    """Proxy-related fetch error that should not count toward retry."""

    def __init__(self, proxy_url: str, original: Exception) -> None:
        super().__init__(str(original))
        self.proxy_url = proxy_url
        self.original = original


class HttpClient:
    """Async HTTP client with retries, timeout, proxy and UA rotation."""

    def __init__(
        self,
        timeout: float = 15.0,
        retries: int = 3,
        proxy_manager: Optional[ProxyManager] = None,
        max_proxy_errors: int = 20,
        proxy_fallback_direct: bool = True,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.proxy_manager = proxy_manager
        self.max_proxy_errors = max_proxy_errors
        self.proxy_fallback_direct = proxy_fallback_direct
        self._default_session: Optional[ClientSession] = None
        self._socks_sessions: Dict[str, ClientSession] = {}

    async def _get_default_session(self) -> ClientSession:
        if not self._default_session or self._default_session.closed:
            self._default_session = ClientSession(timeout=ClientTimeout(total=self.timeout))
        return self._default_session

    async def _get_socks_session(self, proxy_url: str) -> ClientSession:
        session = self._socks_sessions.get(proxy_url)
        if session and not session.closed:
            return session
        session = await create_socks_session(proxy_url, timeout=self.timeout)
        self._socks_sessions[proxy_url] = session
        return session

    def _is_proxy_error(self, exc: Exception) -> bool:
        if isinstance(exc, SOCKS_EXCEPTIONS):
            return True
        if AiohttpSocksProxyError and isinstance(exc, AiohttpSocksProxyError):
            return True
        if isinstance(exc, (aiohttp.ClientProxyConnectionError, aiohttp.ClientHttpProxyError)):
            return True
        if isinstance(exc, (aiohttp.ClientPayloadError, asyncio.IncompleteReadError, ConnectionResetError)):
            return True
        if isinstance(exc, (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError)):
            return True
        if isinstance(exc, OSError) and getattr(exc, "winerror", None) == 10054:
            return True
        if isinstance(exc, asyncio.TimeoutError):
            return True
        msg = str(exc).lower()
        if "proxy" in msg or "socks" in msg:
            return True
        return False

    async def fetch(self, url: str, method: str = "GET", **kwargs: Any) -> str:
        """
        Fetch a URL and return UTF-8 decoded HTML/string.
        Retries with exponential backoff on failure.
        Proxy errors do not count toward retry.
        """
        last_exc: Optional[Exception] = None
        attempts = 0
        proxy_errors = 0
        while attempts < self.retries:
            try:
                return await self._fetch_once(url, method=method, use_proxy=True, **kwargs)
            except ProxyFetchError as exc:
                last_exc = exc
                proxy_errors += 1
                # No warning log for proxy errors
                if self.proxy_manager:
                    await self.proxy_manager.mark_bad(exc.proxy_url)
                if self.proxy_fallback_direct and proxy_errors >= self.max_proxy_errors:
                    try:
                        return await self._fetch_once(url, method=method, use_proxy=False, **kwargs)
                    except Exception as exc2:
                        if isinstance(exc2, asyncio.CancelledError):
                            raise
                        last_exc = exc2
                        attempts += 1
                        logger.warning("Fetch error (%s): %s (attempt %d/%d)", url, exc2, attempts, self.retries)
                        if attempts >= self.retries:
                            break
                        await sleep_backoff(attempts - 1)
                continue
            except Exception as exc:  # broad catch to retry network errors
                if isinstance(exc, asyncio.CancelledError):
                    raise
                last_exc = exc
                attempts += 1
                logger.warning("Fetch error (%s): %s (attempt %d/%d)", url, exc, attempts, self.retries)
                if attempts >= self.retries:
                    break
                await sleep_backoff(attempts - 1)
        assert last_exc
        raise last_exc

    async def _fetch_once(self, url: str, method: str = "GET", use_proxy: bool = True, **kwargs: Any) -> str:
        headers = dict(kwargs.pop("headers", {}) or {})
        # Alias support: allow "form" payload key for form-data posts
        if "form" in kwargs and "data" not in kwargs and "json" not in kwargs:
            kwargs["data"] = kwargs.pop("form")
        headers.setdefault("User-Agent", random.choice(USER_AGENTS))
        allow_http_error = bool(kwargs.pop("allow_http_error", False))
        proxy_lease: Optional[ProxyLease] = None
        proxy_url: Optional[str] = None

        if use_proxy and self.proxy_manager and self.proxy_manager.has_proxies:
            proxy_lease = await self.proxy_manager.acquire_proxy()
            proxy_url = proxy_lease.proxy

        try:
            if proxy_url and proxy_url.startswith(("socks4", "socks5")):
                session = await self._get_socks_session(proxy_url)
                async with proxy_lease:
                    async with session.request(method, url, headers=headers, **kwargs) as resp:
                        content = await resp.read()
                        if not allow_http_error:
                            resp.raise_for_status()
            else:
                session = await self._get_default_session()
                async with proxy_lease if proxy_lease else _null_async_context():
                    async with session.request(method, url, proxy=proxy_url, headers=headers, **kwargs) as resp:
                        content = await resp.read()
                        if not allow_http_error:
                            resp.raise_for_status()
        except Exception as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise
            if proxy_lease and proxy_url and (self._is_proxy_error(exc) or isinstance(exc, aiohttp.ClientError)):
                raise ProxyFetchError(proxy_url, exc) from exc
            if proxy_lease and self.proxy_manager and isinstance(exc, ClientError):
                await self.proxy_manager.mark_bad(proxy_lease.proxy)
            raise

        return content.decode("utf-8", errors="ignore")

    async def close(self) -> None:
        """Close all sessions."""
        if self._default_session and not self._default_session.closed:
            await self._default_session.close()
        for session in self._socks_sessions.values():
            if not session.closed:
                await session.close()
        self._socks_sessions.clear()


class _null_async_context:
    """No-op async context manager."""

    async def __aenter__(self):  # type: ignore
        return None

    async def __aexit__(self, exc_type, exc, tb):  # type: ignore
        return False


def build_headers(user_agent: Optional[str] = None, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Legacy helper kept for compatibility with older imports.
    """
    headers = {
        "User-Agent": user_agent or random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }
    if extra:
        headers.update(extra)
    return headers
