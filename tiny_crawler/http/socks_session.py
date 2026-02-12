"""Factory for SOCKS-enabled aiohttp sessions."""
from aiohttp import ClientSession, ClientTimeout
from aiohttp_socks import ProxyConnector


async def create_socks_session(proxy_url: str, timeout: float) -> ClientSession:
    """
    Create a dedicated aiohttp session bound to a SOCKS proxy.
    Each SOCKS proxy must keep its own session.
    """
    connector = ProxyConnector.from_url(proxy_url)
    return ClientSession(connector=connector, timeout=ClientTimeout(total=timeout))
