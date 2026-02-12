"""Proxy utilities."""
from .fetcher import fetch_free_proxies
from .checker import check_proxy_alive, filter_alive
from .manager import ProxyManager, save_live_proxies, load_proxies

__all__ = [
    "fetch_free_proxies",
    "check_proxy_alive",
    "filter_alive",
    "ProxyManager",
    "save_live_proxies",
    "load_proxies",
]
