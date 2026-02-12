"""Helpers to tame noisy asyncio loop errors (especially on Windows proactor)."""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def quiet_proactor(loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    """
    Install an exception handler that suppresses common benign connection-lost
    errors produced by the Windows proactor transport when sockets close abruptly.
    """
    loop = loop or asyncio.get_event_loop()
    default_handler = loop.get_exception_handler()

    def handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:  # type: ignore
        exc = context.get("exception")
        msg = context.get("message", "")
        if isinstance(exc, ConnectionResetError):
            return
        if isinstance(exc, OSError) and getattr(exc, "winerror", None) == 10054:
            return
        if "_ProactorBasePipeTransport._call_connection_lost" in msg:
            return
        if default_handler:
            default_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    loop.set_exception_handler(handler)
