"""Exponential backoff utilities."""
import random
import asyncio


async def sleep_backoff(attempt: int, base: float = 0.5, cap: float = 8.0) -> None:
    """Sleep with jittered exponential backoff based on attempt index."""
    delay = min(cap, base * (2 ** attempt))
    delay *= random.uniform(0.8, 1.2)
    await asyncio.sleep(delay)
