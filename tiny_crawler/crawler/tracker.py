"""Task counter used to coordinate completion across threads."""
import threading
from typing import Callable, Optional


class TaskTracker:
    """Thread-safe task counter with completion event."""

    def __init__(self, on_add: Optional[Callable[[int], None]] = None) -> None:
        self._count = 0
        self._closed = False
        self._lock = threading.Lock()
        self._done_event = threading.Event()
        self._on_add = on_add

    def increment(self, n: int = 1) -> None:
        with self._lock:
            self._count += n
            self._done_event.clear()
        if self._on_add:
            self._on_add(n)

    def decrement(self, n: int = 1) -> None:
        with self._lock:
            self._count -= n
            if self._closed and self._count == 0:
                self._done_event.set()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._count == 0:
                self._done_event.set()

    def wait_done(self, timeout: Optional[float] = None) -> bool:
        return self._done_event.wait(timeout=timeout)
