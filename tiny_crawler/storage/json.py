"""JSON / JSONL storage."""
import asyncio
import json
import threading
from pathlib import Path
from typing import Any, Union, Iterable

from tiny_crawler.storage.base import StorageTarget
from tiny_crawler.utils.misc import ensure_dir


class JSONStorage(StorageTarget):
    """Accumulate items and write a JSON array on close()."""

    def __init__(self, file_path: Union[str, Path] = "output/data.json") -> None:
        self.path = Path(file_path)
        ensure_dir(self.path.parent)
        self._items = []
        self._lock = threading.Lock()

    async def write(self, data: Any, context) -> None:
        with self._lock:
            if isinstance(data, Iterable) and not isinstance(data, (dict, str, bytes)):
                self._items.extend(list(data))
            else:
                self._items.append(data)

    def close(self) -> None:
        with self._lock:
            payload = json.dumps(self._items, ensure_ascii=False, indent=2)
        self.path.write_text(payload, encoding="utf-8")


class JSONLStorage(StorageTarget):
    """Append records to a JSONL file."""

    def __init__(self, file_path: Union[str, Path] = "output/data.jsonl") -> None:
        self.path = Path(file_path)
        ensure_dir(self.path.parent)
        self._lock = threading.Lock()

    async def write(self, data: Any, context) -> None:
        await asyncio.to_thread(self._write_sync, data)

    def _write_sync(self, data: Any) -> None:
        if isinstance(data, Iterable) and not isinstance(data, (dict, str, bytes)):
            items = data
        else:
            items = [data]
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                for item in items:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
