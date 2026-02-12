"""MongoDB Atlas storage via pymongo."""
import asyncio
import logging
import threading
from typing import Any, Iterable, List

from pymongo import MongoClient, errors as mongo_errors

from tiny_crawler.storage.base import StorageTarget
from tiny_crawler.crawler.pipeline import TaskContext


class MongoStorage(StorageTarget):
    """Insert documents into MongoDB in batches."""

    def __init__(
        self,
        uri: str,
        database: str,
        collection: str,
        batch_size: int = 100,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._disabled = False
        self._error_logged = False
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.collection = self.client[database][collection]
        self.batch_size = batch_size
        self._buffer: List[Any] = []
        self._lock = threading.Lock()

    async def write(self, data: Any, context: TaskContext) -> None:
        await asyncio.to_thread(self._write_sync, data)

    def _write_sync(self, data: Any) -> None:
        if self._disabled:
            return
        try:
            with self._lock:
                if isinstance(data, Iterable) and not isinstance(data, dict):
                    self._buffer.extend(list(data))
                else:
                    self._buffer.append(data)

                if len(self._buffer) >= self.batch_size:
                    self.collection.insert_many(self._buffer)
                    self._buffer.clear()
        except mongo_errors.PyMongoError as exc:
            self._disabled = True
            if not self._error_logged:
                self._error_logged = True
                self._logger.error("Mongo disabled due to error: %s", exc)

    def close(self) -> None:
        if not self._disabled:
            try:
                with self._lock:
                    if self._buffer:
                        self.collection.insert_many(self._buffer)
                        self._buffer.clear()
            except mongo_errors.PyMongoError as exc:
                if not self._error_logged:
                    self._error_logged = True
                    self._logger.error("Mongo disabled due to error: %s", exc)
        self.client.close()
