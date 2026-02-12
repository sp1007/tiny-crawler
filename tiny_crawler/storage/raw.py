"""Raw file storage to filesystem."""
import asyncio
from pathlib import Path
from typing import Any

from tiny_crawler.storage.base import StorageTarget
from tiny_crawler.utils.misc import ensure_dir, safe_filename


class RawStorage(StorageTarget):
    """Save raw bytes or text. Each URL becomes one file under base_dir/raw/."""

    def __init__(self, base_dir: str = "output", force_utf8: bool = True, suffix: str = ".raw") -> None:
        self.base_dir = ensure_dir(Path(base_dir) / "raw")
        self.force_utf8 = force_utf8
        self.suffix = suffix

    async def write(self, data: Any, context) -> None:
        await asyncio.to_thread(self._write_sync, data, context)

    def _write_sync(self, data: Any, context) -> None:
        filename = safe_filename(context.url, suffix=self.suffix)
        path = self.base_dir / filename
        if isinstance(data, bytes):
            if self.force_utf8:
                text = data.decode("utf-8", errors="ignore")
                path.write_text(text, encoding="utf-8", errors="ignore")
            else:
                path.write_bytes(data)
        else:
            text = str(data)
            if self.force_utf8:
                path.write_text(text, encoding="utf-8", errors="ignore")
            else:
                path.write_text(text)
