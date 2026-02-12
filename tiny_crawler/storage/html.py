"""HTML storage to filesystem."""
import asyncio
from pathlib import Path
from typing import Any

from tiny_crawler.storage.base import StorageTarget
from tiny_crawler.utils.misc import ensure_dir, safe_filename


class HtmlStorage(StorageTarget):
    """Save HTML for each URL into base_dir/html/ as .html files."""

    def __init__(self, base_dir: str = "output", force_utf8: bool = True) -> None:
        self.base_dir = ensure_dir(Path(base_dir) / "html")
        self.force_utf8 = force_utf8

    async def write(self, data: Any, context) -> None:
        await asyncio.to_thread(self._write_sync, data, context)

    def _write_sync(self, data: Any, context) -> None:
        filename = safe_filename(context.url, suffix=".html")
        path = self.base_dir / filename
        text = data.decode("utf-8", errors="ignore") if isinstance(data, bytes) else str(data)
        if self.force_utf8:
            path.write_text(text, encoding="utf-8", errors="ignore")
        else:
            path.write_text(text)
