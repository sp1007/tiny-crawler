"""HTML storage to filesystem."""
import asyncio
from pathlib import Path
from typing import Any, Callable, Optional, Union

from tiny_crawler.crawler.pipeline import TaskContext
from tiny_crawler.storage.base import StorageTarget
from tiny_crawler.utils.misc import ensure_dir, safe_filename

HtmlPathBuilder = Callable[[TaskContext, Any], Union[str, Path]]


class HtmlStorage(StorageTarget):
    """Save HTML with optional custom relative path per item."""

    def __init__(
        self,
        base_dir: str = "output",
        force_utf8: bool = True,
        path_builder: Optional[HtmlPathBuilder] = None,
        include_default_subdir: bool = True,
    ) -> None:
        root = Path(base_dir) / "html" if include_default_subdir else Path(base_dir)
        self.base_dir = ensure_dir(root)
        self.force_utf8 = force_utf8
        self.path_builder = path_builder

    async def write(self, data: Any, context: TaskContext) -> None:
        await asyncio.to_thread(self._write_sync, data, context)

    def _write_sync(self, data: Any, context: TaskContext) -> None:
        path = self._resolve_path(context, data)
        text = data.decode("utf-8", errors="ignore") if isinstance(data, bytes) else str(data)
        if self.force_utf8:
            path.write_text(text, encoding="utf-8", errors="ignore")
        else:
            path.write_text(text)

    def _resolve_path(self, context: TaskContext, data: Any) -> Path:
        if self.path_builder:
            custom = self.path_builder(context, data)
            custom_path = Path(custom)
            path = custom_path if custom_path.is_absolute() else self.base_dir / custom_path
        else:
            filename = safe_filename(context.url, suffix=".html")
            path = self.base_dir / filename
        ensure_dir(path.parent)
        return path
