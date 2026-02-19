"""Worker running an asyncio loop inside a ThreadPoolExecutor thread."""
import asyncio
import logging
import threading
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from tiny_crawler.crawler.pipeline import PipelineStep, TaskContext, ParserResult, NextTask
from tiny_crawler.crawler.tracker import TaskTracker
from tiny_crawler.http.client import HttpClient, decode_html_content
from tiny_crawler.storage.base import StorageTarget

logger = logging.getLogger(__name__)


class Worker:
    """A worker owns its own asyncio loop and processes a queue of tasks."""

    def __init__(
        self,
        worker_id: int,
        steps: List[PipelineStep],
        storage_targets: List[StorageTarget],
        tracker: TaskTracker,
        proxy_manager=None,
        http_timeout: float = 15.0,
        http_retries: int = 3,
        per_worker_concurrency: int = 32,
        prioritize_next_step: bool = False,
        progress_cb=None,
        step_progress_cb=None,
        step_total_cb=None,
    ) -> None:
        self.worker_id = worker_id
        self.steps = steps
        self.storage_targets = storage_targets
        self.tracker = tracker
        self.proxy_manager = proxy_manager
        self.http_timeout = http_timeout
        self.http_retries = http_retries
        self.per_worker_concurrency = per_worker_concurrency
        self.prioritize_next_step = prioritize_next_step
        self.progress_cb = progress_cb
        self.step_progress_cb = step_progress_cb
        self.step_total_cb = step_total_cb
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.queue: Optional[asyncio.Queue] = None
        self.priority_queue: Optional[asyncio.Queue] = None
        self._ready = threading.Event()
        self._shutdown_event: Optional[asyncio.Event] = None
        self._runner_tasks: List[asyncio.Task] = []

    def run(self) -> None:
        """Thread entry: create loop and run until shutdown."""
        self.loop = asyncio.new_event_loop()
        self.loop.set_exception_handler(self._handle_loop_exception)
        asyncio.set_event_loop(self.loop)
        self.queue = asyncio.Queue()
        self.priority_queue = asyncio.Queue() if self.prioritize_next_step else None
        self._shutdown_event = asyncio.Event()
        self._ready.set()
        try:
            self.loop.run_until_complete(self._run_loop())
        finally:
            self.loop.close()

    def wait_ready(self, timeout: Optional[float] = None) -> bool:
        return self._ready.wait(timeout=timeout)

    async def _run_loop(self) -> None:
        self.http = HttpClient(
            timeout=self.http_timeout,
            retries=self.http_retries,
            proxy_manager=self.proxy_manager,
        )
        self._runner_tasks = [asyncio.create_task(self._runner()) for _ in range(self.per_worker_concurrency)]
        assert self._shutdown_event is not None
        await self._shutdown_event.wait()
        # Stop runners cleanly
        assert self.queue is not None
        for _ in self._runner_tasks:
            await self.queue.put(None)
        await asyncio.gather(*self._runner_tasks, return_exceptions=True)
        await self.http.close()

    async def _runner(self) -> None:
        while True:
            source: Literal["normal", "priority"] = "normal"
            try:
                task, source = await self._get_task()
            except asyncio.CancelledError:
                break
            if task is None:
                self._task_done(source)
                break
            try:
                await self.process_task(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Worker %s task error: %s", self.worker_id, exc)
            finally:
                self._task_done(source)

    async def _get_task(self) -> Tuple[Optional[TaskContext], Literal["normal", "priority"]]:
        assert self.queue is not None
        if self.prioritize_next_step and self.priority_queue is not None:
            try:
                return self.priority_queue.get_nowait(), "priority"
            except asyncio.QueueEmpty:
                pass
        return await self.queue.get(), "normal"

    def _task_done(self, source: Literal["normal", "priority"]) -> None:
        if source == "priority":
            if self.priority_queue is not None:
                self.priority_queue.task_done()
            return
        if self.queue is not None:
            self.queue.task_done()

    async def process_task(self, task: TaskContext) -> None:
        try:
            step = self.steps[task.step_index]
            req_meta = task.meta.get("request", {}) if isinstance(task.meta, dict) else {}
            req_kwargs = dict(req_meta) if isinstance(req_meta, dict) else {}
            method = req_kwargs.pop("method", step.method if hasattr(step, "method") else "GET")
            # Merge headers: step.headers (defaults) < request.headers (override)
            step_headers = getattr(step, "headers", {}) or {}
            req_headers = req_kwargs.pop("headers", {}) or {}
            merged_headers = dict(step_headers)
            merged_headers.update(req_headers)
            if merged_headers:
                req_kwargs["headers"] = merged_headers
            html = await self.fetch(step, task.url, method=method, request_kwargs=req_kwargs)
            parser_result: ParserResult = await step.parser.parse(html, task)
            task.meta.update(parser_result.meta)

            # Enqueue same-step URLs (discover new URLs while staying in current step).
            same_step_tasks = self._collect_tasks(
                parser_result.same_step_urls,
                parser_result.same_step_tasks,
                label="same-step",
            )
            if same_step_tasks:
                if self.step_total_cb:
                    self.step_total_cb(task.step_index, len(same_step_tasks))
                self.tracker.increment(len(same_step_tasks))
                for url, same_meta in same_step_tasks:
                    merged_meta = dict(task.meta)
                    merged_meta.update(same_meta)
                    same_ctx = TaskContext(
                        url=url,
                        step_index=task.step_index,
                        root_id=task.root_id,
                        meta=merged_meta,
                        parent_url=task.url,
                    )
                    assert self.queue is not None
                    await self.queue.put(same_ctx)

            # Enqueue next-step URLs
            next_index = task.step_index + 1
            next_tasks = self._collect_tasks(
                parser_result.next_urls,
                parser_result.next_tasks,
                label="next-step",
            )
            if next_tasks and next_index < len(self.steps):
                if self.step_total_cb:
                    self.step_total_cb(next_index, len(next_tasks))
                self.tracker.increment(len(next_tasks))
                for url, next_meta in next_tasks:
                    merged_meta = dict(task.meta)
                    merged_meta.update(next_meta)
                    next_ctx = TaskContext(
                        url=url,
                        step_index=next_index,
                        root_id=task.root_id,
                        meta=merged_meta,
                        parent_url=task.url,
                    )
                    if self.prioritize_next_step and self.priority_queue is not None:
                        await self.priority_queue.put(next_ctx)
                    else:
                        assert self.queue is not None
                        await self.queue.put(next_ctx)

            # Persist data if present
            if parser_result.data is not None:
                for storage in self.storage_targets:
                    await storage.write(parser_result.data, task)
        finally:
            if self.progress_cb:
                self.progress_cb()
            if self.step_progress_cb:
                try:
                    self.step_progress_cb(task.step_index)
                except TypeError:
                    self.step_progress_cb()
            self.tracker.decrement(1)

    @staticmethod
    def _normalize_html_for_parser(payload: Union[str, bytes]) -> str:
        """Normalize custom fetcher payloads before handing to parser."""
        if isinstance(payload, bytes):
            return decode_html_content(payload)
        return decode_html_content(payload.encode("utf-8", errors="replace"), response_charset="utf-8")

    @staticmethod
    def _collect_tasks(
        urls: List[str],
        tasks: List[NextTask],
        *,
        label: str,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Normalize URL/task outputs into one list."""
        merged: List[Tuple[str, Dict[str, Any]]] = []

        for url in urls:
            if not isinstance(url, str) or not url:
                logger.warning("Skip invalid %s URL: %r", label, url)
                continue
            merged.append((url, {}))

        for item in tasks:
            if not isinstance(item, NextTask):
                logger.warning("Skip invalid %s task: %r", label, item)
                continue
            if not isinstance(item.url, str) or not item.url:
                logger.warning("Skip %s task with invalid URL: %r", label, item.url)
                continue
            if item.meta is None:
                task_meta: Dict[str, Any] = {}
            elif isinstance(item.meta, dict):
                task_meta = item.meta
            else:
                logger.warning("Skip %s task with invalid meta for URL %s", label, item.url)
                continue
            merged.append((item.url, task_meta))

        return merged

    async def fetch(self, step: PipelineStep, url: str, method: str = "GET", request_kwargs: Optional[dict] = None) -> str:
        """Use step-specific fetcher or default HTTP client."""
        request_kwargs = request_kwargs or {}
        if step.fetcher:
            payload = await step.fetcher(url)
            return self._normalize_html_for_parser(payload)
        return await self.http.fetch(url, method=method, **request_kwargs)

    def submit(self, ctx: TaskContext) -> None:
        """Thread-safe submission of a task."""
        assert self.loop and self.queue
        self.tracker.increment(1)
        asyncio.run_coroutine_threadsafe(self.queue.put(ctx), self.loop)

    def shutdown(self) -> None:
        """Request graceful shutdown of this worker."""
        if self.loop and self._shutdown_event:
            def _cancel() -> None:
                self._shutdown_event.set()
                for task in self._runner_tasks:
                    task.cancel()

            self.loop.call_soon_threadsafe(_cancel)

    def _handle_loop_exception(self, loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        msg = context.get("message", "")
        if isinstance(exc, ConnectionResetError):
            return
        if isinstance(exc, OSError) and getattr(exc, "winerror", None) == 10054:
            return
        if "_ProactorBasePipeTransport._call_connection_lost" in msg:
            return
        loop.default_exception_handler(context)
