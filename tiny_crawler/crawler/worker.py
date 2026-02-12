"""Worker running an asyncio loop inside a ThreadPoolExecutor thread."""
import asyncio
import logging
import threading
from typing import List, Optional

from tiny_crawler.crawler.pipeline import PipelineStep, TaskContext, ParserResult
from tiny_crawler.crawler.tracker import TaskTracker
from tiny_crawler.http.client import HttpClient
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
        self.progress_cb = progress_cb
        self.step_progress_cb = step_progress_cb
        self.step_total_cb = step_total_cb
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.queue: Optional[asyncio.Queue] = None
        self._ready = threading.Event()
        self._shutdown_event: Optional[asyncio.Event] = None
        self._runner_tasks: List[asyncio.Task] = []

    def run(self) -> None:
        """Thread entry: create loop and run until shutdown."""
        self.loop = asyncio.new_event_loop()
        self.loop.set_exception_handler(self._handle_loop_exception)
        asyncio.set_event_loop(self.loop)
        self.queue = asyncio.Queue()
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
        assert self.queue is not None
        while True:
            try:
                task = await self.queue.get()
            except asyncio.CancelledError:
                break
            if task is None:
                self.queue.task_done()
                break
            try:
                await self.process_task(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Worker %s task error: %s", self.worker_id, exc)
            finally:
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

            # Enqueue next-step URLs
            next_index = task.step_index + 1
            if parser_result.next_urls and next_index < len(self.steps):
                assert self.queue is not None
                if self.step_total_cb:
                    self.step_total_cb(next_index, len(parser_result.next_urls))
                self.tracker.increment(len(parser_result.next_urls))
                for url in parser_result.next_urls:
                    await self.queue.put(
                        TaskContext(
                            url=url,
                            step_index=next_index,
                            root_id=task.root_id,
                            meta=dict(task.meta),
                            parent_url=task.url,
                        )
                    )

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

    async def fetch(self, step: PipelineStep, url: str, method: str = "GET", request_kwargs: Optional[dict] = None) -> str:
        """Use step-specific fetcher or default HTTP client."""
        request_kwargs = request_kwargs or {}
        if step.fetcher:
            return await step.fetcher(url)
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
