"""Engine orchestrating workers and pipelines."""
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from tqdm import tqdm

from tiny_crawler.crawler.pipeline import PipelineStep, TaskContext
from tiny_crawler.crawler.worker import Worker
from tiny_crawler.crawler.tracker import TaskTracker
from tiny_crawler.storage.base import StorageTarget

logger = logging.getLogger(__name__)


class CrawlerEngine:
    """
    High-level orchestrator running worker threads.
    Params:
      steps: ordered list of PipelineStep defining the crawl pipeline.
      workers: number of worker threads (each owns its own asyncio loop).
      storage_targets: list of StorageTarget backends to persist parser data.
      proxy_manager: ProxyManager instance for rotation/locking (optional).
      http_timeout: total timeout (seconds) for each HTTP request.
      http_retries: number of retries for non-proxy errors per request.
      per_worker_concurrency: async tasks per worker event loop.
      prioritize_next_step: if True, next-step tasks are pushed to a high-priority queue.
      show_step_progress: if True, show tqdm per pipeline step.
    """

    def __init__(
        self,
        steps: List[PipelineStep],
        workers: int = 8,
        storage_targets: Optional[List[StorageTarget]] = None,
        proxy_manager=None,
        http_timeout: float = 15.0,
        http_retries: int = 3,
        per_worker_concurrency: int = 32,
        prioritize_next_step: bool = False,
        show_step_progress: bool = False,
    ) -> None:
        self.steps = steps
        self.workers_num = workers
        self.storage_targets = storage_targets or []
        self.proxy_manager = proxy_manager
        self.http_timeout = http_timeout
        self.http_retries = http_retries
        self.per_worker_concurrency = per_worker_concurrency
        self.prioritize_next_step = prioritize_next_step
        self.show_step_progress = show_step_progress
        self._workers: List[Worker] = []
        self._executor: Optional[ThreadPoolExecutor] = None
        self._pbar: Optional[tqdm] = None
        self._step_pbars: List[tqdm] = []
        self._pbar_lock = threading.RLock()

    def _progress_tick(self) -> None:
        if self._pbar is not None:
            with self._pbar_lock:
                self._pbar.update(1)

    def _progress_add_total(self, n: int) -> None:
        if self._pbar is not None:
            with self._pbar_lock:
                self._pbar.total = (self._pbar.total or 0) + n
                self._pbar.refresh()

    def _step_progress_tick(self, step_index: int) -> None:
        if self._step_pbars and 0 <= step_index < len(self._step_pbars):
            with self._pbar_lock:
                self._step_pbars[step_index].update(1)

    def _step_add_total(self, step_index: int, n: int) -> None:
        if self._step_pbars and 0 <= step_index < len(self._step_pbars):
            with self._pbar_lock:
                pbar = self._step_pbars[step_index]
                pbar.total = (pbar.total or 0) + n
                pbar.refresh()

    def run(self, start_urls: List[str]) -> None:
        """Blocking run until all tasks complete."""
        if not start_urls:
            logger.warning("No start URLs provided.")
            return

        tqdm.set_lock(self._pbar_lock)
        self._pbar = tqdm(total=0, desc="crawling", dynamic_ncols=True, position=0)
        if self.show_step_progress:
            self._step_pbars = [
                tqdm(total=0, desc=f"step:{step.name}", dynamic_ncols=True, position=i + 1, leave=False)
                for i, step in enumerate(self.steps)
            ]
        tracker = TaskTracker(on_add=self._progress_add_total)

        self._executor = ThreadPoolExecutor(max_workers=self.workers_num)
        for i in range(self.workers_num):
            worker = Worker(
                worker_id=i,
                steps=self.steps,
                storage_targets=self.storage_targets,
                tracker=tracker,
                proxy_manager=self.proxy_manager,
                http_timeout=self.http_timeout,
                http_retries=self.http_retries,
                per_worker_concurrency=self.per_worker_concurrency,
                prioritize_next_step=self.prioritize_next_step,
                progress_cb=self._progress_tick,
                step_progress_cb=self._step_progress_tick if self.show_step_progress else None,
                step_total_cb=self._step_add_total if self.show_step_progress else None,
            )
            self._workers.append(worker)
            self._executor.submit(worker.run)

        for worker in self._workers:
            worker.wait_ready(timeout=10)

        # Distribute start URLs round-robin
        if self.show_step_progress:
            self._step_add_total(0, len(start_urls))
        for idx, url in enumerate(start_urls):
            worker = self._workers[idx % len(self._workers)]
            worker.submit(TaskContext(url=url, step_index=0, root_id=url))

        tracker.close()
        interrupted = False
        try:
            while True:
                if tracker.wait_done(timeout=0.5):
                    break
        except KeyboardInterrupt:
            interrupted = True
            logger.warning("Interrupted by user, shutting down...")
        finally:
            # Stop workers
            for worker in self._workers:
                worker.shutdown()

            if self._executor:
                self._executor.shutdown(wait=True, cancel_futures=True)

            if self._pbar is not None:
                self._pbar.close()
            for pbar in self._step_pbars:
                pbar.close()

            # Allow storages to flush
            for storage in self.storage_targets:
                close_fn = getattr(storage, "close", None)
                if callable(close_fn):
                    close_fn()

        if interrupted:
            return
