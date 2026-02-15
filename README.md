# tiny_crawler

Async-first, multi-thread crawler framework with proxy rotation, per-step pipelines, and pluggable storage.

## Features
- asyncio inside multiple worker threads (default 8) for high concurrency.
- aiohttp + aiohttp-socks with per-proxy semaphores (1 connection/proxy at a time).
- Multi-step pipeline: each step has its own parser, HTTP method, headers, and can enqueue next URLs/tasks.
- HTTP client supports GET/POST (json/form), custom headers per step/task, retries with backoff, UA rotation.
- Proxy system: auto-fetch free proxies, health-check, rotate, mark bad; SOCKS sessions isolated per proxy.
- Progress bars: global crawl and optional per-step progress.
- Storage backends: raw/html files, JSON/JSONL, MongoDB (batch insert).

## Requirements
- Python 3.10+ recommended (tested on 3.13).
- `pip install -r requirements.txt`

## Key concepts
- `PipelineStep`: defines parser, HTTP method, default headers for that step.
- `TaskContext`: carries URL, step index, root_id, meta; meta flows to next steps.
- `ParserResult`: parser returns `data`, `next_urls`/`next_tasks`, and `meta`.
- `ProxyManager`: enforces 1-connection-per-proxy with per-thread + per-loop semaphores.
- `HttpClient`: retries non-proxy errors; proxy errors are suppressed/rotated; supports `json` or `form` payloads.

## Usage
Install deps:
```bash
pip install -r requirements.txt
```

Run examples:
```bash
python -m tiny_crawler.examples.multi_step_proxy   # auto fetch + check proxies, JSONL output
python -m tiny_crawler.examples.multi_step_mongo   # uses proxies if available, JSONL + Mongo (needs MONGO_URI)
python -m tiny_crawler.examples.multi_step_download # 3-step URL -> TXT URL -> save as data/site/category/file.txt
python -m tiny_crawler.examples.single_url         # simple title extractor
```

### Writing a pipeline
```python
steps = [
    PipelineStep(name="list", parser=ListParser(), method="GET"),
    PipelineStep(name="detail", parser=DetailParser(), method="POST",
                 headers={"X-Api-Key": "..."}),  # default headers for this step
]
engine = CrawlerEngine(
    steps=steps,
    workers=8,
    storage_targets=[JSONLStorage("output/data.jsonl")],
    proxy_manager=proxy_manager,          # optional
    show_step_progress=True,              # per-step progress bars
)
engine.run(start_urls)
```

### Passing request options from a parser
Return `meta["request"]` in `ParserResult` to override method/headers/body for next URLs:
```python
return ParserResult(
    next_urls=[next_url],
    meta={
        "request": {
            "method": "POST",
            "json": {"id": item_id},          # or "form": {...}
            "headers": {"X-Trace": trace_id}
        }
    }
)
```

### Passing data from early steps to final step
Use `next_tasks` to attach per-URL metadata (for example category from step 1):
```python
from tiny_crawler import ParserResult, NextTask

# step 1
return ParserResult(
    next_tasks=[
        NextTask(url=detail_url, meta={"category": "science"})
    ]
)

# step N (final)
return ParserResult(
    data={
        "source": context.url,
        "category": context.meta.get("category"),
        "content": content_text,
    }
)
```

### Proxy workflow
- Auto fetch + check (see `examples/multi_step_proxy.py`), saves alive proxies to `proxies.txt`.
- Per proxy: 1 connection at any time; SOCKS gets its own aiohttp session.
- Proxy errors are suppressed and proxies are rotated; after too many proxy failures it can fall back to direct.

### Storage
- `RawStorage` / `HtmlStorage`: writes one file per URL under `output/raw` or `output/html`.
  Supports `path_builder(context, data)` to customize file path per URL.
  Set `include_default_subdir=False` if you want direct root like `data/site/category/file.txt`.
- `JSONLStorage`: appends records to `output/*.jsonl`.
- `JSONStorage`: accumulates then writes a JSON array on close().
- `MongoStorage`: batch inserts (set `MONGO_URI`, `database`, `collection`).

Example custom path:
```python
from pathlib import Path
from urllib.parse import urlparse

def build_path(context, data):
    _ = data
    site = urlparse(context.url).netloc or "unknown_site"
    category = context.meta.get("category", "unknown")
    filename = Path(urlparse(context.url).path).name or "item.txt"
    return Path(site) / str(category) / filename

storage = RawStorage(
    base_dir="data",
    path_builder=build_path,
    include_default_subdir=False,
)
```

## Notes & tips
- Ctrl-C friendly: engine catches KeyboardInterrupt and shuts down workers cleanly.
- To reduce noisy Windows proactor errors, `quiet_proactor()` is called in examples.
- Adjust concurrency: `workers` (threads) and `per_worker_concurrency` (async tasks per loop).
- Each proxy only 1 connection: enforced by `ProxyManager` semaphores.

## Project structure (key files)
- `tiny_crawler/crawler/engine.py` — orchestrator
- `tiny_crawler/crawler/worker.py` — per-thread event loop runner
- `tiny_crawler/crawler/pipeline.py` — PipelineStep, TaskContext, ParserResult, NextTask
- `tiny_crawler/http/client.py` — HTTP client with proxy rotation
- `tiny_crawler/proxy/*` — proxy fetch/check/manage
- `tiny_crawler/storage/*` — storage backends
- `tiny_crawler/examples/*` — runnable examples

## License
MIT (feel free to reuse/adapt).
