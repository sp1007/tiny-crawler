Examples:
- multi_step_mongo.py : multi-step crawl with proxies + MongoDB + JSONL
- multi_step_proxy.py : multi-step crawl with auto proxy fetch+check
- multi_step_download.py : 3-step crawl to discover TXT URLs and download via proxy if available
- single_url.py       : single URL crawl example
Run:
  python -m tiny_crawler.examples.multi_step_mongo
  python -m tiny_crawler.examples.multi_step_proxy
  python -m tiny_crawler.examples.multi_step_download
  python -m tiny_crawler.examples.single_url
