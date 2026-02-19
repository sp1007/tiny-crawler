Examples:
- multi_step_mongo.py : multi-step crawl with proxies + MongoDB + JSONL
- multi_step_proxy.py : multi-step crawl with auto proxy fetch+check
- multi_step_download.py : 3-step crawl to discover TXT URLs and save to data/site/category/file.txt
- site_8080txt_download.py : 3-step crawl for 8080txt.com; supports --crawl-all-pages to discover URLs in step 1 and push back to same-step queue
- single_url.py       : single URL crawl example
Run:
  python -m tiny_crawler.examples.multi_step_mongo
  python -m tiny_crawler.examples.multi_step_proxy
  python -m tiny_crawler.examples.multi_step_download
  python -m tiny_crawler.examples.site_8080txt_download
  python -m tiny_crawler.examples.single_url
