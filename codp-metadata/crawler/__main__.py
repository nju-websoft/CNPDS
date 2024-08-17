import argparse
import json
from math import ceil, floor
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor

from common.constants import (
    FILES_SAVE_PATH,
    METADATA_SAVE_PATH,
    REQUEST_MAX_TIME,
)
from common.utils import log_error
from crawler.crawler import Crawler
from crawler.downloader import Downloader

PROVINCE_CURL_JSON_PATH = os.path.join(os.path.dirname(__file__), "data/curl.json")

parser = argparse.ArgumentParser()
parser.add_argument("--all", action="store_true")
parser.add_argument("--province", type=str)
parser.add_argument("--city", type=str)

parser.add_argument("--workers", type=int, default=0)

parser.add_argument("--resource", type=str, default=PROVINCE_CURL_JSON_PATH)
parser.add_argument("--metadata-output", type=str, default=METADATA_SAVE_PATH)

parser.add_argument("--download-files", action="store_true")
parser.add_argument("--files-output", type=str, default=FILES_SAVE_PATH)

parser.add_argument("--debug", action="store_true")

args = parser.parse_args()
DEBUG = args.debug

requests.packages.urllib3.disable_warnings()

with open(args.resource, "r", encoding="utf-8") as curlFile:
    curls = json.load(curlFile)


def crawl_then_save(province, city):
    crawler = Crawler(province, city, args.metadata_output, curls)
    for _ in range(REQUEST_MAX_TIME):
        try:
            crawler.crawl()
            break
        except Exception as e:
            log_error("global: error at %s - %s", province, city)
            if DEBUG:
                raise e
            time.sleep(50)
    crawler.save_metadata_as_json(args.metadata_output)


download_worker_pool, crawler_worker_pool = None, None
workers = args.workers
if workers > 0:
    if args.download_files:
        download_worker_pool = ThreadPoolExecutor(max_workers=ceil(workers / 2))
        crawler_worker_pool = ThreadPoolExecutor(max_workers=floor(workers / 2))
    else:
        crawler_worker_pool = ThreadPoolExecutor(max_workers=workers)

if args.download_files:
    Crawler.download_files = True
    Downloader.file_dir = args.files_output
    Downloader.pool = download_worker_pool

if args.all:
    if crawler_worker_pool:
        for province in curls:
            for city in curls[province]:
                crawler_worker_pool.submit(crawl_then_save, province, city)
        crawler_worker_pool.shutdown()
        if args.download_files:
            download_worker_pool.shutdown()
    else:
        for province in curls:
            for city in curls[province]:
                crawl_then_save(province, city)
elif args.province and args.city:
    crawl_then_save(args.province, args.city)
