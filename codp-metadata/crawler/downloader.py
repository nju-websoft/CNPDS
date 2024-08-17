import os
import time
import requests

from common.utils import log_error
from common.constants import REQUEST_MAX_TIME


class Downloader:
    pool = None
    file_dir = ""
    request_header = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.69"
        ),
    }

    def __init__(self, province, city) -> None:
        self.province = province
        self.city = city

    def log_download_error(self, link, retry_time):
        log_error(
            "%s_%s download: fail at %s time - %s",
            self.province,
            self.city,
            retry_time,
            link,
        )

    def start_download(self, link, file_name):
        if not os.path.exists(self.file_dir):
            os.makedirs(self.file_dir)
        path = os.path.join(self.file_dir, file_name)
        if Downloader.pool:
            Downloader.pool.submit(self.download, link, path)
        else:
            self.download(link, path)

    def download(self, link, path):
        for _ in range(REQUEST_MAX_TIME):
            try:
                response = requests.get(link, headers=self.request_header, stream=True)
                response.raise_for_status()

                with open(path, "wb") as output:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            output.write(chunk)
                break
            except:
                self.log_download_error(link, _)
                time.sleep(5)
