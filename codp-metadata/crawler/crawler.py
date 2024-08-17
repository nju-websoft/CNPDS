import json
import os
import re
import time
import copy
import urllib
import bs4
import requests

from common.constants import (
    REQUEST_TIME_OUT,
    REQUEST_MAX_TIME,
)
from common.utils import log_error
from common.wrapper import Wrapper
from crawler.detail import Detail
from crawler.resultlist import ResultList


class Crawler:
    download_files = False

    def __init__(self, province, city, output, curls):
        self.province = province
        self.city = city
        self.curls = curls
        self.result_list = ResultList(province, city)
        self.detail = Detail(province, city, Crawler.download_files)
        self.result_list_curl = curls[province][city]["resultList"]
        self.detail_list_curl = curls[province][city]["detail"]
        self.metadata_list = []
        self.output = output

    def crawl(self):
        func_name = f"crawl_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.crawl_other)
        func()

    def log_result_list_error(self, stat):
        log_error(
            "%s_%s crawl: get result list error, %s", self.province, self.city, stat
        )

    def logs_detail_error(self, link, action):
        log_error(
            "%s_%s crawl: get detail error with %s -> %s",
            self.province,
            self.city,
            link,
            action,
        )

    def crawl_beijing_beijing(self):
        pages = Wrapper(1600)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["curPage"] = str(page)
            links = None
            try_cnt = 0
            while not links:
                try_cnt += 1
                if try_cnt >= REQUEST_MAX_TIME:
                    break
                try:
                    links = self.result_list.get_result_list(curl, pages)
                except (
                    requests.exceptions.ProxyError,
                    requests.exceptions.SSLError,
                ) as e:
                    links = None
                    time.sleep(5)
                    self.log_result_list_error(f"retrying at page {page}...")
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] = link
                metadata = None
                try_cnt = 0
                while not metadata:
                    try_cnt += 1
                    if try_cnt >= REQUEST_MAX_TIME:
                        break
                    try:
                        metadata = self.detail.get_detail(curl)
                        self.metadata_list.append(metadata)
                    except (
                        requests.exceptions.SSLError,
                        requests.exceptions.ConnectionError,
                    ) as e:
                        time.sleep(5)
                        metadata = None
                        self.logs_detail_error(link, "continue")
                    except AttributeError as e:
                        # e.g. https://data.beijing.gov.cn/zyml/ajg/sswj1/598f4a53ae9d4cbe88074777572b38d5.htm
                        self.logs_detail_error(link, "break")
                        break
            page += 1

    def crawl_tianjin_tianjin(self):
        curl = self.result_list_curl.copy()
        link_format_data = self.result_list.get_result_list(curl)
        for data in link_format_data:
            curl = self.detail_list_curl.copy()
            curl["url"] = data["link"]
            metadata = None
            try_cnt = 0
            while not metadata:
                try_cnt += 1
                if try_cnt >= REQUEST_MAX_TIME:
                    break
                try:
                    metadata = self.detail.get_detail(curl)
                    metadata["url"] = data["link"]
                    metadata["format"] = data["format"]
                    self.metadata_list.append(metadata)
                except requests.exceptions.ProxyError as e:
                    metadata = None
                    self.logs_detail_error(data["link"], "continue")

    def crawl_hebei_hebei(self):
        pages = Wrapper(128)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNo"] = str(page)
            metadata_ids = self.result_list.get_result_list(curl, pages)
            for metadata_id in metadata_ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["rowkey"] = metadata_id["METADATA_ID"]
                curl["data"]["list_url"] = curl["data"]["list_url"].format(page)
                metadata = self.detail.get_detail(curl)
                metadata["所属主题"] = metadata_id["THEME_NAME"]
                metadata["发布时间"] = metadata_id["CREAT_DATE"]
                metadata["更新日期"] = metadata_id["UPDATE_DATE"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_shanxi_datong(self):
        pages = Wrapper(61)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["pageNumber"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link
                metadata = self.detail.get_detail(curl)
                metadata["url"] = curl["url"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_shanxi_changzhi(self):
        pages = Wrapper(17)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["start"] = str((page - 1) * int(curl["data"]["pageLength"]))
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["cata_id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_shanxi_jincheng(self):
        pages = Wrapper(57)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["curPage"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["url"] = curl["url"].format(_id)
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_shanxi_yuncheng(self):
        # TODO: ERR_EMPTY_RESPONSE
        pages = Wrapper(5)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["pageIndex"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_neimenggu_neimenggu(self):
        pages = Wrapper(8)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["page"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_neimenggu_xinganmeng(self):
        pages = Wrapper(24)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNum"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["url"] = curl["url"].format(_id)
                metadata = self.detail.get_detail(curl)
                metadata["url"] = curl["detail_url"].format(_id)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_dongbei_common(self):
        pages = Wrapper(26)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_liaoning_liaoning(self):
        self.crawl_dongbei_common()

    def crawl_liaoning_shenyang(self):
        self.crawl_dongbei_common()

    def crawl_heilongjiang_harbin(self):
        self.crawl_dongbei_common()

    # def crawl_jilin_jilin(self):
    #     for page in range(1, 25):
    #         curl = self.result_list_curl.copy()
    #         curl['data']['page'] = str(page)
    #         links = self.result_list.get_result_list(curl)
    #         for link in links:
    #             curl = self.detail_list_curl.copy()
    #             curl['url'] += link
    #             metadata = self.detail.get_detail(curl)
    #             self.metadata_list.append(metadata)

    def crawl_shanghai_shanghai(self):
        pages = Wrapper(451)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"] = (
                curl["data"].replace('"pageNum":1', f'"pageNum":{page}').encode()
            )
            dataset_ids = self.result_list.get_result_list(curl, pages)
            for dataset_id in dataset_ids:
                curl = copy.deepcopy(self.detail_list_curl)
                curl["headers"]["Referer"] = curl["headers"]["Referer"].format(
                    dataset_id["datasetId"],
                    urllib.parse.quote(dataset_id["datasetName"]),
                )
                curl["url"] = curl["url"].format(dataset_id["datasetId"])
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_jiangsu(self):
        for city, max_page in zip(["", "all"], [54, 11]):
            pages = Wrapper(max_page)
            page = 0
            while page < pages.obj:
                curl = self.result_list_curl.copy()
                curl["data"] = curl["data"].format(page, city)
                rowGuid_tag_list = self.result_list.get_result_list(curl, pages)
                for rowGuid, tags in rowGuid_tag_list:
                    curl = self.detail_list_curl.copy()
                    curl["url"] = curl["url"].format(rowGuid)
                    curl["data"] = curl["data"].format(rowGuid)
                    metadata = self.detail.get_detail(curl)
                    metadata["数据格式"] = tags
                    metadata["详情页网址"] = curl["headers"]["Referer"].format(rowGuid)
                    self.metadata_list.append(metadata)
                page += 1

    # def crawl_jiangsu_nanjing(self):
    #     curl = self.result_list_curl.copy()
    #     links = self.result_list.get_result_list(curl)

    def crawl_jiangsu_wuxi(self):
        pages = Wrapper(295)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["page"] = str(page)
            try:
                cata_ids = self.result_list.get_result_list(curl, pages)
            except:
                self.log_result_list_error(f"continue at page {page}")
                page += 1
                continue
            for cata_id in cata_ids:
                curl = self.detail_list_curl.copy()
                curl["params"]["cata_id"] = cata_id
                try:
                    metadata = self.detail.get_detail(curl)
                    self.metadata_list.append(metadata)
                except:
                    self.logs_detail_error(f"page {page} cata-id {cata_id}", "continue")
            page += 1

    def crawl_jiangsu_xuzhou(self):
        # TODO: cookies generated by JavaScript (`appCode` in headers)
        pages = Wrapper(43)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["jsonData"]["pageNo"] = page
            mlbhs = self.result_list.get_result_list(curl, pages)
            for mlbh in mlbhs:
                curl = self.detail_list_curl.copy()
                curl["params"]["mlbh"] = mlbh
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_suzhou(self):
        pages = Wrapper(162)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["current"] = str(page)
            curl["jsonData"]["current"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["params"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_nantong(self):
        # TODO: [SSL: CERTIFICATE_VERIFY_FAILED]
        pages = Wrapper(120)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["page"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["params"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_lianyungang(self):
        pages = Wrapper(111)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["pageNum1"] = str(page)
            dmids = self.result_list.get_result_list(curl, pages)
            for dmid in dmids:
                curl = self.detail_list_curl.copy()
                curl["params"]["dmid"] = dmid
                valid, metadata = self.detail.get_detail(curl)
                if valid:
                    self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_huaian(self):
        pages = Wrapper(130)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["pageNum"] = str(page)
            catalogIds = self.result_list.get_result_list(curl, pages)
            for catalogId in catalogIds:
                curl = self.detail_list_curl.copy()
                curl["params"]["catalogId"] = catalogId
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_yancheng(self):
        pages = Wrapper(99)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["pageNumber"] = str(page)
            catalogPks = self.result_list.get_result_list(curl, pages)
            for catalogPk in catalogPks:
                curl = self.detail_list_curl.copy()
                curl["params"]["catalogId"] = catalogPk
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_zhenjiang(self):
        pages = Wrapper(215)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["page"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["params"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_taizhou(self):
        pages = Wrapper(541)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["page"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["url"] = curl["url"].format(_id)
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangsu_suqian(self):
        pages = Wrapper(268)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["page"] = str(page)
            id_infos = self.result_list.get_result_list(curl, pages)
            for _id, update_time in id_infos:
                curl = self.detail_list_curl.copy()
                curl["url"] = curl["url"].format(_id)
                metadata = self.detail.get_detail(curl)
                metadata["更新时间"] = update_time
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_zhejiang(self):
        pages = Wrapper(132)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNumber"] = str(page)
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["queries"] = iid
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_hangzhou(self):
        pages = Wrapper(660)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            post_data_json = json.loads(curl["data"]["postData"])
            post_data_json["pageSplit"]["pageNumber"] = page
            curl["data"]["postData"] = json.dumps(post_data_json)
            id_formats = self.result_list.get_result_list(curl, pages)
            for _id, mformat in id_formats:
                curl = copy.deepcopy(self.detail_list_curl)
                curl["format"] = mformat
                curl["url"] = curl["url"].format(mformat)
                curl["headers"]["Referer"] = curl["headers"]["Referer"].format(
                    mformat, _id
                )
                post_data_json = json.loads(curl["data"]["postData"])
                post_data_json["source_id"] = _id
                curl["data"]["postData"] = json.dumps(post_data_json)
                metadata = self.detail.get_detail(curl)
                if metadata is None:
                    continue
                metadata["数据格式"] = mformat
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_ningbo(self):
        pages = Wrapper(177)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["jsonData"]["pageNo"] = str(page)
            uuids = self.result_list.get_result_list(curl, pages)
            for uuid in uuids:
                curl = self.detail_list_curl.copy()
                curl["json_data"]["uuid"] = uuid
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_wenzhou(self):
        # TODO: Unknown Error
        pages = Wrapper(51)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNumber"] = str(page)
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["iid"] = iid["iid"]
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_jiaxing(self):
        pages = Wrapper(20)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNumber"] = str(page)
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["iid"] = iid["iid"]
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_shaoxing(self):
        pages = Wrapper(75)
        page = 1
        while page <= pages.obj:
            if page == 3:
                page += 1
                continue
            curl = self.result_list_curl.copy()
            curl["jsonData"]["pageNum"] = page
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["dataId"] = iid
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_jinhua(self):
        pages = Wrapper(39)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNumber"] = str(page)
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["iid"] = iid["iid"]
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_quzhou(self):
        # TODO: 502 Bad Gateway
        pages = Wrapper(51)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNumber"] = str(page)
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["iid"] = iid["iid"]
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_zhoushan(self):
        pages = Wrapper(12)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["jsonData"]["pageNo"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["params"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_taizhou(self):
        pages = Wrapper(108)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["jsonData"]["pageNum"] = page
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["dataId"] = iid
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_zhejiang_lishui(self):
        pages = Wrapper(44)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNumber"] = str(page)
            iids = self.result_list.get_result_list(curl, pages)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl["params"]["iid"] = iid["iid"]
                metadata = self.detail.get_detail(curl)
                if metadata is not None:
                    self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_anhui(self):
        pages = Wrapper(100000000)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNum"] = str(page)
            rids = self.result_list.get_result_list(curl, pages)
            if len(rids) == 0:
                break
            for rid in rids:
                curl = self.detail_list_curl.copy()
                curl["headers"]["Referer"] = curl["headers"]["Referer"].format(rid)
                curl["data"]["rid"] = rid
                metadata = self.detail.get_detail(curl)
                metadata["url"] = (
                    "http://data.ahzwfw.gov.cn:8000/dataopen-web/api-data-details.html?rid="
                    + rid
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_hefei(self):
        pages = Wrapper(34)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["currentPageNo"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                self.log_result_list_error(f"break at page {page}.")
                break
            for iid, zyId in ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["id"] = iid
                curl["data"]["zyId"] = zyId
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = (
                    f"https://www.hefei.gov.cn/open-data-web/data/detail-hfs.do?&id={iid}&zyId={zyId}"
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_wuhu(self):
        pages = Wrapper(37)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNo"] = str(page)
            metadata_list = self.result_list.get_result_list(curl, pages)
            if len(metadata_list) == 0:
                break
            self.metadata_list.extend(metadata_list)
            page += 1

    def crawl_anhui_bengbu(self):
        # dataset
        pages = Wrapper(8)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["pageIndex"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                break
            for iid in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["resourceId"] = re.search(
                    r"(?<=resourceId=)\d+", iid
                ).group(0)
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = "https://www.bengbu.gov.cn" + iid
                if metadata["下载格式"][0] == "":
                    metadata["下载格式"] = ["file"]
                self.metadata_list.append(metadata)
            page += 1

        # api
        pages = Wrapper(2)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["pageIndex"] = str(page)
            curl["queries"]["resourceType"] = "api"
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                break
            for iid in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["resourceId"] = re.search(
                    r"(?<=resourceId=)\d+", iid
                ).group(0)
                curl["url"] = "https://www.bengbu.gov.cn/site/tpl/6541"
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = "https://www.bengbu.gov.cn" + iid
                metadata["下载格式"] = ["api"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_huainan(self):
        pages = Wrapper(5)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            data_ids = self.result_list.get_result_list(curl, pages)
            if len(data_ids) == 0:
                break
            for data_id in data_ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["dataId"] = str(data_id)
                metadata = self.detail.get_detail(curl)
                metadata["url"] = (
                    "https://sjzyj.huainan.gov.cn/odssite/view/page/govDataFile?pageIndex="
                    + metadata["数据类型"]
                    + "&dataId="
                    + str(data_id)
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_huaibei(self):
        pages = Wrapper(39)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["curPageNumber"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                break
            for iid in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["id"] = iid
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = (
                    "http://open.huaibeidata.cn:1123/#/data_public/detail/" + iid
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_huangshan(self):
        # dataset
        pages = Wrapper(26)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["pageIndex"] = str(page)
            time.sleep(1)
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                break
            for iid, depart, cata, format_list in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["resourceId"] = re.search(
                    r"(?<=resourceId=)\d+", iid
                ).group(0)
                time.sleep(1)
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = "https://www.huangshan.gov.cn" + iid
                metadata["提供机构"] = depart
                metadata["数据领域"] = cata
                metadata["资源格式"] = format_list
                metadata["开放条件"] = "无条件开放"
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_chuzhou(self):
        pages = Wrapper(86)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            time.sleep(1)
            metadata_list = self.result_list.get_result_list(curl, pages)
            if len(metadata_list) == 0:
                break
            for meta in metadata_list:
                curl = self.detail_list_curl.copy()
                curl["queries"]["name"] = meta["标题"]
                time.sleep(1)
                remains = self.detail.get_detail(curl)
                remains.update(meta)
                self.metadata_list.append(remains)
            page += 1

    def crawl_anhui_suzhou(self):
        pages = Wrapper(54)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                metadata["数据格式"] = link["data_formats"]
                metadata["url"] = curl["url"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_luan(self):
        pages = Wrapper(56)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                metadata["数据格式"] = link["data_formats"]
                metadata["url"] = curl["url"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_anhui_chizhou(self):
        pages = Wrapper(276)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            metadatas = self.result_list.get_result_list(curl, pages)
            if len(metadatas) == 0:
                break
            for metadata in metadatas:
                self.metadata_list.append(metadata)
            page += 1

    def crawl_fujian_fujian(self):
        pages = Wrapper(737)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["params"]["page"] = str(page)
            catalog_ids = self.result_list.get_result_list(curl, pages)
            for catalog_id in catalog_ids:
                curl = self.detail_list_curl.copy()
                curl["params"]["cataId"] = catalog_id
                metadata = self.detail.get_detail(curl)
                metadata["url"] = f'${curl["url"]}?catalogID=${catalog_id}'
                self.metadata_list.append(metadata)
            page += 1
            break

    def crawl_fujian_fuzhou(self):
        # TODO: HTTP ERROR 502
        pages = Wrapper(116)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNo"] = str(page)
            res_ids = self.result_list.get_result_list(curl, pages)
            for res_id in res_ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["resId"] = res_id
                metadata = self.detail.get_detail(curl)
                metadata["url"] = "http://data.fuzhou.gov.cn/data/catalog/toDataCatalog"
                self.metadata_list.append(metadata)
            page += 1

    def crawl_fujian_xiamen(self):
        pages = Wrapper(102)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["page"]["currentPage"] = str(page)
            catalog_ids = self.result_list.get_result_list(curl, pages)
            for catalog_id in catalog_ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["catalogId"] = catalog_id
                metadata = self.detail.get_detail(curl)
                metadata["url"] = curl["url"] + "?" + catalog_id
                self.metadata_list.append(metadata)
            page += 1

    def crawl_jiangxi_jiangxi(self):
        pages = Wrapper(29)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["current"] = page
            data_ids = self.result_list.get_result_list(curl, pages)
            for data_id in data_ids:
                curl = self.detail_list_curl.copy()
                curl["headers"]["Referer"] = curl["headers"]["Referer"].format(
                    data_id["dataId"]
                )
                curl["queries"]["dataId"] = data_id["dataId"]
                metadata = self.detail.get_detail(curl)
                if metadata == {}:
                    continue
                metadata["数据格式"] = (
                    "[" + data_id["filesType"] + "]"
                    if data_id["filesType"] is not None
                    else ""
                )
                metadata["url"] = (
                    "https://data.jiangxi.gov.cn/open-data/detail?id="
                    + data_id["dataId"]
                )
                self.metadata_list.append(metadata)
            page += 1

    # def crawl_jiangxi_ganzhou(self):
    #     for page in range(1, 24):
    #         curl = self.result_list_curl.copy()
    #         curl['url'] = curl['url'].format(page)
    #         ids = self.result_list.get_result_list(curl)
    #         for _id in ids:
    #             curl = self.detail_list_curl.copy()
    #             curl['url'] = curl['url'].format(_id)
    #             metadata = self.detail.get_detail(curl)
    #             if metadata == {}:
    #                 continue
    #             metadata['url'] = 'https://data.jiangxi.gov.cn/open-data/detail?id=' + data_id['dataId']
    #             self.metadata_list.append(metadata)

    def crawl_shandong_common(self, use_cache=True, page_size=10):
        # TODO:debug
        curr_page = 1
        try_cnt = 0  # 每一页的重试次数

        # 遍历每一页直到拿不到result_list
        while True:
            # TODO:加文件类型
            curl = self.result_list_curl.copy()
            curl["params"]["page"] = str(curr_page)
            curr_page += 1
            links = self.result_list.get_result_list(curl)
            if not len(links):
                if try_cnt >= REQUEST_MAX_TIME:
                    break
                try_cnt += 1
                continue
            try_cnt = 0  # 重置重试次数
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                metadata["资源格式"] = link["data_formats"]  # TODO：扔到detail方法里面
                metadata["详情页网址"] = curl["url"]
                self.metadata_list.append(metadata)

    def crawl_shandong_shandong(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_jinan(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_qingdao(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_zibo(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_zaozhuang(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_dongying(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_yantai(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_weifang(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_jining(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_taian(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_weihai(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_rizhao(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_linyi(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_dezhou(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_liaocheng(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_binzhou(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_shandong_heze(self, use_cache=True, page_size=10):
        self.crawl_shandong_common(use_cache, page_size)

    def crawl_hubei_wuhan(self):
        all_ids = []
        pages = Wrapper(236)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["data"]["current"] = page
            curl["data"]["size"] = 6
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["queries"]["cataId"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hubei_huangshi(self):
        all_ids = []
        curl = copy.deepcopy(self.result_list_curl)
        curl["url"] = curl["url"].format("1")
        ids = self.result_list.get_result_list(curl)
        for _id in ids:
            if _id in all_ids:
                continue
            else:
                all_ids.append(_id)
            curl = copy.deepcopy(self.detail_list_curl)
            curl["queries"]["infoid"] = _id
            metadata = self.detail.get_detail(curl)
            self.metadata_list.append(metadata)

        curl = copy.deepcopy(self.result_list_curl)
        curl["url"] = curl["url"].format("0")
        ids = self.result_list.get_result_list(curl)
        for _id in ids:
            if _id in all_ids:
                continue
            else:
                all_ids.append(_id)
            curl = copy.deepcopy(self.detail_list_curl)
            curl["queries"]["infoid"] = _id
            metadata = self.detail.get_detail(curl)
            self.metadata_list.append(metadata)

    def crawl_hubei_yichang(self):
        all_ids = []
        pages = Wrapper(66)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["dataset"]["crawl_type"] = "dataset"
            curl["dataset"]["data"]["pageNum"] = page
            ids = self.result_list.get_result_list(curl["dataset"], pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["dataset"]["crawl_type"] = "dataset"
                curl["dataset"]["queries"]["dataId"] = _id
                metadata = self.detail.get_detail(curl["dataset"])
                self.metadata_list.append(metadata)
            page += 1

        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["interface"]["crawl_type"] = "interface"
            curl["interface"]["data"]["pageNum"] = page
            ids = self.result_list.get_result_list(curl["interface"], pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["interface"]["crawl_type"] = "interface"
                curl["interface"]["data"]["baseDataId"] = _id
                metadata = self.detail.get_detail(curl["interface"])
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hubei_ezhou(self):
        all_ids = []
        pages = Wrapper(30)
        page = 0
        while page < pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["hangye"]["crawl_type"] = "hangye"
            curl["hangye"]["url"] = curl["hangye"]["url"].format(
                "index{}.html".format(f"_{page}" if page else "")
            )
            urls = self.result_list.get_result_list(curl["hangye"], pages)
            for url in urls:
                if url in all_ids:
                    continue
                else:
                    all_ids.append(url)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["url"] = url
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

        pages = Wrapper(1)
        page = 0
        while page < pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["shiji"]["crawl_type"] = "shiji"
            curl["shiji"]["url"] = curl["shiji"]["url"].format(
                "index{}.html".format(f"_{page}" if page else "")
            )
            urls = self.result_list.get_result_list(curl["shiji"], pages)
            for url in urls:
                if url in all_ids:
                    continue
                else:
                    all_ids.append(url)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["url"] = url
                curl["api"] = True
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hubei_jingzhou(self):
        all_ids = []
        pages = Wrapper(121)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["queries"]["page"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["url"] = curl["url"].format(_id)
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hubei_xianning(self):
        curl = copy.deepcopy(self.result_list_curl)
        response = requests.get(curl["url"], headers=curl["headers"])
        data = json.loads(response.text)
        for item in data:
            metadata = {
                "名称": item["title"],
                "详情页网址": item["link"],
                "创建时间": item["pubDate"],
                "更新时间": item["update"],
                "数据来源": item["department"],
                "数据领域": item["theme"],
                "文件类型": item["chnldesc"],
            }
            metadata["更新时间"] = (
                metadata["更新时间"]
                .replace("年", "-")
                .replace("月", "-")
                .replace("日", "")
            )
            metadata["创建时间"] = (
                metadata["创建时间"]
                .replace("年", "-")
                .replace("月", "-")
                .replace("日", "")
            )
            metadata["文件类型"] = "file" if metadata["文件类型"] == "数据集" else "api"
            metadata["文件类型"] = metadata["文件类型"].split(",")
            self.metadata_list.append(metadata)

    def crawl_hubei_suizhou(self):
        all_ids = []
        pages = Wrapper(48)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["url"] = curl["url"].format("dataSet")
            curl["data"]["page"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["crawl_type"] = "dataset"
                curl["url"] = curl["url"].format("dataSet/toDataDetails/" + str(_id))
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

        pages = Wrapper(2)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["url"] = curl["url"].format("dataApi")
            curl["data"]["page"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["crawl_type"] = "api"
                curl["url"] = curl["url"].format("dataApi/toDataDetails/" + str(_id))
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hunan_common(self):
        curl = copy.deepcopy(self.result_list_curl)
        response = requests.get(
            curl["all_type"]["url"],
            params=curl["all_type"]["queries"],
            headers=curl["all_type"]["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        if self.city == "yueyang":
            lis = soup.find_all("li", class_="list-group-item-action")
        else:
            lis = soup.find_all("li", class_="wb-tree-item")
        type_ids = []
        for li in lis:
            text = str(li.find_next("a")["onclick"])
            type_ids.append(text.split("(")[1].split(")")[0].split(","))
        all_links = []
        for _type, _id in type_ids:
            pages = Wrapper(5)
            page = 0
            while page < pages.obj:
                curl = copy.deepcopy(self.result_list_curl)
                curl["frame"]["queries"]["dataInfo.offset"] = page * 6
                curl["frame"]["queries"]["type"] = _type
                curl["frame"]["queries"]["id"] = _id
                links = self.result_list.get_result_list(curl["frame"], pages)
                for link in links:
                    if link in all_links:
                        continue
                    else:
                        all_links.append(link)
                    curl = copy.deepcopy(self.detail_list_curl)
                    curl["queries"]["id"] = link
                    metadata = self.detail.get_detail(curl)
                    self.metadata_list.append(metadata)
                page += 1

    def crawl_hunan_yueyang(self):
        self.crawl_hunan_common()

    def crawl_hunan_changde(self):
        all_ids = []
        pages = Wrapper(3)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["queries"]["page"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["queries"]["cataId"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hunan_yiyang(self):
        self.crawl_hunan_common()

    def crawl_hunan_chenzhou(self):
        self.crawl_hunan_common()

    def crawl_guangdong_guangdong(self):
        all_ids = []
        pages = Wrapper(7574)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["dataset"]["data"]["pageNo"] = page
            curl["dataset"]["crawl_type"] = "dataset"
            ids = self.result_list.get_result_list(curl["dataset"], pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["dataset"]["crawl_type"] = "dataset"
                curl["dataset"]["data"]["resId"] = _id
                metadata = self.detail.get_detail(curl["dataset"])
                self.metadata_list.append(metadata)
            page += 1

        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["api"]["data"]["pageNo"] = page
            curl["api"]["crawl_type"] = "api"
            ids = self.result_list.get_result_list(curl["api"], pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["api"]["data"]["resId"] = _id
                curl["api"]["crawl_type"] = "api"
                metadata = self.detail.get_detail(curl["api"])
                self.metadata_list.append(metadata)
            page += 1

    def crawl_guangdong_guangzhou(self):
        all_ids = []
        pages = Wrapper(129)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["data"]["body"]["useType"] = None
            curl["data"]["page"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["detail"]["url"] = curl["detail"]["url"].format(_id)
                curl["doc"]["queries"]["sid"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
            page += 1

    def crawl_guangdong_shenzhen(self):
        all_ids = []
        pages = Wrapper(553)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["dataset"]["data"]["pageNo"] = page
            curl["dataset"]["crawl_type"] = "dataset"
            ids = self.result_list.get_result_list(curl["dataset"], pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["data"]["resId"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
                time.sleep(1)
            page += 1

        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["api"]["data"]["pageNo"] = page
            curl["api"]["crawl_type"] = "api"
            ids = self.result_list.get_result_list(curl["api"])
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["data"]["resId"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
                time.sleep(1)
            page += 1

    def crawl_guangdong_zhongshan(self):
        all_ids = []
        pages = Wrapper(85)
        page = 1
        while page <= pages.obj:
            curl = copy.deepcopy(self.result_list_curl)
            curl["data"]["page"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                if _id in all_ids:
                    continue
                else:
                    all_ids.append(_id)
                curl = copy.deepcopy(self.detail_list_curl)
                curl["queries"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                self.metadata_list.append(metadata)
                time.sleep(1)
            page += 1

    def crawl_guangxi_common(self):
        pages = Wrapper(2000000)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                try:
                    metadata = self.detail.get_detail(curl)
                    if len(metadata):
                        metadata["详情页网址"] = curl["url"]
                        metadata["数据格式"] = link["data_formats"]
                        self.metadata_list.append(metadata)
                except:
                    self.logs_detail_error(curl["url"], "continue")
            page += 1

    def crawl_guangxi_guangxi(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_nanning(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_liuzhou(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_guilin(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_wuzhou(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_beihai(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_fangchenggang(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_qinzhou(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_guigang(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_yulin(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_baise(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_hezhou(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_hechi(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_laibin(self):
        return self.crawl_guangxi_common()

    def crawl_guangxi_chongzuo(self):
        return self.crawl_guangxi_common()

    def crawl_hainan_hainan(self):
        pages = Wrapper(100000)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["curPage"] = page
            ids = self.result_list.get_result_list(curl, pages)
            if not (ids and len(ids)):
                break
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["url"] = curl["url"].format(_id)
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = curl["url"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_hainan_hainansjj(self):
        self.crawl_hainan_hainan()

    def crawl_hainan_hainansjjk(self):
        self.crawl_hainan_hainan()

    def crawl_chongqing_chongqing(self):
        curl = self.result_list_curl.copy()
        # curl['data']['variables']['input']['offset'] = page * 10
        metadatas = self.result_list.get_result_list(curl)
        # if len(metadatas) == 0:
        #     break
        for metadata in metadatas:
            self.metadata_list.append(metadata)

    def crawl_sichuan_common(self):
        pages = Wrapper(100000)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            try:
                links = self.result_list.get_result_list(curl, pages)
                if len(links) == 0:
                    break
            except:
                self.log_result_list_error(f"break at page {page}")
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                try:
                    metadata = self.detail.get_detail(curl)
                    if bool(metadata):
                        metadata["数据格式"] = (
                            link["data_formats"]
                            if link["data_formats"] != "[]"
                            else "['file']"
                        )
                        metadata["详情页网址"] = curl["url"]
                        self.metadata_list.append(metadata)
                except:
                    self.logs_detail_error(curl["url"], f"continue")
            page += 1

    def crawl_sichuan_sichuan(self):
        self.crawl_sichuan_common()

    def crawl_sichuan_chengdu(self):
        self.crawl_sichuan_common()

    def crawl_sichuan_zigong(self):
        pages = Wrapper(870)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            time.sleep(5)
            curl["queries"]["offset"] = str((page - 1) * 10)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "https://data.zg.cn/snww/sjzy/detail.html?" + _id
                    )
                    # 根据可下载类型获取type
                    turl = self.curls[self.province][self.city]["typeList"].copy()
                    turl["queries"]["id"] = _id
                    response = requests.get(
                        turl["url"],
                        params=turl["queries"],
                        headers=turl["headers"],
                        timeout=REQUEST_TIME_OUT,
                    )
                    if response.status_code != requests.codes.ok:
                        self.logs_detail_error(
                            turl["url"],
                            f"break with response code {response.status_code}",
                        )
                        type_json = dict()
                    else:
                        type_json = json.loads(response.text)["data"]
                    if not bool(type_json):
                        type_json = dict()
                    type_list = []
                    for name, type_info in type_json.items():
                        type_list.append(type_info["type"])
                    metadata["数据格式"] = (
                        str(type_list) if bool(type_list) else "['file']"
                    )
                    self.metadata_list.append(metadata)
            page += 1

    def crawl_sichuan_panzhihua(self):
        # TODO: HTTP ERROR 502
        self.crawl_sichuan_common()

    def crawl_sichuan_luzhou(self):
        pages = Wrapper(701)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            time.sleep(5)
            curl["data"]["page"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id, opens, publisht, updatet in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["id"] = _id
                curl["queries"]["type"] = "opendata"
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["开放条件"] = (
                        "无条件开放" if opens == "1" else "有条件开放"
                    )
                    metadata["详情页网址"] = (
                        "https://data.luzhou.cn/portal/service_detail?id="
                        + _id
                        + "&type=opendata"
                    )
                    metadata["发布时间"] = publisht
                    metadata["更新时间"] = updatet
                    metadata["数据格式"] = "['api']"
                    self.metadata_list.append(metadata)
            page += 1

    def crawl_sichuan_deyang(self):
        pages = Wrapper(99)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            time.sleep(5)
            curl["data"]["pageNo"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["mlbh"] = _id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "https://www.dysdsj.cn/#/DataSet/" + _id.replace("/", "%2F")
                    )
                    self.metadata_list.append(metadata)
            page += 1

    def crawl_sichuan_mianyang(self):
        pages = Wrapper(1297)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            # time.sleep(5)
            curl["queries"]["startNum"] = str((page - 1) * 8)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "https://data.mianyang.cn/zwztzlm/index.jhtml?caseid=" + _id
                    )
                    metadata["数据格式"] = (
                        "['api']"  # 只有数据库和接口类型，实际全是接口
                    )
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_sichuan_guangyuan(self):
        pages = Wrapper(1874)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            # time.sleep(3)
            curl["data"]["currentPage"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["id"] = str(_id)
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "http://data.cngy.gov.cn/open/index.html?id=user&messid="
                        + str(_id)
                    )
                    metadata["领域名称"] = "生活服务"
                    metadata["行业名称"] = "公共管理、社会保障和社会组织"
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_sichuan_suining(self):
        # TODO: cookies generated by JavaScript (`appCode` in headers)
        pages = Wrapper(910)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNo"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id, typeList in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["mlbh"] = _id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "https://www.suining.gov.cn/data#/DataSet/"
                        + _id.replace("/", "%2F")
                    )
                    type_mapping = {
                        "10": "csv",
                        "04": "xlsx",
                        "08": "xml",
                        "09": "json",
                    }
                    type_list = ["api"]
                    for label in typeList:
                        type_list.append(type_mapping[label])
                    metadata["资源格式"] = str(type_list)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_sichuan_neijiang(self):
        pages = Wrapper(317)
        page = 0
        while page < pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["page"] = page
            curl["queries"]["page"] = str(page)
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["id"] = _id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "https://www.neijiang.gov.cn/neiJiangPublicData/resourceCatalog/detail?id="
                        + _id
                    )
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_sichuan_leshan(self):
        pages = Wrapper(1575)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            # time.sleep(3)
            curl["queries"]["pageIndex"] = str(page)
            try:
                ids = self.result_list.get_result_list(curl, pages)
            except:
                self.log_result_list_error(f"continue at page {page}")
                page += 1
                continue
            for _id in ids:
                time.sleep(5)
                curl = self.detail_list_curl.copy()
                curl["queries"]["resourceId"] = _id
                try:
                    metadata = self.detail.get_detail(curl)
                    if bool(metadata):
                        metadata["详情页网址"] = (
                            "https://www.leshan.gov.cn/data/#/source_catalog_detail/"
                            + _id
                            + "/0"
                        )
                        self.metadata_list.append(metadata)
                except:
                    self.logs_detail_error(
                        curl["url"], f"continue at page {page} id {_id}"
                    )
                    continue
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_sichuan_nanchong(self):
        pages = Wrapper(1655)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            try:
                ids = self.result_list.get_result_list(curl, pages)
            except:
                self.log_result_list_error(f"continue at page {page}")
                page += 1
                continue
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["id"] = _id
                try:
                    metadata = self.detail.get_detail(curl)
                    if bool(metadata):
                        metadata["详情页网址"] = (
                            "https://www.nanchong.gov.cn/data/catalog/details.html?id="
                            + _id
                        )
                        self.metadata_list.append(metadata)
                except:
                    self.logs_detail_error(
                        curl["url"], f"continue at page {page} id {_id}"
                    )
            # 响应太慢了，每次都写入吧
            self.save_metadata_as_json(self.output)
            page += 1

    def crawl_sichuan_meishan(self):
        pages = Wrapper(617)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["queries"]["id"] = link["id"]
                metadata = self.detail.get_detail(curl)
                metadata["标题"] = link["serviceName"]
                self.metadata_list.append(metadata)
            time.sleep(5)
            page += 1

    def crawl_sichuan_yibin(self):
        pages = Wrapper(444)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = curl["url"]
                metadata["数据格式"] = link["data_formats"]
                self.metadata_list.append(metadata)
            time.sleep(5)
            page += 1

    def crawl_sichuan_dazhou(self):
        pages = Wrapper(565)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = curl["url"]
                metadata["数据格式"] = link["data_formats"]
                self.metadata_list.append(metadata)
            time.sleep(5)
            page += 1

    def crawl_sichuan_yaan(self):
        pages = Wrapper(840)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = curl["url"]
                metadata["数据格式"] = link["data_formats"]
                self.metadata_list.append(metadata)
            time.sleep(5)
            page += 1

    def crawl_sichuan_bazhong(self):
        pages = Wrapper(1809)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["queries"]["dataCatalogId"] = link
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = (
                    "https://www.bzgongxiang.com/#/dataCatalog/catalogDetail/" + link
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_sichuan_aba(self):
        pages = Wrapper(739)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["queries"]["tableId"] = str(link)
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = (
                    "http://abadata.cn/ABaPrefectureGateway/open/api/getTableDetail?tableId"
                    + str(link)
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_sichuan_ganzi(self):
        # TODO: HTTP ERROR 502
        return
        pages = Wrapper(1192)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageNo"] = page
            links = self.result_list.get_result_list(curl, pages)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["queries"]["mlbh"] = link
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = (
                    "http://182.132.59.11:11180/dexchange/open/#/DataSet/"
                    + link.replace("/", "%2F")
                )
                self.metadata_list.append(metadata)
            page += 1

    def crawl_guizhou_common(self):
        pages = Wrapper(100000)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["pageIndex"] = page
            ids = self.result_list.get_result_list(curl, pages)
            for _id in ids:
                curl = self.detail_list_curl.copy()
                curl["data"]["id"] = _id["id"]
                metadata = self.detail.get_detail(curl)
                metadata["详情页网址"] = (
                    "http://data.guizhou.gov.cn/open-data/" + _id["id"]
                )
                metadata["数据格式"] = _id["resourceFormats"]
                self.metadata_list.append(metadata)
            page += 1

    def crawl_guizhou_guizhou(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_guiyang(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_liupanshui(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_zunyi(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_anshun(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_bijie(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_tongren(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_qianxinan(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_qiandongnan(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_qiannan(self):
        self.crawl_guizhou_common()

    def crawl_guizhou_guianxinqu(self):
        self.crawl_guizhou_common()

    def crawl_shaanxi_shaanxi(self):
        # TODO: HTTP ERROR 502
        pages = Wrapper(16)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page.pageNo"] = str(page)
            metas = self.result_list.get_result_list(curl, pages)
            self.metadata_list.extend(metas)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_ningxia_ningxia(self):
        # TODO: ERR_CONNECTION_RESET
        pages = Wrapper(202)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["queries"]["page"] = str(page)
            links = self.result_list.get_result_list(curl, pages)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl["url"] += link["link"]
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["数据格式"] = (
                        link["data_formats"]
                        if link["data_formats"] != "[]"
                        else "['file']"
                    )
                    metadata["详情页网址"] = curl["url"]
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_ningxia_yinchuan(self):
        pages = Wrapper(169)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["start"] = str((page - 1) * 6)
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                break
            for cata_id, formate in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["cata_id"] = cata_id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "http://data.yinchuan.gov.cn/odweb/catalog/catalogDetail.htm?cata_id="
                        + cata_id
                    )
                    formate_mapping = {"1": "xls", "2": "xml", "3": "json", "4": "csv"}
                    if formate is None:
                        metadata["数据格式"] = "['file']"
                    else:
                        type_list = [
                            formate_mapping[s.strip()] for s in formate.split(",")[:-1]
                        ]
                        metadata["数据格式"] = str(type_list)
                    self.metadata_list.append(metadata)
            page += 1

    def crawl_xinjiang_wulumuqi(self):
        pages = Wrapper(18)
        page = 1
        while page <= pages.obj:
            curl = self.result_list_curl.copy()
            curl["data"]["start"] = str((page - 1) * 6)
            ids = self.result_list.get_result_list(curl, pages)
            if len(ids) == 0:
                break
            for cata_id, formate in ids:
                curl = self.detail_list_curl.copy()
                curl["queries"]["cata_id"] = cata_id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata["详情页网址"] = (
                        "http://zwfw.wlmq.gov.cn/odweb/catalog/catalogDetail.htm?cata_id="
                        + cata_id
                    )
                    formate_mapping = {"1": "xls", "2": "xml", "3": "json", "4": "csv"}
                    if formate is None:
                        metadata["数据格式"] = "['file']"
                    else:
                        type_list = [
                            formate_mapping[s.strip()] for s in formate.split(",")[:-1]
                        ]
                        metadata["数据格式"] = str(type_list)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_metadata_as_json(self.output)
            page += 1

    def crawl_other(self):
        log_error("crawl: 暂无该地 - %s - %s", self.province, self.city)

    def save_metadata_as_json(self, save_dir):
        filename = os.path.join(save_dir, f"{self.province}_{self.city}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.metadata_list, f, ensure_ascii=False)
