import json
import math
import os
import re
import time
import urllib
import bs4
import requests
import execjs

from requests.utils import add_dict_to_cookiejar
from bs4 import BeautifulSoup

from common.constants import REQUEST_MAX_TIME, REQUEST_TIME_OUT
from common.utils import (
    getTotalPagesByTopTitle,
    log_error,
    getCookie,
)
from common.wrapper import Wrapper


class ResultList:
    def __init__(self, province, city) -> None:
        self.province = province
        self.city = city

    def log_request_error(self, status_code, link):
        log_error(
            "%s_%s result list: status code: %d with link %s",
            self.province,
            self.city,
            status_code,
            link,
        )

    def get_result_list(self, curl, pages: "Wrapper" = None):
        func_name = f"result_list_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.result_list_other)
        return func(curl, pages)

    def result_list_common(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        if pages:
            pages.obj = getTotalPagesByTopTitle(soup, 10)  # TODO: items per page
        links = []
        for dataset in (
            soup.find("div", attrs={"class": "bottom-content"})
            .find("ul")
            .find_all("li", recursive=False)
        ):
            link = dataset.find("div", attrs={"class": "cata-title"}).find(
                "a", attrs={"href": re.compile(r"/catalog/.*")}
            )
            data_formats = []
            for data_format in dataset.find(
                "div", attrs={"class": "file-type"}
            ).find_all("li"):
                data_format_text = data_format.get_text()
                if data_format_text == "接口":
                    data_format_text = "api"
                elif data_format_text == "链接":
                    data_format_text = "link"
                data_formats.append(data_format_text.lower())
            links.append({"link": link["href"], "data_formats": str(data_formats)})
        return links

    def result_list_beijing_beijing(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["page"]["countPage"]
        result_list = response_json["object"]["docs"]
        links = [x["url"] for x in result_list]
        return links

    def result_list_tianjin_tianjin(self, curl, pages: "Wrapper"):
        response = requests.get(curl["dataset url"], timeout=REQUEST_TIME_OUT)
        response_json = json.loads(response.text)
        result_list = response_json["dataList"]
        # links = [x['href'] for x in result_list]
        link_format_data = [
            {"link": x["href"], "format": x["documentType"]} for x in result_list
        ]
        response = requests.get(curl["interface url"], timeout=REQUEST_TIME_OUT)
        response_json = json.loads(response.text)
        result_list = response_json["dataList"]
        link_format_data.extend(
            [{"link": x["href"], "format": "api"} for x in result_list]
        )
        return link_format_data

    def result_list_hebei_hebei(self, curl, pages: "Wrapper"):
        session = requests.session()
        session.get(curl["headers"]["Origin"])
        response = session.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["page"]["totalPage"]
        result_list = response_json["page"]["dataList"]
        metadata_ids = [
            {
                "METADATA_ID": x["METADATA_ID"],
                "CREAT_DATE": x["CREAT_DATE"],
                "UPDATE_DATE": x["UPDATE_DATE"],
                "THEME_NAME": x["THEME_NAME"],
            }
            for x in result_list
        ]
        return metadata_ids

    def result_list_shanxi_datong(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        if pages:
            pages.obj = int(
                soup.find("select", attrs={"class": "page-select"})
                .find_all("option")[-1]
                .get_text()
            )
        links = []
        for li in (
            soup.find("div", attrs={"class": "m-cata-list"})
            .find("ul")
            .find_all("li", recursive=False)
        ):
            url = (
                li.find("div", attrs={"class": "item-content"})
                .find("div", attrs={"class": "item-title"})
                .find("a")
                .get("href")
            )
            links.append(url)
            # for info in li.find('div', attrs={'class': 'item-info'}).find_all('div'):
            #     text = info.get_text().strip()  # e.g. 开放状态：无条件开放
        return links

    def result_list_shanxi_changzhi(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            params=curl["queries"],
            cookies=curl["cookies"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["recordsTotal"] / int(curl["data"]["pageLength"])
            )
        result_list = response_json["data"]
        ids = [x["cata_id"] for x in result_list]
        return ids

    def result_list_shanxi_jincheng(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            cookies=curl["cookies"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(json.loads(response.text)["jsonStr"])
        if pages:
            pages.obj = response_json["obj"]["totalPages"]
        result_list = response_json["obj"]["pagingList"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_shanxi_yuncheng(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for li in soup.find("div", attrs={"class": "etab1"}).find("ul").find_all("li"):
            link = li.find("h2", attrs={"class": "tit"}).find("a").get("href")
            links.append(link)
        return links

    def result_list_neimenggu_neimenggu(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["ttlpage"]
        result_list = response_json["data"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_neimenggu_xinganmeng(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            json=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["pages"]
        result_list = response_json["data"]["data"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_dongbei_common(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return []
        soup = BeautifulSoup(response.content, "html.parser")
        if pages:
            pages.obj = getTotalPagesByTopTitle(soup, 10)  # TODO: items per page
        links = []
        for title in soup.find_all("div", attrs={"class": "cata-title"}):
            link = title.find("a", attrs={"href": re.compile("/oportal/catalog/*")})
            links.append(link["href"])
        return links

    def result_list_liaoning_liaoning(self, curl, pages: "Wrapper"):
        return self.result_list_dongbei_common(curl, pages)

    def result_list_liaoning_shenyang(self, curl, pages: "Wrapper"):
        return self.result_list_dongbei_common(curl, pages)

    def result_list_heilongjiang_harbin(self, curl, pages: "Wrapper"):
        return self.result_list_dongbei_common(curl, pages)

    def result_list_shanghai_shanghai(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPageNum"]
        result_list = response_json["data"]["content"]
        dataset_ids = [
            {"datasetId": x["datasetId"], "datasetName": x["datasetName"]}
            for x in result_list
        ]
        return dataset_ids

    def result_list_jiangsu_jiangsu(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(response.text)
        if pages:
            # TODO: pageSize set in `curl.json` : 15
            pages.obj = math.ceil(int(response_json["custom"]["total"]) / 15)
        result_list = response_json["custom"]["resultList"]
        rowGuid_tag_list = [
            (
                x["rowGuid"],
                [n["name"].replace("其他", "file").lower() for n in x["tag"]],
            )
            for x in result_list
        ]
        return rowGuid_tag_list

    def result_list_jiangsu_wuxi(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            params=curl["params"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["recordsTotal"] / int(curl["data"]["pageSize"])
            )
        result_list = response_json["data"]
        cata_ids = [x["cata_id"] for x in result_list]
        return cata_ids

    def result_list_jiangsu_xuzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            json=curl["jsonData"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        response_json = json.loads(response.text)
        result_list = response_json["data"]["rows"]
        mlbhs = [x["mlbh"] for x in result_list]
        return mlbhs

    def result_list_jiangsu_suzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            json=curl["jsonData"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["pages"]
        result_list = response_json["data"]["records"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_jiangsu_nantong(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        result_list = response_json["data"]["content"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_jiangsu_lianyungang(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        if pages:
            pages.obj = int(
                soup.find("div", attrs={"class": "page-footer"}).find("span").get_text()
            )
        result_list = soup.find(
            "div", attrs={"class": "mainz mt30 jQtabcontent"}
        ).find_all("li")
        dmids = []
        for li in result_list:
            title = li.find("h1")
            # s = title.get_text()
            dmid = title.attrs["onclick"].lstrip("view('").rstrip("')")
            dmids.append(dmid)
        return dmids

    def result_list_jiangsu_huaian(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["total"] / int(curl["params"]["pageSize"])
            )
        result_list = response_json["data"]["data"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_jiangsu_yancheng(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["resultData"]["pager"]["pageCount"]
        result_list = response_json["resultData"]["list"]
        catalogPks = [x["catalogPk"] for x in result_list]
        return catalogPks

    def result_list_jiangsu_zhenjiang(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPages"]
        result_list = response_json["data"]["content"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_jiangsu_taizhou(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        if pages:
            pages.obj = getTotalPagesByTopTitle(soup, 10)  # TODO: items per page
        result_list = (
            soup.find("div", attrs={"class": "right-content-catalog"})
            .find("div", attrs={"class": "bottom-content"})
            .find("ul")
            .find_all("li", recursive=False)
        )
        ids = []
        for li in result_list:
            id = (
                li.find("div", attrs={"class": "cata-title"})
                .find("input", attrs={"name": "catalogidinput"})
                .attrs["value"]
            )
            ids.append(id)
        return ids

    def result_list_jiangsu_suqian(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        if pages:
            pages.obj = getTotalPagesByTopTitle(soup, 10)  # TODO: items per page
        result_list = (
            soup.find("div", attrs={"class": "right-content-catalog"})
            .find("div", attrs={"class": "bottom-content"})
            .find("ul")
            .find_all("li", recursive=False)
        )
        id_infos = []
        for li in result_list:
            id = (
                li.find("div", attrs={"class": "cata-title"})
                .find("input", attrs={"name": "catalogidinput"})
                .attrs["value"]
            )
            update_time = (
                li.find("div", attrs={"class": "cata-information"})
                .find("span", text="更新时间：")
                .find_next("span")
                .get_text()
                .strip()
            )
            id_infos.append((id, update_time))
        return id_infos

    def result_list_zhejiang_common(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        html = json.loads(response.text)["data"]
        soup = BeautifulSoup(html, "html.parser")
        if pages:
            # TODO: items per page
            pages.obj = math.ceil(
                int(soup.find("span", attrs={"class": "onColor"}).get_text()) / 10
            )
        iids = []
        for title in soup.find_all("div", attrs={"class": "search_result"}):
            link = title.find("a", attrs={"href": re.compile("../detail/data.do*")})
            parsed_link = urllib.parse.urlparse(link["href"])
            querys = urllib.parse.parse_qs(parsed_link.query)
            querys = {k: v[0] for k, v in querys.items()}
            iids.append(querys)
        return iids

    def result_list_zhejiang_common_2(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            json=curl["jsonData"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["total"] / int(curl["jsonData"]["pageSize"])
            )
        result_list = response_json["data"]["rows"]
        iids = [x["iid"] for x in result_list]
        return iids

    def result_list_zhejiang_zhejiang(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common(curl, pages)

    def result_list_zhejiang_hangzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            # TODO: pageSize set in `curl.json` : 10
            pages.obj = math.ceil(response_json["total"] / 10)
        result_list = response_json["rows"]
        id_formats = [(x["id"], x["source_type"].lower()) for x in result_list]
        return id_formats

    def result_list_zhejiang_ningbo(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            json=curl["jsonData"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["list"]["pages"]
        result_list = response_json["list"]["rows"]
        uuids = [x["uuid"] for x in result_list]
        return uuids

    def result_list_zhejiang_wenzhou(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common(curl, pages)

    def result_list_zhejiang_jiaxing(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common(curl, pages)

    def result_list_zhejiang_shaoxing(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common_2(curl, pages)

    def result_list_zhejiang_jinhua(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common(curl, pages)

    def result_list_zhejiang_quzhou(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common(curl, pages)

    def result_list_zhejiang_zhoushan(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            json=curl["jsonData"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["pages"]
        result_list = response_json["data"]["records"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_zhejiang_taizhou(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common_2(curl, pages)

    def result_list_zhejiang_lishui(self, curl, pages: "Wrapper"):
        return self.result_list_zhejiang_common(curl, pages)

    def result_list_anhui_anhui(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["pages"]
        result_list = response_json["data"]["rows"]
        rids = [x["rid"] for x in result_list]
        return rids

    def result_list_anhui_hefei(self, curl, pages: "Wrapper"):
        # 使用 session 保持会话
        session = requests.session()
        res1 = session.get(curl["url"], headers=curl["headers"], params=curl["queries"])
        jsl_clearance_s = re.findall(r"cookie=(.*?);location", res1.text)[0]
        # 执行 js 代码 # TODO: an available JavaScript runtime needed
        jsl_clearance_s = str(execjs.eval(jsl_clearance_s)).split("=")[1].split(";")[0]
        # add_dict_to_cookiejar 方法添加 cookie
        add_dict_to_cookiejar(session.cookies, {"__jsl_clearance_s": jsl_clearance_s})
        res2 = session.get(curl["url"], headers=curl["headers"], params=curl["queries"])
        # 提取 go 方法中的参数
        data = json.loads(re.findall(r";go\((.*?)\)", res2.text)[0])
        jsl_clearance_s = getCookie(data)
        # 修改 cookie
        add_dict_to_cookiejar(session.cookies, {"__jsl_clearance_s": jsl_clearance_s})
        try:
            response = session.get(
                curl["url"], headers=curl["headers"], params=curl["queries"]
            )
        except:
            self.log_request_error(-1, curl["url"])
            return dict()

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPageCount"]
        result_list = response_json["data"]["result"]
        ids = [(str(x["id"]), x["zyId"]) for x in result_list]
        return ids

    def result_list_anhui_wuhu(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text.replace('\\"', '"')[1:-1])
        if pages:
            pages.obj = response_json["allPage"]
        result_list = response_json["smcDataSetList"]
        # 目前所有数据集中只出现了每日和每年
        frequency_mapping = {
            "1": "实时",
            "2": "每日",
            "3": "每周",
            "4": "每月",
            "5": "每半年",
            "6": "每年",
        }
        dataset_metadata = []
        for result in result_list:
            dataset_id = result["id"]
            metadata_mapping = {
                "标题": result["datasetName"],
                "机构分类": result["isValid"],
                "url": "https://data.wuhu.cn/datagov-ops/data/toDetailPage?id="
                + dataset_id,
                "领域名称": result["dataType"],
                "数据集创建时间": result["createDate"],
                "数据集更新时间": result["updateDate"].split(" ")[0],
                "开放类型": "无条件开放" if result["openType"] == "1" else "有条件开放",
                "更新频率": frequency_mapping[result["dataUpdateFrequency"]],
                "数据简介": result["summary"],
                "资源格式": (
                    ["api"]
                    if result["dataResourceType"] == "2"
                    else [
                        file["fileType"].split(".")[-1] for file in result["fileList"]
                    ]
                ),
            }
            dataset_metadata.append(metadata_mapping)
        return dataset_metadata

    def result_list_anhui_bengbu(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = BeautifulSoup(response.content, "html.parser")
        if pages:
            m = re.search(r"共\((\d+)\)条数据", soup.find("script").get_text())
            if m:
                pages.obj = math.ceil(
                    int(m.group(1)) / int(curl["queries"]["pageSize"])
                )
        links = []
        for dataset in (
            soup.find("div", attrs={"class": "sj_list"})
            .find("ul")
            .find_all("li", recursive=False)
        ):
            link = dataset.find("div", attrs={"class": "sjinfo"}).find(
                "a", attrs={"href": re.compile("site/tpl/.*")}
            )
            links.append(link["href"])
        return links

    def result_list_anhui_huainan(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(response_json["total"] / int(curl["queries"]["size"]))
        result_list = response_json["rows"]
        data_ids = [x["dataId"] for x in result_list]
        return data_ids

    def result_list_anhui_huaibei(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["result"]["count"] / int(curl["queries"]["pageSize"])
            )
        result_list = response_json["result"]["data"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_anhui_huangshan(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = BeautifulSoup(response.content, "html.parser")
        if pages:
            m = re.search(r"pageCount:(\d+),", soup.find("script").get_text())
            if m:
                pages.obj = int(m.group(1))
        links = []
        for dataset in soup.find("ul", attrs={"class": "clearfix"}).find_all(
            "li", recursive=False
        ):
            div = dataset.find("div", attrs={"class": "sjcon clearfix"}).find(
                "div", attrs={"class": "dataresources-con"}
            )
            link = div.find("a", attrs={"href": re.compile("/site/tpl/.*")})
            depart = (
                div.find("p", attrs={"class": "xx clearfix"})
                .find("span", attrs={"class": "n2"})
                .get_text()
                .split("：")[-1]
                .strip()
            )
            cata = (
                div.find("p", attrs={"class": "xx clearfix"})
                .find("span", attrs={"class": "n3"})
                .get_text()
                .split("：")[-1]
                .strip()
            )
            format_list = []
            for data_format in (
                div.find("p", attrs={"class": "zyzy clearfix"})
                .find("span", attrs={"class": "link"})
                .find_all("a", attrs={"class": "j-login"})
            ):
                format_list.append(data_format.find("em").get_text().lower().strip())
            if len(format_list) == 0:
                format_list = ["file"]
            links.append((link["href"], depart, cata, format_list))
        return links

    def result_list_anhui_chuzhou(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["totalElements"] / int(curl["queries"]["size"])
            )
        result_list = response_json["content"]
        dataset_metadata = []
        for result in result_list:
            metadata_mapping = {
                "标题": result["name"],
                "提供单位": result["companys"]["title"],
                "开放主题": result["fields"]["title"],
                "发布时间": result["createtime"].split(" ")[0],
                "更新时间": result["updatetime"].split(" ")[0],
                "开放类型": (
                    "无条件开放" if result["sharetype"] == "2" else "有条件开放"
                ),
                "描述": result["description"],
                "开放领域": result["themes"]["title"],
                "关键词": result["keyword"],
            }
            dataset_metadata.append(metadata_mapping)
        return dataset_metadata

    def result_list_anhui_suzhou(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_anhui_luan(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_anhui_chizhou(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = BeautifulSoup(response.content, "html.parser")
        if pages:
            pages.obj = int(soup.find("pagination").attrs["pagecount"])
        metadata_list = []
        for dataset in soup.find("div", attrs={"id": "listContent"}).find_all(
            "div", attrs={"class": "list-content-item"}
        ):
            dataset_metadata = {}
            dataset_metadata["标题"] = (
                dataset.find("div", attrs={"class": "text ell"}).get_text().strip()
            )
            dataset_metadata["数据格式"] = (
                dataset.find("div", attrs={"class": "file-type-wrap"})
                .get_text()
                .strip()
                .lower()
            )
            for field in dataset.find_all("div", attrs={"class": "content-item ell"}):
                field_name = field.get_text().strip().split("：")[0]
                field_text = field.get_text().strip().split("：")[1]
                dataset_metadata[field_name] = field_text

            dataset_metadata["url"] = (
                f"{curl['url']}?"
                f"{'&'.join([f'{key}={val}' for key, val in curl['queries'].items()])}"
                f"&file={os.path.basename(os.path.splitext(dataset.find('a')['href'])[0])}"
            )
            metadata_list.append(dataset_metadata)
        return metadata_list

    def result_list_fujian_fujian(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            headers=curl["headers"],
            params=curl["params"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["total"] / int(curl["params"]["pageSize"])
            )
        result_list = response_json["data"]["rows"]
        ids = [x["catalogID"] for x in result_list]
        return ids

    def result_list_fujian_fuzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        result_list = json.loads(response_json["dataList"])["list"]
        res_ids = [x["resId"] for x in result_list]
        return res_ids

    def result_list_fujian_xiamen(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["page"]["totalPage"]
        result_list = response_json["data"]["list"]
        catalog_ids = [x["catalogId"] for x in result_list]
        return catalog_ids

    def result_list_jiangxi_jiangxi(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["page"]["pages"]
        result_list = response_json["data"]
        data_ids = [
            {"dataId": x["dataId"], "filesType": x["filesType"]} for x in result_list
        ]
        return data_ids

    def result_list_jiangxi_ganzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        ids = []
        for title in soup.find_all("div", attrs={"class": "com_shiye"}):
            id = title.find("a", attrs={"class": "shiy_rigA1"}).get("id")
            ids.append(id)
        return ids

    def result_list_shandong_common(self, curl, prefix):
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all("div", attrs={"class": "cata-title"}):
            link = title.find("a", attrs={"href": re.compile(f"/{prefix}/catalog/*")})
            file_type_elements = title.parent.find(
                "div", attrs={"class": "file-type"}
            ).findChildren("li")
            data_formats = list(
                map(lambda x: x["class"][0].lower(), file_type_elements)
            )  # class 标签内，“接口”就叫“api”
            if len(data_formats) == 0:
                data_formats.append("file")
            links.append({"link": link["href"], "data_formats": data_formats})
        return links

    def result_list_shandong_shandong(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "portal")

    def result_list_shandong_jinan(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "jinan")

    def result_list_shandong_qingdao(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "qingdao")

    def result_list_shandong_zibo(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "zibo")

    def result_list_shandong_zaozhuang(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "zaozhuang")

    def result_list_shandong_dongying(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "dongying")

    def result_list_shandong_yantai(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "yantai")

    def result_list_shandong_weifang(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "weifang")

    def result_list_shandong_jining(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "jining")

    def result_list_shandong_taian(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "taian")

    def result_list_shandong_weihai(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "weihai")

    def result_list_shandong_rizhao(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "rizhao")

    def result_list_shandong_linyi(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "linyi")

    def result_list_shandong_dezhou(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "dezhou")

    def result_list_shandong_liaocheng(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "liaocheng")

    def result_list_shandong_binzhou(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "binzhou")

    def result_list_shandong_heze(self, curl, pages: "Wrapper"):
        return self.result_list_shandong_common(curl, "heze")

    def result_list_hubei_wuhan(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["pages"]
        result_list = response_json["data"]
        cataIds = list(map(lambda x: x["cataId"], result_list["records"]))
        return cataIds

    def result_list_hubei_huangshi(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"], headers=curl["headers"], verify=False, timeout=REQUEST_TIME_OUT
        )
        data = json.loads(response.text)["data"]["list"]
        ids = list(map(lambda x: x["infoid"], data))
        return ids

    def result_list_hubei_yichang(self, curl, pages: "Wrapper"):
        if curl["crawl_type"] == "dataset":
            response = requests.post(
                curl["url"],
                json=curl["data"],
                headers=curl["headers"],
                verify=False,
                timeout=REQUEST_TIME_OUT,
            )
            response_json = json.loads(response.text)
            if pages:
                pages.obj = math.ceil(
                    response_json["data"]["total"] / int(curl["data"]["pageSize"])
                )
            result_list = response_json["data"]
            cataIds = list(map(lambda x: x["iid"], result_list["rows"]))
            return cataIds
        else:
            response = requests.post(
                curl["url"],
                data=curl["data"],
                headers=curl["headers"],
                timeout=REQUEST_TIME_OUT,
            )
            response_json = json.loads(response.text)
            if pages:
                pages.obj = math.ceil(
                    response_json["data"]["total"] / int(curl["data"]["pageSize"])
                )
            result_list = response_json["data"]
            cataIds = list(map(lambda x: x["iid"], result_list["list"]))
            return cataIds

    def result_list_hubei_ezhou(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"], headers=curl["headers"], verify=False, timeout=REQUEST_TIME_OUT
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        if pages:
            pages.obj = math.ceil(
                int(soup.find("span", attrs={"class": "num"}).get_text())
                / 7  # TODO: items per page
            )
        ul = soup.find("ul", class_="sjj_right_list")
        links = []
        if not ul:
            return []
        for li in ul.find_all("li", class_="fbc"):
            h3 = li.find("h3")
            if h3 is not None:
                a = h3.find("a")
                # links.append(a['href'])
                links.append(
                    "/".join(curl["url"].split("/")[:-1]) + (a["href"].lstrip("."))
                )
        return links

    def result_list_hubei_jingzhou(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        if pages:
            pages.obj = getTotalPagesByTopTitle(soup, 10)  # TODO: items per page
        divs = soup.find_all("div", class_="cata-title")
        ids = []
        for div in divs:
            if div:
                a = div.find("a")
                ids.append(a["href"].split("/")[-1])
        return ids

    def result_list_hubei_suizhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(response_json["count"] / int(curl["data"]["limit"]))
        ids = list(map(lambda x: x["id"], response_json["list"]))
        return ids

    def result_list_hunan_yueyang(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        if pages:
            m = re.search(
                r"\d+/(\d+)页", soup.find("span", class_="current").parent.get_text()
            )
            if m:
                pages.obj = int(m.group(1))
        divs = soup.find_all("div", class_="szkf-box-list")
        ids = []
        for div in divs:
            a = div.find_next("div", class_="name").find_next("a")
            ids.append(a["href"].split("=")[1])
        return ids

    def result_list_hunan_changde(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["pages"]
        cata_ids = list(map(lambda x: x["CATA_ID"], response_json["list"]))
        return cata_ids

    def result_list_hunan_common(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        if pages:
            m = re.search(
                r"\d+/(\d+)页", soup.find("span", class_="current").parent.get_text()
            )
            if m:
                pages.obj = int(m.group(1))
        tables = soup.find_all("table", class_="table-data")
        ids = []
        for table in tables:
            tr = table.find_all("tr")[-1]
            a = tr.find_next("a")
            ids.append(a["href"].split("=")[1])
        return ids

    def result_list_hunan_chenzhou(self, curl, pages: "Wrapper"):
        return self.result_list_hunan_common(curl, pages)

    def result_list_hunan_yiyang(self, curl, pages: "Wrapper"):
        return self.result_list_hunan_common(curl, pages)

    def result_list_guangdong_guangdong(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)["data"]
        if pages:
            pages.obj = response_json["page"]["pages"]
        ids = list(map(lambda x: x["resId"], response_json["page"]["list"]))
        return ids

    def result_list_guangdong_guangzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                int(response_json["total"]) / int(response_json["pageSize"])
            )
        ids = list(map(lambda x: x["sid"], response_json["body"]))
        return ids

    def result_list_guangdong_shenzhen(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if curl["crawl_type"] == "dataset":
            response_json = json.loads(response_json["dataList"])
        else:
            response_json = json.loads(response_json["apiList"])
        if pages:
            pages.obj = math.ceil(
                (
                    response_json["otherData"]["cityCount"]
                    + response_json["otherData"]["countyCount"]
                )
                / int(response_json["pageSize"])
            )
        ids = list(map(lambda x: x["resId"], response_json["list"]))
        return ids

    def result_list_guangdong_zhongshan(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        if pages:
            m = re.search(
                r"javascript:toPage\((\d+)\);",
                soup.find("div", class_="f-page")
                .find("ul")
                .find_all("li")[-1]
                .find("a")["href"],
            )
            if m:
                pages.obj = int(m.group(1))
        dl = soup.find("dl")
        ids = []
        for dd in dl.find_all("dd"):
            href = dd.find("h2").find("a")["href"]
            ids.append(href.split("'")[1])
        return ids

    def result_list_guangxi_guangxi(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_nanning(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_liuzhou(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_guilin(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_wuzhou(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_beihai(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_fangchenggang(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_qinzhou(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_guigang(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_yulin(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_baise(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_hezhou(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_hechi(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_laibin(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_guangxi_chongzuo(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_hainan_hainan(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPages"]
        result_list = response_json["data"]["content"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_hainan_hainansjj(self, curl, pages: "Wrapper"):
        return self.result_list_hainan_hainan(curl, pages)

    def result_list_hainan_hainansjjk(self, curl, pages: "Wrapper"):
        return self.result_list_hainan_hainan(curl, pages)

    def result_list_chongqing_chongqing(self, curl, pages: "Wrapper"):
        key_map = {
            "resourceName": "标题",
            "resourceDesc": "摘要",
            "organizationName": "资源提供方",
            "updateDate": "更新时间",
            "openAttr": "开放类型",
            "fileTypes": "资源格式",
            "renewCycle": "更新周期",
        }
        openAttr_map = {"CONDITIONAL": "有条件开放", "UNCONDITIONAL": "无条件开放"}
        renewCycle_map = {
            "REAL_TIME": "实时",
            "EVERY_DAY": "每日",
            "EVERY_WEEK": "每周",
            "EVERY_MONTH": "每月",
            "EVERY_QUARTER": "每季度",
            "HALF_YEAR": "每半年",
            "EVERY_YEAR": "每年",
            "IRREGULAR": "不定期",
            "OTHER": "其他",
        }
        metadatas = []
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return []
            try:
                response = requests.post(
                    curl["url"],
                    json=curl["data"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT * 1000,
                )
                break
            except:
                time.sleep(5)
        result_list_json = json.loads(response.text)["data"]["result"]["data"]
        for detail_json in result_list_json:
            dataset_metadata = {}
            for key, value in key_map.items():
                if key == "openAttr" and detail_json[key] is not None:
                    detail_json[key] = openAttr_map[detail_json[key]]
                if key == "renewCycle" and detail_json[key] is not None:
                    detail_json[key] = renewCycle_map[detail_json[key]]
                if key == "fileTypes":
                    if detail_json["shareType"] == "FILE":
                        detail_json[key] = (
                            "[" + detail_json[key] + "]"
                            if detail_json[key] is not None
                            else "[]"
                        )
                    else:
                        detail_json[key] = "[api]"
                if key in ["updateDate"] and detail_json[key] is not None:
                    detail_json[key] = detail_json[key][:10]
                dataset_metadata[value] = detail_json[key]
            dataset_metadata["行业分类"] = (
                detail_json["tags"]["INDUSTRY"]
                if "INDUSTRY" in detail_json["tags"]
                else None
            )
            dataset_metadata["主题分类"] = (
                detail_json["tags"]["TOPIC"] if "TOPIC" in detail_json["tags"] else None
            )
            dataset_metadata["url"] = (
                "https://data.cq.gov.cn/rop/assets/detail?resId=" + detail_json["id"]
            )
            metadatas.append(dataset_metadata)
        return metadatas

    def result_list_sichuan_sichuan(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_sichuan_chengdu(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_sichuan_panzhihua(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_sichuan_zigong(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["total"] / int(curl["queries"]["limit"])
            )
        result_list = response_json["data"]["rows"]
        ids = [x["id"] for x in result_list]
        return ids

    def result_list_sichuan_luzhou(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["result"]["pages"]
        result_list = response_json["result"]["rows"]
        ids = [
            (x["id"], x["openType"], x["publishTime"], x["updateTime"])
            for x in result_list
        ]
        return ids

    def result_list_sichuan_deyang(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPage"]
        result_list = response_json["data"]["rows"]
        ids = [x["mlbh"] for x in result_list]
        return ids

    def result_list_sichuan_mianyang(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        try:
            response_json = json.loads(response.text)
            if pages:
                pages.obj = math.ceil(
                    response_json["elementthing"]["listPage"]["total"]
                    / int(curl["queries"]["limitNum"])
                )
            result_list = response_json["elementthing"]["listPage"]["list"]
            ids = [x["id"] for x in result_list]
            return ids
        except:
            self.log_request_error(-1, curl["url"])
            return []

    def result_list_sichuan_guangyuan(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPage"]
        result_list = response_json["data"]["rows"]
        ids = [x["ID"] for x in result_list]
        return ids

    def result_list_sichuan_suining(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        result_list = response_json["data"]["rows"]
        ids = [(x["mlbh"], x["wjlx"]) for x in result_list]
        return ids

    def result_list_sichuan_neijiang(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            params=curl["queries"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["data"]["totalPages"]
        result_list = response_json["data"]["content"]
        ids = [str(x["id"]) for x in result_list]
        return ids

    def result_list_sichuan_leshan(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["recordCount"] / int(curl["queries"]["pageSize"])
            )
        result_list = response_json["data"]["rows"]
        ids = [str(x["resourceId"]) for x in result_list]
        return ids

    def result_list_sichuan_nanchong(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(response_json["count"] / int(curl["queries"]["rows"]))
        result_list = response_json["data"]
        ids = [str(x["ID"]) for x in result_list]
        return ids

    def result_list_sichuan_meishan(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
            stream=True,
            verify=False,
        )
        response_json = json.loads(response.text)
        if pages:
            pages.obj = response_json["result"]["pages"]
        result_list = response_json["result"]["rows"]
        links = [link for link in result_list]
        return links

    def result_list_sichuan_yibin(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_sichuan_dazhou(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_sichuan_yaan(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_sichuan_bazhong(self, curl, pages: "Wrapper"):
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return []
            try:
                response = requests.post(
                    curl["url"],
                    params=curl["queries"],
                    json=curl["data"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                    verify=False,
                )
                break
            except:
                time.sleep(5)
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["total"] / int(curl["queries"]["pageSize"])
            )
        result_list = response_json["data"]["data"]
        links = [link["catalogInfo"]["id"] for link in result_list]
        return links

    def result_list_sichuan_aba(self, curl, pages: "Wrapper"):
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return []
            try:
                response = requests.get(
                    curl["url"],
                    params=curl["queries"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                    verify=False,
                )
                break
            except:
                time.sleep(5)
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["data"]["resultMap"]["total"]
                / int(curl["queries"]["count"])
            )
        result_list = response_json["data"]["resultMap"]["abaTableList"]
        links = [link["tableId"] for link in result_list]
        return links

    def result_list_sichuan_ganzi(self, curl, pages: "Wrapper"):
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return []
            try:
                response = requests.post(
                    curl["url"],
                    json=curl["data"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                    verify=False,
                )
                break
            except:
                time.sleep(5)
        response_json = json.loads(response.text)
        result_list = response_json["data"]["rows"]
        links = [link["mlbh"] for link in result_list]
        return links

    def result_list_guizhou_common(self, curl, pages: "Wrapper"):
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return []
            try:
                response = requests.post(
                    curl["url"],
                    json=curl["data"],
                    headers=curl["headers"],
                    verify=False,
                    timeout=REQUEST_TIME_OUT,
                )
                break
            except:
                time.sleep(5)
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(response_json["total"] / curl["data"]["pageSize"])
        result_list = response_json["data"]
        ids = [
            {"id": x["id"], "resourceFormats": x["resourceFormats"]}
            for x in result_list
        ]
        return ids

    def result_list_guizhou_guizhou(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_guiyang(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_liupanshui(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_zunyi(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_anshun(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_bijie(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_tongren(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_qianxinan(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_qiandongnan(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_qiannan(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_guizhou_guianxinqu(self, curl, pages: "Wrapper"):
        return self.result_list_guizhou_common(curl, pages)

    def result_list_shaanxi_shaanxi(self, curl, pages: "Wrapper"):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        result_list = response_json[0]["result"]

        metadata_list = []

        for result in result_list:
            dataset_metadata = {}

            dataset_metadata["标题"] = result["sdataName"]
            dataset_metadata["来源部门"] = result["sorgName"]
            dataset_metadata["所属主题"] = result["sdataTopic"]
            dataset_metadata["发布时间"] = result["spubDate"].split(" ")[0].strip()
            dataset_metadata["更新时间"] = result["spubDate"].split(" ")[0].strip()
            dataset_metadata["标签"] = result["keywords"] if "keyword" in result else ""
            dataset_metadata["描述"] = result["sdataIntro"]
            dataset_metadata["数据格式"] = result["sdataFormats"]
            dataset_metadata["详情页网址"] = (
                "http://www.sndata.gov.cn/info?id=" + result["id"]
            )

            metadata_list.append(dataset_metadata)
        return metadata_list

    def result_list_ningxia_ningxia(self, curl, pages: "Wrapper"):
        return self.result_list_common(curl, pages)

    def result_list_common_2(self, curl, pages: "Wrapper"):
        response = requests.post(
            curl["url"],
            params=curl["queries"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        response_json = json.loads(response.text)
        if pages:
            pages.obj = math.ceil(
                response_json["recordsTotal"] / int(curl["data"]["length"])
            )
        result_list = response_json["data"]
        ids = [(str(x["cata_id"]), x["conf_catalog_format"]) for x in result_list]
        return ids

    def result_list_ningxia_yinchuan(self, curl, pages: "Wrapper"):
        return self.result_list_common_2(curl, pages)

    def result_list_xinjiang_wulumuqi(self, curl, pages: "Wrapper"):
        return self.result_list_common_2(curl, pages)

    def result_list_other(self):
        log_error("result list: 暂无该地 - %s - %s", self.province, self.city)
