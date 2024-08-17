import base64
import copy
import datetime
import json
import os
import re
import time
import bs4
import execjs
import unicodedata as ucd
import requests

from bs4 import BeautifulSoup
from requests.utils import add_dict_to_cookiejar
from urllib.parse import parse_qs, quote, urlparse

from common.constants import REQUEST_MAX_TIME, REQUEST_TIME_OUT
from common.utils import log_error, getCookie
from crawler.downloader import Downloader


class Detail:
    def __init__(self, province, city, download_files=False) -> None:
        self.province = province
        self.city = city
        self.download_files = download_files
        self.downloader = Downloader(province, city)
        self.downloader.file_dir = os.path.join(
            Downloader.file_dir, f"{self.province}/{self.city}"
        )

    def log_request_error(self, status_code, link):
        log_error(
            "%s_%s detail: status code: %d with link %s",
            self.province,
            self.city,
            status_code,
            link,
        )

    def get_detail(self, curl):
        func_name = f"detail_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.detail_other)
        return func(curl)

    def common_download(self, soup, curl, metadata):
        if self.download_files:
            metadata["file_name"] = []
        org_url = urlparse(curl["url"])
        file_link_fmt = (
            f"{org_url.scheme}://{org_url.netloc}{soup.find('input', attrs={'id':'rootPath'})['value']}/catalog/download"
            f"?cataId={soup.find('input', attrs={'id': 'cata_id'})['value']}"
            f"&cataName={soup.find('input', attrs={'id': 'cata_name'})['value']}"
            "&idInRc={}"
        )

        if "资源格式" not in metadata:
            metadata["资源格式"] = []

        file_list = soup.find("li", attrs={"name": "file-download"})
        if file_list and file_list.find("table").find("tbody").find("tr"):
            for item in file_list.find("table").find("tbody").find_all("tr"):
                file_fmt = item.get("fileformat") or item.get("fileFormat")
                if not file_fmt:
                    continue
                metadata["资源格式"].append(file_fmt)
                if self.download_files:
                    file = item.find("input", attrs={"type": "checkbox"})
                    self.downloader.start_download(
                        file_link_fmt.format(file["file-id"]), file["file-name"]
                    )
                    metadata["file_name"].append(file["file-name"])
            return metadata

        file_list_link = (
            f"{org_url.scheme}://{org_url.netloc}{soup.find('input', attrs={'id':'rootPath'})['value']}/catalog/getResourceWithFormat"
            f"?cataId={soup.find('input', attrs={'id': 'cata_id'})['value']}"
            "&pageNum=1&pageSize=100&fileFormat="
        )
        response = requests.get(
            file_list_link,
            headers=curl["headers"],
        )
        if response.ok and len(response.text) > 0:
            try:
                file_list = json.loads(response.text)
                for file in file_list["object"]["records"]:
                    metadata["资源格式"].append(file["fileFormat"])
                    if self.download_files:
                        file_name = f"{file['fileName']}.{file['fileFormat']}"
                        self.downloader.start_download(
                            file_link_fmt.format(file["idInRc"]), file_name
                        )
                        metadata["file_name"].append(file_name)
            except:
                pass
        return metadata

    def detail_beijing_beijing(self, curl):
        # 资源摘要/摘要 并存
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {"url": curl["url"]}
        information = soup.find("div", attrs={"id": "zydtCont1"}).find(
            "div", attrs={"class": "sjdetails_cardmain_frame"}
        )
        pattern = "(.+)[ ：\t](.+)"
        for line in information.contents:
            if str(line) == "<br/>":
                continue
            try:
                line = line.strip()
                match = re.match(pattern, line)
                if match:
                    key, value = match.groups()
                    dataset_matadata[key] = value
            except TypeError:
                # TypeError: 'NoneType' object is not callable
                # e.g. https://data.beijing.gov.cn/zyml/ajg/sswj/10784.htm
                dataset_matadata = {"url": curl["url"]}
                for x in information.get_text().strip().split("\n"):
                    match = re.match(pattern, x.strip())
                    if match:
                        key, value = match.groups()
                        dataset_matadata[key] = value
                break
        return dataset_matadata

    def detail_tianjin_tianjin(self, curl):
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        if response.status_code == 304:
            new_headers = curl["headers"].copy()
            new_headers["If-None-Natch"] = ""
            new_headers["If-Modified-Since"] = ""
            response = requests.get(
                curl["url"], headers=new_headers, timeout=REQUEST_TIME_OUT
            )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        try:
            title = (
                soup.find("div", attrs={"class": "title f-cb mg-b4"})
                .find("span")
                .get_text()
            )
            dataset_matadata["数据集名称"] = title.strip()
            table = soup.find("div", attrs={"class": "slidecont"})
            for tr in table.find_all("tr"):
                for th, td in zip(tr.find_all("th"), tr.find_all("td")):
                    dataset_matadata[th.get_text().strip()] = td.get_text().strip()
        except AttributeError as e:
            print(curl["url"])
        return dataset_matadata

    def detail_hebei_hebei(self, curl):
        list_fields = ["信息资源分类", "开放条件"]
        table_fields = [
            "信息资源名称",
            "信息资源代码",
            "资源版本",
            "资源提供方",
            "资源提供方内部部门",
            "资源提供方代码",
            "资源摘要",
            "格式分类",
            "标签",
            "格式类型",
            "格式描述",
            "更新周期",
            "资源联系人",
            "联系电话",
            "电子邮箱",
        ]

        response = requests.post(
            curl["url"],
            cookies=curl["cookies"],
            headers=curl["headers"],
            data=curl["data"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        for tr in soup.find("div", attrs={"class": "info"}).find_all("tr"):
            td = tr.find("td")
            td_name = td.get_text().split("：")[0].strip()
            if td_name in list_fields:
                td_text = tr.find_next("td").get_text().strip()
                dataset_matadata[td_name] = td_text
        table = soup.find("div", attrs={"class": "resdetails_table_box page1"})
        p_name = ""
        for p in table.find_all("p"):
            p_text = p.get_text().strip()
            if len(p_text) > 0 and p_text[-1] == ":":
                p_name = p_text[:-1]
            elif p_name != "":
                p_text = ucd.normalize("NFKC", p_text).replace(" ", "")
                dataset_matadata[p_name] = p_text
        return dataset_matadata

    def detail_shanxi_datong(self, curl):
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        for info in soup.find_all("div", attrs={"class": "info-list"}):
            for li in info.find("ul").find_all("li"):
                key = li.find("div", attrs={"class": "info-header"}).get_text()
                value = li.find("div", attrs={"class": "info-body"}).get_text()
                dataset_metadata[key] = value
        data_formats = []
        files_div = soup.find("div", attrs={"target": "data-download"})
        try:
            if files_div:
                for item in files_div.find("tbody").find_all("tr"):
                    data_formats.extend(item["class"])

                    if self.download_files:
                        file_link = item.find("a")["href"]
                        params = parse_qs(urlparse(file_link).query)
                        file_name = params["name"][0] if "name" in params else None
                        if file_name:
                            self.downloader.start_download(file_link, file_name)
                            if "file_name" not in dataset_metadata:
                                dataset_metadata["file_name"] = []
                            dataset_metadata["file_name"].append(file_name)

                dataset_metadata["data_formats"] = data_formats
        except:
            self.log_request_error(-1, response.url)
        dataset_metadata["url"] = response.url
        return dataset_metadata

    def detail_shanxi_changzhi(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        for info in soup.find_all("div", attrs={"class": "info-list"}):
            for li in info.find("ul").find_all("li"):
                key = li.find("div", attrs={"class": "info-header"}).get_text()
                value = li.find("div", attrs={"class": "info-body"}).get_text().strip()
                dataset_metadata[key] = value
        data_formats = []
        if soup.find("li", attrs={"action": "data-download"}) is not None:
            for item in (
                soup.find("li", attrs={"action": "data-download"})
                .find("div", attrs={"class": "tab-header"})
                .find("ul")
                .find_all("li")
            ):
                data_formats.append(item.get_text())
            dataset_metadata["data_formats"] = data_formats
        dataset_metadata["url"] = (
            f"{curl['url']}?"
            f"{'&'.join([f'{key}={val}' for key, val in curl['queries'].items()])}"
        )
        return dataset_metadata

    def detail_shanxi_jincheng(self, curl):
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        dataset_metadata["title"] = (
            soup.find("div", attrs={"class": "dt-title"}).get_text().strip()
        )
        dataset_metadata["资源摘要"] = (
            soup.find("div", attrs={"class": "dl-part"})
            .find("p", attrs={"class": "dl-part-text"})
            .get_text()
            .strip()
        )
        for ul in soup.find("div", attrs={"class": "dt-con-text"}).find_all("ul"):
            for li in ul.find_all("li"):
                key = li.find("span").get_text().strip()
                value = li.find("span").next_sibling.strip()
                dataset_metadata[key] = value
        dataset_metadata["url"] = response.url
        return dataset_metadata

    def detail_shanxi_yuncheng(self, curl):
        response = requests.post(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {"url": response.url}

        table_div = soup.find("div", attrs={"class": "details-con"})
        title = table_div.find("div").find("p").find("b").get_text().strip()
        dataset_metadata["title"] = title
        for tr in (
            table_div.find("table", attrs={"class": "details-tab"})
            .find("tbody")
            .find_all("tr")
        ):
            tr_text = []
            for td in tr.find_all("td"):
                tr_text.append(td.get_text().strip())
            dataset_metadata[tr_text[0]] = tr_text[1]
        tr_text = []
        for td in (
            table_div.find("table", attrs={"class": "details-tab-active"})
            .find("tbody")
            .find("tr")
            .find_all("td")
        ):
            tr_text.append(td.get_text().strip())
        dataset_metadata[tr_text[0]] = tr_text[1]
        return dataset_metadata

    def detail_neimenggu_neimenggu(self, curl):
        key_map = {
            "title": "目录名称",
            "openType": "开放状态",
            "remark": "简介",
            "subjectName": "所属主题",
            "dataUpdatePeriod": "更新频率",
            "orgName": "提供单位",
            "publishDate": "发布日期",
            "updateDate": "更新日期",
        }

        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )

        dataset_matadata = {}
        detail_json = json.loads(response.text)["record"]
        for key, value in key_map.items():
            dataset_matadata[value] = detail_json[key]
        dataset_matadata["url"] = curl["detail_url"] + curl["data"]["id"]
        return dataset_matadata

    def detail_neimenggu_xinganmeng(self, curl):
        key_map = {
            "name": "目录名称",
            "description": "简介",
            "subjectName": "所属主题",
            "deptName": "提供单位",
            "releaseTime": "发布日期",
            "updateTime": "更新日期",
        }

        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT, verify=False
        )
        dataset_metadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_dongbei_common(self, curl):
        list_fields = ["来源部门", "重点领域", "开放条件"]
        table_fields = [
            "接口量",
            "所属行业",
            "更新频率",
            "部门电话",
            "标签",
            "描述",
        ]
        if self.city == "liaoning":
            list_fields.extend(["数据更新时间"])
            table_fields = [
                "数据量",
                "部门邮箱",
            ]
        elif self.city == "shenyang":
            list_fields.extend(["发布时间", "更新时间"])
            table_fields = [
                "文件量",
                "部门邮箱",
            ]
        elif self.city == "harbin":
            list_fields.extend(["发布时间", "更新时间"])
            table_fields = [
                "数据量",
            ]

        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = (
            soup.find("ul", attrs={"class": "d-title pull-left"}).find("h4").get_text()
        )
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            if li.get_text().count("：") < 1:
                continue
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is None:
                continue
            td_text = td_text.find_next("td").get_text().strip()
            td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
            dataset_metadata[td_name] = td_text
        dataset_metadata = self.common_download(soup, curl, dataset_metadata)
        dataset_metadata["url"] = curl["url"]
        return dataset_metadata

    def detail_liaoning_liaoning(self, curl):
        return self.detail_dongbei_common(curl)

    def detail_liaoning_shenyang(self, curl):
        return self.detail_dongbei_common(curl)

    def detail_heilongjiang_harbin(self, curl):
        return self.detail_dongbei_common(curl)

    def detail_shanghai_shanghai(self, curl):
        key_map = {
            "dataset_name": "数据集名称",
            "open_list_abstract": "摘要",
            "data_label": "数据标签",
            "data_domain": "数据领域",
            "cou_theme_cls": "国家主题分类",
            "create_date": "首次发布日期",
            "update_date": "更新日期",
            "open_rate": "更新频度",
            "org_name": "数据提供方",
            "open_type": "开放属性",
            "contact_way": "联系方式",
        }

        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )

        dataset_metadata = {}
        detail_json = json.loads(response.text)["data"]
        detail_json["create_date"] = detail_json["create_date"].split("T")[0]
        detail_json["update_date"] = detail_json["update_date"].split("T")[0]
        data_formats = set()
        parse_res = urlparse(curl["headers"]["Referer"])
        file_link_fmt = (
            f"{parse_res.scheme}://{parse_res.netloc}/zq/api/download_file_pro/"
            "?path={}&sort={}&file_type={}"
        )
        if self.download_files:
            dataset_metadata["file_name"] = []
        for file in detail_json["docType"]:
            for file_type in file["file_type"]:
                data_formats.add(file_type)
                if self.download_files:
                    file_name = f"{os.path.splitext(file['name'])[0]}.{file_type}"
                    self.downloader.start_download(
                        file_link_fmt.format(file["path"], file["sort"], file_type),
                        file_name,
                    )
                    dataset_metadata["file_name"].append(file_name)
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        dataset_metadata["数据格式"] = list(data_formats)
        dataset_metadata["详情页网址"] = curl["headers"]["Referer"]
        return dataset_metadata

    def detail_jiangsu_jiangsu(self, curl):
        key_map = {
            "resourcesName": "资源名称",
            "resourceUsage": "应用场景",
            "openCondition": "开放类型",
            "updateFrequency": "更新周期",
            "updateDate": "资源更新日期",
            "provideDepartName": "所属单位",
            "belongIndustry": "所属行业",
            "resourceSubject": "所属主题",
            "editionNum": "版本号",
            "resourceDetail": "资源描述",
        }

        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )

        dataset_metadata = {}
        detail_json = json.loads(response.text)["custom"]

        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_jiangsu_wuxi(self, curl):
        table_fields = [
            "来源部门",
            "数据量",
            "联系电话",
            "邮箱地址",
            "所属主题",
            "更新频率",
            "目录发布/更新时间",
            "资源发布/更新时间",
            "条件开放",
            "标签",
            "简介",
        ]

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        titleInfo = soup.find("div", attrs={"class": "data-name data-con"})
        title = "".join(
            [txt.strip() for txt in titleInfo.contents if isinstance(txt, str)]
        )
        dataset_metadata["资源标题"] = title
        baseInfo = soup.find("div", attrs={"class": "baseInfo s1"})
        table = baseInfo.find("table")
        for td_name in table_fields:
            td_text = table.find("td", text=td_name + "：")
            if td_text is None:
                continue
            if td_name == "标签":
                tags = td_text.find_next("td").find_all("span")
                tags_list = [tag.get_text().strip() for tag in tags]
                dataset_metadata[td_name] = tags_list
                continue
            td_text = td_text.find_next("td").get_text().strip()
            dataset_metadata[td_name] = td_text
        dataset_metadata["目录发布/更新时间"] = dataset_metadata[
            "目录发布/更新时间"
        ].split(" ")[0]
        dataset_metadata["资源发布/更新时间"] = dataset_metadata[
            "资源发布/更新时间"
        ].split(" ")[0]
        dataset_metadata["详情页网址"] = (
            f"{curl['url']}?cata_id={curl['params']['cata_id']}"
        )

        data_formats = set()
        detail_link = urlparse(curl["url"])
        download_info_link = (
            f"{detail_link.scheme}://{detail_link.netloc}"
            "/data/catalog/CatalogDetail.do?method=getDownLoadPageInfo"
        )
        response = requests.get(
            download_info_link,
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        download_info = json.loads(response.text)
        if self.download_files:
            dataset_metadata["file_name"] = []
        for file in download_info["data"]:
            if file["fileFormat"] == "zip":
                file_name_with_fmt = file["fileName"]
                data_formats.add(
                    file_name_with_fmt[file_name_with_fmt.rfind("_") + 1 :]
                )
            else:
                data_formats.add(file["fileFormat"])
            if self.download_files:
                file_link = (
                    f"{detail_link.scheme}://{detail_link.netloc}"
                    "/catalog/CatalogDetailDownload.do"
                    "?method=getFileDownloadAddrby"
                    f"&fileId={file['fileId']}"
                )
                file_name = f"{file['fileName']}.{file['fileFormat']}"
                self.downloader.start_download(file_link, file_name)
                dataset_metadata["file_name"].append(file_name)
        dataset_metadata["数据格式"] = list(data_formats)
        return dataset_metadata

    def detail_jiangsu_xuzhou(self, curl):
        key_map = {
            "mlmc": "资源名称",
            "xxzytgf": "资源提供方",
            "ssqtlmmc": "领域",
            "kflx": "开放类型",
            "zyzy": "资源摘要",
            "gxzq": "更新周期",
            "ssztlmmc": "主题分类",
        }
        open_type_map = {"01": "有条件开放", "02": "无条件开放"}

        update_frequency_map = {
            "01": "每日",
            "02": "每周",
            "03": "每月",
            "04": "季度",
            "05": "年",
            "06": "半年",
        }

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        dataset_metadata["开放类型"] = open_type_map[dataset_metadata["开放类型"]]

        if dataset_metadata["更新周期"] in list(update_frequency_map.keys()):
            dataset_metadata["更新周期"] = update_frequency_map[
                dataset_metadata["更新周期"]
            ]
        else:
            print(
                f"{curl['headers']['Referer']}#/DataDirectory/{curl['params']['mlbh'].replace('/', '%2F')}"
            )
            exit(-1)

        dataset_metadata["详情页网址"] = (
            f"{curl['headers']['Referer']}#/DataDirectory/{curl['params']['mlbh'].replace('/', '%2F')}"
        )
        return dataset_metadata

    def detail_jiangsu_suzhou(self, curl):
        key_map = {
            "createTime": "创建时间",
            "createrDeptName": "提供机构",
            "resourceName": "资源信息名称",
            "updateFrequency": "更新频率",
            "resourceIntroduction": "描述",
            "resourceType": "资源类型",
            "openAttributeName": "开放属性",
            "catalogName": "资源目录",
            "contacts": "联系人",
            "contactTel": "联系方式",
        }
        update_freq_map = {
            "1": "实时",
            "2": "每日",
            "3": "每周",
            "4": "每月",
            "5": "每季度",
            "6": "每半年",
            "7": "每年",
            "8": "不定时",
            "9": "其它",
        }
        data_formats_map = {"库表": "table", "文件": "file", "接口": "api"}
        response = requests.post(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["创建时间"] = dataset_metadata["创建时间"].split(" ")[0]
        dataset_metadata["资源类型"] = data_formats_map[dataset_metadata["资源类型"]]
        dataset_metadata["更新频率"] = update_freq_map[dataset_metadata["更新频率"]]
        dataset_metadata["详情页网址"] = (
            f"{curl['headers']['Referer']}#/catalog/{detail_json['id']}"
        )
        return dataset_metadata

    def detail_jiangsu_nantong(self, curl):
        key_map = {
            "resName": "资源名称",
            "createTime": "创建时间",
            "updateTime": "更新时间",
            "updateCycle": "资源更新频率",
            "abstracts": "内容简介",
            "industryTypeName": "行业类型",
            "companyName": "提供机构",
            "themeTypeName": "主题分类",
            "linkman": "联系人",
            "phone": "联系方式",
            "isOpenToSociety": "是否向社会开放",
        }

        open_type_map = {"1": "完全开放", "2": "申请后开放"}

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        if detail_json["isOpenToSociety"] not in list(open_type_map.keys()):
            print(curl["params"])
            exit(-1)

        file_curl = curl.copy()
        file_curl["params"] = {"resId": curl["params"]["id"]}
        file_response = requests.get(
            "https://data.nantong.gov.cn/api/anony/portalResource/listFiles",
            params=file_curl["params"],
            headers=file_curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        file_data = json.loads(file_response.text)["data"]
        if file_data is None:
            data_formats = []
        else:
            data_formats = list(
                set([file["fileType"].lower() for file in file_data["childFiles"]])
            )

        dataset_metadata["是否向社会开放"] = open_type_map[
            dataset_metadata["是否向社会开放"]
        ]
        dataset_metadata["数据格式"] = data_formats
        dataset_metadata["详情页网址"] = (
            f"{curl['headers']['Referer']}home/index.html#/catalog/details?id={curl['params']['id']}"
        )
        return dataset_metadata

    def detail_jiangsu_lianyungang(self, curl):
        table_fields = [
            "信息资源名称",
            "信息资源提供方",
            "是否向社会开放",
            "所属主题",
            "上传时间",
        ]
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}

        tables = soup.find_all("table", {"class": "tw-table-form tm-table-form"})

        try:
            basicInfo, dataItem, fileDownload, dataPreview = tables
        except:
            self.log_request_error(-1, f"{curl['url']}?dmid={curl['params']['dmid']}")
            return False, None

        for th_name in table_fields:
            th = basicInfo.find("th", text=th_name + ":")
            td = th.find_next("td")
            dataset_matadata[th_name] = td.get_text()
        data_formats = []
        for th_file in fileDownload.find_all("th"):
            data_formats.append(th_file.get_text().split(" ")[0].lower())

        dataset_matadata["上传时间"] = dataset_matadata["上传时间"].split(" ")[0]
        dataset_matadata["数据格式"] = list(set(data_formats))
        dataset_matadata["详情页网址"] = f"{curl['url']}?dmid={curl['params']['dmid']}"
        return True, dataset_matadata

    def detail_jiangsu_huaian(self, curl):
        key_map = {
            "catalogName": "资源名称",
            "openCondition": "开放方式",
            "dataUpdateTime": "更新时间",
            "dataUpdatePeriod": "更新周期",
            "publishTime": "发布时间",
            "orgName": "所属部门",
        }

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        if dataset_metadata["更新时间"] is not None:
            dataset_metadata["更新时间"] = dataset_metadata["更新时间"].split(" ")[0]
        if dataset_metadata["发布时间"] is not None:
            dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(" ")[0]
        dataset_metadata["url"] = (
            f"{curl['url']}?"
            f"{'&'.join([f'{key}={val}' for key, val in curl['params'].items()])}"
        )
        return dataset_metadata

    def detail_jiangsu_yancheng(self, curl):
        key_map = {
            "catalogName": "目录名称",
            "updateCycle": "更新频率",
            "catalogAbstract": "目录摘要",
            "catalogKeywords": "关键字",
            "createTime": "创建日期",
            "publishTime": "发布日期",
            "deptName": "提供部门",
            "openType": "开放类型",
            "openCondition": "开放条件",
            "subjectName": "领域分类",
            "industryName": "行业分类",
            "sceneName": "场景分类",
        }
        open_type_map = {1: "可开放"}
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["resultData"]

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        if dataset_metadata["开放类型"] in list(open_type_map.keys()):
            dataset_metadata["开放类型"] = open_type_map[dataset_metadata["开放类型"]]
        else:
            print(curl["params"]["catalogId"])
            exit(-1)
        if dataset_metadata["开放条件"] is None:
            dataset_metadata["开放条件"] = "无条件开放"

        data_formats = []
        for format in ["Json", "Excel", "Csv"]:
            if detail_json["is" + format] == 1:
                data_formats.append(format.lower())
        dataset_metadata["资源格式"] = data_formats
        dataset_metadata["创建日期"] = dataset_metadata["创建日期"].split(" ")[0]
        dataset_metadata["发布日期"] = dataset_metadata["发布日期"].split(" ")[0]
        dataset_metadata["详情页网址"] = curl["headers"]["Referer"].format(
            curl["params"]["catalogId"]
        )
        return dataset_metadata

    def detail_jiangsu_zhenjiang(self, curl):
        key_map = {
            "resourceName": "资源名称",
            "publishTime": "发布时间",
            "updateTime": "最近更新",
            "organName": "数据提供方",
            "topicClassify": "数据主题",
            "industryType": "数据行业",
            "isOpen": "开放属性",
            "resourceRes": "资源摘要",
            "updateCycle": "更新周期",
            "keyWords": "关键字",
            "dataRelam": "数据领域",
        }
        open_type_map = {"0": "无条件开放", "1": "有条件开放"}
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        if "resMountType" in detail_json:
            dataset_metadata["数据格式"] = detail_json["resMountType"]
            if dataset_metadata["数据格式"] == "其他":
                dataset_metadata["数据格式"] = "file"
            else:
                dataset_metadata["数据格式"] = dataset_metadata["数据格式"].lower()
        else:
            dataset_metadata["数据格式"] = "file"

        if dataset_metadata["开放属性"] in list(open_type_map.keys()):
            dataset_metadata["开放属性"] = open_type_map[dataset_metadata["开放属性"]]
        else:
            print(curl["params"])
            exit(-1)

        dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(" ")[0]
        dataset_metadata["最近更新"] = dataset_metadata["最近更新"].split(" ")[0]
        dataset_metadata["详情页网址"] = (
            f"{curl['headers']['Referer']}#/open/data-resource/info/{curl['params']['id']}"
        )
        return dataset_metadata

    def detail_jiangsu_taizhou(self, curl):
        table_fields = [
            "来源部门",
            "开放状态",
            "所属行业",
            "发布时间",
            "最后更新时间",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]

        response = requests.get(
            curl["url"], headers=curl["headers"], verify=False, timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        dataset_metadata = {}

        catalog_details = soup.find("div", attrs={"class": "g-main catalog-details"})
        top_details = catalog_details.find("div", attrs={"class": "panel panel-top"})
        dataset_metadata["资源名称"] = top_details.find("h4").get_text()
        dataset_metadata["重点领域"] = (
            top_details.find("div", attrs={"class": "list-details"})
            .find("ul")
            .find("li")
            .find_next("li")
            .find("span")
            .get_text()
            .strip()
        )
        body_details = catalog_details.find(
            "div", attrs={"class": "panel panel-content"}
        ).find("div", attrs={"class": "panel-body"})
        basicinfo = body_details.find("li", attrs={"name": "basicinfo"})
        table = basicinfo.find("table")
        for td_name in table_fields:
            td = table.find("td", text=td_name)
            dataset_metadata[td_name] = td.find_next("td").get_text().strip()

        dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(" ")[0]
        dataset_metadata["最后更新时间"] = dataset_metadata["最后更新时间"].split(" ")[
            0
        ]
        file_download = body_details.find("li", attrs={"name": "file-download"})
        if file_download is None:
            data_formats = []
        else:
            file_list = file_download.find("table").find_all("tr")
            data_formats = [f.attrs["fileformat"].lower() for f in file_list]

        api_service = body_details.find("li", attrs={"name": "api-service"})
        if api_service is not None:
            data_formats.append("api")

        dataset_metadata["数据格式"] = list(set(data_formats))
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_jiangsu_suqian(self, curl):
        table_fields = [
            "来源部门",
            "数据量",
            "开放状态",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        requests.packages.urllib3.disable_warnings()
        response = requests.get(
            curl["url"], headers=curl["headers"], verify=False, timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        dataset_metadata = {}

        catalog_details = soup.find("div", attrs={"class": "g-main catalog-details"})
        top_details = catalog_details.find("div", attrs={"class": "panel panel-top"})
        dataset_metadata["资源名称"] = top_details.find("h4").get_text()
        dataset_metadata["重点领域"] = (
            top_details.find("div", attrs={"class": "list-details"})
            .find("ul")
            .find("li")
            .find_next("li")
            .find("span")
            .get_text()
            .strip()
        )
        body_details = catalog_details.find(
            "div", attrs={"class": "panel panel-content"}
        ).find("div", attrs={"class": "panel-body"})
        basicinfo = body_details.find("li", attrs={"name": "basicinfo"})
        table = basicinfo.find("table")
        for td_name in table_fields:
            td = table.find("td", text=td_name)
            if td is None and td_name == "数据量":
                dataset_metadata[td_name] = str(0)
            else:
                dataset_metadata[td_name] = td.find_next("td").get_text().strip()

        file_download = body_details.find("li", attrs={"name": "file-download"})
        if file_download is None:
            data_formats = []
        else:
            file_list = file_download.find("table").find_all("tr")
            data_formats = [f.attrs["fileformat"].lower() for f in file_list]

        api_service = body_details.find("li", attrs={"name": "api-service"})
        if api_service is not None:
            data_formats.append("api")
        dataset_metadata["数据格式"] = list(set(data_formats))
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_zhejiang_zhejiang(self, curl):
        table_fields = [
            "摘要",
            "标签",
            "更新周期",
            "数源单位",
            "数据领域",
            "行业分类",
            "发布日期",
            "更新日期",
            "开放条件",
            "联系方式",
            "资源格式",
            "数据量",
        ]

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("span", attrs={"class": "sjxqTit1"}).get_text()
        dataset_matadata["标题"] = title
        for tr in soup.find("div", attrs={"class": "box1"}).find_all("tr"):
            if tr.has_attr("style"):
                continue
            td_name = ""
            for td in tr.find_all("td"):
                td_text = td.get_text()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['queries']['iid']}"
        return dataset_matadata

    def detail_zhejiang_hangzhou(self, curl):
        key_map = {
            "source_name": "资源名称",
            "private_dept": "发布部门",
            "area_county": "所属区县",
            "instruction": "摘要",
            "create_date": "发布时间",
            "modify_date": "更新时间",
            "updateCycle": "更新周期",
            "open_level": "开放等级",
            "deptTel": "联系电话",
            "data_count": "数据量",
        }
        open_level_map = {
            "1": "登录开放",
            "2": "申请开放",
            "3": "完全开放",
        }

        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        if curl["format"] == "api":
            try:
                detail_json = json.loads(response.text)["serviceModel"]
            except json.decoder.JSONDecodeError:
                return None
        else:
            detail_json = json.loads(response.text)["resInfo"]

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["开放等级"] = open_level_map[dataset_metadata["开放等级"]]
        dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(" ")[0]
        dataset_metadata["更新时间"] = dataset_metadata["更新时间"].split(" ")[0]
        dataset_metadata["详情页网址"] = curl["headers"]["Referer"]
        return dataset_metadata

    def detail_zhejiang_ningbo(self, curl):
        key_map = {
            "corresFormat": "资源格式",
            "createTime": "发布时间",
            "dataUnitTel": "联系方式",
            "domainName": "数据领域",
            "dataNumber": "数据量",
            "label": "标签",
            "openConditions": "开放条件",
            "resourceName": "资源名称",
            "updateFrequency": "更新周期",
            "updateTime": "更新时间",
        }
        response = requests.post(
            curl["url"],
            headers=curl["headers"],
            json=curl["json_data"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["bTCataResources"]

        dataset_metadata = {}
        for key, value in key_map.items():
            try:
                dataset_metadata[value] = detail_json[key]
            except KeyError:
                dataset_metadata[value] = None
        dataset_metadata["url"] = (
            f"{curl['headers']['Origin']}/sjkfptold/#/dataDetails?"
            f"t={int(time.mktime(datetime.datetime.utcnow().timetuple())) * 1000}"
            f"&params={base64.b64encode(str.encode(json.dumps(detail_json))).decode()}"
            f"&uuid={detail_json['uuid']}"
            "&id=1"
        )
        return dataset_metadata

    def detail_zhejiang_wenzhou(self, curl):
        table_fields = [
            "摘要",
            "标签",
            "更新周期",
            "资源格式",
            "数源单位",
            "联系方式",
            "数据领域",
            "行业分类",
            "开放条件",
            "发布日期",
            "更新日期",
            "数据条数",
        ]

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("span", attrs={"class": "sjxqTit1"}).get_text()
        dataset_matadata["标题"] = title
        for tr in soup.find("div", attrs={"class": "box1"}).find_all("tr"):
            if tr.has_attr("style"):
                continue
            td_name = ""
            for td in tr.find_all("td"):
                td_text = td.get_text()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_zhejiang_jiaxing(self, curl):
        table_fields = [
            "摘要",
            "标签",
            "更新周期",
            "数源单位",
            "数据领域",
            "行业分类",
            "发布日期",
            "更新日期",
            "开放条件",
            "联系方式",
            "资源格式",
            "数据容量",
        ]

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("span", attrs={"class": "sjxqTit1"}).get_text()
        dataset_matadata["标题"] = title
        for tr in soup.find("div", attrs={"class": "box1"}).find_all("tr"):
            if tr.has_attr("style"):
                continue
            td_name = ""
            for td in tr.find_all("td"):
                td_text = td.get_text()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_zhejiang_shaoxing(self, curl):
        key_map = {
            "title": "标题",
            "content": "摘要",
            "keywords": "资源标签",
            "createDate": "发布日期",
            "dataUpdateDate": "更新日期",
            "domainStr": "数据领域",
            "deptName": "数源单位",
            "openCond": "开放条件",
            "dataCount": "数据量",
            "deptContact": "联系方式",
            "updateFreq": "更新周期",
        }
        update_frequency_map = {
            1: "每日",
            2: "每周",
            3: "每月",
            4: "每季度",
            5: "每半年",
            6: "每年",
            7: "不定期",
            8: "不更新",
        }
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["更新日期"] = dataset_metadata["更新日期"].split(" ")[0]
        if dataset_metadata["更新周期"] in update_frequency_map.keys():
            dataset_metadata["更新周期"] = update_frequency_map[
                dataset_metadata["更新周期"]
            ]
        else:
            print(curl["headers"]["Referer"].format(curl["params"]["dataId"]))
            print(detail_json["updateFreq"])
            exit(-1)

        dataset_metadata["详情页网址"] = curl["headers"]["Referer"].format(
            curl["params"]["dataId"]
        )
        return dataset_metadata

    def detail_zhejiang_jinhua(self, curl):
        table_fields = [
            "摘要",
            "标签",
            "更新周期",
            "数源单位",
            "数据领域",
            "行业分类",
            "发布日期",
            "更新日期",
            "开放条件",
            "联系方式",
            "资源格式",
            "数据容量",
        ]

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("span", attrs={"class": "sjxqTit1"}).get_text()
        dataset_matadata["标题"] = title
        for tr in soup.find("div", attrs={"class": "box1"}).find_all("tr"):
            if tr.has_attr("style"):
                continue
            td_name = ""
            for td in tr.find_all("td"):
                td_text = td.get_text()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_zhejiang_quzhou(self, curl):
        table_fields = [
            "摘要",
            "标签",
            "更新周期",
            "数源单位",
            "数据领域",
            "行业分类",
            "发布日期",
            "更新日期",
            "开放条件",
            "联系方式",
            "资源格式",
            "数据容量",
        ]

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("span", attrs={"class": "sjxqTit1"}).get_text()
        dataset_matadata["标题"] = title
        for tr in soup.find("div", attrs={"class": "box1"}).find_all("tr"):
            if tr.has_attr("style"):
                continue
            td_name = ""
            for td in tr.find_all("td"):
                td_text = td.get_text()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_zhejiang_zhoushan(self, curl):
        key_map = {
            "resName": "资源名称",
            "resSummary": "摘要",
            "datadomain": "数据领域",
            "dataCount": "数据量",
            "idPocName": "资源提供方",
            "resFormatName": "资源格式",
            "createDate": "上线日期",
            "updateDate": "更新日期",
        }
        domain_map = {
            "1": "信用服务",
            "2": "医疗卫生",
            "3": "社保就业",
            "4": "公共安全",
            "5": "城建住房",
            "6": "交通运输",
            "7": "教育文化",
            "8": "科技创新",
            "9": "资源能源",
            "10": "生态环境",
            "11": "工业农业",
            "12": "商贸流通",
            "13": "财税金融",
            "14": "安全生产",
            "15": "市场监督",
            "16": "社会救助",
            "17": "法律服务",
            "18": "生活服务",
            "19": "气象服务",
            "20": "地理空间",
            "21": "机构团体",
            "99": "其他",
        }
        response = requests.post(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        dataset_metadata["数据领域"] = [
            domain_map[x] for x in dataset_metadata["数据领域"].split(",")
        ]
        dataset_metadata["详情页网址"] = (
            f"{curl['headers']['Referer']}#/OpenData/DataSet/Detail?id={curl['params']['id']}"
        )
        return dataset_metadata

    def detail_zhejiang_taizhou(self, curl):
        key_map = {
            "title": "标题",
            "content": "摘要",
            "keywords": "资源标签",
            "createDate": "发布日期",
            "dataUpdateDate": "更新日期",
            "domainStr": "数据领域",
            "deptName": "数源单位",
            "openCond": "开放条件",
            "dataCount": "数据量",
            "deptContact": "联系方式",
            "updateFreq": "更新周期",
        }
        update_frequency_map = {
            1: "每日",
            2: "每周",
            3: "每月",
            4: "每季度",
            5: "每半年",
            6: "每年",
            7: "不定期",
            8: "不更新",
            9: "分钟级",
            10: "小时级",
        }
        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        detail_json = json.loads(response.text)["data"]
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["更新日期"] = dataset_metadata["更新日期"].split(" ")[0]
        if dataset_metadata["更新周期"] in update_frequency_map.keys():
            dataset_metadata["更新周期"] = update_frequency_map[
                dataset_metadata["更新周期"]
            ]
        else:
            print(curl["headers"]["Referer"].format(curl["params"]["dataId"]))
            print(detail_json["updateFreq"])
            exit(-1)

        dataset_metadata["详情页网址"] = curl["headers"]["Referer"].format(
            curl["params"]["dataId"]
        )
        return dataset_metadata

    def detail_zhejiang_lishui(self, curl):
        table_fields = [
            "摘要",
            "标签",
            "更新周期",
            "数源单位",
            "数据领域",
            "行业分类",
            "发布日期",
            "更新日期",
            "开放条件",
            "联系方式",
            "资源格式",
            "数据条数",
        ]

        response = requests.get(
            curl["url"],
            params=curl["params"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        try:
            title = soup.find("span", attrs={"class": "sjxqTit1"}).get_text()
        except AttributeError:
            return None
        dataset_matadata["标题"] = title
        for tr in soup.find("div", attrs={"class": "box1"}).find_all("tr"):
            if tr.has_attr("style"):
                continue
            td_name = ""
            for td in tr.find_all("td"):
                td_text = td.get_text()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_anhui_anhui(self, curl):
        key_map = {
            "catalogName": "标题",
            "publishTime": "发布日期",
            "updateTime": "更新日期",
            "providerDept": "提供单位",
            "belongFieldName": "所属领域",
            "openTypeName": "开放属性",
            "summary": "摘要信息",
            "updateCycleTxt": "更新频率",
            "formats": "数据格式",
        }

        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            verify=False,
            timeout=REQUEST_TIME_OUT,
        )

        dataset_metadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key in ["publishTime", "updateTime"]:
                detail_json[key] = (
                    detail_json[key][:4]
                    + "-"
                    + detail_json[key][4:6]
                    + "-"
                    + detail_json[key][6:8]
                )
            if key == "formats":
                detail_json[key] = str(detail_json[key]).lower()
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_anhui_hefei(self, curl):
        key_map = {
            "zy": "标题",
            "tgdwmc": "提供单位",
            "filedName": "所属领域",
            "cjsj": "发布时间",
            "gxsj": "更新时间",
            "gxpl": "更新频率",
            "zymc": "摘要信息",
            "fjhzm": "资源格式",
        }

        # 使用session保持会话
        session = requests.session()
        res1 = session.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        jsl_clearance_s = re.findall(r"cookie=(.*?);location", res1.text)[0]
        # 执行js代码
        jsl_clearance_s = str(execjs.eval(jsl_clearance_s)).split("=")[1].split(";")[0]
        # add_dict_to_cookiejar方法添加cookie
        add_dict_to_cookiejar(session.cookies, {"__jsl_clearance_s": jsl_clearance_s})
        res2 = session.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )
        # 提取go方法中的参数
        data = json.loads(re.findall(r";go\((.*?)\)", res2.text)[0])
        jsl_clearance_s = getCookie(data)
        # 修改cookie
        add_dict_to_cookiejar(session.cookies, {"__jsl_clearance_s": jsl_clearance_s})
        response = session.post(
            curl["url"],
            headers=curl["headers"],
            data=curl["data"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]

        freq_map = {"": "", "6": "每年", "8": "每两年"}

        dataset_matadata["开放条件"] = "无条件开放"

        for key, value in key_map.items():
            if detail_json[key] is None:
                dataset_matadata[value] = ""
                continue
            if key in ["cjsj", "gxsj"]:
                detail_json[key] = (
                    detail_json[key][0:4]
                    + "-"
                    + detail_json[key][4:6]
                    + "-"
                    + detail_json[key][6:8]
                )
            if key == "gxpl":
                detail_json[key] = freq_map[detail_json[key]]
            if key == "fjhzm":
                detail_json[key] = detail_json[key].lower().strip().split(" ")
            dataset_matadata[value] = detail_json[key]

        if dataset_matadata["资源格式"][0] == "":
            dataset_matadata["资源格式"] = ["file"]

        return dataset_matadata

    def detail_anhui_bengbu(self, curl):
        list_fields = [
            "数据提供方",
            "数据主题",
            "发布时间",
            "更新时间",
            "公开属性",
            "更新频率",
            "摘要",
            "下载格式",
            "关键字",
            "数据条数",
        ]
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("div", attrs={"class": "data_newtitle"})
        title = title.find("h1", attrs={"class": "tit"}).get_text()
        dataset_metadata["标题"] = title

        for li in (
            soup.find("div", attrs={"class": "data_sj"})
            .find("ul", attrs={"class": "clearfix"})
            .find_all("li")
        ):
            li_name = (
                li.find("span", attrs={"class": "tit"})
                .get_text()
                .split("：")[0]
                .strip()
            )
            if li_name in list_fields:
                li_text = li.get_text().split("：")[-1].strip()
                if li_name in ["发布时间", "更新时间"]:
                    li_text = li_text.split(" ")[0].strip()
                if li_name == "下载格式":
                    li_text = li_text.split("，")
                dataset_metadata[li_name] = li_text
        return dataset_metadata

    def detail_anhui_huainan(self, curl):
        key_map = {
            "dataName": "标题",
            "dataDomainName": "数据领域",
            "dataTypeName": "数据类型",
            "createTime": "创建时间",
            "pubTime": "发布时间",
            "modifyTime": "更新时间",
            "themeName": "主题分类",
            "openAttrName": "开放属性",
            "pubDeptName": "所属部门",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        dataset_metadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key == "openAttrName":
                detail_json[key] = detail_json[key] + "开发"
            if key == "dataTypeName":
                if detail_json[key] == "接口":
                    detail_json[key] = "api"
                elif detail_json[key] == "数据":
                    detail_json[key] = "table"
                else:
                    detail_json[key] = "file"
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_anhui_huaibei(self, curl):
        key_map = {
            "name": "标题",
            "companyName": "数源单位",
            "appTypeName": "数据领域",
            "createTime": "发布日期",
            "lastUpdateTime": "更新日期",
            "openConditions": "开放条件",
            "dataCount": "数据量",
            "industryType": "行业分类",
            "updateCycle": "更新周期",
            "phone": "联系方式",
            "label": "标签",
            "summary": "摘要",
        }

        response = requests.post(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["result"]

        dataset_matadata["资源格式"] = []
        if detail_json["hascsv"]:
            dataset_matadata["资源格式"].append("csv")
        if detail_json["hasjson"]:
            dataset_matadata["资源格式"].append("json")
        if detail_json["hasrdf"]:
            dataset_matadata["资源格式"].append("rdf")
        if detail_json["hasxls"]:
            dataset_matadata["资源格式"].append("xls")
        if detail_json["hasxml"]:
            dataset_matadata["资源格式"].append("xml")

        for key, value in key_map.items():
            if detail_json[key] is None:
                dataset_matadata[value] = ""
                continue
            if key in ["createTime", "lastUpdateTime"]:
                detail_json[key] = detail_json[key].split(" ")[0].strip()
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_anhui_huangshan(self, curl):
        key_map = {
            "name": "标题",
            "publishDate": "发布时间",
            "refreshDate": "更新时间",
            "dataCounts": "数据量",
            "updateCycle": "更新周期",
            "resourceAbstract": "摘要",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)

        # 目前的200个数据集都是不定期
        freq_map = {9: "不定期"}

        for key, value in key_map.items():
            if key not in detail_json.keys() or detail_json[key] is None:
                dataset_matadata[value] = ""
                continue
            if key in ["publishDate", "refreshDate"]:
                detail_json[key] = detail_json[key].split(" ")[0].strip()
            if key == "updateCycle":
                detail_json[key] = freq_map[detail_json[key]]
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_anhui_chuzhou(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        detail_json = json.loads(response.text)["data"]

        format_list = []

        if "url" in detail_json:
            file_list = json.loads(detail_json["url"])
            for file in file_list:
                format_list.append(file["name"].split(".")[-1].strip().lower())
        if "fileUrl" in detail_json:
            file_list = json.loads(detail_json["fileUrl"])
            for file in file_list:
                format_list.append(file["name"].split(".")[-1].strip().lower())

        if "phone" not in detail_json:
            detail_json["phone"] = ""

        metadata_mapping = {
            "联系电话": detail_json["phone"],
            "资源格式": format_list,
            "详情页网址": "https://www.chuzhou.gov.cn/data/#/wdfwDetail?id="
            + str(detail_json["id"]),
        }

        return metadata_mapping

    def detail_anhui_suzhou(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = [
            "数据量",
            "文件数",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        return dataset_metadata

    def detail_anhui_luan(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = [
            "数据量",
            "文件数",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        return dataset_metadata

    def detail_fujian_fujian(self, curl):
        key_map = {
            "catalogName": "title",
            "catalogDes": "description",
            "orgName": "department",
            "themeName": "category",
            "industryName": "industry",
            "dataVol": "data_volume",
            "updateTime": "update_time",
            "openType": "is_open",
            "organPhone": "telephone",
            "organEmail": "email",
            "tags": "tags",
            "releasedTime": "publish_time",
            "updateCycle": "update_frequency",
        }

        response = requests.get(
            curl["url"],
            headers=curl["headers"],
            params=curl["params"],
            timeout=REQUEST_TIME_OUT,
        )
        dataset_metadata = {}
        detail_json = json.loads(response.text)
        if detail_json["code"] != 200:
            return
        detail_json = detail_json["data"]
        for origin_key, mapped_key in key_map.items():
            if origin_key == "openType":
                if detail_json[origin_key] == "0":
                    detail_json[origin_key] = "有条件开放"
                else:
                    detail_json[origin_key] = "无条件开放"
            dataset_metadata[mapped_key] = detail_json[origin_key]
        return dataset_metadata

    def detail_fujian_fuzhou(self, curl):
        key_map = {
            "resTitle": "名称",
            "resAbstract": "摘要",
            "openMode": "开放方式",
            "publishDate": "发布日期",
            "dataUpdateTime": "更新日期",
            "sourceSuffix": "资源格式",
            "fullName": "数据提供方",
            "subjectName": "主题分类",
            "phone": "联系方式",
            "keyword": "关键字",
        }

        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        dataset_metadata = {}
        detail_json = json.loads(response.text)
        for key, value in key_map.items():
            if key == "openMode":
                if detail_json[key] == "普遍开放":
                    detail_json[key] = "无条件开放"
                else:
                    detail_json[key] = "有条件开放"
            if key in ["publishDate", "dataUpdateTime"]:
                detail_json[key] = detail_json[key][:10]
            if key == "sourceSuffix":
                detail_json[key] = "[" + detail_json[key] + "]"
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_fujian_xiamen(self, curl):
        key_map = {
            "catalogName": "目录名称",
            "openTypeName": "开放类型",
            "openDataFormat": "数据格式",
            "provideDept": "数据来源",
            "contactMethod": "联系方式",
            "theme": "主题领域",
            "industry": "行业分类",
            "tag": "标签",
            "summary": "简介",
            "dataUpdateRate": "更新频率",
            "publishTime": "发布时间",
            "updatedTime": "更新时间",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        dataset_metadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key == "openMode":
                if detail_json[key] == "普遍开放":
                    detail_json[key] = "无条件开放"
                else:
                    detail_json[key] = "有条件开放"
            if key in ["publishTime", "updatedTime"] and detail_json[key] is not None:
                detail_json[key] = detail_json[key][:10]
            if key == "openDataFormat":
                detail_json[key] = "[" + detail_json[key].lower() + "]"
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_jiangxi_jiangxi(self, curl):
        key_map = {
            "dataName": "标题",
            "resOrgName": "数源单位",
            "dataFieldName": "数据领域",
            "createTime": "发布时间",
            "updateTime": "数据更新时间",
            "resShareType": "开放类型",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        dataset_metadata = {}
        detail_json = json.loads(response.text)
        if detail_json["code"] != 200:
            return {}
        detail_json = detail_json["data"]
        for key, value in key_map.items():
            if key == "resShareType":
                if detail_json[key] == "402882a75885fd150158860e3d170006":
                    detail_json[key] = "有条件开放"
                else:
                    detail_json[key] = "无条件开放"
            if key in ["createTime", "updateTime"] and detail_json[key] is not None:
                detail_json[key] = detail_json[key][:10]
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_shandong_common(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = [
            "数据量",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        for _ in range(REQUEST_MAX_TIME):
            try:
                response = requests.get(
                    curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
                )
                break
            except:
                self.log_request_error(-1, curl["url"])
                time.sleep(5)

        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            if td_name == "数据量":  # 可能叫接口数，或文件数
                alias_list = ["数据量", "接口数", "文件数"]
                for name in alias_list:
                    if table.find("td", text=name) is not None:
                        td_name = name
                        break
            try:
                td_text = (
                    table.find("td", text=td_name).find_next("td").get_text().strip()
                )
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
            except Exception as e:
                print(e)
                print("title = ", title)
                print("url = ", curl["url"])
        dataset_metadata = self.common_download(soup, curl, dataset_metadata)
        return dataset_metadata

    def detail_shandong_shandong(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_jinan(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_qingdao(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_zibo(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_zaozhuang(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_dongying(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_yantai(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_weifang(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_jining(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_taian(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_weihai(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_rizhao(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_linyi(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_dezhou(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_liaocheng(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_binzhou(self, curl):
        return self.detail_shandong_common(curl)

    def detail_shandong_heze(self, curl):
        return self.detail_shandong_common(curl)

    def detail_hubei_wuhan(self, curl):
        key_map = {
            "名称": "cataTitle",
            "摘要": "summary",
            "标签": "resLabel",
            "数据提供方": "registerOrgName",
            "主题分类": "dataTopicDetail",
            "行业分类": "industryTypeDetail",
            "发布日期": "createTime",
            "更新日期": "updateTime",
            "更新频率": {"cataFileDTO": "updateCycle"},
            "开放条件": "openLevelDetail",
            "提供方联系方式": "providerPhon",
            "资源格式": "fileFormatDetail",
        }
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        data = json.loads(response.text)["data"]
        metadata = {}

        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if k in ["createTime", "updateTime"]:
                    metadata[name] = metadata[name].split(" ")[0]
        if "资源格式" in metadata:
            metadata["资源格式"] = list(metadata["资源格式"].lower().split(","))
        metadata["详情页网址"] = (
            "http://data.wuhan.gov.cn/page/data/data_set_details.html?cataId={}".format(
                curl["queries"]["cataId"]
            )
        )
        return metadata

    def detail_hubei_huangshi(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )

        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        key_map = {
            "名称": "title",
            "摘要": "summary",
            "资源标签": "tag",
            "数源单位": "departmentname",
            "数据领域": "lyid",
            "行业分类": "bshy",
            "发布日期": "c_date",
            "更新日期": "e_data",
            "更新周期": "None_field",
            "开放条件": "dataopen",
            "版本": "None",
            "联系方式": "phone",
            "邮箱": "None",
            "资源格式": "datafmt",
        }

        lylist = [
            {"id": 1, "name": "教育培训", "value": 4},
            {"id": 2, "name": "企业扶持", "value": 1},
            {"id": 3, "name": "就业招聘", "value": 3},
            {"id": 4, "name": "城市安全", "value": 2},
            {"id": 5, "name": "生活服务", "value": 18},
            {"id": 6, "name": "结婚生育", "value": 0},
            {"id": 7, "name": "交通出行", "value": 4},
            {"id": 8, "name": "社会保险", "value": 2},
            {"id": 9, "name": "退休养老", "value": 0},
            {"id": 10, "name": "医疗保健", "value": 2},
        ]
        hylist = [
            {"id": 1, "name": "信用服务", "value": 2},
            {"id": 2, "name": "医疗卫生", "value": 2},
            {"id": 3, "name": "社保就业", "value": 4},
            {"id": 4, "name": "公共安全", "value": 1},
            {"id": 5, "name": "城建住房", "value": 2},
            {"id": 6, "name": "交通运输", "value": 4},
            {"id": 7, "name": "教育文化", "value": 4},
            {"id": 8, "name": "科技创新", "value": 0},
            {"id": 9, "name": "资源能源", "value": 0},
            {"id": 10, "name": "生态环境", "value": 1},
            {"id": 11, "name": "工业农业", "value": 0},
            {"id": 12, "name": "商贸流通", "value": 2},
            {"id": 13, "name": "财税金融", "value": 0},
            {"id": 14, "name": "安全生产", "value": 1},
            {"id": 15, "name": "市场监督", "value": 1},
            {"id": 16, "name": "社会救助", "value": 3},
            {"id": 17, "name": "法律服务", "value": 4},
            {"id": 18, "name": "生活服务", "value": 6},
            {"id": 19, "name": "气象服务", "value": 1},
            {"id": 20, "name": "地理空间", "value": 0},
            {"id": 21, "name": "机构团体", "value": 0},
            {"id": 22, "name": "其他", "value": 0},
        ]

        data = json.loads(response.text)["data"]
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ["发布日期", "更新日期"]:
                    metadata[name] = time.strftime(
                        "%Y-%m-%d", time.localtime(metadata[name])
                    )
                elif name in ["数据领域"]:
                    item = list(
                        filter(lambda x: x["id"] == int(metadata[name]), lylist)
                    )
                    if len(item) > 0:
                        metadata[name] = item[0]["name"]
                elif name in ["行业分类"]:
                    item = list(
                        filter(lambda x: x["id"] == int(metadata[name]), hylist)
                    )
                    if len(item) > 0:
                        metadata[name] = item[0]["name"]
                elif name in ["开放条件"]:
                    metadata[name] = (
                        "无条件开放" if int(metadata[name]) == 1 else "申请公开"
                    )
        if "资源格式" in metadata:
            metadata["资源格式"] = list(metadata["资源格式"].lower().split(","))
        metadata["详情页网址"] = (
            "http://data.huangshi.gov.cn/html/#/opentableinfo?infoid={}".format(
                curl["queries"]["infoid"]
            )
        )
        return metadata

    def detail_hubei_yichang(self, curl):
        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        if curl["crawl_type"] == "dataset":
            key_map = {
                "名称": "title",
                "摘要": "content",
                "资源标签": "keywords",
                "数源单位": "deptName",
                "数据领域": "domainStr",
                "行业分类": {"downloadDataInfo": "themeName"},
                "发布日期": "createDate",
                "更新日期": "dataUpdateDate",
                "更新周期": "None",
                "开放条件": {"downloadDataInfo": "openCondition"},
                "版本": "None",
                "联系方式": "deptContact",
                "邮箱": "None",
                "数据下载": "None",
            }
            response = requests.get(
                curl["url"],
                params=curl["queries"],
                headers=curl["headers"],
                timeout=REQUEST_TIME_OUT,
            )
            data = json.loads(response.text)["data"]
            metadata = {}
            for name in key_map:
                k = key_map[name]
                value, k = get_meta_data(data, k)
                if value:
                    metadata[name] = value
            metadata["详情页网址"] = (
                "https://data.yichang.gov.cn/kf/open/table/detail/{}".format(
                    curl["queries"]["dataId"]
                )
            )
        else:
            key_map = {
                "名称": "title",
                "摘要": "content",
                "资源标签": "keywords",
                "数据提供单位": "source",
                "数源单位": "deptName",
                "数据领域": "domainStr",
                "行业分类": {"downloadDataInfo": "themeName"},
                "发布日期": "createDate",
                "更新日期": "dataUpdateDate",
                "更新周期": "None",
                "开放条件": {"downloadDataInfo": "openCondition"},
                "版本": "None",
                "联系方式": "deptContact",
                "邮箱": "None",
                "数据下载": "None",
            }
            response = requests.post(
                curl["url"],
                data=curl["data"],
                headers=curl["headers"],
                timeout=REQUEST_TIME_OUT,
            )
            data = json.loads(response.text)["data"]
            metadata = {}
            for name in key_map:
                k = key_map[name]
                value, k = get_meta_data(data, k)
                if value:
                    metadata[name] = value
            metadata["数据下载"] = ["api"]
            metadata["详情页网址"] = (
                "https://data.yichang.gov.cn/kf/open/interface/detail/{}".format(
                    curl["data"]["baseDataId"]
                )
            )
        return metadata

    def detail_hubei_ezhou(self, curl):
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        soup = bs4.BeautifulSoup(response.content, "html.parser")
        table = soup.find("ul", class_="table_p1")
        metadata_key = [
            "摘要",
            "资源标签",
            "更新周期",
            "资源格式",
            "数据单位",
            "数据领域",
            "",
            "行业分类",
            "",
            "数据分级",
            "开放条件",
            "更新日期",
            "发布日期",
        ]
        number = 0
        metadata = {}
        for content in table.find_all("div", class_="content-td"):
            if not metadata_key[number]:
                number += 1
                continue
            text = content.text
            text = text.replace(" ", "").strip()
            if text:
                metadata[metadata_key[number]] = text
            number += 1
        if "资源格式" in metadata:
            metadata["资源格式"] = ["file"]
        if "api" in curl:
            metadata["资源格式"] = ["api"]
        metadata["详情页网址"] = curl["url"]
        return metadata

    def detail_hubei_jingzhou(self, curl):
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        soup = bs4.BeautifulSoup(response.content, "html.parser")
        metadata = {}

        top = soup.find("div", class_="directory-media")
        metadata["名称"] = (
            top.find("ul", class_="d-title").text.replace(" ", "").strip()
        )

        details = top.find("div", class_="list-details")
        lis = details.find_all("li")
        content = lis[1].find_next("span").text
        metadata["重点领域"] = content.replace(" ", "").strip()

        table = soup.find("div", class_="panel-content")
        table = table.find("li", attrs={"name": "basicinfo"})
        key = None
        for content in table.find_all("td"):
            text = content.text
            text = text.replace(" ", "").strip()
            if not key:
                key = text
                continue
            else:
                if text:
                    metadata[key] = text
                if key in ["发布时间", "最后更新时间"]:
                    metadata[key] = metadata[key][:10]
                key = None
        metadata["详情页网址"] = curl["url"]
        return metadata

    def detail_hubei_suizhou(self, curl):
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        if curl["crawl_type"] == "dataset":
            data = json.loads(response.text)["setMetadata"]
        else:
            data = json.loads(response.text)["apiMetadata"]

        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        key_map = {
            "名称": "resTitle",
            "摘要": "resAbstract",
            "关键字": "keyword",
            "提供方": "officeName",
            "主题": "classifName",
            "行业分类": "None",
            "发布时间": "publishDate",
            "数据更新时间": "dataUpdateTime",
            "更新频率": "UpdateCycle",
            "开放等级": "openLevel",
            "版本": "None",
            "联系方式": "None",
            "邮箱": "None",
            "文件格式": "metadataFileSuffix",
        }
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ["数据更新时间", "发布时间"]:
                    metadata[name] = metadata[name][:10]
        if "dataApi" in curl["url"]:
            metadata["文件格式"] = "api"
        if "文件格式" in metadata:
            metadata["文件格式"] = list(metadata["文件格式"].lower().split(","))
        metadata["详情页网址"] = curl["url"].replace("toDataDetails", "toDataSet")
        return metadata

    def detail_hunan_yueyang(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        tbody = soup.find("table")
        metadata = {}

        metadata["名称"] = (
            soup.find("h1", class_="content-title").text.replace(" ", "").strip()
        )

        for tr in tbody.find_all("tr"):
            tds = tr.find_all_next("td")
            key = tds[0].text.replace(" ", "").strip().strip("：")
            value = tds[1].text.replace(" ", "").strip()
            if value:
                metadata[key] = value
                if key in ["首次发布时间"]:
                    metadata[key] = metadata[key].replace("/", "-")

        if "数据格式" in metadata:
            metadata["数据格式"] = list(metadata["数据格式"].lower().split(","))
        metadata["详情页网址"] = curl["url"] + "?id={}".format(curl["queries"]["id"])
        return metadata

    def detail_hunan_changde(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        metadata = {}

        div = soup.find("div", class_="info-content")
        top_info = div.find("div", class_="top-info")
        metadata["名称"] = (
            top_info.find("div", class_="catalog-name").text.replace(" ", "").strip()
        )
        metadata["资源发布日期"] = (
            top_info.find("div", class_="time")
            .text.split("：")[-1]
            .replace(" ", "")
            .strip()
        )

        rows = div.find("div", class_="row-content")
        keys = rows.find_all("div", "info-name")
        values = rows.find_all("div", "info-detail")
        for key, value in zip(keys, values):
            key = key.text.replace(" ", "").strip()
            value = value.text.replace(" ", "").strip()
            if value:
                metadata[key] = value
        metadata["详情页网址"] = (
            "https://www.changde.gov.cn/cdwebsite/dataopen/detail?cataId={}".format(
                curl["queries"]["cataId"]
            )
        )
        return metadata

    def detail_hunan_chenzhou(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", class_="panel-body")
        metadata = {}

        metadata["名称"] = div.find("h2").text.replace(" ", "").strip()
        for tr in div.find("table", class_="table-license").find_all("tr"):
            tds = tr.find_all_next("td")
            key = tds[0].text.replace(" ", "").strip().strip("：")
            value = tds[1].text.replace(" ", "").strip()
            if value:
                metadata[key] = value
                if key in ["首次发布时间", "更新时间"]:
                    metadata[key] = metadata[key].replace("/", "-")
        if "数据格式" in metadata:
            metadata["数据格式"] = list(metadata["数据格式"].lower().split(","))
        metadata["详情页网址"] = curl["url"] + "?id={}".format(curl["queries"]["id"])
        return metadata

    def detail_hunan_yiyang(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", class_="panel-body")
        metadata = {}

        metadata["名称"] = div.find("h2").text.replace(" ", "").strip()
        for tr in div.find("table", class_="table-license").find_all("tr"):
            tds = tr.find_all_next("td")
            key = tds[0].text.replace(" ", "").strip().strip("：")
            value = tds[1].text.replace(" ", "").strip()
            if value:
                metadata[key] = value
                if key in ["首次发布时间", "更新时间"]:
                    metadata[key] = metadata[key].replace("/", "-")
        if "数据格式" in metadata:
            metadata["数据格式"] = list(metadata["数据格式"].lower().split(","))
        metadata["详情页网址"] = curl["url"] + "?id={}".format(curl["queries"]["id"])
        return metadata

    def detail_guangdong_guangdong(self, curl):
        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        key_map = {
            "名称": "resTitle",
            "简介": "resAbstract",
            "关键字": "keyword",
            "数据提供方": "officeName",
            "主题分类": "subjectName",
            "行业分类": "None",
            "发布日期": "publishDate",
            "更新日期": "lastModifyTime",
            "更新频率": "None",
            "开放方式": "openMode",
            "版本": "None",
            "联系方式": "None",
            "邮箱": "email",
            "资源格式": "sourceSuffix",
        }
        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        data = json.loads(response.text)["data"]
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ["发布日期", "更新日期"]:
                    metadata[name] = metadata[name][:10]
        if "资源格式" in metadata:
            metadata["资源格式"] = list(metadata["资源格式"].lower().split(","))
        metadata["详情页网址"] = (
            f"{curl['headers']['Referer']}?"
            f"chooseValue=collectForm&id={quote(data['resId'])}"
        )
        return metadata

    def detail_guangdong_guangzhou(self, curl):
        doc_curl = curl["doc"]
        curl = curl["detail"]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        data = json.loads(response.text)["body"]

        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        key_map = {
            "名称": "name",
            "简介": "description",
            "标签": "tags",
            "来源部门": "orgName",
            "所属主题": "subjectName",
            "所属行业": "industryName",
            "发布时间": "created",
            "最后更新": "lastUpdated",
            "更新频率": "updateCycle",
            "开放方式": "openStatus",
            "版本": "None",
            "联系电话": "None",
            "联系邮箱": "None",
            "数据格式": "None",
        }
        updateTime = {
            "1": "不定期",
            "2": "每天",
            "3": "每周",
            "4": "每月",
            "5": "每季度",
            "6": "每半年",
            "7": "每年",
            "8": "实时",
        }
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ["更新频率"]:
                    metadata[name] = updateTime[metadata[name]]
                if name in ["发布时间", "最后更新"]:
                    metadata[name] = metadata[name][:10]
                if name in ["开放方式"]:
                    metadata[name] = (
                        "有条件开放" if metadata[name] == 3 else "无条件开放"
                    )

        metadata["数据格式"] = list(
            map(lambda x: x["fileFormat"], data["dataFileList"])
        )

        response = requests.get(
            doc_curl["url"],
            params=doc_curl["queries"],
            headers=doc_curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.content, "html.parser")
        divs = soup.find_all("div", class_="p-tit")
        for div in divs:
            if "数据集发布者联系方式" not in div.text:
                continue
            ul = div.find_next("ul")
            spans = ul.find_all("span")
            metadata["联系电话"] = spans[1].text
            metadata["联系邮箱"] = spans[3].text
        metadata["详情页网址"] = doc_curl["url"] + "?sid={}".format(
            doc_curl["queries"]["sid"]
        )
        return metadata

    def detail_guangdong_shenzhen(self, curl):
        def get_meta_data(data, key):
            if not data:
                return "", key
            all_data = copy.deepcopy(data)
            try_cnt = 0
            while not isinstance(key, str):
                try_cnt += 1
                if try_cnt > REQUEST_MAX_TIME:
                    break
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ""), key

        key_map = {
            "名称": "resTitle",
            "摘要": "resAbstract",
            "关键字": "keyword",
            "数据提供方": "officeName",
            "服务分类": "subjectName",
            "行业分类": "tradeName",
            "上架日期": "publishDate",
            "更新日期": "dataUpdateTime",
            "更新频率": "updateCycle",
            "开放方式": "openLevelName",
            "版本": "None",
            "联系方式": "phone",
            "邮箱": "None",
            "资源格式": "sourceSuffix",
        }
        response = requests.post(
            curl["url"],
            data=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        data = json.loads(response.text)
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ["上架日期", "更新日期"]:
                    metadata[name] = metadata[name][:10]
        if "资源格式" in metadata:
            metadata["资源格式"] = list(metadata["资源格式"].lower().split(","))
        metadata["详情页网址"] = (
            "https://opendata.sz.gov.cn/data/dataSet/toDataDetails/{}".format(
                curl["data"]["resId"].replace("/", "_")
            )
        )
        return metadata

    def detail_guangdong_zhongshan(self, curl):
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="table-main")
        ths = table.find_all("th")
        tds = table.find_all("td")
        metadata = {}
        metadata["名称"] = soup.find("h2").text

        for th, td in zip(ths, tds):
            th = th.text.replace(" ", "").strip()
            td = td.text.replace(" ", "").strip()
            if td:
                metadata[th] = td
                if th in ["创建时间", "更新时间"]:
                    metadata[th] = (
                        metadata[th]
                        .replace("年", "-")
                        .replace("月", "-")
                        .replace("日", "")
                    )
        metadata["详情页网址"] = curl["url"] + "?id={}&pageNum={}".format(
            curl["queries"]["id"], curl["queries"]["pageNum"]
        )
        return metadata

    def detail_guangxi_common(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放条件"]
        table_fields = [
            "数据量",
            "文件数",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        try:
            response = requests.get(
                curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
            )
        except requests.exceptions.RequestException:
            self.log_request_error(-1, curl["url"])
            return {}
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_matadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_matadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_guangxi_guangxi(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_nanning(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_liuzhou(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_guilin(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_wuzhou(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_beihai(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_fangchenggang(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_qinzhou(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_guigang(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_yulin(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_baise(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_hezhou(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_hechi(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_laibin(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_guangxi_chongzuo(self, curl):
        return self.detail_guangxi_common(curl)

    def detail_hainan_hainan(self, curl):
        table_fields = [
            "摘要",
            "目录名称",
            "开放状态",
            "所属主题",
            "来源部门",
            "目录发布时间",
            "数据更新时间",
            "更新频率",
        ]

        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        t1 = soup.find_all("span", attrs={"class": "tag2"})
        ls1 = []
        for o in t1:
            ls1.append(o.get_text())

        dataset_matadata = {}
        for tr in (
            soup.find("div", attrs={"class": "gp-column1"}).find("table").find_all("tr")
        ):
            lt = tr.find_all("th")
            for o in lt:
                th_name = o.get_text().strip()
                if th_name in table_fields:
                    td_text = o.find_next("td").get_text().strip()
                    td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                    dataset_matadata[th_name] = td_text

        dataset_matadata["数据格式"] = ls1

        return dataset_matadata

    def detail_hainan_hainansjj(self, curl):
        return self.detail_hainan_hainan(curl)

    def detail_hainan_hainansjjk(self, curl):
        return self.detail_hainan_hainan(curl)

    def detail_sichuan_sichuan(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放条件"]
        table_fields = [
            "数据量",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_sichuan_chengdu(self, curl):
        list_fields = ["来源部门", "主题", "发布时间", "更新时间", "开放条件"]
        table_fields = [
            "数据量",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        return dataset_metadata

    def detail_sichuan_panzhihua(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = [
            "数据量",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_sichuan_zigong(self, curl):
        key_map = {
            "catalogTitle": "标题",
            "catalogDesc": "描述",
            "supplyOrg": "来源部门",
            "domainName": "主题",
            "createdTime": "发布时间",
            "modifiedTime": "更新时间",
            "isOpen": "开放条件",
            "industryName": "所属行业",
            "updateCycle": "更新频率",
            "tel": "部门电话",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"][0]
        for key, value in key_map.items():
            if key in ["modifiedTime", "createdTime"]:
                detail_json[key] = time.strftime(
                    "%Y-%m-%d", time.localtime(detail_json[key] / 1000)
                )
            if key == "isOpen":
                detail_json[key] = (
                    "有条件开放" if detail_json[key] == "0" else "无条件开放"
                )
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_luzhou(self, curl):
        list_fields = ["提供方", "数据主题", "数据领域", "联系电话"]
        table_fields = ["更新周期", "关键字", "资源摘要"]
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("h2", attrs={"class": "dt-name"})
        title = title.find("p").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "desc-list-info clearfix"}).find_all(
            "li", attrs={}
        ):
            li_name = li.find("span").get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = li.find_all("span")[1].get_text().strip()
                dataset_metadata[li_name] = li_text
        table = soup.find(
            "table",
            attrs={
                "class": "table table-striped table-bordered table-advance table-hover"
            },
        )
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        return dataset_metadata

    def detail_sichuan_deyang(self, curl):
        key_map = {
            "xxzymc": "标题",
            "xxzytgf": "资源提供方",
            "ssztlmmc": "领域名称",
            "fbrq": "发布时间",
            "gxrq": "更新时间",
            "kflx": "开放类型",  # "01": "有条件开放", "02": "无条件开放"
            "sjl": "数据量",
            "ssqtlmmc": "所属行业",
            "gxzq": "更新周期",
            "zjhm": "联系电话",
            "mlbqmcList": "标签",
            "zyzy": "资源摘要",
            "zygs": "资源格式",
        }

        frequency_mapping = {
            "01": "每日",
            "02": "每周",
            "03": "每月",
            "04": "季度",
            "05": "年",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key in ["fbrq", "gxrq"]:
                detail_json[key] = time.strftime(
                    "%Y-%m-%d", time.localtime(int(detail_json[key]) / 1000)
                )
            if key == "kflx":
                detail_json[key] = (
                    "有条件开放" if detail_json[key] == "01" else "无条件开放"
                )
            if key == "gxzq":
                detail_json[key] = frequency_mapping[detail_json[key]]
            if key == "zygs":
                detail_json[key] = str(detail_json[key].split("\\"))
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_mianyang(self, curl):
        key_map = {
            "dir_name": "标题",
            "dir_office": "提供部门",
            "sszt": "领域",
            "gmt_modified": "更新时间",
            "sshy": "所属行业",
            "dir_updatetime": "更新频率",
            "dir_phone": "联系电话",
            "ssbq": "标签",
        }

        frequency_mapping = {
            "0": "实时",
            "1": "每日",
            "2": "每周",
            "3": "每月",
            "4": "每季度",
            "5": "每半年",
            "6": "每年",
        }

        try:
            response = requests.get(
                curl["url"],
                params=curl["queries"],
                headers=curl["headers"],
                verify=False,
                timeout=REQUEST_TIME_OUT,
            )
            if response.status_code != requests.codes.ok:
                self.log_request_error(response.status_code, curl["url"])
                return dict()
        except:
            self.log_request_error(-1, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["elementthing"]["showBasicList"]
        share_level = detail_json["dir_statistics_data"][0]["sharelevel"]
        if share_level == "0":
            dataset_matadata["开放类型"] = "无条件开放"
        elif share_level == "1":
            dataset_matadata["开放类型"] = "有条件开放"
        else:
            dataset_matadata["开放类型"] = "不予开放"
        for key, value in key_map.items():
            if key == "gmt_modified":
                detail_json[key] = detail_json[key].split(" ")[0].strip()
            if key == "dir_updatetime":
                detail_json[key] = frequency_mapping[detail_json[key]]
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_guangyuan(self, curl):
        key_map = {
            "name": "标题",
            "department": "资源提供单位",
            "add_time": "发布时间",
            "release_time": "更新时间",
            "SharedStructuredRecords": "开放类型",
            "cycle": "更新频率",
            "PHONE": "联系电话",
            "remarks": "资源摘要",
            "resourcrtype": "资源格式",
        }

        frequency_mapping = {
            "8": "实时",
            "1": "其他",
            "2": "每日",
            "3": "每周",
            "4": "每月",
            "5": "每季度",
            "6": "每半年",
            "7": "每年",
        }

        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]["details"][0]
        for key, value in key_map.items():
            if key in ["add_time", "release_time"]:
                detail_json[key] = detail_json[key].split(" ")[0].strip()
            if key == "cycle":
                if key in detail_json:
                    detail_json[key] = (
                        frequency_mapping[detail_json[key]]
                        if detail_json[key] in frequency_mapping
                        else "其他"
                    )
                else:
                    detail_json[key] = "其他"
            if key == "SharedStructuredRecords":
                detail_json[key] = (
                    "有条件开放" if detail_json[key] == "1" else "无条件开放"
                )
            if key == "resourcrtype":
                detail_json[key] = (
                    "['api']" if detail_json[key] == "数据库" else "['file']"
                )
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_suining(self, curl):
        key_map = {
            "xxzymc": "标题",
            "xxzytgf": "资源提供方",
            "sslymc": "领域名称",
            "fbrq": "发布时间",
            "gxrq": "更新时间",
            "kflx": "开放类型",  # "01": "有条件开放", "02": "无条件开放"
            "sshy": "所属行业",
            "gxzq": "更新周期",
            "zjhm": "联系电话",
            "mlbqmcList": "标签",
            "zyzy": "资源摘要",
        }

        frequency_mapping = {
            "01": "每日",
            "02": "每周",
            "03": "每月",
            "04": "季度",
            "05": "年",
            "06": "半年",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key in ["fbrq", "gxrq"]:
                detail_json[key] = time.strftime(
                    "%Y-%m-%d", time.localtime(int(detail_json[key]) / 1000)
                )
            if key == "kflx":
                detail_json[key] = (
                    "有条件开放" if detail_json[key] == "01" else "无条件开放"
                )
            if key == "gxzq":
                detail_json[key] = frequency_mapping[detail_json[key]]
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_neijiang(self, curl):
        key_map = {
            "catalogName": "标题",
            "departmentName": "来源部门",
            "topicNameList": "主题",
            "createdDate": "发布时间",
            "lastUpdatedDate": "更新时间",
            "openTypeValue": "开放条件",
            "dataNum": "数据量",
            "industryName": "行业名称",
            "accessFrequency": "更新频率",
            "departPhone": "技术支持电话",
            "departEmail": "技术支持邮箱",
            "resourceFormatValue": "资源格式",
            "catalogDesc": "描述",
        }

        response = requests.post(
            curl["url"],
            json=curl["data"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key in ["createdDate", "lastUpdatedDate"]:
                detail_json[key] = time.strftime(
                    "%Y-%m-%d", time.localtime(int(detail_json[key]) / 1000)
                )
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_leshan(self, curl):
        key_map = {
            "resourceName": "标题",
            "resourceProvider": "来源部门",
            "associativeClassification": "领域名称",
            "publishTime": "发布时间",
            "updateTime": "更新时间",
            "sharedType": "开放属性",
            "industryClassification": "行业名称",
            "updateCycle": "更新频率",
            "resourceFormat": "资源格式",
            "remark": "描述",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key == "publishTime":
                detail_json[key] = detail_json[key].split(" ")[0].strip()
            if key == "updateTime":
                if bool(detail_json[key]):
                    detail_json[key] = detail_json[key].split(" ")[0].strip()
                else:
                    detail_json[key] = detail_json["publishTime"]
            if key == "resourceFormat":
                if detail_json == "doc":
                    detail_json[key] = ["doc", "docx", "xls", "xml", "json", "csv"]
                else:
                    detail_json[key] = ["xls", "xml", "json", "csv"]
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_nanchong(self, curl):
        key_map = {
            "NAME": "标题",
            "PROVIDEDEPT": "资源提供单位信息",
            "TAG_ID1": "领域名称",
            "CREATETIME": "发布时间",
            "UPDATE_TIME": "更新时间",
            "OPENTYPE": "开放条件",
            "RESOURCESNUM": "数据量",
            "TAG_ID2": "行业名称",
            "FORMATNAME": "资源格式",
            "ABSRACTINFO": "描述",
        }

        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"][0]

        provide_detail = detail_json["provideDetail"]
        if provide_detail == "":
            dataset_matadata["部门电话"] = ""
            dataset_matadata["部门邮箱"] = ""
        else:
            dataset_matadata["部门电话"] = provide_detail["PHONENUM"]
            dataset_matadata["部门邮箱"] = provide_detail["EMAIL"]

        for key, value in key_map.items():
            if key in ["CREATETIME", "UPDATE_TIME"]:
                detail_json[key] = detail_json[key].split(" ")[0].strip()
            if key == "OPENTYPE":
                detail_json[key] = (
                    "无条件开放" if detail_json[key] == 1 else "有条件开放"
                )
            if key == "FORMATNAME":
                detail_json[key] = detail_json[key].lower()
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_sichuan_meishan(self, curl):
        dataset_matadata = {}
        dataset_matadata["详情页网址"] = (
            f"{curl['url']}?"
            f"{'&'.join([f'{key}={val}' for key, val in curl['queries'].items()])}"
        )
        list_fields = [
            "提供单位",
            "数据领域",
            "发布时间",
            "行业名称",
            "联系电话",
            "服务描述",
        ]
        table_fields = ["更新周期", "开放属性", "关键字", "格式类型"]
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
            verify=False,
        )
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        for li in soup.find("ul", attrs={"class": "desc-list-info clearfix"}).find_all(
            "li", attrs={}
        ):
            li_text = [
                li_name.get_text().split("：")[0].strip()
                for li_name in li.find_all("span")
            ]
            if li_text[0] in list_fields:
                dataset_matadata[li_text[0]] = li_text[1]
        table = soup.find(
            "table",
            attrs={
                "class": "table table-striped table-bordered table-advance table-hover"
            },
        )
        for td_name in table_fields:
            td_text = table.find("td", text=td_name).find_next("td").get_text().strip()
            td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_sichuan_yibin(self, curl):
        key_map = {
            "标题": "title",
            "来源部门": "department",
            "重点领域": "category",
            "发布时间": "publish_time",
            "更新时间": "update_time",
            "开放条件": "is_open",
            "数据量": "data_volume",
            "所属行业": "industry",
            "更新频率": "update_frequency",
            "部门电话": "telephone",
            "部门邮箱": "email",
            "标签": "tags",
            "描述": "description",
            "数据格式": "data_formats",
            "详情页网址": "url",
        }
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放条件"]
        table_fields = [
            "文件数",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
            try:
                response = requests.get(
                    curl["url"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                    verify=False,
                )
                break
            except:
                time.sleep(5)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_matadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_matadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_sichuan_dazhou(self, curl):
        key_map = {
            "标题": "title",
            "来源部门": "department",
            "重点领域": "category",
            "发布时间": "publish_time",
            "更新时间": "update_time",
            "开放条件": "is_open",
            "数据量": "data_volume",
            "所属行业": "industry",
            "更新频率": "update_frequency",
            "部门电话": "telephone",
            "部门邮箱": "email",
            "标签": "tags",
            "描述": "description",
            "数据格式": "data_formats",
            "详情页网址": "url",
        }
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放条件"]
        table_fields = [
            "数据量",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
            try:
                response = requests.get(
                    curl["url"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                    verify=False,
                )
                break
            except:
                time.sleep(5)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_matadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_matadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_sichuan_yaan(self, curl):
        key_map = {
            "标题": "title",
            "来源部门": "department",
            "重点领域": "category",
            "发布时间": "publish_time",
            "更新时间": "update_time",
            "开放条件": "is_open",
            "数据量": "data_volume",
            "所属行业": "industry",
            "更新频率": "update_frequency",
            "部门电话": "telephone",
            "部门邮箱": "email",
            "标签": "tags",
            "描述": "description",
            "数据格式": "data_formats",
            "详情页网址": "url",
        }
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放条件"]
        table_fields = [
            "数据量",
            "所属行业",
            "更新频率",
            "部门电话",
            "部门邮箱",
            "标签",
            "描述",
        ]
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
            try:
                response = requests.get(
                    curl["url"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                    verify=False,
                )
                break
            except:
                time.sleep(5)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_matadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_matadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_sichuan_bazhong(self, curl):
        # key_map = {
        #     "title": "标题",
        #     "department": "来源部门",
        #     "category": "所属领域",
        #     "publish_time":"发布时间",
        #     "update_time": "更新时间",
        #     "is_open": "是否无条件开放",
        #     "data_volume": "数据量",
        #     "industry": "所属行业",
        #     "update_frequency": "更新频率",
        #     "telephone": "部门电话",
        #     "email": "部门邮箱",
        #     "tags": "标签",
        #     "description": "描述",
        #     "data_formats": "数据格式",
        #     "province": "所属省份",
        #     "city": "所属城市",
        #     "url": "详情页网址"
        # }
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
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
        resultList = json.loads(response.text)["data"]["catalogInfo"]
        list_table = [
            "resourceName",
            "dataSource",
            "industryCategory",
            "publishTime",
            "updateTime",
            "openType",
            "dataVolume",
            "industryField",
            "dataUpdatePeriod",
            "departmentContactTel",
            "email",
            "remark",
            "resourceType",
        ]
        dataset_matadata = {}
        for li in list_table:
            dataset_matadata[li] = resultList[li]
        return dataset_matadata

    def detail_sichuan_aba(self, curl):
        # key_map = {
        #     "title": "标题",
        #     "department": "来源部门",
        #     "category": "所属领域",
        #     "publish_time":"发布时间",
        #     "update_time": "更新时间",
        #     "is_open": "是否无条件开放",
        #     "data_volume": "数据量",
        #     "industry": "所属行业",
        #     "update_frequency": "更新频率",
        #     "telephone": "部门电话",
        #     "email": "部门邮箱",
        #     "tags": "标签",
        #     "description": "描述",
        #     "data_formats": "数据格式",
        #     "province": "所属省份",
        #     "city": "所属城市",
        #     "url": "详情页网址"
        # }

        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
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

        resultList = json.loads(response.text)["data"]["resultMap"]["abaTableDetail"]
        list_table = [
            "subjectBaseName",
            "orgName",
            "fieldName",
            "releaseTime",
            "updateTime",
            "openTypeName",
            "dataNumSum",
            "industry",
            "frequency",
            "orgPhone",
            "orgEmail",
            "label",
            "tableRemark",
            "dataType",
        ]
        dataset_matadata = {}
        for li in resultList:
            if li in list_table:
                dataset_matadata[li] = resultList[li]
        return dataset_matadata

    def detail_sichuan_ganzi(self, curl):
        key_map = {
            "mlmc": "标题",
            "xxzytgf": "资源提供方",
            "ssztlmmc": "领域名称",
            "fbrq": "发布时间",
            "gxrq": "更新时间",
            "kflx": "开放类型",
            "sjl": "数据量",
            "ssqtlmmc": "所属行业",
            "gxzq": "更新周期",
            "sjhm": "联系电话",
            "mlbqmcList": "标签",
            "xxzymc": "资源摘要",
            "zygs": "资源格式",
        }

        frequency_mapping = {
            "05": "年",
            "04": "季度",
            "03": "每月",
            "02": "每周",
            "01": "每日",
        }
        open_mapping = {"01": "有条件开放", "02": "无条件开放"}
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
            try:
                response = requests.get(
                    curl["url"],
                    params=curl["queries"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                )
                break
            except:
                time.sleep(5)

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key in ["fbrq", "gxrq"]:
                detail_json[key] = time.strftime(
                    "%Y-%m-%d", time.localtime(int(detail_json[key]) / 1000)
                )
            if key == "gxzq":
                detail_json[key] = frequency_mapping[detail_json[key]]
            if key == "kflx":
                detail_json[key] = open_mapping[detail_json[key]]
            if key == "zygs":
                detail_json[key] = detail_json[key].split("\\")
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_guizhou_common(self, curl):
        key_map = {
            "name": "标题",
            "description": "数据摘要",
            "topicName": "主题名称",
            "orgName": "数据提供方",
            "industryName": "所属行业",
            "updateTime": "更新时间",
            "createTime": "发布时间",
            "openAttribute": "开放属性",
            "frequency": "更新频率",
        }

        frequency_mapping = {
            0: "每年",
            1: "每季度",
            2: "每月",
            3: "每周",
            4: "每天",
            5: "实时",
            6: "每半年",
            7: "每年",
            None: "实时",
        }
        try_cnt = 0
        while True:
            try_cnt += 1
            if try_cnt >= REQUEST_MAX_TIME:
                return {}
            try:
                response = requests.post(
                    curl["url"],
                    json=curl["data"],
                    headers=curl["headers"],
                    timeout=REQUEST_TIME_OUT,
                )
                break
            except:
                time.sleep(5)

        dataset_matadata = {}
        detail_json = json.loads(response.text)["data"]
        for key, value in key_map.items():
            if key in ["updateTime", "createTime"]:
                detail_json[key] = time.strftime(
                    "%Y-%m-%d", time.localtime(detail_json[key] / 1000)
                )
            if key == "frequency":
                detail_json[key] = frequency_mapping[detail_json[key]]
            if key == "openAttribute":
                detail_json[key] = (
                    "有条件开放" if detail_json[key] == 1 else "无条件开放"
                )
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_guizhou_guizhou(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_guiyang(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_liupanshui(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_zunyi(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_anshun(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_bijie(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_tongren(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_qianxinan(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_qiandongnan(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_qiannan(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_guizhou_guianxinqu(self, curl):
        return self.detail_guizhou_common(curl)

    def detail_ningxia_ningxia(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放条件"]
        table_fields = ["所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]
        response = requests.get(
            curl["url"], headers=curl["headers"], timeout=REQUEST_TIME_OUT, verify=False
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("ul", attrs={"class": "d-title pull-left"})
        title = title.find("h4").get_text()
        dataset_metadata["标题"] = title
        for li in soup.find("ul", attrs={"class": "list-inline"}).find_all(
            "li", attrs={}
        ):
            li_name = li.get_text().split("：")[0].strip()
            if li_name in list_fields:
                li_text = (
                    li.find("span", attrs={"class": "text-primary"}).get_text().strip()
                )
                dataset_metadata[li_name] = li_text
        table = soup.find("li", attrs={"name": "basicinfo"})
        for td_name in table_fields:
            td_text = table.find("td", text=td_name)
            if td_text is not None:
                td_text = td_text.find_next("td").get_text().strip()
                td_text = ucd.normalize("NFKC", td_text).replace(" ", "")
                dataset_metadata[td_name] = td_text
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_ningxia_yinchuan(self, curl):
        list_fields = [
            "来源部门",
            "所属主题",
            "发布时间",
            "最后更新",
            "开放状态",
            "所属行业",
            "更新频率",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("div", attrs={"class": "detail-header"})
        title = title.find("span", attrs={"class": "detail-title"}).get_text()
        dataset_metadata["标题"] = title

        for divs in (
            soup.find("div", attrs={"class": "detail-info-list tab-body"})
            .find("li", attrs={"action": "data-info"})
            .find_all("div", attrs={"class": "info-list"})
        ):
            for li in divs.find_all("li"):
                li_name = (
                    li.find("div", attrs={"class": "info-header"}).get_text().strip()
                )
                if li_name in list_fields:
                    div = li.find("div", attrs={"class": "info-body"})
                    li_text = div.get_text().strip()
                    if li_name == "标签":
                        li_text = div["tags"]
                    if li_name in ["发布时间", "最后更新"]:
                        li_text = li_text.split(" ")[0].strip()
                    dataset_metadata[li_name] = li_text
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_xinjiang_wulumuqi(self, curl):
        list_fields = [
            "来源部门",
            "所属主题",
            "发布时间",
            "最后更新",
            "开放状态",
            "所属行业",
            "更新频率",
            "标签",
            "描述",
        ]
        response = requests.get(
            curl["url"],
            params=curl["queries"],
            headers=curl["headers"],
            timeout=REQUEST_TIME_OUT,
        )
        if response.status_code != requests.codes.ok:
            self.log_request_error(response.status_code, curl["url"])
            return dict()
        html = response.content.decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        title = soup.find("div", attrs={"class": "detail-header"})
        title = title.find("span", attrs={"class": "detail-title"}).get_text()
        dataset_metadata["标题"] = title

        for divs in (
            soup.find("div", attrs={"class": "detail-info-list tab-body"})
            .find("li", attrs={"action": "data-info"})
            .find_all("div", attrs={"class": "info-list"})
        ):
            for li in divs.find_all("li"):
                li_name = (
                    li.find("div", attrs={"class": "info-header"}).get_text().strip()
                )
                if li_name in list_fields:
                    div = li.find("div", attrs={"class": "info-body"})
                    li_text = div.get_text().strip()
                    if li_name == "标签":
                        li_text = div["tags"]
                    if li_name in ["发布时间", "最后更新"]:
                        li_text = li_text.split(" ")[0].strip()
                    dataset_metadata[li_name] = li_text
        dataset_metadata["详情页网址"] = curl["url"]
        return dataset_metadata

    def detail_other(self, curl):
        log_error("detail: 暂无该地 - %s - %s", self.province, self.city)
