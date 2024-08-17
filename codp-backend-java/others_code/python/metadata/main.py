import json
import re
import time
import urllib

import requests
from tqdm import tqdm

from constants import (METADATA_SAVE_PATH, PROVINCE_CURL_JSON_PATH, REQUEST_TIME_OUT)
from detail import Detail
from resultlist import ResultList

curls = {}


class Crawler:
    def __init__(self, province, city):
        self.province = province
        self.city = city
        self.result_list = ResultList(self.province, self.city)
        self.detail = Detail(self.province, self.city)
        self.result_list_curl = curls[province][city]['resultList']
        self.detail_list_curl = curls[province][city]['detail']
        self.metadata_list = []

    def crawl(self):
        func_name = f"crawl_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.crawl_other)
        func()

    # TODO: 反爬虫 ConnectionResetError: [WinError 10054]
    def crawl_sichuan_sichuan(self):
        for page in tqdm(range(1, 1249)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link['link']
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['数据格式'] = link['data_formats'] if link['data_formats'] != '[]' else "['file']"
                    metadata['详情页网址'] = curl['url']
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_sichuan_chengdu(self):
        for page in tqdm(range(1, 704)):
            # for page in range(1, 704):
            #     print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            time.sleep(5)
            links = self.result_list.get_result_list(curl)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link['link']
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['数据格式'] = link['data_formats'] if link['data_formats'] != '[]' else "['file']"
                    metadata['详情页网址'] = curl['url']
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_sichuan_zigong(self):
        for page in tqdm(range(1, 870)):
            # print(page)
            curl = self.result_list_curl.copy()
            time.sleep(5)
            curl['queries']['offset'] = str((page - 1) * 10)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['id'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = 'https://data.zg.cn/snww/sjzy/detail.html?' + id
                    # 根据可下载类型获取type
                    turl = curls[self.province][self.city]['typeList'].copy()
                    turl['queries']['id'] = id
                    response = requests.get(turl['url'],
                                            params=turl['queries'],
                                            headers=turl['headers'],
                                            timeout=REQUEST_TIME_OUT)
                    if response.status_code != requests.codes.ok:
                        print("error " + str(response.status_code) + ": " + turl['url'])
                        type_json = dict()
                    else:
                        type_json = json.loads(response.text)['data']
                    if not bool(type_json):
                        type_json = dict()
                    type_list = []
                    for name, type_info in type_json.items():
                        type_list.append(type_info['type'])
                    metadata['数据格式'] = str(type_list) if bool(type_list) else "['file']"
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_sichuan_panzhihua(self):
        for page in tqdm(range(1, 700)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link['link']
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['数据格式'] = link['data_formats'] if link['data_formats'] != '[]' else "['file']"
                    metadata['详情页网址'] = curl['url']
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_sichuan_luzhou(self):
        for page in tqdm(range(1, 701)):
            # for page in range(1, 701):
            # print(page)
            curl = self.result_list_curl.copy()
            time.sleep(5)
            curl['data']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id, opens, publisht, updatet in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['id'] = id
                curl['queries']['type'] = "opendata"
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['开放条件'] = "无条件开放" if opens == '1' else "有条件开放"
                    metadata['详情页网址'] = 'https://data.luzhou.cn/portal/service_detail?id=' + id + '&type=opendata'
                    metadata['发布时间'] = publisht
                    metadata['更新时间'] = updatet
                    metadata['数据格式'] = "['api']"
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_sichuan_deyang(self):
        for page in tqdm(range(1, 99)):
            # print(page)
            curl = self.result_list_curl.copy()
            time.sleep(5)
            curl['data']['pageNo'] = page
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['mlbh'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "https://www.dysdsj.cn/#/DataSet/" + id.replace("/", "%2F")
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_sichuan_mianyang(self):
        for page in tqdm(range(1, 1297)):
            # print(page)
            curl = self.result_list_curl.copy()
            # time.sleep(5)
            curl['queries']['startNum'] = str((page - 1) * 8)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['id'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "https://data.mianyang.cn/zwztzlm/index.jhtml?caseid=" + id
                    metadata['数据格式'] = "['api']"  # 只有数据库和接口类型，实际全是接口
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_sichuan_guangyuan(self):
        for page in tqdm(range(1, 1874)):
            # print(page)
            curl = self.result_list_curl.copy()
            # time.sleep(3)
            curl['data']['currentPage'] = page
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['data']['id'] = str(id)
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "http://data.cngy.gov.cn/open/index.html?id=user&messid=" + str(id)
                    metadata['领域名称'] = "生活服务"
                    metadata['行业名称'] = "公共管理、社会保障和社会组织"
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_sichuan_suining(self):
        for page in tqdm(range(1, 910)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNo'] = page
            ids = self.result_list.get_result_list(curl)
            for id, typeList in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['mlbh'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "https://www.suining.gov.cn/data#/DataSet/" + id.replace("/", "%2F")
                    type_mapping = {'10': 'csv', '04': 'xlsx', '08': 'xml', '09': 'json'}
                    type_list = ['api']
                    for label in typeList:
                        type_list.append(type_mapping[label])
                    metadata['资源格式'] = str(type_list)
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_sichuan_neijiang(self):
        for page in tqdm(range(0, 317)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['data']['page'] = page
            curl['queries']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['data']['id'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "https://www.neijiang.gov.cn/neiJiangPublicData/resourceCatalog/detail?id=" + id
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_sichuan_leshan(self):
        for page in tqdm(range(1, 1575)):
            # print(page)
            curl = self.result_list_curl.copy()
            # time.sleep(3)
            curl['queries']['pageIndex'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['resourceId'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "https://www.leshan.gov.cn/data/#/source_catalog_detail/" + id + "/0"
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_sichuan_nanchong(self):
        for page in tqdm(range(1, 1655)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['id'] = id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "https://www.nanchong.gov.cn/data/catalog/details.html?id=" + id
                    # print(metadata)
                    self.metadata_list.append(metadata)
            # 响应太慢了，每次都写入吧
            self.save_matadata_as_json(METADATA_SAVE_PATH)
            self.metadata_list.clear()
            print('write to file')

    def crawl_shaanxi_shaanxi(self):
        for page in tqdm(range(1, 16)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page.pageNo'] = str(page)
            metas = self.result_list.get_result_list(curl)
            self.metadata_list.extend(metas)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_ningxia_ningxia(self):
        for page in tqdm(range(1, 202)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            if len(links) == 0:
                break
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link['link']
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['数据格式'] = link['data_formats'] if link['data_formats'] != '[]' else "['file']"
                    metadata['详情页网址'] = curl['url']
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_ningxia_yinchuan(self):
        for page in tqdm(range(1, 169)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['data']['start'] = str((page - 1) * 6)
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for cata_id, formate in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['cata_id'] = cata_id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "http://data.yinchuan.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=" + cata_id
                    formate_mapping = {'1': 'xls', '2': 'xml', '3': 'json', '4': 'csv'}
                    if formate is None:
                        metadata['数据格式'] = "['file']"
                    else:
                        type_list = [formate_mapping[s.strip()] for s in formate.split(',')[:-1]]
                        metadata['数据格式'] = str(type_list)
                    # print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_xinjiang_wulumuqi(self):
        for page in tqdm(range(1, 18)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['data']['start'] = str((page - 1) * 6)
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for cata_id, formate in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['cata_id'] = cata_id
                metadata = self.detail.get_detail(curl)
                if bool(metadata):
                    metadata['详情页网址'] = "http://zwfw.wlmq.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=" + cata_id
                    formate_mapping = {'1': 'xls', '2': 'xml', '3': 'json', '4': 'csv'}
                    if formate is None:
                        metadata['数据格式'] = "['file']"
                    else:
                        type_list = [formate_mapping[s.strip()] for s in formate.split(',')[:-1]]
                        metadata['数据格式'] = str(type_list)
                    # print(metadata)
                    self.metadata_list.append(metadata)
            if page % 100 == 0:
                self.save_matadata_as_json(METADATA_SAVE_PATH)
                self.metadata_list.clear()
                print('write to file')

    def crawl_anhui_hefei(self):
        for page in tqdm(range(1, 34)):
            # print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['currentPageNo'] = str(page)
            # curl['queries']['_'] = str(int(round(time.time() * 1000)))
            # curl['headers']['Referer'] = curl['headers']['Referer'].format(str(page))
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for iid, zyId in ids:
                curl = self.detail_list_curl.copy()
                curl['data']['id'] = iid
                curl['data']['zyId'] = zyId
                metadata = self.detail.get_detail(curl)
                metadata[
                    "详情页网址"] = 'https://www.hefei.gov.cn/open-data-web/data/detail-hfs.do?&id=' + iid + '&zyId=' + zyId
                self.metadata_list.append(metadata)

    def crawl_anhui_wuhu(self):
        for page in range(1, 37):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNo'] = str(page)
            metadata_list = self.result_list.get_result_list(curl)
            if len(metadata_list) == 0:
                break
            self.metadata_list.extend(metadata_list)

    def crawl_anhui_bengbu(self):
        # dataset
        for page in range(0, 8):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['pageIndex'] = str(page)
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for iid in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['resourceId'] = re.search(r"(?<=resourceId=)\d+", iid).group(0)
                # print(curl['queries']['resourceId'])
                metadata = self.detail.get_detail(curl)
                metadata['详情页网址'] = 'https://www.bengbu.gov.cn' + iid
                if metadata['下载格式'][0] == '':
                    metadata['下载格式'] = ['file']
                # print(metadata)
                self.metadata_list.append(metadata)
        # api
        for page in range(0, 2):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['pageIndex'] = str(page)
            curl['queries']['resourceType'] = 'api'
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for iid in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['resourceId'] = re.search(r"(?<=resourceId=)\d+", iid).group(0)
                curl['url'] = 'https://www.bengbu.gov.cn/site/tpl/6541'
                metadata = self.detail.get_detail(curl)
                metadata['详情页网址'] = 'https://www.bengbu.gov.cn' + iid
                metadata['下载格式'] = ['api']
                # print(metadata)
                self.metadata_list.append(metadata)

    def crawl_anhui_huaibei(self):
        for page in range(1, 39):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['curPageNumber'] = str(page)
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for iid in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['id'] = iid
                print(curl['queries']['resourceId'])
                metadata = self.detail.get_detail(curl)
                metadata['详情页网址'] = 'http://open.huaibeidata.cn:1123/#/data_public/detail/' + iid
                # print(metadata)
                self.metadata_list.append(metadata)

    def crawl_anhui_huangshan(self):
        # dataset
        for page in range(1, 26):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['pageIndex'] = str(page)
            time.sleep(1)
            ids = self.result_list.get_result_list(curl)
            if len(ids) == 0:
                break
            for iid, depart, cata, format_list in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['resourceId'] = re.search(r"(?<=resourceId=)\d+", iid).group(0)
                time.sleep(1)
                metadata = self.detail.get_detail(curl)
                metadata['详情页网址'] = 'https://www.huangshan.gov.cn' + iid
                metadata['提供机构'] = depart
                metadata['数据领域'] = cata
                metadata['资源格式'] = format_list
                metadata['开放条件'] = '无条件开放'
                # print(metadata)
                self.metadata_list.append(metadata)

    def crawl_anhui_chuzhou(self):
        for page in range(0, 86):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            time.sleep(1)
            metadata_list = self.result_list.get_result_list(curl)
            if len(metadata_list) == 0:
                break
            for meta in metadata_list:
                curl = self.detail_list_curl.copy()
                curl['queries']['name'] = meta['标题']
                time.sleep(1)
                remains = self.detail.get_detail(curl)
                # print(metadata)
                self.metadata_list.append(meta | remains)

    def crawl_other(self):
        print("暂无该省市")

    def save_matadata_as_json(self, save_dir):
        filename = save_dir + self.province + '_' + self.city + '.json'
        with open(filename, 'a', encoding='utf-8') as f:
            json.dump(self.metadata_list, f, ensure_ascii=False)


if __name__ == '__main__':
    with open(PROVINCE_CURL_JSON_PATH, 'r', encoding='utf-8') as curlFile:
        curls = json.load(curlFile)

    crawler = Crawler("anhui", "hefei")
    crawler.crawl()
    crawler.save_matadata_as_json(METADATA_SAVE_PATH)
    # for province in provinces:
    #     crawler = Crawler(province)
    #     crawler.crawl()
