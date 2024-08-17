import copy
import json
import os.path
import urllib

from constants import (METADATA_SAVE_PATH, PROVINCE_CURL_JSON_PATH, PROVINCE_LIST)
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
    def crawl_beijing_beijing(self):
        for page in range(1, 2):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['curPage'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] = link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_tianjin_tianjin(self):
        curl = self.result_list_curl.copy()
        links = self.result_list.get_result_list(curl)
        for link in links[:10]:
            curl = self.detail_list_curl.copy()
            curl['url'] = link
            metadata = self.detail.get_detail(curl)
            print(metadata)
            self.metadata_list.append(metadata)

    def crawl_hebei_hebei(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNo'] = str(page)
            metadata_ids = self.result_list.get_result_list(curl)
            for metadata_id in metadata_ids:
                curl = self.detail_list_curl.copy()
                curl['data']['rowkey'] = metadata_id['METADATA_ID']
                curl['data']['list_url'] = curl['data']['list_url'].format(page)
                metadata = self.detail.get_detail(curl)
                metadata['所属主题'] = metadata_id['THEME_NAME']
                metadata['发布时间'] = metadata_id['CREAT_DATE']
                metadata['更新日期'] = metadata_id['UPDATE_DATE']
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_neimenggu_neimenggu(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['data']['id'] = id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_liaoning_liaoning(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_shandong_shandong(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_jiangsu(self):
        for city, max_page in zip(['', 'all'], [54, 11]):
            for page in range(0, max_page):
                print(page)
                curl = self.result_list_curl.copy()
                curl['data'] = curl['data'].format(page, city)
                rowGuid_tag_list = self.result_list.get_result_list(curl)
                for rowGuid, tags in rowGuid_tag_list:
                    curl = self.detail_list_curl.copy()
                    curl['url'] = curl['url'].format(rowGuid)
                    curl['data'] = curl['data'].format(rowGuid)
                    metadata = self.detail.get_detail(curl)
                    metadata["数据格式"] = tags
                    metadata["详情页网址"] = curl['headers']['Referer'].format(rowGuid)
                    print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_jiangsu_nanjing(self):
        curl = self.result_list_curl.copy()
        links = self.result_list.get_result_list(curl)

    def crawl_jiangsu_wuxi(self):
        for page in range(1, 295):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['page'] = str(page)
            cata_ids = self.result_list.get_result_list(curl)
            for cata_id in cata_ids:
                curl = self.detail_list_curl.copy()
                curl['params']['cata_id'] = cata_id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_xuzhou(self):
        for page in range(1, 43):
            print(page)
            curl = self.result_list_curl.copy()
            curl['json_data']['pageNo'] = page
            mlbhs = self.result_list.get_result_list(curl)
            for mlbh in mlbhs:
                curl = self.detail_list_curl.copy()
                curl['params']['mlbh'] = mlbh
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_suzhou(self):
        for page in range(1,162):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['current'] = str(page)
            curl['json_data']['current'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['params']['id'] = id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_nantong(self):
        for page in range(120):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['params']['id'] = id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_lianyungang(self):
        for page in range(1, 111):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['pageNum1'] = str(page)
            dmids = self.result_list.get_result_list(curl)
            for dmid in dmids:
                curl = self.detail_list_curl.copy()
                curl['params']['dmid'] = dmid
                valid, metadata = self.detail.get_detail(curl)
                if valid:
                    print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_jiangsu_huaian(self):
        for page in range(1, 130):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['pageNum'] = str(page)
            catalogIds = self.result_list.get_result_list(curl)
            for catalogId in catalogIds:
                curl = self.detail_list_curl.copy()
                curl['params']['catalogId'] = catalogId
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_yancheng(self):
        for page in range(1, 99):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['pageNumber'] = str(page)
            catalogPks = self.result_list.get_result_list(curl)
            for catalogPk in catalogPks:
                curl = self.detail_list_curl.copy()
                curl['params']['catalogId'] = catalogPk
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_zhenjiang(self):
        for page in range(215):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['params']['id'] = id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_taizhou(self):
        for page in range(1, 541):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['page'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['url'] = curl['url'].format(id)
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangsu_suqian(self):
        for page in range(1, 268):
            print(page)
            curl = self.result_list_curl.copy()
            curl['params']['page'] = str(page)
            id_infos = self.result_list.get_result_list(curl)
            for id, update_time in id_infos:
                curl = self.detail_list_curl.copy()
                curl['url'] = curl['url'].format(id)
                metadata = self.detail.get_detail(curl)
                metadata["更新时间"] = update_time
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_shanghai_shanghai(self):
        for page in range(1, 451):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data'] = curl['data'].replace('\"pageNum\":1', f'\"pageNum\":{page}').encode()
            dataset_ids = self.result_list.get_result_list(curl)
            for dataset_id in dataset_ids:
                curl = self.detail_list_curl.copy()
                curl['headers']['Referer'] = curl['headers']['Referer'].format(
                    dataset_id['datasetId'], urllib.parse.quote(dataset_id['datasetName']))
                curl['url'] = curl['url'].format(dataset_id['datasetId'])
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_zhejiang(self):
        for page in range(1, 132):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNumber'] = str(page)
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['queries'] = iid
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_hangzhou(self):
        for page in range(1, 660):
            print(page)
            curl = self.result_list_curl.copy()
            post_data_json = json.loads(curl['data']['postData'])
            post_data_json['pageSplit']['pageNumber'] = page
            curl['data']['postData'] = json.dumps(post_data_json)
            id_formats = self.result_list.get_result_list(curl)
            for id, mformat in id_formats:
                curl = copy.deepcopy(self.detail_list_curl)
                curl['format'] = mformat
                curl['url'] = curl['url'].format(mformat)
                curl['headers']['Referer'] = curl['headers']['Referer'].format(mformat, id)
                post_data_json = json.loads(curl['data']['postData'])
                post_data_json['source_id'] = id
                curl['data']['postData'] = json.dumps(post_data_json)
                metadata = self.detail.get_detail(curl)
                if metadata is None:
                    continue
                metadata["数据格式"] = mformat
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_ningbo(self):
        for page in range(1, 177):
            print(page)
            curl = self.result_list_curl.copy()
            curl['json_data']['pageNo'] = str(page)
            uuids = self.result_list.get_result_list(curl)
            for uuid in uuids:
                curl = self.detail_list_curl.copy()
                curl['json_data']['uuid'] = uuid
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_wenzhou(self):
        for page in range(1, 51):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNumber'] = str(page)
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['iid'] = iid['iid']
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_jiaxing(self):
        for page in range(1, 20):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNumber'] = str(page)
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['iid'] = iid['iid']
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_shaoxing(self):
        for page in range(1, 75):
            if page == 3:
                continue
            print(page)
            curl = self.result_list_curl.copy()
            curl['json_data']['pageNum'] = page
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['dataId'] = iid
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_jinhua(self):
        for page in range(1, 39):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNumber'] = str(page)
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['iid'] = iid['iid']
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_quzhou(self):
        for page in range(1, 51):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNumber'] = str(page)
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['iid'] = iid['iid']
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_zhoushan(self):
        for page in range(1, 12):
            print(page)
            curl = self.result_list_curl.copy()
            curl['json_data']['pageNo'] = page
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['params']['id'] = id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_taizhou(self):
        for page in range(1, 108):
            print(page)
            curl = self.result_list_curl.copy()
            curl['json_data']['pageNum'] = page
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['dataId'] = iid
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_zhejiang_lishui(self):
        for page in range(1, 44):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNumber'] = str(page)
            iids = self.result_list.get_result_list(curl)
            for iid in iids:
                curl = self.detail_list_curl.copy()
                curl['params']['iid'] = iid['iid']
                metadata = self.detail.get_detail(curl)
                if metadata is not None:
                    print(metadata)
                    self.metadata_list.append(metadata)


    def crawl_anhui_anhui(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNum'] = str(page)
            rids = self.result_list.get_result_list(curl)
            for rid in rids:
                curl = self.detail_list_curl.copy()
                curl['headers']['Referer'] = curl['headers']['Referer'].format(rid)
                curl['data']['rid'] = rid
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_jiangxi_jiangxi(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['current'] = page
            data_ids = self.result_list.get_result_list(curl)
            for data_id in data_ids:
                curl = self.detail_list_curl.copy()
                curl['headers']['Referer'] = curl['headers']['Referer'].format(data_id)
                curl['queries']['dataId'] = data_id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_fujian_fujian(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_guangdong_guangdong(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageNo'] = page
            res_ids = self.result_list.get_result_list(curl)
            for res_id in res_ids:
                curl = self.detail_list_curl.copy()
                curl['data']['resId'] = res_id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_guangxi_guangxi(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_hainan_hainan(self):
        for page in range(0, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['curPage'] = page
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['url'] = curl['url'].format(id)
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_ningxia_ningxia(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_shaanxi_shaanxi(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page.pageNo'] = str(page)
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['queries']['id'] = str(id)
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_sichuan_sichuan(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['queries']['page'] = str(page)
            links = self.result_list.get_result_list(curl)
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_guizhou_guizhou(self):
        for page in range(1, 5):
            print(page)
            curl = self.result_list_curl.copy()
            curl['data']['pageIndex'] = page
            ids = self.result_list.get_result_list(curl)
            for id in ids:
                curl = self.detail_list_curl.copy()
                curl['data']['id'] = id
                metadata = self.detail.get_detail(curl)
                print(metadata)
                self.metadata_list.append(metadata)

    def crawl_other(self):
        print("暂无该省")

    def save_metadata_as_json(self, save_dir):
        filename = os.path.join(save_dir, self.province + '_' + self.city + '.json')
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_list, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    provinces = PROVINCE_LIST
    with open(PROVINCE_CURL_JSON_PATH, 'r', encoding='utf-8') as curlFile:
        curls = json.load(curlFile)

    crawler = Crawler("jiangsu", "xuzhou")
    # crawler = Crawler("shanghai", "shanghai")
    # crawler = Crawler("zhejiang", "taizhou")
    crawler.crawl()
    crawler.save_metadata_as_json(METADATA_SAVE_PATH)
    # for province in provinces:
    #     crawler = Crawler(province)
    #     crawler.crawl()