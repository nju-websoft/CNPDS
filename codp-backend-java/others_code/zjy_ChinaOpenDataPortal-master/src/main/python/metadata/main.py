import json
import os.path
import urllib

from constants import (METADATA_SAVE_PATH, PROVINCE_CURL_JSON_PATH, PROVINCE_LIST)
from detail import Detail
from resultlist import ResultList
import time

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

    def crawl_shandong_common(self, use_cache=True, page_size=10):
        # TODO:debug
        # use_cache = False
        max_retry = 3
        page = 1
        retry_time = 0
        cache_dir = "./results/cache/"
        index_cache_dir = cache_dir + f"{self.province}_{self.city}_index_cache.txt"
        data_cache_dir = cache_dir + f"{self.province}_{self.city}_data_cache.json"
        if use_cache:
            cache_page = self.prepare_cache(cache_dir, index_cache_dir, data_cache_dir, page_size)
            page += cache_page

        # for page in range(637, 638):
        while (True):
            print(f"第 {page} 页...")
            # TODO:加文件类型
            curl = self.result_list_curl.copy()
            curl['params']['page'] = str(page)
            page += 1
            links = self.result_list.get_result_list(curl)
            if not len(links):
                if retry_time >= max_retry:
                    print("已爬完")
                    return
                else:
                    print("无数据，重试中...")
                    retry_time += 1
                    continue
            retry_time = 0  # 重置
            for link in links:
                curl = self.detail_list_curl.copy()
                curl['url'] += link['link']
                metadata = self.detail.get_detail(curl)
                metadata["数据格式"] = link['data_formats']  # TODO：扔到detail方法里面
                metadata["详情页网址"] = curl['url']
                self.metadata_list.append(metadata)
            if use_cache:
                with open(index_cache_dir, 'w', encoding='utf-8') as page_writer:
                    page_writer.write(str(page))
                # TODO:append来提高效率
                with open(data_cache_dir, 'w', encoding='utf-8') as data_writer:
                    json.dump(self.metadata_list, data_writer, ensure_ascii=False)

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

    def crawl_jiangsu_jiangsu(self):
        for city in ['', 'all']:
            for page in range(0, 10):
                print(page)
                curl = self.result_list_curl.copy()
                curl['data'] = curl['data'].format(page, city)
                rowGuids = self.result_list.get_result_list(curl)
                for rowGuid in rowGuids:
                    curl = self.detail_list_curl.copy()
                    curl['url'] = curl['url'].format(rowGuid)
                    curl['data'] = curl['data'].format(rowGuid)
                    metadata = self.detail.get_detail(curl)
                    print(metadata)
                    self.metadata_list.append(metadata)

    def crawl_shanghai_shanghai(self):
        for page in range(1, 5):
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
        for page in range(1, 5):
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

    def save_matadata_as_json(self, save_dir):
        filename = save_dir + self.province + '_' + self.city + '.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_list, f, ensure_ascii=False)

    def prepare_cache(self, cache_dir, index_cache_dir, data_cache_dir, page_size=10):
        print("开启缓存")
        page = 0
        if os.path.exists(index_cache_dir) and os.path.exists(data_cache_dir):
            print("读取缓存文件")
            with open(index_cache_dir, encoding='utf-8') as f_reader:
                page = int(f_reader.readline())
                print("读取当前缓存page为：", page)
            with open(data_cache_dir, encoding='utf-8') as f_reader:
                self.metadata_list.extend(json.load(f_reader))
                print(f"缓存数据读取成功，共 {len(self.metadata_list)} 条")
            # TODO:强制检查，可以取消；校验不正确的情况处理
            try:
                assert (page) * page_size == len(self.metadata_list), "缓存页码和缓存数据数量不匹配"
            except Exception as e:
                print(e)
                page = len(self.metadata_list) // page_size
                self.metadata_list = self.metadata_list[:page * page_size]
                print("修改缓存页码为", page)
                print("保留缓存数据", len(self.metadata_list), "条")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        time.sleep(5)  # 防止手残
        return page


if __name__ == '__main__':
    with open(PROVINCE_CURL_JSON_PATH, 'r', encoding='utf-8') as curlFile:
        curls = json.load(curlFile)
    # cities = curls["shandong"].keys()
    # cities=['shandong', 'jinan', 'qingdao', 'zibo', 'zaozhuang', 'dongying', 'yantai', 'weifang', 'jining', 'taian', 'weihai', 'rizhao', 'linyi', 'dezhou', 'liaocheng', 'binzhou', 'heze']
    # cities = ['shandong']
    # cities=['jinan', 'qingdao', 'zibo', 'zaozhuang', 'dongying', 'yantai', 'weifang', 'jining', 'taian', 'weihai', 'rizhao', 'linyi', 'dezhou', 'liaocheng', 'binzhou', 'heze']
    cities = ['heze']
    print(cities)
    for city in cities:
        print("当前城市：", city)
        crawler = Crawler("shandong", city)
        crawler.crawl()
        save_path = "./results/"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        crawler.save_matadata_as_json(save_path)
