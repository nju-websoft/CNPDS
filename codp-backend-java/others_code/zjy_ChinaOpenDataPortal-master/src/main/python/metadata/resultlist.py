import json
import re
import urllib

import requests
from bs4 import BeautifulSoup
from constants import REQUEST_TIME_OUT


class ResultList:
    def __init__(self, province, city) -> None:
        self.province = province
        self.city = city

    def get_result_list(self, curl):
        func_name = f"result_list_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.result_list_other)
        return func(curl)

    def result_list_beijing_beijing(self, curl):
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['object']['docs']
        print(resultList)
        links = [x['url'] for x in resultList]
        return links

    def result_list_tianjin_tianjin(self, curl):
        response = requests.get(curl['url'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['dataList']
        links = [x['href'] for x in resultList]
        return links

    def result_list_hebei_hebei(self, curl):
        response = requests.post(curl['url'],
                                 headers=curl['headers'],
                                 data=curl['data'],
                                 timeout=REQUEST_TIME_OUT,
                                 verify=False)
        resultList = json.loads(response.text)['page']['dataList']
        metadata_ids = [{
            'METADATA_ID': x['METADATA_ID'],
            'CREAT_DATE': x['CREAT_DATE'],
            'UPDATE_DATE': x['UPDATE_DATE'],
            'THEME_NAME': x['THEME_NAME']
        } for x in resultList]
        return metadata_ids

    def result_list_neimenggu_neimenggu(self, curl):
        response = requests.post(curl['url'],
                                 headers=curl['headers'],
                                 data=curl['data'],
                                 timeout=REQUEST_TIME_OUT,
                                 verify=False)
        resultList = json.loads(response.text)['data']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_liaoning_liaoning(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'cata-title'}):
            link = title.find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            links.append(link['href'])
        return links

    def result_list_shandong_common(self, curl, prefix):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'cata-title'}):
            link = title.find('a', attrs={'href': re.compile(f"/{prefix}/catalog/*")})
            file_type_elements = title.parent.find('div', attrs={'class': 'file-type'}).findChildren('li')
            data_formats = list(map(lambda x: x['class'][0].lower(), file_type_elements))  # class 标签内，“接口”就叫“api”
            if len(data_formats) == 0:
                data_formats.append("file")
            links.append({'link': link['href'], 'data_formats': data_formats})
        return links

    def result_list_shandong_shandong(self, curl):
        return self.result_list_shandong_common(curl, "portal")

    def result_list_shandong_jinan(self, curl):
        return self.result_list_shandong_common(curl, "jinan")

    def result_list_shandong_qingdao(self, curl):
        return self.result_list_shandong_common(curl, "qingdao")

    def result_list_shandong_zibo(self, curl):
        return self.result_list_shandong_common(curl, "zibo")

    def result_list_shandong_zaozhuang(self, curl):
        return self.result_list_shandong_common(curl, "zaozhuang")

    def result_list_shandong_dongying(self, curl):
        return self.result_list_shandong_common(curl, "dongying")

    def result_list_shandong_yantai(self, curl):
        return self.result_list_shandong_common(curl, "yantai")

    def result_list_shandong_weifang(self, curl):
        return self.result_list_shandong_common(curl, "weifang")

    def result_list_shandong_jining(self, curl):
        return self.result_list_shandong_common(curl, "jining")

    def result_list_shandong_taian(self, curl):
        return self.result_list_shandong_common(curl, "taian")

    def result_list_shandong_weihai(self, curl):
        return self.result_list_shandong_common(curl, "weihai")

    def result_list_shandong_rizhao(self, curl):
        return self.result_list_shandong_common(curl, "rizhao")

    def result_list_shandong_linyi(self, curl):
        return self.result_list_shandong_common(curl, "linyi")

    def result_list_shandong_dezhou(self, curl):
        return self.result_list_shandong_common(curl, "dezhou")

    def result_list_shandong_liaocheng(self, curl):
        return self.result_list_shandong_common(curl, "liaocheng")

    def result_list_shandong_binzhou(self, curl):
        return self.result_list_shandong_common(curl, "binzhou")

    def result_list_shandong_heze(self, curl):
        return self.result_list_shandong_common(curl, "heze")

    def result_list_jiangsu_jiangsu(self, curl):
        response = requests.post(curl['url'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 timeout=REQUEST_TIME_OUT,
                                 verify=False)
        resultList = json.loads(response.text)['custom']['resultList']
        rowGuids = [x['rowGuid'] for x in resultList]
        return rowGuids

    def result_list_shanghai_shanghai(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['content']
        dataset_ids = [{'datasetId': x['datasetId'], 'datasetName': x['datasetName']} for x in resultList]
        return dataset_ids

    def result_list_zhejiang_zhejiang(self, curl):
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = json.loads(response.text)['data']
        soup = BeautifulSoup(html, "html.parser")
        iids = []
        for title in soup.find_all('div', attrs={'class': 'search_result'}):
            link = title.find('a', attrs={'href': re.compile("../detail/data.do*")})
            parsed_link = urllib.parse.urlparse(link['href'])
            querys = urllib.parse.parse_qs(parsed_link.query)
            querys = {k: v[0] for k, v in querys.items()}
            iids.append(querys)
        return iids

    def result_list_anhui_anhui(self, curl):
        response = requests.post(curl['url'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['rows']
        rids = [x['rid'] for x in resultList]
        return rids

    def result_list_jiangxi_jiangxi(self, curl):
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']
        data_ids = [x['dataId'] for x in resultList]
        return data_ids

    def result_list_fujian_fujian(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'mrline1-title'}):
            link = title.find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            links.append(link['href'])
        return links

    def result_list_guangdong_guangdong(self, curl):
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['page']['list']
        res_ids = [x['resId'] for x in resultList]
        return res_ids

    def result_list_guangxi_guangxi(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'cata-title'}):
            link = title.find('a', attrs={'href': re.compile("/portal/catalog/*")})
            links.append(link['href'])
        return links

    def result_list_hainan_hainan(self, curl):
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['content']
        res_ids = [x['id'] for x in resultList]
        return res_ids

    def result_list_ningxia_ningxia(self, curl):
        response = requests.get(curl['url'],
                                params=curl['queries'],
                                cookies=curl['cookies'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'cata-title'}):
            link = title.find('a', attrs={'href': re.compile("/portal/catalog/*")})
            links.append(link['href'])
        return links

    def result_list_shaanxi_shaanxi(self, curl):
        response = requests.get(curl['url'],
                                params=curl['queries'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)[0]['result']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_sichuan_sichuan(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'cata-title'}):
            link = title.find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            links.append(link['href'])
        return links

    def result_list_guizhou_guizhou(self, curl):
        response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
        print(response)
        resultList = json.loads(response.text)['data']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_other(self):
        print("暂无该省")
