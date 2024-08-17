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

    def result_list_shandong_shandong(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for title in soup.find_all('div', attrs={'class': 'cata-title'}):
            link = title.find('a', attrs={'href': re.compile("/portal/catalog/*")})
            links.append(link['href'])
        return links

    def result_list_jiangsu_jiangsu(self, curl):
        response = requests.post(curl['url'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 timeout=REQUEST_TIME_OUT,
                                 verify=False)
        resultList = json.loads(response.text)['custom']['resultList']
        rowGuid_tag_list = [(x['rowGuid'], [n['name'].replace("其他", 'file').lower() for n in x['tag']]) for x in resultList]
        return rowGuid_tag_list

    def result_list_jiangsu_nanjing(self, curl):
        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT, verify=False)
        html = response.content
        print(html)
        soup = BeautifulSoup(html, "html.parser")
        a = soup.find('span', text="雨花台区-居民小区垃圾分类收集点明细")
        links = []
        # for title in soup.find_all('div', attrs={''})

    def result_list_jiangsu_wuxi(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], params=curl['params'], data=curl['data'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']
        cata_ids = [x['cata_id'] for x in resultList]
        return cata_ids

    def result_list_jiangsu_xuzhou(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], json=curl['json_data'], timeout=REQUEST_TIME_OUT, verify=False)
        resultList = json.loads(response.text)['data']['rows']
        mlbhs = [x['mlbh'] for x in resultList]
        return mlbhs

    def result_list_jiangsu_suzhou(self, curl):
        response = requests.post(curl['url'], params=curl['params'], headers=curl['headers'], json=curl['json_data'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['records']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_jiangsu_nantong(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['content']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_jiangsu_lianyungang(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], verify=False, timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        result_list = soup.find('div', attrs={'class': 'mainz mt30 jQtabcontent'}).find_all('li')
        dmids = []
        for li in result_list:
            title = li.find('h1')
            # s = title.get_text()
            dmid = title.attrs['onclick'].lstrip("view('").rstrip("')")
            dmids.append(dmid)
        return dmids

    def result_list_jiangsu_huaian(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['data']['data']
        ids = [x['id'] for x in result_list]
        return ids

    def result_list_jiangsu_yancheng(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['resultData']['list']
        catalogPks = [x['catalogPk'] for x in result_list]
        return catalogPks

    def result_list_jiangsu_zhenjiang(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['data']['content']
        ids = [x['id'] for x in result_list]
        return ids

    def result_list_jiangsu_taizhou(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], verify=False, timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        result_list = soup.find('div', attrs={'class': 'right-content-catalog'}).find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False)
        ids = []
        for li in result_list:
            id = li.find('div', attrs={'class': 'cata-title'}).find('input', attrs={'name': 'catalogidinput'}).attrs['value']
            ids.append(id)
        return ids

    def result_list_jiangsu_suqian(self, curl):
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        result_list = soup.find('div', attrs={'class': 'right-content-catalog'}).find('div', attrs={
            'class': 'bottom-content'}).find('ul').find_all('li', recursive=False)
        id_infos = []
        for li in result_list:
            id = li.find('div', attrs={'class': 'cata-title'}).find('input', attrs={'name': 'catalogidinput'}).attrs['value']
            update_time = li.find('div', attrs={'class': 'cata-information'}).find('span', text='更新时间：').find_next('span').get_text().strip()
            id_infos.append((id, update_time))
        return id_infos

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

    def result_list_zhejiang_hangzhou(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['rows']
        id_formats = [(x['id'], x['source_type'].lower()) for x in result_list]
        return id_formats

    def result_list_zhejiang_ningbo(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], json=curl['json_data'], verify=False, timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['list']['rows']
        uuids = [x['uuid'] for x in result_list]
        return uuids

    def result_list_zhejiang_wenzhou(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], verify=False, timeout=REQUEST_TIME_OUT)
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

    def result_list_zhejiang_jiaxing(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], timeout=REQUEST_TIME_OUT)
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

    def result_list_zhejiang_shaoxing(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], json=curl['json_data'], timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['data']['rows']
        iids = [x['iid'] for x in result_list]
        return iids

    def result_list_zhejiang_jinhua(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], verify=False, timeout=REQUEST_TIME_OUT)
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

    def result_list_zhejiang_quzhou(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], timeout=REQUEST_TIME_OUT)
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

    def result_list_zhejiang_zhoushan(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], json=curl['json_data'], verify=False, timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['data']['records']
        ids = [x['id'] for x in result_list]
        return ids

    def result_list_zhejiang_taizhou(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], json=curl['json_data'], timeout=REQUEST_TIME_OUT)
        result_list = json.loads(response.text)['data']['rows']
        iids = [x['iid'] for x in result_list]
        return iids

    def result_list_zhejiang_lishui(self, curl):
        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], verify=False, timeout=REQUEST_TIME_OUT)
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