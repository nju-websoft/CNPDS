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

    def result_list_anhui_hefei(self, curl):
        response = requests.get(curl['url'],
                                params=curl['queries'],
                                cookies=curl['cookies'],
                                headers=curl['headers'],
                                timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['data']['result']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_anhui_wuhu(self, curl):
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        resultList = json.loads(response.text)['smcDataSetList']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_anhui_suzhou(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []

        for dataset in soup.find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={
                'class': 'cata-title'
            }).find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            data_formats = []
            for data_format in dataset.find('div', attrs={'class': 'file-type'}).find_all('li'):
                data_format_text = data_format.get_text()
                if data_format_text == '接口':
                    data_format_text = 'api'
                data_formats.append(data_format_text.lower())
            links.append({'link': link['href'], 'data_formats': str(data_formats)})
        return links

    def result_list_anhui_luan(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []

        for dataset in soup.find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={
                'class': 'cata-title'
            }).find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            data_formats = []
            for data_format in dataset.find('div', attrs={'class': 'file-type'}).find_all('li'):
                data_format_text = data_format.get_text()
                if data_format_text == '接口':
                    data_format_text = 'api'
                data_formats.append(data_format_text.lower())
            links.append({'link': link['href'], 'data_formats': str(data_formats)})
        return links

    def result_list_anhui_chizhou(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        matadata_list = []
        for dataset in soup.find('div', attrs={
                'id': 'listContent'
        }).find_all('div', attrs={'class': 'list-content-item'}):
            dataset_matadata = {}
            dataset_matadata["标题"] = dataset.find('div', attrs={'class': 'text ell'}).get_text().strip()
            dataset_matadata["数据格式"] = dataset.find('div', attrs={'class': 'file-type-wrap'}).get_text().strip().lower()
            for field in dataset.find_all('div', attrs={'class': 'content-item ell'}):
                field_name = field.get_text().strip().split('：')[0]
                field_text = field.get_text().strip().split('：')[1]
                dataset_matadata[field_name] = field_text

            matadata_list.append(dataset_matadata)
        return matadata_list

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
    
    def result_list_sichuan_meishan(self, curl):
        #print(curl['queries'])
        #print(curl['headers'])
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT,stream = True, verify = False)
        resultList = json.loads(response.text)['result']['rows']
        links = [link for link in resultList]
        # html = response.content
        # soup = BeautifulSoup(html, "html.parser")
        # links = []
        # for dataset_test in soup.find('div', attrs={'class': 'dire-list-main'}).find_all('ul',attrs={}):#.find_all('li',attrs={}):#find_all('div', attrs={'class': 'tit'}):
        #     print(len(dataset_test))
        #     print('Find a dataset')
        # for dataset in soup.find('div', attrs={'class': 'dire-list-main'}).find('ul', attrs={'class': 'dlm-box'}).find_all('li'):
        #     link = dataset.find('div', attrs={'class': 'tit'}).find('h3').find('a', attrs={'href': re.compile("/portal/service_detail?id=*")})
        #     data_formats = []
        #     for data_format in dataset.find('div', attrs={'class': 'tit'}).find('h3').find_all('span'):
        #         data_format_text = data_format.get_text()
        #         if data_format_text == '接口':
        #             data_format_text = 'api'
        #         data_formats.append(data_format_text.lower())
        #     links.append({'link': link['href'], 'data_formats': str(data_formats)})
        # print(links)
        return links

    def result_list_sichuan_yibin(self, curl):
        while True:
            try:
                response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT, verify=False)
                break
            except:
                time.sleep(5)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []

        for dataset in soup.find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={
                'class': 'cata-title'
            }).find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            data_formats = []
            for data_format in dataset.find('div', attrs={'class': 'file-type'}).find_all('li'):
                data_format_text = data_format.get_text()
                data_formats.append(data_format_text)
            links.append({'link': link['href'], 'data_formats': str(data_formats)})
        return links
    
    def result_list_sichuan_dazhou(self, curl):
        while True:
            try:
                response = requests.get(curl['url'],params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT, verify=False)
                break
            except:
                time.sleep(5)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []

        for dataset in soup.find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={
                'class': 'cata-title'
            }).find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            data_formats = []
            for data_format in dataset.find('div', attrs={'class': 'file-type'}).find_all('li'):
                data_format_text = data_format.get_text()
                data_formats.append(data_format_text)
            links.append({'link': link['href'], 'data_formats': str(data_formats)})
        return links

    def result_list_sichuan_yaan(self, curl):
        while True:
            try:
                response = requests.get(curl['url'],params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT, verify=False)
                break
            except:
                time.sleep(5)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        links = []

        for dataset in soup.find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={
                'class': 'cata-title'
            }).find('a', attrs={'href': re.compile("/oportal/catalog/*")})
            data_formats = []
            for data_format in dataset.find('div', attrs={'class': 'file-type'}).find_all('li'):
                data_format_text = data_format.get_text()
                data_formats.append(data_format_text)
            links.append({'link': link['href'], 'data_formats': str(data_formats)})
        return links

    def result_list_sichuan_bazhong(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],params = curl['queries'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT,verify=False)
                break
            except:
                time.sleep(5)
        resultList = json.loads(response.text)['data']['data']
        #print(json.loads(response.text))
        links = [link['catalogInfo']['id'] for link in resultList]
        return links

    def result_list_sichuan_aba(self, curl):
        while True:
            try:
                response = requests.get(curl['url'],params = curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT,verify=False)
                break
            except:
                time.sleep(5)
        resultList = json.loads(response.text)['data']['resultMap']['abaTableList']
        #print(json.loads(response.text))
        links = [link['tableId'] for link in resultList]
        return links

    def result_list_sichuan_ganzi(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],json = curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT,verify=False)
                break
            except:
                time.sleep(5)
        resultList = json.loads(response.text)['data']['rows']
        #print(json.loads(response.text))
        links = [link['mlbh'] for link in resultList]
        return links

    def result_list_guizhou_guizhou(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids
        
    def result_list_guizhou_guiyang(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_liupanshui(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_zunyi(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_anshun(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_bijie(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_tongren(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_qianxinan(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_qiandongnan(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_guizhou_qiannan(self, curl):
        while True:
            try:
                response = requests.post(curl['url'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
                break
            except:
                time.sleep(5)
        #print(response)
        resultList = json.loads(response.text)['data']
        ids = [{'id': x['id'],'resourceFormats': x['resourceFormats']} for x in resultList]
        return ids

    def result_list_other(self):
        print("暂无该省")