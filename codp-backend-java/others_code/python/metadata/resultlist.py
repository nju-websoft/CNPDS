import hashlib
import json
import re
import urllib

import requests
from bs4 import BeautifulSoup
from requests.utils import add_dict_to_cookiejar

from constants import REQUEST_TIME_OUT
import execjs


def getCookie(data):
    """
    通过加密对比得到正确cookie参数
    :param data: 参数
    :return: 返回正确cookie参数
    """
    chars = len(data['chars'])
    for i in range(chars):
        for j in range(chars):
            clearance = data['bts'][0] + data['chars'][i] + data['chars'][j] + data['bts'][1]
            encrypt = None
            if data['ha'] == 'md5':
                encrypt = hashlib.md5()
            elif data['ha'] == 'sha1':
                encrypt = hashlib.sha1()
            elif data['ha'] == 'sha256':
                encrypt = hashlib.sha256()
            encrypt.update(clearance.encode())
            result = encrypt.hexdigest()
            if result == data['ct']:
                return clearance


class ResultList:
    def __init__(self, province, city) -> None:
        self.province = province
        self.city = city

    def get_result_list(self, curl):
        func_name = f"result_list_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.result_list_other)
        return func(curl)

    def result_list_sichuan_sichuan(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content.decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        # soup = BeautifulSoup(html, "lxml")
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

    def result_list_sichuan_chengdu(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content.decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        # soup = BeautifulSoup(html, "lxml")
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

    def result_list_sichuan_panzhihua(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content.decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        # soup = BeautifulSoup(html, "lxml")
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

    def result_list_sichuan_zigong(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        # print(response)
        resultList = json.loads(response.text)['data']['rows']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_sichuan_luzhou(self, curl):
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        # print(response)
        resultList = json.loads(response.text)['result']['rows']
        ids = [(x['id'], x['openType'], x['publishTime'], x['updateTime']) for x in resultList]
        return ids

    def result_list_sichuan_deyang(self, curl):
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        # print(response)
        resultList = json.loads(response.text)['data']['rows']
        ids = [x['mlbh'] for x in resultList]
        return ids

    def result_list_sichuan_mianyang(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        # print(response)
        resultList = json.loads(response.text)['elementthing']['listPage']['list']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_sichuan_guangyuan(self, curl):
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']['rows']
        ids = [x['ID'] for x in resultList]
        return ids

    def result_list_sichuan_suining(self, curl):
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']['rows']
        ids = [(x['mlbh'], x['wjlx']) for x in resultList]
        return ids

    def result_list_sichuan_neijiang(self, curl):
        response = requests.post(curl['url'],
                                 params=curl['queries'],
                                 json=curl['data'],
                                 headers=curl['headers'],
                                 timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']['content']
        ids = [str(x['id']) for x in resultList]
        return ids

    def result_list_sichuan_leshan(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']['rows']
        ids = [str(x['resourceId']) for x in resultList]
        return ids

    def result_list_sichuan_nanchong(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']
        ids = [str(x['ID']) for x in resultList]
        return ids

    def result_list_shaanxi_shaanxi(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)[0]['result']

        metadata_list = []

        for result in resultList:
            dataset_metadata = {}

            dataset_metadata["标题"] = result['sdataName']
            dataset_metadata["来源部门"] = result['sorgName']
            dataset_metadata["所属主题"] = result['sdataTopic']
            dataset_metadata["发布时间"] = result['spubDate'].split(' ')[0].strip()
            dataset_metadata["更新时间"] = result['spubDate'].split(' ')[0].strip()
            dataset_metadata["标签"] = result['keywords'] if 'keyword' in result else ""
            dataset_metadata["描述"] = result['sdataIntro']
            dataset_metadata["数据格式"] = result['sdataFormats']
            dataset_metadata["详情页网址"] = "http://www.sndata.gov.cn/info?id=" + result['id']

            metadata_list.append(dataset_metadata)
        return metadata_list

    def result_list_ningxia_ningxia(self, curl):
        response = requests.get(curl['url'],
                                params=curl['queries'],
                                headers=curl['headers'],
                                timeout=REQUEST_TIME_OUT,
                                verify=False)
        html = response.content.decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        # soup = BeautifulSoup(html, "lxml")
        links = []

        for dataset in soup.find('div', attrs={'class': 'bottom-content'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={
                'class': 'cata-title'
            }).find('a', attrs={'href': re.compile("/portal/catalog/*")})
            data_formats = []
            for data_format in dataset.find('div', attrs={'class': 'file-type'}).find_all('li'):
                data_format_text = data_format.get_text()
                if data_format_text == '接口':
                    data_format_text = 'api'
                if data_format_text == '链接':
                    data_format_text = 'link'
                data_formats.append(data_format_text.lower())
            links.append({'link': link['href'], 'data_formats': str(data_formats)})
        return links

    def result_list_ningxia_yinchuan(self, curl):
        response = requests.post(curl['url'],
                                 params=curl['queries'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']
        ids = [(str(x['cata_id']), x['conf_catalog_format']) for x in resultList]
        return ids

    def result_list_xinjiang_wulumuqi(self, curl):
        response = requests.post(curl['url'],
                                 params=curl['queries'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']
        ids = [(str(x['cata_id']), x['conf_catalog_format']) for x in resultList]
        return ids

    def result_list_anhui_hefei(self, curl):
        # 使用session保持会话
        session = requests.session()
        res1 = session.get(curl['url'], headers=curl['headers'], params=curl['queries'])
        jsl_clearance_s = re.findall(r'cookie=(.*?);location', res1.text)[0]
        # 执行js代码
        jsl_clearance_s = str(execjs.eval(jsl_clearance_s)).split('=')[1].split(';')[0]
        # add_dict_to_cookiejar方法添加cookie
        add_dict_to_cookiejar(session.cookies, {'__jsl_clearance_s': jsl_clearance_s})
        res2 = session.get(curl['url'], headers=curl['headers'], params=curl['queries'])
        # 提取go方法中的参数
        data = json.loads(re.findall(r';go\((.*?)\)', res2.text)[0])
        jsl_clearance_s = getCookie(data)
        # 修改cookie
        add_dict_to_cookiejar(session.cookies, {'__jsl_clearance_s': jsl_clearance_s})
        response = session.get(curl['url'], headers=curl['headers'], params=curl['queries'])

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            print(response.text)
            return dict()
        # print(response)
        resultList = json.loads(response.text)['data']['result']

        ids = [(str(x['id']), x['zyId']) for x in resultList]
        return ids

    def result_list_anhui_wuhu(self, curl):
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        resultList = json.loads(response.text.replace('\\"', '"')[1:-1])['smcDataSetList']
        # 目前所有数据集中只出现了每日和每年
        frequency_mapping = {'1': "实时", '2': "每日", '3': "每周", '4': "每月", '5': "每半年", '6': "每年"}
        dataset_metadata = []
        for result in resultList:
            dataset_id = result['id']
            metadata_mapping = {
                "标题":
                result['datasetName'],
                "机构分类":
                result['isValid'],
                "url":
                "https://data.wuhu.cn/datagov-ops/data/toDetailPage?id=" + dataset_id,
                "领域名称":
                result['dataType'],
                "数据集创建时间":
                result['createDate'],
                "数据集更新时间":
                result['updateDate'].split(' ')[0],
                "开放类型":
                "无条件开放" if result['openType'] == '1' else "有条件开放",
                "更新频率":
                frequency_mapping[result['dataUpdateFrequency']],
                "数据简介":
                result['summary'],
                "资源格式": ['api'] if result['dataResourceType'] == '2' else
                [file['fileType'].split('.')[-1] for file in result['fileList']]
            }
            dataset_metadata.append(metadata_mapping)
        return dataset_metadata

    def result_list_anhui_bengbu(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content.decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        # soup = BeautifulSoup(html, "lxml")
        links = []

        for dataset in soup.find('div', attrs={'class': 'sj_list'}).find('ul').find_all('li', recursive=False):
            link = dataset.find('div', attrs={'class': 'sjinfo'}).find('a', attrs={'href': re.compile("site/tpl/.*")})
            links.append(link['href'])
        return links

    def result_list_anhui_huaibei(self, curl):
        response = requests.post(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        # print(response)
        resultList = json.loads(response.text)['result']['data']
        ids = [x['id'] for x in resultList]
        return ids

    def result_list_anhui_huangshan(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content.decode('utf-8')
        soup = BeautifulSoup(html, "html.parser")
        # soup = BeautifulSoup(html, "lxml")
        links = []

        for dataset in soup.find('ul', attrs={'class': 'clearfix'}).find_all('li', recursive=False):
            div = dataset.find('div', attrs={
                'class': 'sjcon clearfix'
            }).find('div', attrs={'class': 'dataresources-con'})
            link = div.find('a', attrs={'href': re.compile("/site/tpl/.*")})
            depart = div.find('p', attrs={
                'class': 'xx clearfix'
            }).find('span', attrs={
                'class': 'n2'
            }).get_text().split("：")[-1].strip()
            cata = div.find('p', attrs={
                'class': 'xx clearfix'
            }).find('span', attrs={
                'class': 'n3'
            }).get_text().split("：")[-1].strip()
            format_list = []
            for data_format in div.find('p', attrs={
                    'class': 'zyzy clearfix'
            }).find('span', attrs={
                    'class': 'link'
            }).find_all('a', attrs={'class': 'j-login'}):
                format_list.append(data_format.find('em').get_text().lower().strip())
            if len(format_list) == 0:
                format_list = ['file']
            links.append((link['href'], depart, cata, format_list))
        return links

    def result_list_anhui_chuzhou(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        if response.status_code != requests.codes.ok:
            print("error " + str(response.status_code) + ": " + curl['url'])
            return dict()
        resultList = json.loads(response.text)['content']
        dataset_metadata = []
        for result in resultList:
            metadata_mapping = {
                "标题": result['name'],
                "提供单位": result['companys']['title'],
                "开放主题": result['fields']['title'],
                "发布时间": result['createtime'].split(' ')[0],
                "更新时间": result['updatetime'].split(' ')[0],
                "开放类型": "无条件开放" if result['sharetype'] == '2' else "有条件开放",
                "描述": result['description'],
                "开放领域": result['themes']['title'],
                "关键词": result['keyword']
            }
            dataset_metadata.append(metadata_mapping)
        return dataset_metadata

    def result_list_other(self):
        print("暂无该省")