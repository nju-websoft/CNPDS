import json
import re
import time
import urllib

import unicodedata as ucd

import requests
import urllib3
from bs4 import BeautifulSoup
from constants import REQUEST_TIME_OUT


class Detail:
    def __init__(self, province, city) -> None:
        self.province = province
        self.city = city

    def get_detail(self, curl):
        func_name = f"detail_{str(self.province)}_{str(self.city)}"
        func = getattr(self, func_name, self.detail_other)
        return func(curl)

    def detail_beijing_beijing(self, curl):

        key_list = ["资源名称", "资源出版日期", "资源分类", "摘要", "资源所有权单位", "关键字说明", "资源类型", "资源记录数"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        information = soup.find('div', attrs={
            "id": "zydtCont1"
        }).find('div', attrs={
            "class": "sjdetails_cardmain_frame"
        }).get_text().strip()
        for line in information.split("\n"):
            print(line)
            line = line.strip().split(" ")
            dataset_matadata[line[0]] = line[1]
        return dataset_matadata

    def detail_tianjin_tianjin(self, curl):

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('div', attrs={'class': 'title f-cb mg-b4'}).find('span').get_text()
        dataset_matadata['数据集名称'] = title.strip()
        table = soup.find('div', attrs={'class': 'slidecont'})
        for tr in table.find_all('tr'):
            for th, td in zip(tr.find_all('th'), tr.find_all('td')):
                dataset_matadata[th.get_text().strip()] = td.get_text().strip()
        return dataset_matadata

    def detail_hebei_hebei(self, curl):

        list_fields = ["信息资源分类", "开放条件"]
        table_fields = [
            "信息资源名称", "信息资源代码", "资源版本", "资源提供方", "资源提供方内部部门", "资源提供方代码", "资源摘要", "格式分类", "标签", "格式类型", "格式描述", "更新周期",
            "资源联系人", "联系电话", "电子邮箱"
        ]

        response = requests.post(curl['url'],
                                 cookies=curl['cookies'],
                                 headers=curl['headers'],
                                 data=curl['data'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        for tr in soup.find('div', attrs={'class': 'info'}).find_all('tr'):
            td = tr.find('td')
            td_name = td.get_text().split('：')[0].strip()
            if td_name in list_fields:
                td_text = tr.find_next('td').get_text().strip()
                dataset_matadata[td_name] = td_text
        table = soup.find('div', attrs={'class': 'resdetails_table_box page1'})
        p_name = ""
        for p in table.find_all('p'):
            p_text = p.get_text().strip()
            if len(p_text) > 0 and p_text[-1] == ":":
                p_name = p_text[:-1]
            elif p_name != "":
                p_text = ucd.normalize('NFKC', p_text).replace(' ', '')
                dataset_matadata[p_name] = p_text
        return dataset_matadata

    def detail_neimenggu_neimenggu(self, curl):

        key_map = {
            'title': "目录名称",
            'openType': "开放状态",
            'remark': "简介",
            'subjectName': "所属主题",
            'dataUpdatePeriod': "更新频率",
            'orgName': "提供单位",
            'publishDate': "发布日期",
            'updateDate': "更新日期",
        }

        response = requests.post(curl['url'],
                                 headers=curl['headers'],
                                 data=curl['data'],
                                 timeout=REQUEST_TIME_OUT,
                                 verify=False)

        dataset_matadata = {}
        detail_json = json.loads(response.text)['record']
        for key, value in key_map.items():
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_liaoning_liaoning(self, curl):

        list_fields = ["来源部门", "重点领域", "数据更新时间", "开放条件"]
        table_fields = ["数据量", "接口量", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('ul', attrs={'class': 'd-title pull-left'})
        title = title.find('h4').get_text()
        dataset_matadata['标题'] = title
        for li in soup.find('ul', attrs={'class': 'list-inline'}).find_all('li', attrs={}):
            li_name = li.get_text().split('：')[0].strip()
            if li_name in list_fields:
                li_text = li.find('span', attrs={'class': 'text-primary'}).get_text().strip()
                dataset_matadata[li_name] = li_text
        table = soup.find('li', attrs={'name': 'basicinfo'})
        for td_name in table_fields:
            td_text = table.find('td', text=td_name)
            if td_text is None:
                continue
            td_text = td_text.find_next('td').get_text().strip()
            td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_shandong_shandong(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", " 更新时间", "开放类型"]
        table_fields = ["数据量", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]
        item_fields = ["英文信息项名", "中文信息项名", "数据类型", "中文描述"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('ul', attrs={'class': 'd-title pull-left'})
        title = title.find('h4').get_text()
        dataset_matadata['标题'] = title
        for li in soup.find('ul', attrs={'class': 'list-inline'}).find_all('li', attrs={}):
            li_name = li.get_text().split('：')[0].strip()
            if li_name in list_fields:
                li_text = li.find('span', attrs={'class': 'text-primary'}).get_text().strip()
                dataset_matadata[li_name] = li_text
        table = soup.find('li', attrs={'name': 'basicinfo'})
        for td_name in table_fields:
            td_text = table.find('td', text=td_name).find_next('td').get_text().strip()
            td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_jiangsu_jiangsu(self, curl):

        key_map = {
            'resourcesName': "资源名称",
            'resourceUsage': "应用场景",
            'openCondition': "开放类型",
            'updateFrequency': "更新周期",
            'updateDate': "资源更新日期",
            'provideDepartName': "所属单位",
            'belongIndustry': "所属行业",
            'resourceSubject': "所属主题",
            'editionNum': "版本号",
            'resourceDetail': "资源描述"
        }

        response = requests.post(curl['url'],
                                 headers=curl['headers'],
                                 data=curl['data'],
                                 timeout=REQUEST_TIME_OUT,
                                 verify=False)

        dataset_metadata = {}
        detail_json = json.loads(response.text)['custom']

        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        return dataset_metadata

    def detail_jiangsu_wuxi(self, curl):

        table_fields = ["来源部门", "数据量", "联系电话", "邮箱地址", "所属主题", "更新频率", "目录发布/更新时间", "资源发布/更新时间", "条件开放", "标签", "简介"]

        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_metadata = {}
        titleInfo = soup.find('div', attrs={'class': 'data-name data-con'})
        title = titleInfo.get_text().strip()
        dataset_metadata["资源标题"] = title
        baseInfo = soup.find('div', attrs={'class': 'baseInfo s1'})
        table = baseInfo.find('table')
        for td_name in table_fields:
            td_text = table.find('td', text=td_name + '：')
            if td_text is None:
                continue
            if td_name == "标签":
                tags = td_text.find_next('td').find_all('span')
                tags_list = [tag.get_text().strip() for tag in tags]
                dataset_metadata[td_name] = tags_list
                continue
            td_text = td_text.find_next('td').get_text().strip()
            dataset_metadata[td_name] = td_text

        data_formats = []
        for li in soup.find('div', attrs={'class': 'service s4'}).find('ul').find_all('li'):
            if li.get_text() != "全部":
                data_formats.append(li.get_text().lower())
        dataset_metadata["数据格式"] = data_formats
        dataset_metadata["目录发布/更新时间"] = dataset_metadata["目录发布/更新时间"].split(' ')[0]
        dataset_metadata["资源发布/更新时间"] = dataset_metadata["资源发布/更新时间"].split(' ')[0]
        dataset_metadata["详情页网址"] = f"{curl['url']}?cata_id={curl['params']['cata_id']}"
        return dataset_metadata

    def detail_jiangsu_xuzhou(self, curl):
        key_map = {
            'mlmc': "资源名称",
            'xxzytgf': "资源提供方",
            'ssqtlmmc': "领域",
            'kflx': "开放类型",
            'zyzy': "资源摘要",
            'gxzq': "更新周期",
            'ssztlmmc': "主题分类",
        }
        open_type_map = {"01": "有条件开放", "02": "无条件开放"}

        update_frequency_map = {"01": "每日", "02": "每周", "03": "每月", "04": "季度", "05": "年", "06": "半年"}

        response = requests.get(curl['url'],
                                params=curl['params'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        dataset_metadata["开放类型"] = open_type_map[dataset_metadata["开放类型"]]

        if dataset_metadata["更新周期"] in list(update_frequency_map.keys()):
            dataset_metadata["更新周期"] = update_frequency_map[dataset_metadata["更新周期"]]
        else:
            print(f"{curl['headers']['Referer']}#/DataDirectory/{curl['params']['mlbh'].replace('/', '%2F')}")
            exit(-1)

        dataset_metadata[
            "详情页网址"] = f"{curl['headers']['Referer']}#/DataDirectory/{curl['params']['mlbh'].replace('/', '%2F')}"
        return dataset_metadata

    def detail_jiangsu_suzhou(self, curl):
        key_map = {
            'createTime': "创建时间",
            'createrDeptName': "提供机构",
            'resourceName': "资源信息名称",
            'updateFrequency': "更新频率",
            'resourceIntroduction': "描述",
            'resourceType': "资源类型",
            'openAttributeName': "开放属性",
            'catalogName': "资源目录",
            'contacts': "联系人",
            'contactTel': "联系方式"
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
        data_formats_map = {"库表": 'table', "文件": 'file', "接口": 'api'}
        response = requests.post(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["创建时间"] = dataset_metadata["创建时间"].split(' ')[0]
        dataset_metadata["资源类型"] = data_formats_map[dataset_metadata["资源类型"]]
        dataset_metadata["更新频率"] = update_freq_map[dataset_metadata["更新频率"]]
        dataset_metadata["详情页网址"] = f"{curl['headers']['Referer']}#/catalog/{detail_json['id']}"
        return dataset_metadata

    def detail_jiangsu_nantong(self, curl):
        key_map = {
            'resName': "资源名称",
            'createTime': "创建时间",
            'updateTime': "更新时间",
            'updateCycle': "资源更新频率",
            'abstracts': "内容简介",
            'industryTypeName': "行业类型",
            'companyName': "提供机构",
            'themeTypeName': "主题分类",
            'linkman': "联系人",
            'phone': "联系方式",
            'isOpenToSociety': "是否向社会开放",
        }

        open_type_map = {"1": "完全开放", "2": "申请后开放"}

        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        if detail_json['isOpenToSociety'] not in list(open_type_map.keys()):
            print(curl['params'])
            exit(-1)

        file_curl = curl.copy()
        file_curl['params'] = {'resId': curl['params']['id']}
        file_response = requests.get("https://data.nantong.gov.cn/api/anony/portalResource/listFiles",
                                     params=file_curl['params'],
                                     headers=file_curl['headers'],
                                     timeout=REQUEST_TIME_OUT)
        file_data = json.loads(file_response.text)['data']
        if file_data is None:
            data_formats = []
        else:
            data_formats = list(set([file['fileType'].lower() for file in file_data['childFiles']]))

        dataset_metadata["是否向社会开放"] = open_type_map[dataset_metadata["是否向社会开放"]]
        dataset_metadata["数据格式"] = data_formats
        dataset_metadata[
            "详情页网址"] = f"{curl['headers']['Referer']}home/index.html#/catalog/details?id={curl['params']['id']}"
        return dataset_metadata

    def detail_jiangsu_lianyungang(self, curl):
        table_fields = ["信息资源名称", "信息资源提供方", "是否向社会开放", "所属主题", "上传时间"]
        response = requests.get(curl['url'],
                                params=curl['params'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}

        try:
            assert len(soup.find_all('table')) == 4
        except AssertionError:
            print(f"{curl['url']}?dmid={curl['params']['dmid']}")
            return False, None

        basicInfo, dataItem, fileDownload, dataPreview = soup.find_all('table')

        for th_name in table_fields:
            th = basicInfo.find('th', text=th_name + ':')
            td = th.find_next('td')
            dataset_matadata[th_name] = td.get_text()
        data_formats = []
        for th_file in fileDownload.find_all('th'):
            data_formats.append(th_file.get_text().split(' ')[0].lower())

        dataset_matadata["上传时间"] = dataset_matadata["上传时间"].split(' ')[0]
        dataset_matadata["数据格式"] = list(set(data_formats))
        dataset_matadata["详情页网址"] = f"{curl['url']}?dmid={curl['params']['dmid']}"
        return True, dataset_matadata

    def detail_jiangsu_huaian(self, curl):
        key_map = {
            'catalogName': "资源名称",
            'openCondition': "开放方式",
            'dataUpdateTime': "更新时间",
            'dataUpdatePeriod': "更新周期",
            'publishTime': "发布时间",
            'orgName': "所属部门"
        }

        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        if dataset_metadata["更新时间"] is not None:
            dataset_metadata["更新时间"] = dataset_metadata["更新时间"].split(' ')[0]
        if dataset_metadata["发布时间"] is not None:
            dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(' ')[0]
        return dataset_metadata

    def detail_jiangsu_yancheng(self, curl):
        key_map = {
            'catalogName': "目录名称",
            'updateCycle': "更新频率",
            'catalogAbstract': "目录摘要",
            'catalogKeywords': "关键字",
            'createTime': "创建日期",
            'publishTime': "发布日期",
            'deptName': "提供部门",
            'openType': "开放类型",
            'openCondition': "开放条件",
            'subjectName': "领域分类",
            'industryName': "行业分类",
            'sceneName': "场景分类",
        }
        open_type_map = {1: "可开放"}
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['resultData']

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        if dataset_metadata["开放类型"] in list(open_type_map.keys()):
            dataset_metadata["开放类型"] = open_type_map[dataset_metadata["开放类型"]]
        else:
            print(curl['params']['catalogId'])
            exit(-1)
        if dataset_metadata["开放条件"] is None:
            dataset_metadata["开放条件"] = "无条件开放"

        data_formats = []
        for format in ["Json", "Excel", "Csv"]:
            if detail_json["is" + format] == 1:
                data_formats.append(format.lower())
        dataset_metadata["资源格式"] = data_formats
        dataset_metadata["创建日期"] = dataset_metadata["创建日期"].split(' ')[0]
        dataset_metadata["发布日期"] = dataset_metadata["发布日期"].split(' ')[0]
        dataset_metadata["详情页网址"] = curl['headers']['Referer'].format(curl['params']['catalogId'])
        return dataset_metadata

    def detail_jiangsu_zhenjiang(self, curl):
        key_map = {
            'resourceName': "资源名称",
            'publishTime': "发布时间",
            'updateTime': "最近更新",
            'organName': "数据提供方",
            'topicClassify': "数据主题",
            'industryType': "数据行业",
            'isOpen': "开放属性",
            'resourceRes': "资源摘要",
            'updateCycle': "更新周期",
            'keyWords': "关键字",
            'dataRelam': "数据领域"
        }
        open_type_map = {"0": "无条件开放", "1": "有条件开放"}
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        if 'resMountType' in detail_json:
            dataset_metadata["数据格式"] = detail_json['resMountType']
            if dataset_metadata["数据格式"] == "其他":
                dataset_metadata["数据格式"] = 'file'
            else:
                dataset_metadata["数据格式"] = dataset_metadata["数据格式"].lower()
        else:
            dataset_metadata["数据格式"] = 'file'

        if dataset_metadata["开放属性"] in list(open_type_map.keys()):
            dataset_metadata["开放属性"] = open_type_map[dataset_metadata["开放属性"]]
        else:
            print(curl['params'])
            exit(-1)

        dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(' ')[0]
        dataset_metadata["最近更新"] = dataset_metadata["最近更新"].split(' ')[0]
        dataset_metadata["详情页网址"] = f"{curl['headers']['Referer']}#/open/data-resource/info/{curl['params']['id']}"
        return dataset_metadata

    def detail_jiangsu_taizhou(self, curl):
        table_fields = ["来源部门", "开放状态", "所属行业", "发布时间", "最后更新时间", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]

        response = requests.get(curl['url'], headers=curl['headers'], verify=False, timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        dataset_metadata = {}

        catalog_details = soup.find('div', attrs={'class': 'g-main catalog-details'})
        top_details = catalog_details.find('div', attrs={'class': 'panel panel-top'})
        dataset_metadata["资源名称"] = top_details.find('h4').get_text()
        dataset_metadata["重点领域"] = top_details.find('div', attrs={
            'class': 'list-details'
        }).find('ul').find('li').find_next('li').find('span').get_text().strip()
        body_details = catalog_details.find('div', attrs={
            'class': 'panel panel-content'
        }).find('div', attrs={'class': 'panel-body'})
        basicinfo = body_details.find('li', attrs={'name': 'basicinfo'})
        table = basicinfo.find('table')
        for td_name in table_fields:
            td = table.find('td', text=td_name)
            dataset_metadata[td_name] = td.find_next('td').get_text().strip()

        dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(' ')[0]
        dataset_metadata["最后更新时间"] = dataset_metadata["最后更新时间"].split(' ')[0]
        file_download = body_details.find('li', attrs={'name': 'file-download'})
        if file_download is None:
            data_formats = []
        else:
            file_list = file_download.find('table').find_all('tr')
            data_formats = [f.attrs['fileformat'].lower() for f in file_list]

        api_service = body_details.find('li', attrs={'name': 'api-service'})
        if api_service is not None:
            data_formats.append('api')

        dataset_metadata["数据格式"] = list(set(data_formats))
        dataset_metadata["详情页网址"] = curl['url']
        return dataset_metadata

    def detail_jiangsu_suqian(self, curl):
        table_fields = ["来源部门", "数据量", "开放状态", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]
        requests.packages.urllib3.disable_warnings()
        response = requests.get(curl['url'], headers=curl['headers'], verify=False, timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")

        dataset_metadata = {}

        catalog_details = soup.find('div', attrs={'class': 'g-main catalog-details'})
        top_details = catalog_details.find('div', attrs={'class': 'panel panel-top'})
        dataset_metadata["资源名称"] = top_details.find('h4').get_text()
        dataset_metadata["重点领域"] = top_details.find('div', attrs={
            'class': 'list-details'
        }).find('ul').find('li').find_next('li').find('span').get_text().strip()
        body_details = catalog_details.find('div', attrs={
            'class': 'panel panel-content'
        }).find('div', attrs={'class': 'panel-body'})
        basicinfo = body_details.find('li', attrs={'name': 'basicinfo'})
        table = basicinfo.find('table')
        for td_name in table_fields:
            td = table.find('td', text=td_name)
            if td is None and td_name == "数据量":
                dataset_metadata[td_name] = str(0)
            else:
                dataset_metadata[td_name] = td.find_next('td').get_text().strip()

        file_download = body_details.find('li', attrs={'name': 'file-download'})
        if file_download is None:
            data_formats = []
        else:
            file_list = file_download.find('table').find_all('tr')
            data_formats = [f.attrs['fileformat'].lower() for f in file_list]

        api_service = body_details.find('li', attrs={'name': 'api-service'})
        if api_service is not None:
            data_formats.append('api')
        dataset_metadata["数据格式"] = list(set(data_formats))
        dataset_metadata["详情页网址"] = curl['url']
        return dataset_metadata

    def detail_shanghai_shanghai(self, curl):

        key_map = {
            'dataset_name': "数据集名称",
            'open_list_abstract': "摘要",
            'data_label': "数据标签",
            'data_domain': "数据领域",
            'cou_theme_cls': "国家主题分类",
            'create_date': "首次发布日期",
            'update_date': "更新日期",
            'open_rate': "更新频度",
            'org_name': "数据提供方",
            'open_type': "开放属性",
            'contact_way': "联系方式",
        }

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        dataset_metadata = {}
        detail_json = json.loads(response.text)['data']
        detail_json['create_date'] = detail_json['create_date'].split('T')[0]
        detail_json['update_date'] = detail_json['update_date'].split('T')[0]
        data_formats = set()
        for file in detail_json['docType']:
            for file_type in file['file_type']:
                data_formats.add(file_type)
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        dataset_metadata["数据格式"] = list(data_formats)
        dataset_metadata["详情页网址"] = curl['headers']['Referer']
        return dataset_metadata

    def detail_zhejiang_zhejiang(self, curl):

        table_fields = ["摘要", "标签", "更新周期", "数源单位", "数据领域", "行业分类", "发布日期", "更新日期", "开放条件", "联系方式", "资源格式", "数据量"]

        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('span', attrs={'class': 'sjxqTit1'}).get_text()
        dataset_matadata['标题'] = title
        for tr in soup.find('div', attrs={'class': 'box1'}).find_all('tr'):
            if tr.has_attr('style'):
                continue
            td_name = ""
            for td in tr.find_all('td'):
                td_text = td.get_text()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
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
            'source_name': "资源名称",
            'private_dept': "发布部门",
            'area_county': "所属区县",
            'instruction': "摘要",
            'create_date': "发布时间",
            'modify_date': "更新时间",
            'updateCycle': "更新周期",
            'open_level': "开放等级",
            'deptTel': "联系电话",
            'data_count': "数据量",
        }
        open_level_map = {
            "1": "登录开放",
            "2": "申请开放",
            "3": "完全开放",
        }

        response = requests.post(curl['url'], headers=curl['headers'], data=curl['data'], timeout=REQUEST_TIME_OUT)
        if curl['format'] == 'api':
            try:
                detail_json = json.loads(response.text)['serviceModel']
            except json.decoder.JSONDecodeError:
                return None
        else:
            detail_json = json.loads(response.text)['resInfo']

        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["开放等级"] = open_level_map[dataset_metadata["开放等级"]]
        dataset_metadata["发布时间"] = dataset_metadata["发布时间"].split(' ')[0]
        dataset_metadata["更新时间"] = dataset_metadata["更新时间"].split(' ')[0]
        dataset_metadata["详情页网址"] = curl['headers']['Referer']
        return dataset_metadata

    def detail_zhejiang_ningbo(self, curl):
        key_map = {
            'corresFormat': "资源格式",
            'createTime': "发布时间",
            'dataUnitTel': "联系方式",
            'domainName': "数据领域",
            'dataNumber': "数据量",
            'label': "标签",
            'openConditions': "开放条件",
            'resourceName': "资源名称",
            'updateFrequency': "更新周期",
            'updateTime': "更新时间",
        }
        response = requests.post(curl['url'],
                                 headers=curl['headers'],
                                 json=curl['json_data'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['bTCataResources']

        dataset_metadata = {}
        for key, value in key_map.items():
            try:
                dataset_metadata[value] = detail_json[key]
            except KeyError:
                dataset_metadata[value] = None
        return dataset_metadata

    def detail_zhejiang_wenzhou(self, curl):
        table_fields = ["摘要", "标签", "更新周期", "资源格式", "数源单位", "联系方式", "数据领域", "行业分类", "开放条件", "发布日期", "更新日期", "数据条数"]

        response = requests.get(curl['url'],
                                params=curl['params'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('span', attrs={'class': 'sjxqTit1'}).get_text()
        dataset_matadata['标题'] = title
        for tr in soup.find('div', attrs={'class': 'box1'}).find_all('tr'):
            if tr.has_attr('style'):
                continue
            td_name = ""
            for td in tr.find_all('td'):
                td_text = td.get_text()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_zhejiang_jiaxing(self, curl):
        table_fields = ["摘要", "标签", "更新周期", "数源单位", "数据领域", "行业分类", "发布日期", "更新日期", "开放条件", "联系方式", "资源格式", "数据容量"]

        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('span', attrs={'class': 'sjxqTit1'}).get_text()
        dataset_matadata['标题'] = title
        for tr in soup.find('div', attrs={'class': 'box1'}).find_all('tr'):
            if tr.has_attr('style'):
                continue
            td_name = ""
            for td in tr.find_all('td'):
                td_text = td.get_text()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
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
            'title': "标题",
            'content': "摘要",
            'keywords': "资源标签",
            'createDate': "发布日期",
            'dataUpdateDate': "更新日期",
            'domainStr': "数据领域",
            'deptName': "数源单位",
            'openCond': "开放条件",
            'dataCount': "数据量",
            'deptContact': "联系方式",
            'updateFreq': "更新周期",
        }
        update_frequency_map = {1: "每日", 2: "每周", 3: "每月", 4: "每季度", 5: "每半年", 6: "每年", 7: "不定期", 8: "不更新"}
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["更新日期"] = dataset_metadata["更新日期"].split(' ')[0]
        if dataset_metadata["更新周期"] in update_frequency_map.keys():
            dataset_metadata["更新周期"] = update_frequency_map[dataset_metadata["更新周期"]]
        else:
            print(curl['headers']['Referer'].format(curl['params']['dataId']))
            print(detail_json['updateFreq'])
            exit(-1)

        dataset_metadata["详情页网址"] = curl['headers']['Referer'].format(curl['params']['dataId'])
        return dataset_metadata

    def detail_zhejiang_jinhua(self, curl):
        table_fields = ["摘要", "标签", "更新周期", "数源单位", "数据领域", "行业分类", "发布日期", "更新日期", "开放条件", "联系方式", "资源格式", "数据容量"]

        response = requests.get(curl['url'],
                                params=curl['params'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('span', attrs={'class': 'sjxqTit1'}).get_text()
        dataset_matadata['标题'] = title
        for tr in soup.find('div', attrs={'class': 'box1'}).find_all('tr'):
            if tr.has_attr('style'):
                continue
            td_name = ""
            for td in tr.find_all('td'):
                td_text = td.get_text()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
                if len(td_text) > 0 and td_text[-1] == ":":
                    td_name = td_text[:-1]
                    if td_name not in table_fields:
                        td_name = ""
                elif td_name != "":
                    dataset_matadata[td_name] = td_text

        dataset_matadata["详情页网址"] = f"{curl['url']}?iid={curl['params']['iid']}"
        return dataset_matadata

    def detail_zhejiang_quzhou(self, curl):
        table_fields = ["摘要", "标签", "更新周期", "数源单位", "数据领域", "行业分类", "发布日期", "更新日期", "开放条件", "联系方式", "资源格式", "数据容量"]

        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('span', attrs={'class': 'sjxqTit1'}).get_text()
        dataset_matadata['标题'] = title
        for tr in soup.find('div', attrs={'class': 'box1'}).find_all('tr'):
            if tr.has_attr('style'):
                continue
            td_name = ""
            for td in tr.find_all('td'):
                td_text = td.get_text()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
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
            'resName': "资源名称",
            'resSummary': "摘要",
            'datadomain': "数据领域",
            'dataCount': "数据量",
            'idPocName': "资源提供方",
            'resFormatName': "资源格式",
            'createDate': "上线日期",
            'updateDate': "更新日期"
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
            "99": "其他"
        }
        response = requests.post(curl['url'],
                                 params=curl['params'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]
        dataset_metadata["数据领域"] = [domain_map[x] for x in dataset_metadata["数据领域"].split(',')]
        dataset_metadata["详情页网址"] = f"{curl['headers']['Referer']}#/OpenData/DataSet/Detail?id={curl['params']['id']}"
        return dataset_metadata

    def detail_zhejiang_taizhou(self, curl):
        key_map = {
            'title': "标题",
            'content': "摘要",
            'keywords': "资源标签",
            'createDate': "发布日期",
            'dataUpdateDate': "更新日期",
            'domainStr': "数据领域",
            'deptName': "数源单位",
            'openCond': "开放条件",
            'dataCount': "数据量",
            'deptContact': "联系方式",
            'updateFreq': "更新周期",
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
            10: "小时级"
        }
        response = requests.get(curl['url'], params=curl['params'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        detail_json = json.loads(response.text)['data']
        dataset_metadata = {}
        for key, value in key_map.items():
            dataset_metadata[value] = detail_json[key]

        dataset_metadata["更新日期"] = dataset_metadata["更新日期"].split(' ')[0]
        if dataset_metadata["更新周期"] in update_frequency_map.keys():
            dataset_metadata["更新周期"] = update_frequency_map[dataset_metadata["更新周期"]]
        else:
            print(curl['headers']['Referer'].format(curl['params']['dataId']))
            print(detail_json['updateFreq'])
            exit(-1)

        dataset_metadata["详情页网址"] = curl['headers']['Referer'].format(curl['params']['dataId'])
        return dataset_metadata

    def detail_zhejiang_lishui(self, curl):
        table_fields = ["摘要", "标签", "更新周期", "数源单位", "数据领域", "行业分类", "发布日期", "更新日期", "开放条件", "联系方式", "资源格式", "数据条数"]

        response = requests.get(curl['url'],
                                params=curl['params'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        try:
            title = soup.find('span', attrs={'class': 'sjxqTit1'}).get_text()
        except AttributeError:
            return None
        dataset_matadata['标题'] = title
        for tr in soup.find('div', attrs={'class': 'box1'}).find_all('tr'):
            if tr.has_attr('style'):
                continue
            td_name = ""
            for td in tr.find_all('td'):
                td_text = td.get_text()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
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
            'catalogName': "标题",
            'publishTime': "发布日期",
            'updateTime': "更新日期",
            'providerDept': "提供单位",
            'belongFieldName': "所属领域",
            'shareTypeName': "开放属性",
            'summary': "摘要信息",
            'updateCycleTxt': "更新频率"
        }

        response = requests.post(curl['url'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)

        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_jiangxi_jiangxi(self, curl):

        key_map = {
            'dataName': "标题",
            'resOrgName': "数源单位",
            'dataFieldName': "数据领域",
            'createTime': "发布时间",
            'updateTime': "数据更新时间",
            'resShareType': "开放类型"
        }

        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            if key == 'resShareType':
                if detail_json[key] == "402882a75885fd150158860e3d170006":
                    detail_json[key] = "有条件开放"
                else:
                    detail_json[key] = "无条件开放"
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_fujian_fujian(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", " 更新时间", "开放条件"]
        table_fields = ["数据量", "所属行业", "更新频率", "部门电话", "部门邮箱", "描述"]
        item_fields = ["英文信息项名", "中文信息项名", "数据类型", "中文描述"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('ul', attrs={'class': 'd-title pull-left'})
        title = title.find('h4').get_text()
        dataset_matadata['标题'] = title
        for li in soup.find('ul', attrs={'class': 'list-inline'}).find_all('li', attrs={}):
            li_name = li.get_text().split('：')[0].strip()
            if li_name in list_fields:
                li_text = li.find('span', attrs={'class': 'text-primary'}).get_text().strip()
                dataset_matadata[li_name] = li_text
        table = soup.find('li', attrs={'name': 'basicinfo'})
        for td_name in table_fields:
            td_text = table.find('td', text=td_name).find_next('td').get_text().strip()
            td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_guangdong_guangdong(self, curl):

        key_map = {
            'resTitle': "名称",
            'resAbstract': "简介",
            'subjectName': "主题分类",
            'resAbstract': "数据更新时间",
            'openMode': "开放方式",
            'resAbstract': "更新频率",
            'publishDate': "发布日期",
            'dataUpdateTime': "更新时间",
            'officeName': "数据提供方",
            'sourceSuffix': "资源格式",
            'email': "邮箱",
            'landLine': "座机",
        }

        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_guangxi_guangxi(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", " 更新时间", "开放条件"]
        table_fields = ["数据量", "所属行业", "更新频率", "标签", "描述"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('ul', attrs={'class': 'd-title pull-left'})
        title = title.find('h4').get_text()
        dataset_matadata['标题'] = title
        for li in soup.find('ul', attrs={'class': 'list-inline'}).find_all('li', attrs={}):
            li_name = li.get_text().split('：')[0].strip()
            if li_name in list_fields:
                li_text = li.find('span', attrs={'class': 'text-primary'}).get_text().strip()
                dataset_matadata[li_name] = li_text
        table = soup.find('li', attrs={'name': 'basicinfo'})
        for td_name in table_fields:
            td_text = table.find('td', text=td_name).find_next('td').get_text().strip()
            td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_hainan_hainan(self, curl):

        table_fields = ["摘要", "目录名称", "开放状态", "所属主题", "来源部门", "目录发布时间", "数据更新时间", "更新频率"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        for tr in soup.find('div', attrs={'class': 'gp-column1'}).find('table').find_all('tr'):
            th_name = tr.find('th').get_text().strip()
            if th_name in table_fields:
                td_text = tr.find_next('td').get_text().strip()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
                dataset_matadata[th_name] = td_text
        return dataset_matadata

    def detail_ningxia_ningxia(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", " 更新时间", "开放条件"]
        table_fields = ["数据量", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]

        response = requests.get(curl['url'],
                                cookies=curl['cookies'],
                                headers=curl['headers'],
                                verify=False,
                                timeout=REQUEST_TIME_OUT)
        print(response)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('ul', attrs={'class': 'd-title pull-left'})
        title = title.find('h4').get_text()
        dataset_matadata['标题'] = title
        for li in soup.find('ul', attrs={'class': 'list-inline'}).find_all('li', attrs={}):
            li_name = li.get_text().split('：')[0].strip()
            if li_name in list_fields:
                li_text = li.find('span', attrs={'class': 'text-primary'}).get_text().strip()
                dataset_matadata[li_name] = li_text
        table = soup.find('li', attrs={'name': 'basicinfo'})
        for td_name in table_fields:
            td_text = table.find('td', text=td_name).find_next('td').get_text().strip()
            td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_shaanxi_shaanxi(self, curl):

        list_fields = ["发布机构", "资源格式", "所属主题", "发布日期", "更新日期"]

        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        print(response)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('div', attrs={'class': 'title'}).get_text().strip()
        dataset_matadata['标题'] = title
        description = soup.find('div', attrs={'class': 'syno word-break'}).get_text().strip()
        dataset_matadata['简介'] = description.replace("【简介】", "")
        for div in soup.find('dl', attrs={
                'class': 'synoInfo commonBox'
        }).find('dd').find_all('div', attrs={'class': 'sub'}):
            div_text = div.get_text().strip()
            div_name = div_text.split("：")[0].strip()
            div_text = div_text.split("：")[1].strip()
            div_text = ucd.normalize('NFKC', div_text).replace(' ', '')
            if div_name in list_fields:
                dataset_matadata[div_name] = div_text

        return dataset_matadata

    def detail_sichuan_sichuan(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", " 更新时间", "开放条件"]
        table_fields = ["数据量", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]

        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        html = response.content
        soup = BeautifulSoup(html, "html.parser")
        dataset_matadata = {}
        title = soup.find('ul', attrs={'class': 'd-title pull-left'})
        title = title.find('h4').get_text()
        dataset_matadata['标题'] = title
        for li in soup.find('ul', attrs={'class': 'list-inline'}).find_all('li', attrs={}):
            li_name = li.get_text().split('：')[0].strip()
            if li_name in list_fields:
                li_text = li.find('span', attrs={'class': 'text-primary'}).get_text().strip()
                dataset_matadata[li_name] = li_text
        table = soup.find('li', attrs={'name': 'basicinfo'})
        for td_name in table_fields:
            td_text = table.find('td', text=td_name).find_next('td').get_text().strip()
            td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
            dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_guizhou_guizhou(self, curl):

        key_map = {
            'name': "标题",
            'description': "数据摘要",
            'topicName': "主题名称",
            'orgName': "数据提供方",
            'industryName': "所属行业",
            'updateTime': "更新时间",
            'createTime': "发布时间",
            'openAttribute': "开放属性",
            'frequency': "更新频率",
        }

        frequency_mapping = {0: "每年", 1: "每季度", 2: "每月", 3: "每周", 4: "每天", 5: "实时", 6: "每半年"}

        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            if key in ['updateTime', 'createTime']:
                detail_json[key] = time.strftime("%Y-%m-%d", time.localtime(detail_json[key] / 1000))
            if key == 'frequency':
                detail_json[key] = frequency_mapping[detail_json[key]]
            if key == 'openAttribute':
                detail_json[key] = "有条件开放" if detail_json[key] == 1 else "无条件开放"
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_other(self, curl):
        print("暂无该省")