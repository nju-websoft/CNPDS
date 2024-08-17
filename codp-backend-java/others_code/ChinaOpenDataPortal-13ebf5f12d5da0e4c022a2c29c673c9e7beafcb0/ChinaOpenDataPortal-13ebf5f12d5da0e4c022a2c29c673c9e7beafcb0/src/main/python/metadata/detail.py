import copy
import json
import re
import time
import unicodedata as ucd

import bs4
import requests
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

        dataset_matadata = {}
        detail_json = json.loads(response.text)['custom']
        for key, value in key_map.items():
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

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

        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

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
            'updateCycleTxt': "更新频率",
            'formats': "数据格式"
        }

        response = requests.post(curl['url'],
                                 data=curl['data'],
                                 headers=curl['headers'],
                                 verify=False,
                                 timeout=REQUEST_TIME_OUT)

        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            if key in ['publishTime', 'updateTime']:
                detail_json[key] = detail_json[key][:4] + '-' + detail_json[key][4:6] + '-' + detail_json[key][6:8]
            if key == 'formats':
                detail_json[key] = str(detail_json[key]).lower()
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_anhui_hefei(self, curl):

        key_map = {
            'zy': "标题",
            'zymc': "摘要信息",
            'zxtbsj': "发布时间",
            'gxsj': "更新时间",
            'tgdwmc': "提供单位",
            'filedName': "所属领域",
            'fjhzm': "数据格式"
        }

        response = requests.post(curl['url'],
                                 data=curl['data'],
                                 cookies=curl['cookies'],
                                 headers=curl['headers'],
                                 timeout=REQUEST_TIME_OUT)
        print(response)
        dataset_matadata = {}
        detail_json = json.loads(response.text)['data']
        for key, value in key_map.items():
            if key in ['zxtbsj', 'gxsj']:
                detail_json[key] = detail_json[key][:4] + '-' + detail_json[key][4:6] + '-' + detail_json[key][6:8]
            if key == 'fjhzm':
                detail_json[key] = '[' + detail_json[key].lower().replace(' ', ',') + ']'
            dataset_matadata[value] = detail_json[key]
        return dataset_matadata

    def detail_anhui_suzhou(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = ["数据量", "文件数", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]
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
            if td_text is not None:
                td_text = td_text.find_next('td').get_text().strip()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
                dataset_matadata[td_name] = td_text
        return dataset_matadata

    def detail_anhui_luan(self, curl):

        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = ["数据量", "文件数", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]
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
            if td_text is not None:
                td_text = td_text.find_next('td').get_text().strip()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
                dataset_matadata[td_name] = td_text
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

    def detail_hubei_wuhan(self, curl):

        key_map = {
            '名称': "cataTitle",
            '摘要': "summary",
            '标签': "resLabel",
            '数据提供方': "registerOrgName",
            '主题分类': "dataTopicDetail",
            '行业分类': "industryTypeDetail",
            '发布日期': "createTime",
            '更新日期': "updateTime",
            '更新频率': {
                'cataFileDTO': "updateCycle"
            },
            '开放条件': 'openLevelDetail',
            '提供方联系方式': 'providerPhon',
            '资源格式': 'fileFormatDetail',
        }
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        data = json.loads(response.text)['data']
        metadata = {}

        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if k in ['createTime', 'updateTime']:
                    metadata[name] = metadata[name].split(' ')[0]
        if '资源格式' in metadata:
            metadata['资源格式'] = list(metadata['资源格式'].lower().split(','))
        metadata['详情页网址'] = 'http://data.wuhan.gov.cn/page/data/data_set_details.html?cataId={}'.format(
            curl['queries']['cataId'])
        return metadata

    def detail_hubei_huangshi(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)

        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        key_map = {
            '名称': "title",
            '摘要': "summary",
            '资源标签': "tag",
            '数源单位': "departmentname",
            '数据领域': "lyid",
            '行业分类': "bshy",
            '发布日期': "c_date",
            '更新日期': "e_data",
            '更新周期': "None_field",
            '开放条件': "dataopen",
            '版本': 'None',
            '联系方式': 'phone',
            '邮箱': 'None',
            '资源格式': 'datafmt',
        }

        lylist = [{
            "id": 1,
            "name": "教育培训",
            "value": 4
        }, {
            "id": 2,
            "name": "企业扶持",
            "value": 1
        }, {
            "id": 3,
            "name": "就业招聘",
            "value": 3
        }, {
            "id": 4,
            "name": "城市安全",
            "value": 2
        }, {
            "id": 5,
            "name": "生活服务",
            "value": 18
        }, {
            "id": 6,
            "name": "结婚生育",
            "value": 0
        }, {
            "id": 7,
            "name": "交通出行",
            "value": 4
        }, {
            "id": 8,
            "name": "社会保险",
            "value": 2
        }, {
            "id": 9,
            "name": "退休养老",
            "value": 0
        }, {
            "id": 10,
            "name": "医疗保健",
            "value": 2
        }]
        hylist = [{
            "id": 1,
            "name": "信用服务",
            "value": 2
        }, {
            "id": 2,
            "name": "医疗卫生",
            "value": 2
        }, {
            "id": 3,
            "name": "社保就业",
            "value": 4
        }, {
            "id": 4,
            "name": "公共安全",
            "value": 1
        }, {
            "id": 5,
            "name": "城建住房",
            "value": 2
        }, {
            "id": 6,
            "name": "交通运输",
            "value": 4
        }, {
            "id": 7,
            "name": "教育文化",
            "value": 4
        }, {
            "id": 8,
            "name": "科技创新",
            "value": 0
        }, {
            "id": 9,
            "name": "资源能源",
            "value": 0
        }, {
            "id": 10,
            "name": "生态环境",
            "value": 1
        }, {
            "id": 11,
            "name": "工业农业",
            "value": 0
        }, {
            "id": 12,
            "name": "商贸流通",
            "value": 2
        }, {
            "id": 13,
            "name": "财税金融",
            "value": 0
        }, {
            "id": 14,
            "name": "安全生产",
            "value": 1
        }, {
            "id": 15,
            "name": "市场监督",
            "value": 1
        }, {
            "id": 16,
            "name": "社会救助",
            "value": 3
        }, {
            "id": 17,
            "name": "法律服务",
            "value": 4
        }, {
            "id": 18,
            "name": "生活服务",
            "value": 6
        }, {
            "id": 19,
            "name": "气象服务",
            "value": 1
        }, {
            "id": 20,
            "name": "地理空间",
            "value": 0
        }, {
            "id": 21,
            "name": "机构团体",
            "value": 0
        }, {
            "id": 22,
            "name": "其他",
            "value": 0
        }]

        data = json.loads(response.text)['data']
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ['发布日期', '更新日期']:
                    metadata[name] = time.strftime('%Y-%m-%d', time.localtime(metadata[name]))
                if name in ['数据领域']:
                    item = list(filter(lambda x: x['id'] == int(metadata[name]), lylist))
                    metadata[name] = item[0]['name']
                if name in ['行业分类']:
                    item = list(filter(lambda x: x['id'] == int(metadata[name]), hylist))
                    metadata[name] = item[0]['name']
                if name in ['开放条件']:
                    metadata[name] = '无条件开放' if int(metadata[name]) == 1 else '申请公开'
        if '资源格式' in metadata:
            metadata['资源格式'] = list(metadata['资源格式'].lower().split(','))
        metadata['详情页网址'] = 'http://data.huangshi.gov.cn/html/#/opentableinfo?infoid={}'.format(
            curl['queries']['infoid'])
        return metadata

    def detail_hubei_yichang(self, curl):
        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        if curl['crawl_type'] == 'dataset':
            key_map = {
                '名称': "title",
                '摘要': "content",
                '资源标签': "keywords",
                '数源单位': "deptName",
                '数据领域': "domainStr",
                '行业分类': {
                    "downloadDataInfo": "themeName"
                },
                '发布日期': "createDate",
                '更新日期': "dataUpdateDate",
                '更新周期': "None",
                '开放条件': {
                    'downloadDataInfo': "openCondition"
                },
                '版本': 'None',
                '联系方式': 'deptContact',
                '邮箱': 'None',
                '数据下载': 'None',
            }
            response = requests.get(curl['url'],
                                    params=curl['queries'],
                                    headers=curl['headers'],
                                    timeout=REQUEST_TIME_OUT)
            data = json.loads(response.text)['data']
            metadata = {}
            for name in key_map:
                k = key_map[name]
                value, k = get_meta_data(data, k)
                if value:
                    metadata[name] = value
            metadata['详情页网址'] = 'https://data.yichang.gov.cn/kf/open/table/detail/{}'.format(curl['queries']['dataId'])
        else:
            key_map = {
                '名称': "title",
                '摘要': "content",
                '资源标签': "keywords",
                '数据提供单位': 'source',
                '数源单位': "deptName",
                '数据领域': "domainStr",
                '行业分类': {
                    "downloadDataInfo": "themeName"
                },
                '发布日期': "createDate",
                '更新日期': "dataUpdateDate",
                '更新周期': "None",
                '开放条件': {
                    'downloadDataInfo': "openCondition"
                },
                '版本': 'None',
                '联系方式': 'deptContact',
                '邮箱': 'None',
                '数据下载': 'None',
            }
            response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
            data = json.loads(response.text)['data']
            metadata = {}
            for name in key_map:
                k = key_map[name]
                value, k = get_meta_data(data, k)
                if value:
                    metadata[name] = value
            metadata['数据下载'] = ['api']
            metadata['详情页网址'] = 'https://data.yichang.gov.cn/kf/open/interface/detail/{}'.format(
                curl['data']['baseDataId'])
        return metadata

    def detail_hubei_ezhou(self, curl):
        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.content, 'html.parser')
        table = soup.find('ul', class_='table_p1')
        metadata_key = ['摘要', '资源标签', '更新周期', '资源格式', '数据单位', '数据领域', '', '行业分类', '', '数据分级', '开放条件', '更新日期', '发布日期']
        number = 0
        metadata = {}
        for content in table.find_all('div', class_='content-td'):
            if not metadata_key[number]:
                number += 1
                continue
            text = content.text
            text = text.replace(' ', '').strip()
            if text:
                metadata[metadata_key[number]] = text
            number += 1
        if '资源格式' in metadata:
            metadata['资源格式'] = ['file']
        if 'api' in curl:
            metadata['资源格式'] = ['api']
        metadata['详情页网址'] = curl['url']
        return metadata

    def detail_hubei_jingzhou(self, curl):
        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.content, 'html.parser')
        metadata = {}

        top = soup.find('div', class_='directory-media')
        metadata['名称'] = top.find('ul', class_='d-title').text.replace(' ', '').strip()

        details = top.find('div', class_='list-details')
        lis = details.find_all('li')
        content = lis[1].find_next('span').text
        metadata['重点领域'] = content.replace(' ', '').strip()

        table = soup.find('div', class_='panel-content')
        table = table.find('li', attrs={'name': 'basicinfo'})
        key = None
        for content in table.find_all('td'):
            text = content.text
            text = text.replace(' ', '').strip()
            if not key:
                key = text
                continue
            else:
                if text:
                    metadata[key] = text
                if key in ['发布时间', '最后更新时间']:
                    metadata[key] = metadata[key][:10]
                key = None
        metadata['详情页网址'] = curl['url']
        return metadata

    def detail_hubei_suizhou(self, curl):
        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        if curl['crawl_type'] == 'dataset':
            data = json.loads(response.text)['setMetadata']
        else:
            data = json.loads(response.text)['apiMetadata']

        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        key_map = {
            '名称': "resTitle",
            '摘要': "resAbstract",
            '关键字': "keyword",
            '提供方': 'officeName',
            '主题': "classifName",
            '行业分类': "None",
            '发布时间': "publishDate",
            '数据更新时间': "dataUpdateTime",
            '更新频率': "UpdateCycle",
            '开放等级': 'openLevel',
            '版本': 'None',
            '联系方式': 'None',
            '邮箱': 'None',
            '文件格式': 'metadataFileSuffix',
        }
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ['数据更新时间', '发布时间']:
                    metadata[name] = metadata[name][:10]
        if 'dataApi' in curl['url']:
            metadata['文件格式'] = 'api'
        if '文件格式' in metadata:
            metadata['文件格式'] = list(metadata['文件格式'].lower().split(','))
        metadata['详情页网址'] = curl['url']
        return metadata

    def detail_hunan_yueyang(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        tbody = soup.find('table')
        metadata = {}

        metadata['名称'] = soup.find('h1', class_='content-title').text.replace(' ', '').strip()

        for tr in tbody.find_all('tr'):
            tds = tr.find_all_next('td')
            key = tds[0].text.replace(' ', '').strip().strip('：')
            value = tds[1].text.replace(' ', '').strip()
            if value:
                metadata[key] = value
                if key in ['首次发布时间']:
                    metadata[key] = metadata[key].replace('/', '-')

        if '数据格式' in metadata:
            metadata['数据格式'] = list(metadata['数据格式'].lower().split(','))
        metadata['详情页网址'] = curl['url'] + "?id={}".format(curl['queries']['id'])
        return metadata

    def detail_hunan_changde(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        metadata = {}

        div = soup.find('div', class_='info-content')
        top_info = div.find('div', class_='top-info')
        metadata['名称'] = top_info.find('div', class_='catalog-name').text.replace(' ', '').strip()
        metadata['资源发布日期'] = top_info.find('div', class_='time').text.split('：')[-1].replace(' ', '').strip()

        rows = div.find('div', class_='row-content')
        keys = rows.find_all('div', 'info-name')
        values = rows.find_all('div', 'info-detail')
        for key, value in zip(keys, values):
            key = key.text.replace(' ', '').strip()
            value = value.text.replace(' ', '').strip()
            if value:
                metadata[key] = value
        metadata['详情页网址'] = 'https://www.changde.gov.cn/cdwebsite/dataopen/detail?cataId={}'.format(
            curl['queries']['cataId'])
        return metadata

    def detail_hunan_chenzhou(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        div = soup.find('div', class_='panel-body')
        metadata = {}

        metadata['名称'] = div.find('h2').text.replace(' ', '').strip()
        for tr in div.find('table', class_='table-license').find_all('tr'):
            tds = tr.find_all_next('td')
            key = tds[0].text.replace(' ', '').strip().strip('：')
            value = tds[1].text.replace(' ', '').strip()
            if value:
                metadata[key] = value
                if key in ['首次发布时间', '更新时间']:
                    metadata[key] = metadata[key].replace('/', '-')
        if '数据格式' in metadata:
            metadata['数据格式'] = list(metadata['数据格式'].lower().split(','))
        metadata['详情页网址'] = curl['url'] + '?id={}'.format(curl['queries']['id'])
        return metadata

    def detail_hunan_yiyang(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        div = soup.find('div', class_='panel-body')
        metadata = {}

        metadata['名称'] = div.find('h2').text.replace(' ', '').strip()
        for tr in div.find('table', class_='table-license').find_all('tr'):
            tds = tr.find_all_next('td')
            key = tds[0].text.replace(' ', '').strip().strip('：')
            value = tds[1].text.replace(' ', '').strip()
            if value:
                metadata[key] = value
                if key in ['首次发布时间', '更新时间']:
                    metadata[key] = metadata[key].replace('/', '-')
        if '数据格式' in metadata:
            metadata['数据格式'] = list(metadata['数据格式'].lower().split(','))
        metadata['详情页网址'] = curl['url'] + '?id={}'.format(curl['queries']['id'])
        return metadata

    def detail_guangdong_guangdong(self, curl):
        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        key_map = {
            '名称': "resTitle",
            '简介': "resAbstract",
            '关键字': "keyword",
            '数据提供方': "officeName",
            '主题分类': "subjectName",
            '行业分类': "None",
            '发布日期': "publishDate",
            '更新日期': "lastModifyTime",
            '更新频率': "None",
            '开放方式': "openMode",
            '版本': 'None',
            '联系方式': 'None',
            '邮箱': 'email',
            '资源格式': 'sourceSuffix',
        }
        response = requests.post(curl['url'], json=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        data = json.loads(response.text)['data']
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ['发布日期', '更新日期']:
                    metadata[name] = metadata[name][:10]
        if '资源格式' in metadata:
            metadata['资源格式'] = list(metadata['资源格式'].lower().split(','))
        metadata['详情页网址'] = 'https://gddata.gd.gov.cn/opdata/index'
        return metadata

    def detail_guangdong_guangzhou(self, curl):
        doc_curl = curl['doc']
        curl = curl['detail']
        response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        data = json.loads(response.text)['body']

        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        key_map = {
            '名称': "name",
            '简介': "description",
            '标签': "tags",
            '来源部门': "orgName",
            '所属主题': "subjectName",
            '所属行业': "industryName",
            '发布时间': "created",
            '最后更新': "lastUpdated",
            '更新频率': "updateCycle",
            '开放方式': "openStatus",
            '版本': 'None',
            '联系电话': 'None',
            '联系邮箱': 'None',
            '数据格式': 'None',
        }
        updateTime = {'1': '不定期', '2': '每天', '3': '每周', '4': '每月', '5': '每季度', '6': '每半年', '7': '每年', '8': '实时'}
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ['更新频率']:
                    metadata[name] = updateTime[metadata[name]]
                if name in ['发布时间', '最后更新']:
                    metadata[name] = metadata[name][:10]
                if name in ['开放方式']:
                    metadata[name] = '有条件开放' if metadata[name] == 3 else '无条件开放'

        metadata['数据格式'] = list(map(lambda x: x['fileFormat'], data['dataFileList']))

        response = requests.get(doc_curl['url'],
                                params=doc_curl['queries'],
                                headers=doc_curl['headers'],
                                timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.content, 'html.parser')
        divs = soup.find_all('div', class_='p-tit')
        for div in divs:
            if '数据集发布者联系方式' not in div.text:
                continue
            ul = div.find_next('ul')
            spans = ul.find_all('span')
            metadata['联系电话'] = spans[1].text
            metadata['联系邮箱'] = spans[3].text
        metadata['详情页网址'] = doc_curl['url'] + '?sid={}'.format(doc_curl['queries']['sid'])
        return metadata

    def detail_guangdong_shenzhen(self, curl):
        def get_meta_data(data, key):
            if not data:
                return '', key
            all_data = copy.deepcopy(data)
            while not isinstance(key, str):
                now_key = list(key.keys())[0]
                key = key[now_key]
                if now_key in all_data:
                    all_data = all_data[now_key]
                else:
                    all_data = {}
            return (all_data[key] if (key in all_data) else ''), key

        key_map = {
            '名称': "resTitle",
            '摘要': "resAbstract",
            '关键字': "keyword",
            '数据提供方': "officeName",
            '服务分类': "subjectName",
            '行业分类': "tradeName",
            '上架日期': "publishDate",
            '更新日期': "dataUpdateTime",
            '更新频率': "updateCycle",
            '开放方式': "openLevelName",
            '版本': 'None',
            '联系方式': 'phone',
            '邮箱': 'None',
            '资源格式': 'sourceSuffix',
        }
        response = requests.post(curl['url'], data=curl['data'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        data = json.loads(response.text)
        metadata = {}
        for name in key_map:
            k = key_map[name]
            value, k = get_meta_data(data, k)
            if value:
                metadata[name] = value
                if name in ['上架日期', '更新日期']:
                    metadata[name] = metadata[name][:10]
        if '资源格式' in metadata:
            metadata['资源格式'] = list(metadata['资源格式'].lower().split(','))
        metadata['详情页网址'] = 'https://opendata.sz.gov.cn/data/dataSet/toDataDetails/{}'.format(
            curl['data']['resId'].replace('/', '_'))
        return metadata

    def detail_guangdong_zhongshan(self, curl):
        response = requests.get(curl['url'], params=curl['queries'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='table-main')
        ths = table.find_all('th')
        tds = table.find_all('td')
        metadata = {}
        metadata['名称'] = soup.find('h2').text

        for th, td in zip(ths, tds):
            th = th.text.replace(' ', '').strip()
            td = td.text.replace(' ', '').strip()
            if td:
                metadata[th] = td
                if th in ['创建时间', '更新时间']:
                    metadata[th] = metadata[th].replace('年', '-').replace('月', '-').replace('日', '')
        metadata['详情页网址'] = curl['url'] + '?id={}&pageNum={}'.format(curl['queries']['id'], curl['queries']['pageNum'])
        return metadata

    def detail_other(self, curl):
        print("暂无该省")