import json
import re
import time
import unicodedata as ucd
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

    def detail_shandong_common(self, curl):
        list_fields = ["来源部门", "重点领域", "发布时间", "更新时间", "开放类型"]
        table_fields = ["数据量", "所属行业", "更新频率", "部门电话", "部门邮箱", "标签", "描述"]
        item_fields = ["英文信息项名", "中文信息项名", "数据类型", "中文描述"]
        max_retry = 5
        for i in range(max_retry):
            try:
                response = requests.get(curl['url'], headers=curl['headers'], timeout=REQUEST_TIME_OUT)
                break
            except Exception as e:
                print("重试次数", i + 1)
                time.sleep(5)

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
            if td_name == "数据量":  # 可能叫接口数，或文件数
                alias_list = ["数据量", "接口数", "文件数"]
                for name in alias_list:
                    if table.find('td', text=name) is not None:
                        td_name = name
                        break
            try:
                td_text = table.find('td', text=td_name).find_next('td').get_text().strip()
                td_text = ucd.normalize('NFKC', td_text).replace(' ', '')
                dataset_matadata[td_name] = td_text
            except Exception as e:
                print(e)
                print("title = ", title)
                print("url = ", curl['url'])
        return dataset_matadata

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
