# -*- coding = utf-8 -*-
# @Time : 2023/6/14 10:52
# @Author : Jiaying Zhou
# @File : postprocess.py
# @Software : PyCharm
import json


def add_default_data_format():
    with open("./results/cache/shandong_shandong_data_cache - 副本 - 副本.json", encoding='utf-8') as f_reader:
        raw_data = json.load(f_reader)
    for sample in raw_data:
        if len(sample["数据格式"]) == 0:
            sample["数据格式"].append("file")
    with open("./results/cache/shandong_shandong_data_cache_new.json", 'w', encoding='utf-8') as f_writer:
        json.dump(raw_data, f_writer, ensure_ascii=False)


def add_mapping():
    cities = ['shandong', 'jinan', 'qingdao', 'zibo', 'zaozhuang', 'dongying', 'yantai', 'weifang', 'jining', 'taian',
              'weihai', 'rizhao', 'linyi', 'dezhou', 'liaocheng', 'binzhou', 'heze']
    output_root_path = "../../../../results/mapping"
    map_dict = {
        "标题": "title",
        "来源部门": "department",
        "重点领域": "category",
        "发布时间": "publish_time",
        "更新时间": "update_time",
        "开放类型": "is_open",
        "数据量": "data_volume",
        "所属行业": "industry",
        "更新频率": "update_frequency",
        "部门电话": "telephone",
        "部门邮箱": "email",
        "标签": "tags",
        "描述": "description",
        "数据格式": "data_formats",
        "详情页网址": "url"
    }
    for city in cities:
        print(city)
        output_path = f"{output_root_path}/shandong_{city}.json"
        with open(output_path, 'w', encoding='utf-8') as f_writer:
            json.dump(map_dict, f_writer, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    # add_default_data_format()
    add_mapping()
    pass
