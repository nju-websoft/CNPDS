import requests
import json
from bs4 import BeautifulSoup

# {
#     "url": "http://182.132.59.11:11180/exchangeopengateway/v1.0/mh/sjml/getMlxxList",
#     "raw_url": "http://182.132.59.11:11180/exchangeopengateway/v1.0/mh/sjml/getMlxxList",
#     "method": "post",
#     "cookies": {
#         "mozi-assist": "{%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}",
#         "_appId": "2b7e307e4095d1d15a9f8cfbe8961088"
#     },
#     "headers": {
#         "Accept": "application/json, text/plain, */*",
#         "Accept-Language": "zh-CN,zh-TW;q=0.9,zh;q=0.8,en;q=0.7",
#         "Connection": "keep-alive",
#         "Content-Type": "application/json;charset=UTF-8",
#         "Cookie": "mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; _appId=2b7e307e4095d1d15a9f8cfbe8961088",
#         "Origin": "http://182.132.59.11:11180",
#         "Referer": "http://182.132.59.11:11180/dexchange/open/",
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
#         "appCode": "2b7e307e4095d1d15a9f8cfbe8961088",
#         "token": "null"
#     },
#     "data": {
#         "pageNo": 1,
#         "pageSize": 10,
#         "zylx": [
#             "01",
#             "02"
#         ],
#         "ssbmId": [
#             "00"
#         ],
#         "mlmc": "",
#         "gxsjPx": 2,
#         "llslPx": "",
#         "sqslPx": "",
#         "pfPx": "",
#         "xzslPx": "",
#         "ssztlmId": [
#             "00"
#         ],
#         "ssjclmId": [
#             "00"
#         ],
#         "ssqtlmId": [
#             "00"
#         ],
#         "kflx": [
#             "00"
#         ],
#         "wjlx": [
#             "00"
#         ]
#     },
#     "compressed": true,
#     "insecure": false
# }

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    # 'Cookie': 'mozi-assist={%22show%22:false%2C%22audio%22:false%2C%22speed%22:%22middle%22%2C%22zomm%22:1%2C%22cursor%22:false%2C%22pointer%22:false%2C%22bigtext%22:false%2C%22overead%22:false}; _appId=71cd6e85e4e3c16720e3442035ab95a5',
    'Referer': 'http://182.132.59.11:11180/dexchange/open/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'token': 'null',
}

params = {
    'mlbh': '03012/000838',
    'appCode': '71cd6e85e4e3c16720e3442035ab95a5',
}

response = requests.get(
    'http://182.132.59.11:11180/exchangeopengateway/v1.0/mlxx/getSjmlxq',
    params=params,
    headers=headers,
    verify=False,
)
result = json.loads(response.text)
print(result)
list_table = ['mlmc','xxzytgf','ssztlmmc','fbrq', 'gxrq','kflx','sjl','ssqtlmmc','gxzq','sjhm','mlbqmcList','xxzymc','zygs']
for li_name in list_table:
    print(result['data'][li_name])
# html = response.content
# soup = BeautifulSoup(html, "html.parser")
# print(soup)
# 03: 每月，05： 每年
# for tit in soup.find_all('div',attrs={'class':'col-item'}):#.find_all('div', attrs={'class': 'text'}):
#    print(tit)

# 1.甘孜数据问题
# 2.凉山无法打开
# 3.毕节67-122页无法打开，PC段直接查看没有问题，但是到获取cURL请求那一步就会出现404错误