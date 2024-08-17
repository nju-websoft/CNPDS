import requests

cookies = {
    'Admin-Token': 'anon-37071c5d-a097-4002-a89b-d1f3e77fe76d',
}

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Authorization': 'Bearer',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    # 'Cookie': 'Admin-Token=anon-37071c5d-a097-4002-a89b-d1f3e77fe76d',
    'Origin': 'https://data.jiangxi.gov.cn',
    'Referer': 'https://data.jiangxi.gov.cn/open-data',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

json_data = {
    'current': 1,
    'pageSize': 10,
    'dataName': '',
    'resResourceType': '',
    'resOrgCode': [],
    'dataFieldCode': [],
    'resShareType': '',
    'responsibilityListFlag': '',
    'orderHotNum': 'desc',
}

response = requests.post(
    'https://data.jiangxi.gov.cn/opendata-portal/prod-api/openData/page',
    headers=headers,
    json=json_data,
)

print(response)
# Note: json_data will not be serialized by requests
# exactly as it was in the original request.
#data = '{"current":1,"pageSize":10,"dataName":"","resResourceType":"","resOrgCode":[],"dataFieldCode":[],"resShareType":"","responsibilityListFlag":"","orderHotNum":"desc"}'
#response = requests.post(
#    'https://data.jiangxi.gov.cn/opendata-portal/prod-api/openData/page',
#    cookies=cookies,
#    headers=headers,
#    data=data,
#    verify=False,
#)