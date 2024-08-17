import requests

cookies = {
    'JSESSIONID': '8509564138688C19BB0D8503471A0B67',
    '__jsluid_s': 'bce53d32b20966d1c4cf88d968a33fbe',
    'hefei_gova_SHIROJSESSIONID': 'e3475fda-dbae-4206-8161-efa962b41397',
    '__jsl_clearance_s': '1686647896.82|0|SDCKcw0ENmDTnPgCve3ucO5N4%2BY%3D',
}

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=8509564138688C19BB0D8503471A0B67; __jsluid_s=bce53d32b20966d1c4cf88d968a33fbe; hefei_gova_SHIROJSESSIONID=e3475fda-dbae-4206-8161-efa962b41397; __jsl_clearance_s=1686647896.82|0|SDCKcw0ENmDTnPgCve3ucO5N4%2BY%3D',
    'Referer': 'https://www.hefei.gov.cn/open-data-web/data/list-hfs.do?pageIndex=2',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

params = {
    'zylxdm': '1',
    'currentPageNo': '3',
    'pageSize': '10',
    'rtcode': '03',
    'rcid': '',
    'flag': '0',
    'sortSign': '',
    '_': '1686648088504',
}

response = requests.get(
    'https://www.hefei.gov.cn/open-data-web/kfzy/queryKfZyPage.do',
    params=params,
    cookies=cookies,
    headers=headers,
)

print(response)