"""Constants"""
import os.path

DEFAULT_ENCODING = 'utf-8'
REQUEST_TIME_OUT = 10

PROVINCE_CURL_JSON_PATH = os.path.join('config', 'curl.json')

PROVINCE_LIST = ['shanghai', 'jiangsu', 'zhejiang']

# METADATA_SAVE_PATH = '/home/qschen/Data/ChinaOpenData/metadata/'
METADATA_SAVE_PATH = os.path.join('metadata')