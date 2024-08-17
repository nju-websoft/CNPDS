import argparse
import json
import os
import pymysql

from hashlib import sha256

from common.constants import METADATA_SAVE_PATH
from common.utils import log_error

FIELD_MAPPING_JSON_PATH = os.path.join(
    os.path.dirname(__file__), "data/field_mapping.json"
)
NAME_MAPPING_JSON_PATH = os.path.join(
    os.path.dirname(__file__), "data/name_mapping.json"
)

parser = argparse.ArgumentParser()
parser.add_argument("--db-host", type=str)
parser.add_argument("--db-port", type=int)
parser.add_argument("--db-user", type=str)
parser.add_argument("--db-pswd", type=str)
parser.add_argument("--database", type=str)
parser.add_argument("--table", type=str)
parser.add_argument("--ref-table", type=str, default="metadata")

parser.add_argument("--metadata-path", type=str, default=METADATA_SAVE_PATH)
parser.add_argument("--field-map-path", type=str, default=FIELD_MAPPING_JSON_PATH)
parser.add_argument("--name-map-path", type=str, default=NAME_MAPPING_JSON_PATH)

parser.add_argument("--url-hash", action="store_true")

args = parser.parse_args()

DB_HOST = args.db_host
DB_PORT = args.db_port
DB_USER = args.db_user
DB_PSWD = args.db_pswd
DATABASE_NAME = args.database
TABLE_NAME = args.table
REF_TABLE_NAME = args.ref_table

metadata_path = args.metadata_path
field_map_path = args.field_map_path
name_map_path = args.name_map_path

URL_HASH = args.url_hash

db = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PSWD,
    database=DATABASE_NAME,
    charset="utf8",
)

c = db.cursor()


def write_metadata():
    field_names = [
        "dataset_id",
        "title",
        "description",
        "tags",
        "department",
        "category",
        "publish_time",
        "update_time",
        "is_open",
        "data_volume",
        "industry",
        "update_frequency",
        "telephone",
        "email",
        "data_formats",
        "url",
        "province",
        "city",
        "standard_industry",
    ]

    if URL_HASH:
        field_names.append("url_hash")

    with open(name_map_path, "r", encoding="utf-8") as f:
        name_mapping = json.load(f)
    with open(field_map_path, "r", encoding="utf-8") as f:
        field_mapping = json.load(f)

    province_city = {}
    path = metadata_path
    file_list = os.listdir(metadata_path)

    sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} LIKE {REF_TABLE_NAME}"
    c.execute(sql)

    if URL_HASH:
        sql = (
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT {', '.join(['%s'] * len(field_names))} FROM DUAL "
            f"WHERE NOT EXISTS (SELECT * FROM {TABLE_NAME} WHERE url_hash = %s and url = %s)"
        )
    else:
        sql = f"INSERT INTO {TABLE_NAME} VALUES({', '.join(['%s'] * len(field_names))})"

    for file in file_list:
        file_name = file.split(".")[0]
        province, city = file_name.split("_")
        if not name_mapping.get(province):
            continue
        if not name_mapping[province].get(city):
            continue
        city = name_mapping[province][city]
        province = name_mapping[province][province]

        if province not in province_city:
            province_city[province] = []
        province_city[province].append(city)

        metadata_file_path = os.path.join(path, file)

        if not os.path.exists(metadata_file_path):
            log_error("database-writer: file '%s' does not exist.", metadata_file_path)
            continue
        with open(metadata_file_path, "r", encoding="utf-8") as json_file:
            metadata_list = json.load(json_file)
        dataset_list = []

        ad_hoc_field_mapping = None
        if (
            province in field_mapping["ad-hoc"]
            and city in field_mapping["ad-hoc"][province]
        ):
            ad_hoc_field_mapping = field_mapping["ad-hoc"][province][city]

        for dataset in metadata_list:
            metadata = {"province": province, "city": city}
            for key, value in dataset.items():
                if ad_hoc_field_mapping and key in ad_hoc_field_mapping:
                    metadata[ad_hoc_field_mapping[key]] = str(value)
                elif key in field_mapping["common"]:
                    metadata[field_mapping["common"][key]] = str(value)

            di = [
                metadata[field] if field in metadata else None for field in field_names
            ]

            if "url" in metadata and metadata["url"]:
                if URL_HASH:
                    url_hash = sha256(metadata["url"].encode()).hexdigest()
                    di[-1] = url_hash
                    di.extend([url_hash, metadata["url"]])
                dataset_list.append(di)

        c.executemany(sql, dataset_list)
        db.commit()


def stastic():
    format_cnt = {}
    sql = f"SELECT data_formats FROM {TABLE_NAME}"
    c.execute(sql)
    formats = c.fetchall()
    for fi in formats:
        if fi[0] is None:
            continue
        format_list = fi[0].split(",")
        for i in format_list:
            if i not in format_cnt:
                format_cnt[i] = 0
            format_cnt[i] += 1
    format_cnt = sorted(format_cnt.items(), key=lambda x: x[1], reverse=True)
    for i in format_cnt:
        print(i[0], i[1])


write_metadata()
# stastic()

c.close()
