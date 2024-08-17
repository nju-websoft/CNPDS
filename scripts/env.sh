#!/usr/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $0))
THIS_REPO_PATH="${SCRIPT_DIR}/.."

SERVER_JAVA_REPO_PATH="${THIS_REPO_PATH}/codp-backend-java"
SERVER_PY_REPO_PATH="${THIS_REPO_PATH}/codp-backend-flask"
METADATA_REPO_PATH="${THIS_REPO_PATH}/codp-metadata"
INDEXBUILDER_REPO_PATH="${THIS_REPO_PATH}/codp-index-builder"

METADATA_PATH="${THIS_REPO_PATH}/metadata"
DATAFILE_PATH="${THIS_REPO_PATH}/datafile"
LUCENE_INDICES_PATH="${THIS_REPO_PATH}/indices"

BACKEND_URL=http://localhost:9999/cn-public

# Tools
MAVEN_PATH="mvn"
JAVA_PATH="java"
PYTHON_PATH="python"
FLASK_PATH="flask"

# Common
LUCENE_CURRENT_INDEX_PATH="${LUCENE_INDICES_PATH}/current"
STOPWORDS_PATH="${SERVER_JAVA_REPO_PATH}/src/main/resources/data/nltk-chinese-stopwords.txt"
# Security
ADMIN_USER="****"
ADMIN_PSWD="****"

# Database
DB_ADDR="****"
DB_PORT="****"
DB_USER="****"
DB_PSWD="****"
DATABASE_NAME="****"
REF_TABLE_NAME="****"
PRD_TABLE_NAME="****"

# Options
CRAWL_WORKERS=60
CRAWL_FILES="" # OR "--download-files"
FLASK_PORT=5007
PYTHON_RETRY_TIMES=1

echo "[LOG] Environment loaded."

# Custom Environment
__CUSTOM_ENV_PATH="${SCRIPT_DIR}/env.custom.sh"
if [ -f "${__CUSTOM_ENV_PATH}" ]; then
    source ${__CUSTOM_ENV_PATH}
    echo "[LOG] Custom Environment loaded."
fi
