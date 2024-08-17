#!/usr/bin/bash

echo "================================`date -I`================================"

SCRIPT_DIR=$(dirname $(readlink -f $0))
source ${SCRIPT_DIR}/env.sh

pushd ${SERVER_JAVA_REPO_PATH}
  ${MAVEN_PATH} clean install
popd

TARGET_PATH="${SERVER_JAVA_REPO_PATH}/target/ChinaOpenDataPortal-0.0.1-SNAPSHOT.jar"

PORTALS_PATH="${SERVER_JAVA_REPO_PATH}/src/main/resources/data/portals.json"
NEWS_PATH="${SERVER_JAVA_REPO_PATH}/src/main/resources/data/news.json"

${JAVA_PATH} -jar ${TARGET_PATH} \
  --com.chinaopendataportal.indices.load=${LUCENE_CURRENT_INDEX_PATH} \
  --com.chinaopendataportal.stopwords=${STOPWORDS_PATH} \
  --com.chinaopendataportal.portal=${PORTALS_PATH} \
  --com.chinaopendataportal.news=${NEWS_PATH} \
  --com.chinaopendataportal.security.user=${ADMIN_USER} \
  --com.chinaopendataportal.security.pswd=${ADMIN_PSWD} \
  --com.chinaopendataportal.python.api=http://127.0.0.1:${FLASK_PORT}/apis \
  --com.chinaopendataportal.python.maxreranktimes=${PYTHON_RETRY_TIMES} \
  "$@"
