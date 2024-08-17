#!/usr/bin/bash

echo "================================`date -I`================================"

SCRIPT_DIR=$(dirname $(readlink -f $0))
source ${SCRIPT_DIR}/env.sh

DATE_SUFFIX=`date +"%Y-%m-%d"`

# fetch metadata & write to database
CRAWLER_MODULE="crawler"
DBWRITER_MODULE="db-writer"

METADATA_SAVING_PATH="${METADATA_PATH}/metadata-${DATE_SUFFIX}"
DATAFILE_SAVING_PATH="${DATAFILE_PATH}/datafile-${DATE_SUFFIX}"

    ## fetch metadata from websites
    echo "[LOG `date`] Fetching METADATA..."
    mkdir -p ${METADATA_SAVING_PATH}
    pushd ${METADATA_REPO_PATH}
        ${PYTHON_PATH} -m ${CRAWLER_MODULE} --all --workers ${CRAWL_WORKERS} \
            ${CRAWL_FILES} --files-output ${DATAFILE_SAVING_PATH} \
            --metadata-output ${METADATA_SAVING_PATH}
    popd

    ## write metadata into database
    echo "[LOG `date`] Writing Database..."
    pushd ${METADATA_REPO_PATH}

        ### save a copy of crawled metadata
        ARC_TABLE_NAME=`echo metadata-${DATE_SUFFIX} | sed 's/-/_/g'`
        echo "[LOG `date`] Writing to TABLE ${ARC_TABLE_NAME}"
        ${PYTHON_PATH} -m ${DBWRITER_MODULE} \
            --db-host ${DB_ADDR} \
            --db-port ${DB_PORT} \
            --db-user ${DB_USER} \
            --db-pswd "${DB_PSWD}" \
            --database ${DATABASE_NAME} \
            --ref-table ${REF_TABLE_NAME} \
            --table ${ARC_TABLE_NAME} \
            --metadata-path ${METADATA_SAVING_PATH}

        ### merge into a single table
        echo "[LOG `date`] Writing to TABLE ${PRD_TABLE_NAME}"
        ${PYTHON_PATH} -m ${DBWRITER_MODULE} \
            --db-host ${DB_ADDR} \
            --db-port ${DB_PORT} \
            --db-user ${DB_USER} \
            --db-pswd "${DB_PSWD}" \
            --database ${DATABASE_NAME} \
            --table ${PRD_TABLE_NAME} \
            --url-hash \
            --metadata-path ${METADATA_SAVING_PATH}

    popd

echo "[LOG `date`] Finish Data Fetching."

# build indices from database
function __size() {
    du -D $1 | awk -F ' ' '{print$1}'
}

INDICES_SAVING_PATH="${LUCENE_INDICES_PATH}/indices-${DATE_SUFFIX}"

pushd ${INDEXBUILDER_REPO_PATH}
    ${MAVEN_PATH} clean install
popd

TARGET_PATH="${INDEXBUILDER_REPO_PATH}/target/index-builder-0.0.1-SNAPSHOT.jar"

echo "[LOG `date`] Building Indices..."
${JAVA_PATH} -jar ${TARGET_PATH} \
    --com.chinaopendataportal.indices.store=${INDICES_SAVING_PATH} \
    --com.chinaopendataportal.stopwords=${STOPWORDS_PATH} \
    --spring.datasource.url="jdbc:mysql://${DB_ADDR}:${DB_PORT}/${DATABASE_NAME}" \
    --spring.datasource.username=${DB_USER} \
    --spring.datasource.password="${DB_PSWD}" \
    --com.chinaopendataportal.table=${PRD_TABLE_NAME}

## update current indices
if [ ! -e "${LUCENE_CURRENT_INDEX_PATH}" ] || \
    [ `__size ${LUCENE_CURRENT_INDEX_PATH}` -lt `__size ${INDICES_SAVING_PATH}` ]
then
    echo "[LOG `date`] Linking Indices..."
    if [ -e "${LUCENE_CURRENT_INDEX_PATH}" ]
    then
        rm ${LUCENE_CURRENT_INDEX_PATH}
    fi
    ln -s ${INDICES_SAVING_PATH} ${LUCENE_CURRENT_INDEX_PATH}
    echo "[LOG `date`] `curl -X POST -u ${ADMIN_USER}:${ADMIN_PSWD} "${BACKEND_URL}/apis/update?index=${INDICES_SAVING_PATH}"`"
else
    echo "[LOG `date`] Indices Linking Canceled."
fi
