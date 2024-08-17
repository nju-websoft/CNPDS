# CN-PDS

This is source code and prompts for paper "CN-PDS: China’s Public Data Search Enabled by LLMs".

## Prompts

Prompts for metadata consolidation, metadata enrichment, dataset reranking, and relevance explanation are at `./promprs/`.

## Vue Frontend

`./cnpds-frontend-vue`, based on Vue3 and [Tabler: An HTML Dashboard UI Kit built on Bootstrap](https://github.com/tabler/tabler).

### Deployment

#### Docker (Nginx)

Deploy the frontend using nginx inside of a docker container.

> Expose: Port 80.
> Env-Vars: `VITE_BACKEND_HOST`; `VITE_BEIAN_NUMBER`.

Image build command:

```bash
# PWD: ./ChinaOpenDataPortal-Frontend-Vue
docker build -f docker/Dockerfile -t username/imagename .
```

Push image to Docker Hub:

```bash
docker push username/imagename:latest
```

---

> Use this command to create a basic `env.custom.sh`:
>
> ```bash
> echo "PYTHON_PATH=$(realpath $(which python))" >> ./scripts/env.custom.sh
> ```

## API Server & Backend

### Java Backend

`./cnpds-backend-java`, based on Sprint Boot and Thymeleaf, with ability of acting as API server for other frontend service.
Default index path is `indices/current`.

#### Usage

Use `scripts/start-server.java.sh` to start a process as API server and backend.
Append more arguments as you need like `./scripts/start-server.java.sh --server.port=9998`.

Environmet variables you may want to specify in `env.custom.sh`:

1. MAVEN_PATH
1. JAVA_PATH (Java 11 Recommended)
1. ADMIN_USER
1. ADMIN_PSWD
3. PYTHON_RETRY_TIMES

### Python Backend

`./cnpds-backend-flask`, based on Flask, providing ability of communicating with Large Language Models and querying MySQL database.

#### Usage

Use `scripts/start-server.py.sh` to start a process as python backend.
Append more arguments as you need like `./scripts/start-server.py.sh --host 0.0.0.0`.

Environmet variables you may want to specify in `env.custom.sh`:

1. FLASK_PATH
2. FLASK_PORT

## Data Processor

1. `./cnpds-metadata`:
   Python scripts crawling metadata from each portals, multi-threads supported.
   Crawled metadata will be written to database for next step usage.
2. `./cnpds-index-builder`

### Metadata

Use `scripts/fetch-data.sh` to start a process for metadata fetching.
PS: There is one table that serves as an archive table for each metadata crawl and another table for index building.

Environmet variables you may want to specify in `env.custom.sh`:

1. About Crawler Control:
   1. CRAWL_WORKERS
   1. CRAWL_FILES (whether or not to download datafiles)
1. About database (Only MySQL Supported):
   1. DB_ADDR
   1. DB_PORT
   1. DB_USER
   1. DB_PSWD
   1. DATABASE_NAME
   1. REF_TABLE_NAME (specify a table as template)
   1. PRD_TABLE_NAME (specift a table which used in production)
1. Others:
   1. PYTHON_PATH (Python 3.6 Recommended)

### Lucene Index

After writing into database, the index builder will be started.
If necessary, it will link the latest index to the path `indices/current`.
If current index has been updated,the server will receive a POST request and refresh index path.

Environmet variables you may want to specify in `env.custom.sh`:

1. BACKEND_URL

## Appendix

### Metadata Table Template

```
+-------------------+--------------+------+-----+---------+----------------+
| Field             | Type         | Null | Key | Default | Extra          |
+-------------------+--------------+------+-----+---------+----------------+
| dataset_id        | int          | NO   | PRI | NULL    | auto_increment |
| title             | varchar(255) | YES  |     | NULL    |                |
| description       | text         | YES  |     | NULL    |                |
| tags              | text         | YES  |     | NULL    |                |
| department        | varchar(255) | YES  |     | NULL    |                |
| category          | varchar(255) | YES  |     | NULL    |                |
| publish_time      | varchar(255) | YES  |     | NULL    |                |
| update_time       | varchar(255) | YES  |     | NULL    |                |
| is_open           | varchar(255) | YES  |     | NULL    |                |
| data_volume       | varchar(255) | YES  |     | NULL    |                |
| industry          | varchar(255) | YES  |     | NULL    |                |
| update_frequency  | varchar(255) | YES  |     | NULL    |                |
| telephone         | varchar(255) | YES  |     | NULL    |                |
| email             | varchar(255) | YES  |     | NULL    |                |
| data_formats      | varchar(255) | YES  |     | NULL    |                |
| url               | text         | YES  |     | NULL    |                |
| province          | varchar(255) | YES  |     | NULL    |                |
| city              | varchar(255) | YES  |     | NULL    |                |
| standard_industry | varchar(255) | YES  |     | NULL    |                |
+-------------------+--------------+------+-----+---------+----------------+
```

### Metadata Table for Production Template

```
+-------------------+--------------+------+-----+---------+----------------+
| Field             | Type         | Null | Key | Default | Extra          |
+-------------------+--------------+------+-----+---------+----------------+
| dataset_id        | int          | NO   | PRI | NULL    | auto_increment |
| title             | varchar(255) | YES  |     | NULL    |                |
| description       | text         | YES  |     | NULL    |                |
| tags              | text         | YES  |     | NULL    |                |
| publish_time      | varchar(255) | YES  |     | NULL    |                |
| update_time       | varchar(255) | YES  |     | NULL    |                |
| industry          | varchar(255) | YES  |     | NULL    |                |
| department        | varchar(255) | YES  |     | NULL    |                |
| is_open           | varchar(255) | YES  |     | NULL    |                |
| data_volume       | varchar(255) | YES  |     | NULL    |                |
| update_frequency  | varchar(255) | YES  |     | NULL    |                |
| data_formats      | varchar(255) | YES  |     | NULL    |                |
| url               | text         | NO   |     | NULL    |                |
| province          | varchar(255) | YES  |     | NULL    |                |
| city              | varchar(255) | YES  |     | NULL    |                |
| standard_industry | varchar(255) | YES  |     | NULL    |                |
| url_hash          | varchar(255) | NO   | UNI | NULL    |                |
| origin_metadata   | mediumtext   | YES  |     | NULL    |                |
| fetch_time        | varchar(255) | YES  |     | NULL    |                |
+-------------------+--------------+------+-----+---------+----------------+
```

### Enhanced Descripion Table Template

```
+----------------------+------+------+-----+---------+-------+
| Field                | Type | Null | Key | Default | Extra |
+----------------------+------+------+-----+---------+-------+
| dataset_id           | int  | YES  |     | NULL    |       |
| description_enhanced | text | YES  |     | NULL    |       |
+----------------------+------+------+-----+---------+-------+
```

### Data File Path Table Template

```
+------------+------+------+-----+---------+-------+
| Field      | Type | Null | Key | Default | Extra |
+------------+------+------+-----+---------+-------+
| dataset_id | int  | NO   |     | NULL    |       |
| path       | text | YES  |     | NULL    |       |
+------------+------+------+-----+---------+-------+
```

### Docker Compose YAML

``` YAML
version: '3.4'
services:
  frontend:
    image: 'username/imagename:latest'
    container_name: codp-frontend-vue
    restart: unless-stopped
    ports:
      - 'HOST_PORT:80'
    environment:
      - 'VITE_BACKEND_HOST=VITE_BACKEND_HOST_PLACEHOLDER'
      - 'VITE_BEIAN_NUMBER=VITE_BEIAN_NUMBER_PLACEHOLDER'
```

Params:
1. Image Name.
2. Host Port.
3. Env-Var: VITE_BACKEND_HOST.
4. Env-Var: VITE_BEIAN_NUMBER.

### Script Usages

#### start-server

##### Java

Recommended:

```bash
nohup bash ./scripts/start-server.java.sh >> ./logs/server.java.txt 2>&1 &
```

##### Python

Recommended:

```bash
nohup bash ./scripts/start-server.py.sh >> ./logs/server.py.txt 2>&1 &
```

#### fetch-data

Recommended:

```bash
# the first of every month at 0:00
echo "0 0 1 */1 * __ROOT=\"`realpath .`\"; bash \${__ROOT}/scripts/fetch-data.sh > \"\${__ROOT}/logs/fd-\`date --i\`.txt\" 2>&1" >> logs/auto-task.txt
crontab logs/auto-task.txt
```
