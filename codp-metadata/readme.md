
## Debug crawler
``` shell
python -m crawler --debug --province xxx --city xxx
```

## Run crawler
``` shell
python -m crawler --all --workers ?? \
--metadata-output OUTDIR
```

## Write to database
``` shell
python -m db-writer --db-host DB_ADDR \
--db-port DB_PORT \
--db-user DB_USER \
--db-pswd DB_PSWD \
--database DATABASE_NAME \
--table TABLE_NAME \
--metadata-path METADATA_SAVING_PATH
```

## Data download-debugging
It is recommended to limit the amount of data crawled by the crawler to shorten the test time.
``` shell
python -m crawler --debug --download-files --province xxx --city xxx
```

# Notes

1. Python 3.6 and below are required to run the crawler.
Refer to [CSDN: SSL: SSLV3_ALERT_HANDSHAKE_FAILURE](https://blog.csdn.net/qq_37435462/article/details/121564961).

2. If you want to use Black Formatter in VS Code, you can temporarily switch to Python 3.7 or above.