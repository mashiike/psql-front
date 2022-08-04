# psql-front

[![Documentation](https://godoc.org/github.com/mashiike/psql-front?status.svg)](https://godoc.org/github.com/mashiike/psql-front)
![Latest GitHub release](https://img.shields.io/github/release/mashiike/psql-front.svg)
![Github Actions test](https://github.com/mashiike/psql-front/workflows/Test/badge.svg?branch=main)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/psql-front/blob/master/LICENSE)

**NOTICE: Currently psql-front is an experimental application**

psql-front is a cache server that implements the PostgreSQL wire protocol v3.
It temporarily stores data retrieved from a configured Origin in a PostgreSQL cache database so that PostgresSQL clients can reference the data with SQL.

## Usage 

For example, if you set up
config.yaml

```yaml
required_version: ">=v0.0.0"

cache_database:
  host: "{{ env `POSTGRES_HOST` `localhost` }}"
  username: "{{ env `PSOTGRES_USER` `postgres` }}"
  password: "{{ env `PSOTGRES_PASSWORD` `postgres` }}"
  port: 5432
  database: "postgres"

default_ttl: 24h

certificates:
  - cert: server.crt
    key: server.key

origins:
  - id: open_data
    type: HTTP
    schema: public
    tables:
      - name: syukujitsu
        url: https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv
        format: csv
        ignore_lines: 1
        text_encoding: Shift_JIS
        columns:
          - name: ymd
            data_type: DATE
            constraint: NOT NULL
          - name: name
            data_type: VARCHAR
            length: 64
            constraint: NOT NULL
```

You can try to set up an example using this config.yaml by using docker-compose.example.yaml.

```shell
$ docker compose -f docker-compose.example.yaml up
[+] Running 3/3
 ⠿ Network psql-front_app             Created 0.1s
 ⠿ Container psql-front-postgres-1    Created 0.1s
 ⠿ Container psql-front-psql-front-1  Created 0.1s
```

Access with psql client in this state.
```shell 
$ psql -h localhost -U postgres -p 5434
Password for user postgres: 
psql (14.2, server 14.4)
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_128_GCM_SHA256, bits: 128, compression: off)
Type "help" for help.

postgres=# select * from public.syukujitsu;
postgres=# select * from public.syukujitsu LIMIT 1;
NOTICE:  cache hit: ["public"."syukujitsu"]
    ymd     | name 
------------+------
 1955-01-01 | 元日
(1 row)

postgres=# 
```

### Install 

#### Binary packages

[Releases](https://github.com/mashiike/psql-front/releases)

#### Docker

[GitHub Packages](https://github.com/users/mashiike/packages/container/package/psql-front)

```console
$ docker pull ghcr.io/mashiike/psql-front:latest
```

### Options

```shell
$ psql-front -h    
Usage of psql-front:
Version: v0.0.0
  -config string
        psql-front config
  -log-level string
        log level (default "info")
  -port uint
        psql-front port (default 5434)
```

### Monitoring

You can configure settings related to Stats monitoring in the configuration file.

```yaml
required_version: ">= v0.1.0"

cache_database:
  host: "localhost"
  username: "postgres"
  password: "{{ env `PSOTGRES_DB_PASSWORD` `postgres` }}"
  port: 5432
  database: "postgres"
  ssl_mode: "{{ env `PSOTGRES_SSLMODE` `prefer` }}"


stats:
  enable: true
  monitoring_interval: 30s # Interval at which stats are collected
  store_database: true # If true, output to psqlfront.stats in the cache database.
```

If `stats.store_database` is true, the cache database can be accessed and aggregated.

```shell
$ psql -h localhost -U postgres -p 5432
Password for user postgres: 
psql (14.2, server 14.4)
Type "help" for help.

postgres=# select * from psqlfront.stats;
   hostname   | pid | uptime |        time         | version | curr_connections | total_connections | queries | cache_hits | cache_misses | memory_alloc 
--------------+-----+--------+---------------------+---------+------------------+-------------------+---------+------------+--------------+--------------
 d11d18ed9af7 |   1 |     30 | 2022-08-04 16:27:54 |  v0.1.0 |               10 |                12 |    4250 |        348 |            4 |      7428576
 d11d18ed9af7 |   1 |     60 | 2022-08-04 16:28:24 |  v0.1.0 |               10 |                12 |   12510 |       1036 |            4 |      9275328
 d11d18ed9af7 |   1 |     90 | 2022-08-04 16:28:54 |  v0.1.0 |               10 |                12 |   20679 |       1719 |            4 |      8571120
 d11d18ed9af7 |   1 |    120 | 2022-08-04 16:29:24 |  v0.1.0 |               10 |                12 |   27544 |       2289 |            4 |      6078544
 d11d18ed9af7 |   1 |    150 | 2022-08-04 16:29:54 |  v0.1.0 |               10 |                12 |   34749 |       2890 |            4 |      6225504
 d11d18ed9af7 |   1 |    180 | 2022-08-04 16:30:23 |  v0.1.0 |               10 |                12 |   41841 |       3481 |            4 |      6345872
 d11d18ed9af7 |   1 |    210 | 2022-08-04 16:30:53 |  v0.1.0 |               10 |                12 |   50436 |       4197 |            4 |      7655392
 750020e74b36 |   1 |     30 | 2022-08-04 16:41:42 |  v0.1.0 |                0 |                 0 |       0 |          0 |            0 |      4993256
```

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
