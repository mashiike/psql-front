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
  host: "{{ env `PSOTGRES_DB_HOST` `localhost` }}"
  username: "{{ env `PSOTGRES_DB_USER` `postgres` }}"
  password: "{{ env `PSOTGRES_DB_PASSWORD` `postgres` }}"
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

Start with this config.yaml as follows
```shell
$ docker compose up -d
[+] Running 2/2
 ⠿ Network psql-front_default       Created                 0.1s
 ⠿ Container psql-front-postgres-1  Started                 0.9s

$ psql-front --config config.yaml
2022/08/02 12:28:20 [info] load origin type: HTTP
2022/08/02 12:28:20 [info] load origin type: Static
2022/08/02 12:28:20 [info] use TLS
2022/08/02 12:28:20 [info] need execute migration
-- Apply --
CREATE SCHEMA IF NOT EXISTS "psqlfront";
CREATE SCHEMA IF NOT EXISTS "public";

CREATE TABLE "psqlfront"."cache" (
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    origin_id VARCHAR(255) NOT NULL,
    cached_at TIMESTAMP NOT NULL,
    expired_at TIMESTAMP NOT NULL,
    PRIMARY KEY(schema_name,table_name)
);
CREATE TABLE "public"."syukujitsu" (
    "ymd" DATE ,
    "name" VARCHAR(64) 
);
2022/08/02 12:28:20 [info][-] execute: ANALYZE "public"."syukujitsu";
2022/08/02 12:28:20 [info] PostgreSQL server is up and running at [[::]:5434]
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

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
