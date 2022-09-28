CREATE SCHEMA IF NOT EXISTS "psqlfront";
CREATE TABLE IF NOT EXISTS "psqlfront"."stats" (
    hostname VARCHAR(255),
    pid BIGINT,
    uptime BIGINT,
    "time" TIMESTAMP,
    "version" VARCHAR(255),
    curr_connections BIGINT,
    total_connections BIGINT,
    queries BIGINT,
    cache_hits BIGINT,
    cache_misses BIGINT,
    memory_alloc BIGINT
);

CREATE INDEX IF NOT EXISTS uptime_idx ON "psqlfront"."stats" (uptime desc, hostname, pid);
CREATE INDEX IF NOT EXISTS time_idx ON "psqlfront"."stats" ("time" desc, hostname, pid);

CREATE TABLE IF NOT EXISTS "psqlfront"."cache" (
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    origin_id VARCHAR(255) NOT NULL,
    cached_at TIMESTAMP NOT NULL,
    expired_at TIMESTAMP NOT NULL,
    PRIMARY KEY(schema_name,table_name)
);
