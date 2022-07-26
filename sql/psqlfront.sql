CREATE TABLE "psqlfront"."cache" (
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    origin_id VARCHAR(255) NOT NULL,
    cached_at TIMESTAMP NOT NULL,
    expired_at TIMESTAMP NOT NULL,
    PRIMARY KEY(schema_name,table_name)
);
