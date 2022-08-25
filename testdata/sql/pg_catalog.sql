SELECT
    attname,
    format_type(atttypid, atttypmod),
    attnum,
    attnotnull,
    atthasdef
FROM pg_class pgc
JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
LEFT JOIN pg_attribute pga ON pga.attrelid = pgc.oid AND pga.attnum > 0 AND NOT pga.attisdropped
WHERE
    pgc.relkind in ('r', 'v', 'm', 'p')
    AND pgn.nspname = 'example'
    AND pgc.relname = 'fuga'
ORDER BY pga.attnum
