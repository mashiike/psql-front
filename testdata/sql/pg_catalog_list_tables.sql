SELECT
    relkind,
    nspname,
    relname
FROM pg_class pgc
JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
WHERE pgc.relkind in ('r', 'v', 'm', 'p')
