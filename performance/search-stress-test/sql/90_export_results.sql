-- Streams every result row for :run_id to stdout as CSV via server-side COPY (no
-- server-filesystem access needed - it goes over the same connection). run.sh
-- redirects this script's stdout to the actual output file; nothing else in this
-- file should print to stdout.
--
-- (Deliberately not using \copy: psql's \copy meta-command has its own, more
-- primitive argument tokenizer that does not reliably support :'variable'
-- substitution inside a parenthesized query - it works fine in ordinary SQL
-- statements read via -f, which is what this uses instead.)

\set ON_ERROR_STOP on
\i _defaults.sql

COPY (
    SELECT * FROM perf.search_perf_results
    WHERE run_id = :'run_id'
    ORDER BY scenario, sort_column, sort_order, offset_value, id
) TO STDOUT WITH CSV HEADER
