-- Shared default values, \i'd from the top of every other script in this folder so
-- each one still runs standalone (`psql -f sql/10_bench_root.sql`) with sane
-- defaults, while still being overridable via `psql -v name=value`.

\if :{?run_id}
\else
  \set run_id manual
\endif
\if :{?function_label}
\else
  \set function_label unlabeled
\endif
\if :{?bucket_id}
\else
  \set bucket_id perf-stress-test
\endif
\if :{?bench_limit}
\else
  \set bench_limit 100
\endif
\if :{?sort_columns}
\else
  \set sort_columns 'ARRAY[''name'', ''created_at'']'
\endif
