#!/bin/sh
set -eu

test -f CH028_MAX_THREADS.md
rg -q 'CH028_MAX_THREADS\.md' README.md
rg -q 'CH-028.*SETTINGS max_threads' ENGINE_IDEAS.md
rg -q '## CH-028 Query `max_threads`' BENCHMARK.md
rg -q 'make test-ch028-max-threads' CH028_MAX_THREADS.md
rg -q 'MaxSQLQueryThreads' hat/hatSql/query.go hat/hatCache/sql_query.go
