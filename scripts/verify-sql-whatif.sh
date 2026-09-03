#!/bin/sh
set -eu

test -f SQL_WHATIF.md
rg -n 'SQL_WHATIF\.md|ExplainSQLWhatIf' README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SQL_WHATIF.md hat/hatSql/whatif.go hat/hatCache/sql_query.go api.go
