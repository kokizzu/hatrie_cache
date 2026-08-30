#!/bin/sh
set -eu

files='
Makefile
README.md
BENCHMARK.md
INDEX_PROPOSAL.md
hat/hatCache/main.go
hat/hatCache/monitoring.go
hat/hatCache/sql_query.go
hat/hatCache/sql_typed_composite_test.go
hat/hatCache/sql_typed_composite_benchmark_test.go
hat/hatSql/contracts.go
hat/hatSql/query.go
scripts/benchmark-sql-typed-composite.sh
scripts/deliver-sql-typed-composite.sh
scripts/format-sql-typed-composite.sh
scripts/inspect-sql-typed-composite-benchmark.sh
scripts/inspect-sql-typed-composite-docs.sh
scripts/inspect-typed-composite-planner.sh
scripts/status-sql-typed-composite.sh
scripts/test-sql-typed-composite.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'feat(sql): add typed composite range indexes'
git push
