#!/bin/sh
set -eu

files='
Makefile
README.md
BENCHMARK.md
hat/hatSql/query.go
hat/hatSql/execution_single_source_envelope_test.go
hat/hatSql/execution_single_source_envelope_benchmark_test.go
scripts/benchmark-sql-single-source-envelope.sh
scripts/deliver-sql-single-source-envelope.sh
scripts/format-sql-typed-composite.sh
scripts/inspect-sql-single-source-envelope-docs.sh
scripts/inspect-sql-single-source-envelopes.sh
scripts/test-sql-single-source-envelope.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): avoid maps for single-source rows'
git push
