#!/usr/bin/env sh
set -eu

test -s SQL_IMPROVEMENTS_100.md
rg -q '^# SQL Improvement Backlog: 100 Measured Candidates$' SQL_IMPROVEMENTS_100.md
rg -q '^100\. `P2` Add per-operator CPU, allocations, peak memory, spill bytes, and rows to `EXPLAIN ANALYZE`\.$' SQL_IMPROVEMENTS_100.md
git diff --check -- SQL_IMPROVEMENTS_100.md scripts/audit-sql-improvements.sh scripts/inspect-sql-source-ownership.sh scripts/review-sql-improvements-100.sh scripts/commit-sql-improvements-100.sh Makefile
git diff -- Makefile
go test ./hat/hatSql ./hat/hatCache
