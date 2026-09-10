#!/usr/bin/env bash
set -euo pipefail

gofmt -d \
  hat/hatSql/sql_source_frontier.go \
  hat/hatSql/sql_source_frontier_test.go \
  hat/hatSql/sql_common_frontier_benchmark_test.go
go test ./hat/hatSql -run '^TestSQLSourceFrontierTracker' -count=1
go test -race ./hat/hatSql -run '^TestSQLSourceFrontierTracker' -count=1
go vet ./hat/hatSql
git diff --check
git status --short
