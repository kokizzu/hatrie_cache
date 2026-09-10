#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkSQLDataflowFragmentExecution/(direct|executor)$' \
  -benchmem \
  -benchtime=1s \
  -count=5
