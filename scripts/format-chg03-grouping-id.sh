#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/grouping_sets.go \
  hat/hatSql/query.go \
  hat/hatSql/grouping_identifier_test.go \
  hat/hatSql/grouping_identifier_benchmark_test.go
