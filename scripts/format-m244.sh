#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/m244_compaction_debt_benchmark_test.go \
  hat/hatSql/m244_compaction_debt_test.go
