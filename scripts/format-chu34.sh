#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/model.go \
  hat/hatCache/sql.go \
  hat/hatCache/journal.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/chu34_sql_mutation_http_test.go \
  hat/hatCache/chu34_sql_mutation_benchmark_test.go
