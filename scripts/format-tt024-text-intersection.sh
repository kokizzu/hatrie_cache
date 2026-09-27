#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/text_proximity.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/sql_text_phrase.go \
  hat/hatCache/sql_text_phrase_test.go \
  hat/hatCache/tt024_text_intersection_benchmark_test.go \
  hat/hatSchema/text_index_resolver.go
