#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/text_phrase_test.go \
  hat/hatSql/text_proximity.go \
  hat/hatCache/sql_text_phrase.go \
  hat/hatCache/sql_text_phrase_benchmark_test.go \
  hat/hatCache/sql_text_phrase_test.go \
  hat/hatSchema/text_index.go \
  hat/hatSchema/text_index_resolver.go \
  hat/hatSchema/tt024_text_proximity_index_test.go
