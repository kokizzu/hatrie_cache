#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_text_phrase.go \
  hat/hatCache/sql_text_phrase_benchmark_test.go \
  hat/hatCache/sql_text_phrase_test.go \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/rewrite.go \
  hat/hatSql/text_phrase.go \
  hat/hatSql/text_phrase_benchmark_test.go \
  hat/hatSql/text_phrase_test.go \
  hat/hatSql/text_proximity.go
