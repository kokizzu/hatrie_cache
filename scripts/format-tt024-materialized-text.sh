#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/index_advisor.go \
  hat/hatSchema/text_index.go \
  hat/hatSchema/text_index_resolver.go \
  hat/hatSchema/tt024_text_auto_index_test.go \
  hat/hatSchema/tt024_text_auto_index_benchmark_test.go \
  hat/hatSql/query.go
