#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatCache/tt024_cross_field_benchmark_test.go \
  hat/hatCache/sql_text_phrase.go
