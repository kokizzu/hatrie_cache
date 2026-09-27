#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/text_proximity.go hat/hatSql/query.go hat/hatSql/text_phrase_test.go hat/hatSql/tt024_mixed_boolean_benchmark_test.go
