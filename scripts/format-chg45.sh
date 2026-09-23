#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/tooling.go \
  hat/hatSql/ch045_explain_estimate_test.go \
  hat/hatSql/ch045_explain_estimate_benchmark_test.go
