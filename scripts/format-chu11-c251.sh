#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/index_advisor.go \
  hat/hatSql/index_advisor_persistence.go \
  hat/hatSql/index_advisor_persistence_test.go \
  hat/hatSql/ch_u11_skip_index_advisor_test.go \
  hat/hatSql/ch_u11_skip_index_advisor_benchmark_test.go
