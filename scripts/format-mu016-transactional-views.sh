#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/session.go \
  hat/hatSql/mu016_transactional_view.go \
  hat/hatSql/mu016_transactional_view_test.go \
  hat/hatSql/mu016_transactional_view_baseline_benchmark_test.go \
  hat/hatSql/mu016_transactional_view_benchmark_test.go
git diff --check
