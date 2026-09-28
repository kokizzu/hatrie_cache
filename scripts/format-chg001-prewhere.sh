#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/chg001_prewhere_test.go \
  hat/hatSql/chg001_prewhere_benchmark_test.go \
  hat/hatSql/columnar_prewhere.go \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/session.go \
  hat/hatSql/catalog.go
