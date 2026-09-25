#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/materialized.go \
  hat/hatSql/projection_store.go \
  hat/hatSql/session.go \
  hat/hatSql/ch011_projection_persistence_test.go \
  hat/hatSql/ch011_projection_persistence_benchmark_test.go
