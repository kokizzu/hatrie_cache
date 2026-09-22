#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/c230_memory_overcommit_test.go \
  hat/hatSql/c230_memory_overcommit_benchmark_test.go
