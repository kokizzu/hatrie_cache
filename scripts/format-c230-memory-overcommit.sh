#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch230_memory_overcommit.go \
  hat/hatSql/ch230_memory_overcommit_test.go \
  hat/hatSql/ch230_memory_overcommit_benchmark_test.go \
  hat/hatSql/query.go
