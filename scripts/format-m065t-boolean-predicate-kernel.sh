#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/columnar_bool_predicate.go \
  hat/hatSql/columnar_bool_predicate_test.go \
  hat/hatSql/columnar_bool_predicate_benchmark_test.go
