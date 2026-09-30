#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/columnar_null_predicate.go \
	 hat/hatSql/chu61_nullable_predicate_test.go \
	 hat/hatSql/chu61_nullable_predicate_benchmark_test.go \
	 hat/hatSql/round20_build_compat.go
