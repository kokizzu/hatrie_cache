#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
	hat/hatSql/mu028_monotonicity.go \
	hat/hatSql/mu028_monotonicity_test.go \
	hat/hatSql/mu028_monotonicity_benchmark_test.go
