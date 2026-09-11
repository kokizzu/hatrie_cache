#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
hat/hatCache/mz022_index_readiness_benchmark_test.go \
hat/hatCache/mz022_index_readiness_test.go \
hat/hatCache/sql_index_readiness.go
