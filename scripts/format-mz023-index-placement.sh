#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/contracts.go \
	hat/hatSql/index_hint.go \
	hat/hatSql/index_strategy.go \
	hat/hatSql/index_usage.go \
	hat/hatSql/mz023_index_placement_test.go \
	hat/hatSql/mz023_index_placement_benchmark_test.go \
	hat/hatSql/query.go
