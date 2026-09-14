#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/c213_incremental_top_k.go \
	hat/hatSql/c213_incremental_top_k_test.go \
	hat/hatSql/c213_incremental_top_k_benchmark_test.go
