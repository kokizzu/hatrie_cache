#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/ch004_final.go \
	hat/hatSql/ch004_final_test.go \
	hat/hatSql/ch004_final_benchmark_test.go \
	hat/hatSql/query.go
