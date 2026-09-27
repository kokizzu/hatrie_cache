#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/mz037_sql_topk.go \
	hat/hatSql/mz037_sql_topk_test.go \
	hat/hatSql/mz037_sql_topk_benchmark_test.go
