#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m210_retained_sql_snapshot.go \
	hat/hatSql/m210_retained_sql_snapshot_test.go \
	hat/hatSql/m210_retained_sql_snapshot_baseline_benchmark_test.go \
	hat/hatSql/m210_retained_sql_snapshot_benchmark_test.go
