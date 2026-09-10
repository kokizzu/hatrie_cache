#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/sql_snapshot_provider.go \
	hat/hatSql/sql_snapshot_provider_test.go \
hat/hatSql/sql_snapshot_provider_benchmark_test.go
