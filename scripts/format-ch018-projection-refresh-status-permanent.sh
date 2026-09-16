#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/ch018_projection_refresh_status.go \
	hat/hatSql/ch018_projection_refresh_status_test.go \
	hat/hatSql/ch018_projection_refresh_status_benchmark_test.go \
	hat/hatSql/incremental_projection.go
