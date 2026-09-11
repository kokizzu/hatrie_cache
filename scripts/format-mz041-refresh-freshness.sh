#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/refresh_scheduler.go hat/hatSql/refresh_scheduler_test.go hat/hatSql/refresh_scheduler_freshness_benchmark_test.go
