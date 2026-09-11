#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/time_zone.go hat/hatSql/query.go hat/hatSql/rewrite.go hat/hatSql/mz009_temporal_validity_test.go hat/hatSql/mz009_temporal_validity_benchmark_test.go
