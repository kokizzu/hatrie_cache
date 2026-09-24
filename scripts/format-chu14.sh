#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/chu14_spill_runtime_filter_test.go hat/hatSql/chu14_spill_runtime_filter_benchmark_test.go
