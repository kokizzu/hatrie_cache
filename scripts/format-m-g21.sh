#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/typed_table_arrangement_progress.go hat/hatSql/typed_table_arrangement_hydration_progress_test.go hat/hatSql/typed_table_arrangement_progress_benchmark_test.go
