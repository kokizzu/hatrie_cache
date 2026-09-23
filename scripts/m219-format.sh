#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/index_rebuild_queue.go hat/hatSql/materialized.go hat/hatSql/m219_materialized_view_point_build_test.go hat/hatSql/m219_materialized_view_point_build_benchmark_test.go
