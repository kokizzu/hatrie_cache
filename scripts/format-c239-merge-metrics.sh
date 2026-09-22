#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/c239_part_merge_metrics_baseline_benchmark_test.go \
  hat/hatSql/c239_part_merge_metrics_benchmark_test.go \
  hat/hatSql/c239_part_merge_metrics_test.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_part_merge_metrics.go \
  hat/hatSql/typed_table_patch_parts.go
