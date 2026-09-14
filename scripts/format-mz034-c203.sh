#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/differential_rows.go \
  hat/hatSql/differential_operators.go \
  hat/hatSql/differential_difference.go \
  hat/hatSql/differential_intersect.go \
  hat/hatSql/differential_count_sum.go \
  hat/hatSql/differential_group_by.go \
  hat/hatSql/differential_sum.go \
  hat/hatSql/differential_average.go \
  hat/hatSql/differential_min_max.go \
  hat/hatSql/m034_generic_negative_diff_example_test.go
