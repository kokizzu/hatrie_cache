#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch030_map_subcolumn_test.go hat/hatSql/ch030_map_subcolumn_benchmark_test.go hat/hatSql/columnar_map_scan.go
