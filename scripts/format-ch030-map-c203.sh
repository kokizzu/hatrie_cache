#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_map.go hat/hatSql/columnar_map_scan.go hat/hatSql/ch030_map_subcolumn_test.go hat/hatSql/ch030_map_subcolumn_benchmark_test.go hat/hatSql/contracts.go hat/hatSql/columnar_vertical_merge.go hat/hatSql/json_path.go hat/hatSql/catalog.go hat/hatSql/query.go
