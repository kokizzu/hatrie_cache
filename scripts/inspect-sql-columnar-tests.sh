#!/usr/bin/env sh
set -eu

sed -n '1,280p' hat/hatCache/sql_columnar_scan_test.go
sed -n '1,260p' hat/hatCache/sql_columnar_scan_benchmark_test.go
