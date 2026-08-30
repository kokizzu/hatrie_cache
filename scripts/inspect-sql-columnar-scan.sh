#!/bin/sh
set -eu

rg -n -A 300 -B 20 '^func executeSQLColumnarScan' hat/hatSql/query.go
rg -n -A 120 -B 10 '^func sqlColumnar(Vector|Numeric|Dictionary)|^func sqlColumnarScanFields' hat/hatSql/query.go
rg -n 'ColumnarBatch|executeSQLColumnarScan|COLUMNAR SCAN|columnar scan' hat/hatSql/*_test.go
