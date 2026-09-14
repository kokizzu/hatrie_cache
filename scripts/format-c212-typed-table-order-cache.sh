#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/typed_table.go hat/hatSql/typed_table_columnar_order.go hat/hatSql/c212_typed_table_order_test.go hat/hatSql/c212_typed_table_order_benchmark_test.go
