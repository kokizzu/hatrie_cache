#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/typed_table_columnar_order.go hat/hatSql/round14_columnar_int64_order_test.go
