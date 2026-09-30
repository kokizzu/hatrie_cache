#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_columnar_order.go \
  hat/hatSql/round15_columnar_composite_order_test.go
