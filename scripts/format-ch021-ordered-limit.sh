#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatCache/ch021_read_in_order_test.go
