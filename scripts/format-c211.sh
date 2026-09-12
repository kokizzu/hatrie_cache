#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/contracts.go hat/hatSql/catalog.go hat/hatSql/typed_table.go hat/hatSql/query.go hat/hatSql/join_order_stats.go hat/hatSql/c211_join_order_test.go
