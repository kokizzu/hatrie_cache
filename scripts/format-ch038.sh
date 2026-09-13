#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/bitmap_aggregates.go hat/hatSql/bitmap_aggregate_test.go hat/hatSql/query.go
