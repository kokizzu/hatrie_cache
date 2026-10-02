#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/sql_query.go \
	hat/hatCache/ch060_bitmap_secondary_test.go \
	hat/hatCache/ch060_bitmap_secondary_benchmark_test.go
