#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
gofmt -w hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_bitmap_index_test.go
