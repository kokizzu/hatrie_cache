#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTR017IndexedOrderStream$' -benchmem -count=5
