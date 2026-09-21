#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH046CurrentColumnarStream$' -benchmem -count=5
