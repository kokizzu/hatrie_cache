#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH046(Legacy|Adaptive|HuffmanOnly)ColumnarStream$' -benchmem -count=5
