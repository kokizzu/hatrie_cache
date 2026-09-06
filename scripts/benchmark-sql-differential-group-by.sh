#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkGroupCountDifferentialRows$' -benchmem -count=5
