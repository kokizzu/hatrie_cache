#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM217MaterializedView(PointLookup|Refresh)' -benchmem -count=5
