#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialIntersect/(RebuildSnapshot|Incremental)$' -benchmem -count=5
