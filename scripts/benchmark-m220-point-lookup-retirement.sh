#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM220PointLookupRetirement$' -benchmem -benchtime=10x -count=5
