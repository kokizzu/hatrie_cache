#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM219PointLookupBuild$' -benchmem -benchtime=20x -count=5
