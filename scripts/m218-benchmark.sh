#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM218MaterializedViewPlanner$' -benchmem -count=1
