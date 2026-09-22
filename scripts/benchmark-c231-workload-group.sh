#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCHU39Admission|BenchmarkC231Admission)' -benchmem -count=5
