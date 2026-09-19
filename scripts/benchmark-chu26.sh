#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCH032QueryProfilerRecord|BenchmarkCHU26QueryProfilerRecordMemory)$' -benchmem -count=5
