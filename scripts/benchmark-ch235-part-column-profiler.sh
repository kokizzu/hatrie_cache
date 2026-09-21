#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCH032QueryProfilerRecordSampled|BenchmarkCH235PartColumnProfilerRecord|BenchmarkCH235PartColumnProfilerSnapshot)$' -benchmem -count=5
