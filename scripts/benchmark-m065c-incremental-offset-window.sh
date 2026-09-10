#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkIncrementalOffsetWindowMaintenance/(lag|lead)_(full_scan|incremental)$' -benchmem -benchtime=200ms -count=5
