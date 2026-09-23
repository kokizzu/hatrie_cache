#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH005RuntimeJoinPartitionFilter(Baseline)?$' -benchtime=2s -benchmem -count=5
