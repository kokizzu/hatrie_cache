#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLPlannerStatistics' -benchmem -count=5
