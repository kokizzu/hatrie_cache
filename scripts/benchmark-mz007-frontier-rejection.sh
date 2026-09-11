#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSourceFrontierRequirement(Baseline|Disabled|Enabled)$' -benchmem -count=5
