#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkApplyDifferentialLateDataPolicy$' -benchmem -count=5
