#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH038OrNullColumnarGroup(Baseline|Fallback|FastPath)$' -benchmem -count=5
