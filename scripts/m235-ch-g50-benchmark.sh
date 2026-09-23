#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH050(PlanReproducibilityHash|QueryFingerprintBaseline)$' -benchmem -count=5
