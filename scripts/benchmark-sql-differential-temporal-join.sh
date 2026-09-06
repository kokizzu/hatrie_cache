#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialTemporalJoin$' -benchmem -count=5
