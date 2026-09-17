#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=${GOMAXPROCS:-1} go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU019' -benchmem -count=5 -benchtime=500ms
