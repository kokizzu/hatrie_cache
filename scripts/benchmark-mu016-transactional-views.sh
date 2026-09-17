#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU016' -benchmem -benchtime=500ms -count=5
