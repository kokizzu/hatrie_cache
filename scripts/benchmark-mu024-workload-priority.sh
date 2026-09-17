#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU024After(LegacyFIFOSelection|PrioritySelection)$' -benchmem -benchtime=500ms -count=5
