#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH030(ExecutionSteps(Default|Bounded)|Query(Default|Bounded))$' -benchtime=500ms -count=5 -benchmem
