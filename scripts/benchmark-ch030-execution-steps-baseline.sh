#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -tags ch030baseline -run '^$' -bench '^BenchmarkCH030(ExecutionStepsDefault|QueryDefault)$' -benchtime=500ms -count=5 -benchmem
