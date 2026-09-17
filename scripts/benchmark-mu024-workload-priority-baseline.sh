#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatSql/mu024_workload_priority_baseline_benchmark_test.go -run '^$' -bench '^BenchmarkMU024BeforeFIFOSelection$' -benchmem -benchtime=500ms -count=5
