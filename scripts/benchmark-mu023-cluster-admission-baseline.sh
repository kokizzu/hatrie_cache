#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatSql/mu023_cluster_admission_baseline_benchmark_test.go -run '^$' -bench '^BenchmarkMU023BeforeDirectExecution$' -benchmem -benchtime=500ms -count=5
