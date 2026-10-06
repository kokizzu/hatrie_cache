#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/task_profiler.go \
	hat/hatSql/c235_task_profiler_test.go \
	hat/hatSql/c235_task_profiler_benchmark_test.go \
	hat/hatSql/c235_task_profiler_baseline_benchmark_test.go \
	hat/hatSql/codex_compile_shim.go
