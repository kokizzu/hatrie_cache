#!/usr/bin/env bash
set -euo pipefail

paths=(
	BENCHMARK.md
	Makefile
	hat/hatSql/m052p_auto_native_dataflow.go
	hat/hatSql/m052z_native_join.go
	hat/hatSql/m052z_native_join_benchmark_test.go
	hat/hatSql/m052z_native_join_test.go
	scripts/benchmark-m052z.sh
	scripts/commit-m052z.sh
	scripts/push-m052z.sh
	scripts/race-m052z.sh
	scripts/rename-m052z-branch.sh
	scripts/status-m052z.sh
	scripts/test-m052z.sh
)
git add -- "${paths[@]}"
git diff --cached --check
git commit -m "feat: add automatic native inner hash join [skip ci]"
