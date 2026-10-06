#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
paths=(
	BENCHMARK.md
	Makefile
	hat/hatSql/m052p_auto_native_dataflow.go
	hat/hatSql/m052z_native_join.go
	hat/hatSql/m052z_native_join_test.go
	hat/hatSql/m052z_native_join_benchmark_test.go
	scripts/benchmark-m052z.sh
	scripts/race-m052z.sh
	scripts/status-m052z.sh
	scripts/test-m052z.sh
)
git status --short --untracked-files=all -- "${paths[@]}"
git diff --check -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
