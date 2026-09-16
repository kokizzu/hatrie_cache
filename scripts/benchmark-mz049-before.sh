#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-mz049-before.XXXXXX)

cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$worktree" HEAD >/dev/null
cp "$repo_root/hat/hatSql/mz049_schema_drift_baseline_benchmark_test.go" "$worktree/hat/hatSql/mz049_schema_drift_baseline_benchmark_test.go"
cd "$worktree"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ049SchemaDrift.*Baseline$' -benchmem -benchtime=200ms -count=5
