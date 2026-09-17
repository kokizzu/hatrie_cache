#!/usr/bin/env bash
set -euo pipefail

baseline_root=/tmp/hatrie-cache-mu018-baseline
baseline_ref=${BASELINE_REF:-HEAD^}
cleanup() {
	git worktree remove --force "$baseline_root" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
git worktree add --detach "$baseline_root" "$baseline_ref" >/dev/null
cp hat/hatSql/mu018_exactly_once_sink_baseline_benchmark_test.go "$baseline_root/hat/hatSql/"
cd "$baseline_root"
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU018BaselineExistingCommitCoordinator$' -benchmem -benchtime=500ms -count=5
