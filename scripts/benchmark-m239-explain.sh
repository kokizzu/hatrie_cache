#!/usr/bin/env bash
set -euo pipefail

root="$(pwd)"
baseline="/tmp/hatrie-cache-m239-baseline-$$"
cleanup() {
	git -C "$root" worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$baseline" HEAD >/dev/null
cp "$root/hat/hatSql/m239_explain_frontier_test.go" "$baseline/hat/hatSql/m239_explain_frontier_test.go"

printf '%s\n' 'M239 benchmark baseline (parent, feature test copied for comparable workload):'
(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkM239ExplainTemporalQuery$|^BenchmarkM238ExplainQuery$' -benchtime=500ms -benchmem -count=5)
printf '%s\n' 'M239 benchmark current:'
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM239ExplainTemporalQuery$|^BenchmarkM238ExplainQuery$' -benchtime=500ms -benchmem -count=5
