#!/usr/bin/env bash
set -euo pipefail

root="$(pwd)"
baseline="$(mktemp -d /tmp/hatrie-cache-m241-baseline.XXXXXX)"
trap 'git worktree remove --force "$baseline" >/dev/null 2>&1 || true' EXIT

git worktree add --detach "$baseline" HEAD >/dev/null
cp "$root/hat/hatSql/m241_optimizer_trace_benchmark_test.go" "$baseline/hat/hatSql/m241_optimizer_trace_benchmark_test.go"

printf '%s\n' '--- M240 baseline ---'
(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkM241Optimizer$' -benchtime=1s -benchmem -count=5)

printf '%s\n' '--- M241 current ---'
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM241Optimizer(Trace(JSON)?)?$' -benchtime=1s -benchmem -count=5
