#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
baseline=$(mktemp -d /tmp/hatrie-cache-m242-benchmark.XXXXXX)

cleanup() {
	git worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$baseline" HEAD >/dev/null
cp "$repo/hat/hatSql/m242_operator_metrics_benchmark_test.go" "$baseline/hat/hatSql/"

printf '%s\n' '== baseline (pre-M242 observer) =='
cd "$baseline"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM242OperatorObserver$' -benchmem -count=5 -benchtime=1s

printf '%s\n' '== current (observer + M242 metrics) =='
cd "$repo"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM242OperatorObserver$|^BenchmarkM242OperatorMetrics$|^BenchmarkM242OperatorMetricsJSON$' -benchmem -count=5 -benchtime=1s
