#!/usr/bin/env bash
set -euo pipefail

root="$(pwd)"
baseline="/tmp/hatrie-cache-m240-baseline-$$"
cleanup() {
	git -C "$root" worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$baseline" HEAD >/dev/null
cp "$root/hat/hatSql/m240_dataflow_exchange_benchmark_test.go" "$baseline/hat/hatSql/m240_dataflow_exchange_benchmark_test.go"

printf '%s\n' 'M240 benchmark baseline (M239 parent):'
(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkM240ExplainDataflowGraphExchange$|^BenchmarkM240MarshalExplainDataflowJSONExchange$|^BenchmarkBuildExplainDataflowGraph$|^BenchmarkExplainDataflowDOT$|^BenchmarkExplainDOTLinear$' -benchtime=1s -benchmem -count=5)
printf '%s\n' 'M240 benchmark current:'
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM240ExplainDataflowGraphExchange$|^BenchmarkM240MarshalExplainDataflowJSONExchange$|^BenchmarkBuildExplainDataflowGraph$|^BenchmarkExplainDataflowDOT$|^BenchmarkExplainDOTLinear$' -benchtime=1s -benchmem -count=5
