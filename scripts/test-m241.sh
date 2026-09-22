#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"

case "$mode" in
test)
	go test ./hat/hatSql -run '^TestM241' -count=1
	;;
package)
	go test ./hat/hatSql -count=1
	;;
race)
	go test -race ./hat/hatSql -run '^TestM241' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
format)
	gofmt -w hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/optimizer_rules.go hat/hatSql/m241_optimizer_trace_test.go hat/hatSql/m241_optimizer_trace_benchmark_test.go hat/hatSql/m241_optimizer_trace_current_benchmark_test.go
	;;
docs)
	test -f M241_OPTIMIZER_TRACE.md
	rg -q 'M241_OPTIMIZER_TRACE.md' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
	rg -q 'OptimizerTrace|RejectAlternative|hatrie-cache-sql-optimizer-trace/v1' M241_OPTIMIZER_TRACE.md
	;;
*)
	printf 'usage: %s {test|package|race|vet|format|docs}\n' "$0" >&2
	exit 2
	;;
esac
