#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
	go test ./hat/hatSql -run '^TestM240' -count=1
	;;
package)
	go test ./hat/hatSql -count=1
	;;
race)
	go test -race ./hat/hatSql -run '^TestM240' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
regression)
	go test ./hat/hatSql -run '(^TestM240|ExplainDataflow|MZ050)' -count=1
	;;
docs)
	test -s M240_EXPLAIN_DATAFLOW.md
	rg -q 'M240_EXPLAIN_DATAFLOW.md' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
	rg -q 'Exchanges' M240_EXPLAIN_DATAFLOW.md ADOPTED_QUERY_ENGINE_IDEAS.md
	rg -q 'stage-aware' M240_EXPLAIN_DATAFLOW.md BENCHMARK.md
	;;
format)
	gofmt -w hat/hatSql/explain_dataflow.go hat/hatSql/m240_dataflow_exchange_test.go hat/hatSql/m240_dataflow_exchange_benchmark_test.go
	;;
*)
	echo "unknown M240 test mode: $mode" >&2
	exit 2
	;;
esac
