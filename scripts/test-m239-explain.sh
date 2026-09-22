#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
	go test ./hat/hatSql -run '^TestM239Explain' -count=1
	;;
package)
	go test ./hat/hatSql -count=1
	;;
race)
	go test -race ./hat/hatSql -run '^TestM239Explain' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
regression)
	go test ./hat/hatSql -run '(^TestM239|Explain|MZ007|MZ050)' -count=1
	;;
docs)
	test -s M239_EXPLAIN_FRONTIER.md
	rg -q 'M239_EXPLAIN_FRONTIER.md' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
	rg -q 'LOGICAL_TIMESTAMP' M239_EXPLAIN_FRONTIER.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md
	rg -q 'FRONTIER_REQUIREMENT' M239_EXPLAIN_FRONTIER.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md
	;;
format)
	gofmt -w hat/hatSql/m239_explain_frontier.go hat/hatSql/m239_explain_frontier_test.go
	;;
*)
	echo "unknown M239 test mode: $mode" >&2
	exit 2
	;;
esac
