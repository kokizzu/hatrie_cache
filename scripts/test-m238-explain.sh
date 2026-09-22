#!/usr/bin/env bash
set -euo pipefail

case "${1:-test}" in
  test)
    go test ./hat/hatSql -run '^TestM238ExplainReportsFilterPushdownAndArrangementReuse$' -count=1
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  race)
    go test -race ./hat/hatSql -run '^TestM238ExplainReportsFilterPushdownAndArrangementReuse$' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  regression)
    go test ./hat/hatSql -run '(^TestM238|Explain|MZ024|MZ045)' -count=1
    ;;
  docs)
    test -s M238_EXPLAIN_PUSHDOWN.md
    rg -q 'M238|FILTER_PUSHDOWN|Arrangements.*Reused|29,125' M238_EXPLAIN_PUSHDOWN.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION_ROUND2.md
    ;;
  format)
    gofmt -w hat/hatSql/m238_explain_pushdown_test.go hat/hatSql/mu012_arrangement_explain.go hat/hatSql/query.go
    ;;
  *)
    echo "usage: $0 {test|package|race|vet|regression|docs|format}" >&2
    exit 2
    ;;
esac
