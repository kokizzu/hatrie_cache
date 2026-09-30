#!/usr/bin/env bash
set -eu

case "${1:-test}" in
format)
	gofmt -w hat/hatSql/c193_limit_zero_short_circuit_test.go
;;
test)
	go test ./hat/hatSql -run '^TestC193LimitZero'
;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkC193LimitZero$' -benchmem -count=5
;;
race)
	go test -race ./hat/hatSql -run '^TestC193LimitZero'
;;
vet)
	go vet ./hat/hatSql
;;
package)
	go test ./hat/hatSql
;;
status)
	git status --short --branch
	git diff --stat
;;
stage)
	git add BENCHMARK.md C193_LIMIT_ZERO.md INSPIRATION.md Makefile hat/hatSql/c193_limit_zero_short_circuit.go hat/hatSql/c193_limit_zero_short_circuit_test.go hat/hatSql/query.go scripts/run-c193-limit-zero.sh
;;
commit)
	git commit -m 'feat(sql): short-circuit direct LIMIT 0 queries [skip ci]'
;;
push)
	git push origin HEAD
;;
*)
	printf 'usage: %s {format|test|benchmark|race|vet|package|status|stage|commit|push}\n' "$0"
	exit 2
;;
esac
