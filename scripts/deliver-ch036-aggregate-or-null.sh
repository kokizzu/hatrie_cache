#!/bin/sh
set -eu

mode=${1:-}
case "$mode" in
stage)
	git add -- AGGREGATE_COMBINATORS.md BENCHMARK.md INSPIRATION_BACKLOG.md Makefile hat/hatSql/query.go hat/hatSql/ch036_aggregate_or_null.go hat/hatSql/ch036_aggregate_or_null_benchmark_test.go hat/hatSql/ch036_aggregate_or_null_test.go scripts/benchmark-ch036-aggregate-or-null-before.sh scripts/benchmark-ch036-aggregate-or-null.sh scripts/deliver-ch036-aggregate-or-null.sh scripts/format-ch036-aggregate-or-null.sh scripts/race-ch036-aggregate-or-null.sh scripts/review-ch036-aggregate-or-null.sh scripts/test-ch036-aggregate-or-null-package.sh scripts/test-ch036-aggregate-or-null.sh scripts/vet-ch036-aggregate-or-null.sh
	;;
commit)
	git commit -m 'feat: add SQL OrNull aggregate combinators [skip ci]'
	;;
push)
	git push origin HEAD:master
	;;
verify)
	git diff --cached --check
	git status --short
	git log -1 --oneline
	;;
*)
	echo "usage: $0 {stage|commit|push|verify}" >&2
	exit 2
	;;
esac
