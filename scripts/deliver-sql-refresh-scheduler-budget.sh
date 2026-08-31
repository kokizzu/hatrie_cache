#!/bin/sh
set -eu

paths='Makefile README.md REFRESH_SCHEDULER.md hat/hatSql/refresh_scheduler.go hat/hatSql/refresh_scheduler_budget_test.go hat/hatSql/refresh_scheduler_budget_benchmark_test.go scripts/test-sql-refresh-scheduler-budget.sh scripts/benchmark-sql-refresh-scheduler-budget.sh scripts/format-sql-refresh-scheduler-budget.sh scripts/verify-sql-refresh-scheduler-budget.sh scripts/test-race-sql-refresh-scheduler-budget.sh scripts/security-sql-refresh-scheduler-budget.sh scripts/inspect-sql-refresh-scheduler-budget.sh scripts/deliver-sql-refresh-scheduler-budget.sh'

case "${1:-}" in
commit)
	git diff --check -- $paths
	git add -- $paths
	git commit -m 'bound managed SQL refresh scheduler cycles' -- $paths
	;;
push)
	git push
	;;
*)
	echo "usage: $0 {commit|push}" >&2
	exit 2
	;;
esac
