#!/usr/bin/env sh
set -eu

mode=${1:-}
case "$mode" in
stage)
	git diff --check
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md Makefile Makefile.codex-chu51 README.md SQL_QUERY_LOG.md hat/hatCache/chu51_query_log_flush_test.go hat/hatCache/monitoring.go hat/hatCache/monitoring_sql_query_log.go scripts/deliver-chu51.sh scripts/test-chu51.sh
	;;
commit)
	git diff --cached --check
	git commit -m 'feat(monitoring): add query log flush barrier [skip ci]'
	;;
push)
	git push origin codex/chu51-query-log-flush-20261005
	;;
*)
	printf 'unknown deliver-chu51 mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
