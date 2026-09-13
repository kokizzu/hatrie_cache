#!/usr/bin/env bash
set -euo pipefail

mode="${1:-commit}"
case "$mode" in
status)
	git status --short
	git rev-parse --short HEAD
	;;
commit)
	git add \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		INSPIRATION_BACKLOG.md \
		Makefile \
		README.md \
		SQL_QUERY_LOG.md \
		hat/hatSql/ch004_query_log_baseline_benchmark_test.go \
		hat/hatSql/ch004_query_log_benchmark_test.go \
		hat/hatSql/ch004_query_log_rotation_test.go \
		hat/hatSql/query_log.go \
		hat/hatSql/query_manager.go \
		scripts/audit-inspiration-backlog.sh \
		scripts/commit-ch004.sh \
		scripts/push-ch004.sh \
		scripts/test-ch004.sh
	git commit -m "feat(hatSql): add retained query log rotation"
	;;
*)
	printf 'usage: %s [status|commit]\n' "$0" >&2
	exit 2
	;;
esac
