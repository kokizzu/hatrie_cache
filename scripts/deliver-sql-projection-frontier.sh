#!/bin/sh
set -eu

paths='Makefile README.md PROJECTION_FRONTIERS.md hat/hatCache/sql_projection_frontier.go hat/hatCache/sql_projection_frontier_test.go hat/hatCache/sql_projection_frontier_benchmark_test.go scripts/inspect-sql-projection-frontier.sh scripts/test-sql-projection-frontier.sh scripts/format-sql-projection-frontier.sh scripts/verify-sql-projection-frontier.sh scripts/benchmark-sql-projection-frontier.sh scripts/test-race-sql-projection-frontier.sh scripts/security-sql-projection-frontier.sh scripts/deliver-sql-projection-frontier.sh'

case "${1:-}" in
commit)
	git diff --check -- $paths
	git add -- $paths
	git commit -m 'add coordinated SQL projection retention frontiers' -- $paths
	;;
push)
	git push
	;;
*)
	echo "usage: $0 {commit|push}" >&2
	exit 2
	;;
esac
