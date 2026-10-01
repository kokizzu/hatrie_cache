#!/bin/sh
set -eu

mode=${1:-status}

case "$mode" in
check)
	git diff --check HEAD
	;;
status)
	git status --short --branch
	;;
stage)
	git add -- \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		PRODUCT_IDEA_GAPS.md \
		README.md \
		TU09_SNAPSHOT_WAL_JOIN_BOOTSTRAP.md \
		hat/hatReplication/tr009_snapshot_wal_bootstrap.go \
		hat/hatReplication/tr009_snapshot_wal_bootstrap_test.go \
		Makefile \
		scripts/deliver-chg08-bootstrap.sh \
		scripts/test-chg08-bootstrap.sh
	;;
commit)
	git commit -m 'feat(replication): add snapshot WAL join bootstrap [skip ci]'
	;;
push)
	git push -u origin codex/chg08-cursor-pagination
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
