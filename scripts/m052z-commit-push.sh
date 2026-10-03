#!/usr/bin/env bash
set -euo pipefail

usage() {
	printf 'usage: %s status|commit-push\n' "$0" >&2
}

case "${1:-status}" in
status)
	git status --short
	git diff --check
	git diff --stat
	;;
commit-push)
	git diff --check
	git add \
		BENCHMARK.md \
		INSPIRATION.md \
		M052Z_AUTO_NATIVE_UNBOUNDED_ORDER.md \
		Makefile \
		hat/hatSql/m052c_native_dataflow.go \
		hat/hatSql/m052p_auto_native_dataflow.go \
		hat/hatSql/m052r_auto_native_ordered_test.go \
		hat/hatSql/m052z_auto_native_unbounded_order_test.go \
		hat/hatSql/query.go \
		scripts/m052z-commit-push.sh \
		scripts/m052z-unbounded-order.sh
	git diff --cached --check
	git commit -m 'feat(sql): auto-select unbounded native ordering [skip ci]'
	git push -u origin HEAD
	;;
*)
	usage
	exit 2
	;;
esac
