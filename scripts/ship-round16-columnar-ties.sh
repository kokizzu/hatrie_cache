#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
commit_message='perf(sql): support columnar limit with ties [skip ci]'
paths=(
	Makefile
	README.md
	ENGINE_INSPIRATION_GAPS.md
	hat/hatSql/query.go
	hat/hatSql/round16_columnar_limit_with_ties_test.go
	scripts/benchmark-round16-columnar-ties.sh
	scripts/test-round16-columnar-ties.sh
	scripts/test-round16-package.sh
	scripts/verify-round16-catalog.sh
	scripts/verify-round16-columnar-ties.sh
	scripts/ship-round16-columnar-ties.sh
)

case "$mode" in
	status)
		git status --short
		git diff --stat
		git diff --cached --stat
		;;
	stage)
		git add "${paths[@]}"
		git diff --cached --check
		git diff --cached --stat
		;;
	commit)
		git diff --cached --check
		git commit -m "$commit_message"
		;;
	push)
		git push -u origin codex/next-inspiration-round16
		;;
	*)
		printf 'usage: %s {status|stage|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
