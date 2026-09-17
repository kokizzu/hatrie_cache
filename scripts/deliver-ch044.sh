#!/bin/sh
set -eu

mode=${1:-verify}
case "$mode" in
stage)
	git add \
		BENCHMARK.md \
		CH044_JSON_DYNAMIC_SUBCOLUMNS.md \
		ENGINE_IDEAS.md \
		Makefile \
		README.md \
		hat/hatCache/ch044_json_subcolumn_benchmark_test.go \
		hat/hatCache/ch044_json_subcolumn_test.go \
		hat/hatCache/main.go \
		hat/hatCache/sql_json_subcolumn.go \
		hat/hatSql/ch031_automatic_json_subcolumn.go \
		scripts/benchmark-ch044.sh \
		scripts/deliver-ch044.sh \
		scripts/format-ch044.sh \
		scripts/race-ch044.sh \
		scripts/review-ch044.sh \
		scripts/test-ch044-package.sh \
		scripts/test-ch044.sh \
		scripts/vet-ch044.sh
	git diff --cached --check
	git diff --cached --name-only
	;;
commit)
	git diff --cached --quiet && exit 1
	git commit -m 'feat: integrate bounded dynamic JSON subcolumns [skip ci]'
	;;
push)
	git push origin HEAD:refs/heads/master
	;;
verify)
	git status --short
	git log -1 --oneline
	;;
*)
	printf '%s\n' "usage: $0 {stage|commit|push|verify}" >&2
	exit 2
	;;
esac
