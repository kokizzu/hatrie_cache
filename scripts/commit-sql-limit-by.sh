#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
mode=${1:-check}

stage_feature() {
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md README.md SQL.md SQL_LIMIT_BY.md
	git add hat/hatSql/collation.go hat/hatSql/limit_by.go hat/hatSql/limit_by_test.go hat/hatSql/query.go hat/hatSql/subquery.go
	git add scripts/benchmark-sql-limit-by.sh scripts/format-sql-limit-by.sh scripts/test-sql-limit-by.sh scripts/commit-sql-limit-by.sh
	patch_file=$(mktemp)
	trap 'rm -f "$patch_file"' EXIT HUP INT TERM
	tab=$(printf '\t')
	{
		printf '%s\n' \
			'diff --git a/Makefile b/Makefile' \
			'--- a/Makefile' \
			'+++ b/Makefile' \
			'@@ -691,5 +691,26 @@' \
			' test-sql-plan-guards:'
		printf ' %ssh ./scripts/test-sql-plan-guards.sh\n' "$tab"
		printf '%s\n' ' '
		printf '%s\n' \
			'+test-sql-limit-by:'
		printf '+%ssh ./scripts/test-sql-limit-by.sh\n' "$tab"
		printf '%s\n' '+' \
			'+format-sql-limit-by:'
		printf '+%ssh ./scripts/format-sql-limit-by.sh\n' "$tab"
		printf '%s\n' '+' \
			'+benchmark-sql-limit-by:'
		printf '+%ssh ./scripts/benchmark-sql-limit-by.sh\n' "$tab"
		printf '%s\n' '+' \
			'+benchmark-sql-limit-by-all:'
		printf '+%sLIMIT_BY_BENCH_MODE=all sh ./scripts/benchmark-sql-limit-by.sh\n' "$tab"
		printf '%s\n' '+' \
			'+stage-sql-limit-by:'
		printf '+%ssh ./scripts/commit-sql-limit-by.sh stage\n' "$tab"
		printf '%s\n' '+' \
			'+commit-sql-limit-by:'
		printf '+%ssh ./scripts/commit-sql-limit-by.sh commit\n' "$tab"
		printf '%s\n' '+' \
			'+push-sql-limit-by:'
		printf '+%ssh ./scripts/commit-sql-limit-by.sh push\n' "$tab"
		printf '%s\n' '+' \
			' test-sql-table-functions:'
		printf ' %ssh ./scripts/test-sql-table-functions.sh\n' "$tab"
	} > "$patch_file"
	git apply --cached "$patch_file"
}

case "$mode" in
stage)
	stage_feature
	git diff --cached --check
	git diff --cached --name-status
	git diff --cached --stat
	;;
check)
	git diff --check -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md README.md SQL.md SQL_LIMIT_BY.md hat/hatSql/collation.go hat/hatSql/limit_by.go hat/hatSql/limit_by_test.go hat/hatSql/query.go hat/hatSql/subquery.go scripts/benchmark-sql-limit-by.sh scripts/format-sql-limit-by.sh scripts/test-sql-limit-by.sh scripts/commit-sql-limit-by.sh
	;;
commit)
	if git diff --cached --quiet; then
		stage_feature
	fi
	git diff --cached --check
	git diff --cached --stat
	git commit -m "feat(sql): add per-group limit by"
	;;
push)
	git push origin HEAD
	;;
*)
	echo "usage: $0 check|stage|commit|push" >&2
	exit 2
	;;
esac
