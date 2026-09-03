#!/bin/sh
set -eu

mode=${1:-check}
feature_files='ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
KEYSET_PAGINATION.md
README.md
hat/hatCache/monitoring.go
hat/hatCache/sql_keyset_monitoring_test.go
hat/hatCache/sql_keyset_pagination_benchmark_test.go
hat/hatCache/sql_keyset_pagination_test.go
hat/hatCache/sql_query.go
hat/hatSql/client.go
hat/hatSql/contracts.go
hat/hatSql/keyset.go
hat/hatSql/keyset_client_test.go
hat/hatSql/keyset_pagination_test.go
hat/hatSql/model.go
hat/hatSql/query.go
scripts/benchmark-sql-keyset-hattrie.sh
scripts/benchmark-sql-keyset.sh
scripts/deliver-sql-keyset.sh
scripts/format-sql-keyset.sh
scripts/test-sql-keyset-hattrie.sh
scripts/test-sql-keyset.sh'

is_allowed() {
	case "$1" in
		Makefile|api.go|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|KEYSET_PAGINATION.md|README.md|hat/hatCache/monitoring.go|hat/hatCache/sql_keyset_monitoring_test.go|hat/hatCache/sql_keyset_pagination_benchmark_test.go|hat/hatCache/sql_keyset_pagination_test.go|hat/hatCache/sql_query.go|hat/hatSql/client.go|hat/hatSql/contracts.go|hat/hatSql/keyset.go|hat/hatSql/keyset_client_test.go|hat/hatSql/keyset_pagination_test.go|hat/hatSql/model.go|hat/hatSql/query.go|scripts/benchmark-sql-keyset-hattrie.sh|scripts/benchmark-sql-keyset.sh|scripts/deliver-sql-keyset.sh|scripts/format-sql-keyset.sh|scripts/test-sql-keyset-hattrie.sh|scripts/test-sql-keyset.sh)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

check_staged() {
	staged=$(git diff --cached --name-only)
	for path in $staged; do
		if ! is_allowed "$path"; then
			printf 'refusing SQL keyset delivery with unrelated staged path: %s\n' "$path" >&2
			exit 1
		fi
	done
}

stage_append() {
	file=$1
	text=$2
	base=$(mktemp)
	desired=$(mktemp)
	staged=$(mktemp)
	patch=$(mktemp)
	git show "HEAD:$file" > "$base"
	cp "$base" "$desired"
	printf '%b' "$text" >> "$desired"
	if git show ":$file" > "$staged" 2>/dev/null; then
		if cmp -s "$staged" "$desired"; then
			rm -f "$base" "$desired" "$staged" "$patch"
			return 0
		fi
		if ! cmp -s "$staged" "$base"; then
			printf 'refusing to replace unexpected staged content in %s\n' "$file" >&2
			rm -f "$base" "$desired" "$staged" "$patch"
			exit 1
		fi
	fi
	diff_status=0
	diff -u --label "a/$file" --label "b/$file" "$base" "$desired" > "$patch" || diff_status=$?
	if [ "$diff_status" -ne 0 ] && [ "$diff_status" -ne 1 ]; then
		rm -f "$base" "$desired" "$staged" "$patch"
		exit "$diff_status"
	fi
	if [ "$diff_status" -eq 1 ]; then
		git apply --cached --recount "$patch"
	fi
	rm -f "$base" "$desired" "$staged" "$patch"
}

stage_feature() {
	git diff --check
	check_staged
	for path in $feature_files; do
		git add -- "$path"
	done
	stage_append Makefile '\n\nformat-sql-keyset:\n\tsh ./scripts/format-sql-keyset.sh\n\ntest-sql-keyset:\n\tsh ./scripts/test-sql-keyset.sh\n\nbenchmark-sql-keyset:\n\tsh ./scripts/benchmark-sql-keyset.sh\n\ntest-sql-keyset-hattrie:\n\tsh ./scripts/test-sql-keyset-hattrie.sh\n\nbenchmark-sql-keyset-hattrie:\n\tsh ./scripts/benchmark-sql-keyset-hattrie.sh\n\ndeliver-sql-keyset:\n\tsh ./scripts/deliver-sql-keyset.sh apply\n\ncommit-sql-keyset:\n\tsh ./scripts/deliver-sql-keyset.sh commit\n\npush-sql-keyset:\n\tsh ./scripts/deliver-sql-keyset.sh push\n'
	stage_append api.go '\n\ntype SQLKeysetOrderedStreamSourceResolver = core.SQLKeysetOrderedStreamSourceResolver\ntype SQLKeysetPosition = core.SQLKeysetPosition\n\nvar ExecuteQueryKeysetPage = core.ExecuteQueryKeysetPage\nvar ExecuteSQLQueryKeysetPage = core.ExecuteSQLQueryKeysetPage\n'
	git diff --cached --check
	check_staged
}

case "$mode" in
	apply)
		stage_feature
		;;
	check)
		git diff --check
		check_staged
		git diff --cached --check
		;;
	commit)
		check_staged
		git diff --cached --check
		git commit -m 'feat(sql): add keyset pagination'
		;;
	push)
		check_staged
		git push
		;;
	*)
		printf 'usage: %s {apply|check|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
