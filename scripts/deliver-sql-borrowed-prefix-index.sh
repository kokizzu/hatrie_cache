#!/bin/sh
set -eu

feature_files='ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
Makefile
SQL_LIKE_PREFIX_INDEX.md
api.go
hat/hatCache/sql_borrowed_index.go
hat/hatCache/sql_borrowed_prefix_index_test.go
hat/hatCache/sql_prefix_index_benchmark_test.go
hat/hatCache/sql_query.go
hat/hatSql/contracts.go
hat/hatSql/query.go
scripts/deliver-sql-borrowed-prefix-index.sh'

is_allowed() {
	case "$1" in
		ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|Makefile|SQL_LIKE_PREFIX_INDEX.md|api.go|hat/hatCache/sql_borrowed_index.go|hat/hatCache/sql_borrowed_prefix_index_test.go|hat/hatCache/sql_prefix_index_benchmark_test.go|hat/hatCache/sql_query.go|hat/hatSql/contracts.go|hat/hatSql/query.go|scripts/deliver-sql-borrowed-prefix-index.sh)
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
			printf 'refusing borrowed prefix delivery with unrelated staged path: %s\n' "$path" >&2
			exit 1
		fi
	done
}

check_expected_staged() {
	for path in $feature_files; do
		if git diff --cached --quiet -- "$path"; then
			printf 'borrowed prefix delivery did not stage expected path: %s\n' "$path" >&2
			exit 1
		fi
	done
}

check_feature_whitespace() {
	for path in $feature_files; do
		git diff --check -- "$path"
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
	check_feature_whitespace
	check_staged
	for path in $feature_files; do
		case "$path" in
			Makefile|api.go) ;;
			*) git add -- "$path" ;;
		esac
	done
	stage_append Makefile '\n\ndeliver-sql-borrowed-prefix-index:\n\tbash scripts/deliver-sql-borrowed-prefix-index.sh apply\n\ncheck-sql-borrowed-prefix-index-stage:\n\tbash scripts/deliver-sql-borrowed-prefix-index.sh check\n\ncommit-sql-borrowed-prefix-index:\n\tbash scripts/deliver-sql-borrowed-prefix-index.sh commit\n\npush-sql-borrowed-prefix-index:\n\tbash scripts/deliver-sql-borrowed-prefix-index.sh push\n'
	stage_append api.go '\ntype SQLBorrowedPrefixIndexedSourceResolver = core.SQLBorrowedPrefixIndexedSourceResolver\n'
	git diff --cached --check
	check_staged
	check_expected_staged
}

case "${1:-check}" in
	apply)
		stage_feature
		;;
	check)
		check_feature_whitespace
		check_staged
		git diff --cached --check
		check_expected_staged
		;;
	commit)
		check_staged
		git diff --cached --check
		check_expected_staged
		git commit -m 'feat(sql): borrow prefix index candidates'
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
