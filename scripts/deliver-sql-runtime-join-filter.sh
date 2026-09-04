#!/bin/sh
set -eu

commit_message='feat(sql): add runtime join filtering'
expected_files='ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md Makefile README.md SQL_RUNTIME_JOIN_FILTER.md hat/hatSql/query.go hat/hatSql/runtime_join_filter_benchmark_test.go hat/hatSql/runtime_join_filter_test.go scripts/benchmark-sql-runtime-join-filter.sh scripts/deliver-sql-runtime-join-filter.sh scripts/format-sql-runtime-join-filter.sh scripts/test-sql-runtime-join-filter.sh'

is_expected_file() {
	case "$1" in
		ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|Makefile|README.md|SQL_RUNTIME_JOIN_FILTER.md|hat/hatSql/query.go|hat/hatSql/runtime_join_filter_benchmark_test.go|hat/hatSql/runtime_join_filter_test.go|scripts/benchmark-sql-runtime-join-filter.sh|scripts/deliver-sql-runtime-join-filter.sh|scripts/format-sql-runtime-join-filter.sh|scripts/test-sql-runtime-join-filter.sh)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

is_staged() {
	path_to_find=$1
	staged_paths=$(git diff --cached --name-only)
	for staged_path in $staged_paths; do
		if [ "$staged_path" = "$path_to_find" ]; then
			return 0
		fi
	done
	return 1
}

check_staged() {
	staged=$(git diff --cached --name-only)
	for path in $staged; do
		if ! is_expected_file "$path"; then
			printf 'unexpected staged path: %s\n' "$path" >&2
			return 1
		fi
		done
	for path in $expected_files; do
		if ! is_staged "$path"; then
				printf 'missing staged path: %s\n' "$path" >&2
				return 1
		fi
	done
	git diff --cached --check
}

stage_makefile_hunk() {
	tmpdir=$(mktemp -d)
	trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM
	mkdir -p "$tmpdir/base" "$tmpdir/next"
	git show HEAD:Makefile > "$tmpdir/base/Makefile"
	awk '
		{
			print
			if ($0 == "\tsh ./scripts/test-sql-borrowed-indexed-join.sh") {
				print ""
				print "test-sql-runtime-join-filter:"
				print "\tsh ./scripts/test-sql-runtime-join-filter.sh"
				print ""
				print "benchmark-sql-runtime-join-filter:"
				print "\tsh ./scripts/benchmark-sql-runtime-join-filter.sh"
				print ""
				print "format-sql-runtime-join-filter:"
				print "\tsh ./scripts/format-sql-runtime-join-filter.sh"
				print ""
				print "deliver-sql-runtime-join-filter:"
				print "\tsh ./scripts/deliver-sql-runtime-join-filter.sh apply"
				print ""
				print "check-sql-runtime-join-filter-stage:"
				print "\tsh ./scripts/deliver-sql-runtime-join-filter.sh check"
				print ""
				print "commit-sql-runtime-join-filter:"
				print "\tsh ./scripts/deliver-sql-runtime-join-filter.sh commit"
				print ""
				print "push-sql-runtime-join-filter:"
				print "\tsh ./scripts/deliver-sql-runtime-join-filter.sh push"
			}
		}
	' "$tmpdir/base/Makefile" > "$tmpdir/next/Makefile"
	patch_file="$tmpdir/runtime-join-filter.patch"
	set +e
	git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$tmpdir/base/Makefile" "$tmpdir/next/Makefile" > "$patch_file"
	diff_status=$?
	set -e
	if [ "$diff_status" -ne 1 ]; then
		printf 'unexpected Makefile patch status: %s\n' "$diff_status" >&2
		return 1
	fi
	sed -i "s#a${tmpdir}/base/#a/#g; s#b${tmpdir}/next/#b/#g" "$patch_file"
	git apply --cached "$patch_file"
}

stage_feature() {
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md README.md SQL_RUNTIME_JOIN_FILTER.md hat/hatSql/query.go hat/hatSql/runtime_join_filter_benchmark_test.go hat/hatSql/runtime_join_filter_test.go scripts/benchmark-sql-runtime-join-filter.sh scripts/deliver-sql-runtime-join-filter.sh scripts/format-sql-runtime-join-filter.sh scripts/test-sql-runtime-join-filter.sh
	if ! is_staged Makefile; then
		stage_makefile_hunk
	fi
	check_staged
}

case "${1:-}" in
	apply|stage)
		stage_feature
		;;
	check)
		check_staged
		;;
	commit)
		check_staged
		git commit -m "$commit_message"
		;;
	push)
		branch=$(git branch --show-current)
	if [ -z "$branch" ]; then
		printf '%s\n' 'cannot push without a checked-out branch' >&2
		exit 1
	fi
	git push origin "$branch"
		;;
	*)
		printf 'usage: %s {apply|check|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
