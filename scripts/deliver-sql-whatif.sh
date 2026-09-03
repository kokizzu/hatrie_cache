#!/bin/sh
set -eu

mode=${1:-check}
feature_files='ADOPTED_QUERY_ENGINE_IDEAS.md
BENCHMARK.md
README.md
SQL_WHATIF.md
hat/hatCache/sql_query.go
hat/hatCache/sql_whatif_test.go
hat/hatSql/whatif.go
hat/hatSql/whatif_benchmark_test.go
hat/hatSql/whatif_test.go
scripts/benchmark-sql-whatif.sh
scripts/format-sql-whatif.sh
scripts/generate-root-api.sh
scripts/test-sql-whatif.sh
scripts/verify-sql-whatif.sh
scripts/deliver-sql-whatif.sh'

is_allowed() {
	case "$1" in
		Makefile|api.go|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|README.md|SQL_WHATIF.md|hat/hatCache/sql_query.go|hat/hatCache/sql_whatif_test.go|hat/hatSql/whatif.go|hat/hatSql/whatif_benchmark_test.go|hat/hatSql/whatif_test.go|scripts/benchmark-sql-whatif.sh|scripts/format-sql-whatif.sh|scripts/generate-root-api.sh|scripts/test-sql-whatif.sh|scripts/verify-sql-whatif.sh|scripts/deliver-sql-whatif.sh|scripts/test-sql-whatif.sh)
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
			printf 'refusing SQL what-if delivery with unrelated staged path: %s\n' "$path" >&2
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
	stage_append Makefile '\n\ngen-root-api:\n\tsh ./scripts/generate-root-api.sh\n\nformat-sql-whatif:\n\tsh ./scripts/format-sql-whatif.sh\n\ntest-sql-whatif:\n\tsh ./scripts/test-sql-whatif.sh\n\nbenchmark-sql-whatif:\n\tsh ./scripts/benchmark-sql-whatif.sh\n\nverify-sql-whatif:\n\tsh ./scripts/verify-sql-whatif.sh\n\ndeliver-sql-whatif:\n\tsh ./scripts/deliver-sql-whatif.sh apply\n\ncommit-sql-whatif:\n\tsh ./scripts/deliver-sql-whatif.sh commit\n\npush-sql-whatif:\n\tsh ./scripts/deliver-sql-whatif.sh push\n'
	stage_append api.go '\n\ntype SQLWhatIfFieldStatistics = core.SQLWhatIfFieldStatistics\ntype SQLWhatIfIndex = core.SQLWhatIfIndex\ntype SQLWhatIfIndexKind = core.SQLWhatIfIndexKind\ntype SQLWhatIfReport = core.SQLWhatIfReport\ntype SQLWhatIfRequest = core.SQLWhatIfRequest\ntype SQLWhatIfSourceStatistics = core.SQLWhatIfSourceStatistics\ntype SQLWhatIfSourceStatisticsResolver = core.SQLWhatIfSourceStatisticsResolver\n\nconst SQLWhatIfIndexEquality = core.SQLWhatIfIndexEquality\nconst SQLWhatIfIndexGroup = core.SQLWhatIfIndexGroup\nconst SQLWhatIfIndexOrder = core.SQLWhatIfIndexOrder\nconst SQLWhatIfIndexRange = core.SQLWhatIfIndexRange\n\nvar ExplainSQLWhatIf = core.ExplainSQLWhatIf\n'
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
		git commit -m 'feat(sql): add hypothetical index advisor'
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
