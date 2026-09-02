#!/bin/sh
set -eu

mode="${1:-status}"
case "$mode" in
status)
	git status --short
	git diff --cached --check
	;;
apply)
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md TRANSPARENT_PROJECTIONS.md \
		hat/hatSql/materialized.go hat/hatSql/query.go hat/hatSql/projection_selection_test.go \
		scripts/benchmark-sql-projection-selection.sh scripts/format-sql-projection-selection.sh \
		scripts/test-sql-projection-selection.sh scripts/deliver-transparent-projections.sh
	patch_file="$(mktemp /tmp/hatrie-transparent-projections.XXXXXX)"
	additions_file="$(mktemp /tmp/hatrie-transparent-projections-additions.XXXXXX)"
	trap 'rm -f "$patch_file" "$additions_file"' EXIT
	printf '%s\n' \
		'' \
		'test-sql-projection-selection:' \
		'\tsh ./scripts/test-sql-projection-selection.sh' \
		'' \
		'benchmark-sql-projection-selection:' \
		'\tsh ./scripts/benchmark-sql-projection-selection.sh' \
		'' \
		'format-sql-projection-selection:' \
		'\tsh ./scripts/format-sql-projection-selection.sh' \
		'' \
		'deliver-transparent-projections:' \
		'\tsh ./scripts/deliver-transparent-projections.sh apply' \
		'' \
		'commit-transparent-projections:' \
		'\tsh ./scripts/deliver-transparent-projections.sh commit' \
		'' \
		'push-transparent-projections:' \
		'\tsh ./scripts/deliver-transparent-projections.sh push' \
		> "$additions_file"
	line_count="$(git show HEAD:Makefile | wc -l)"
	last_line="$(git show HEAD:Makefile | tail -n 1)"
	addition_count="$(wc -l < "$additions_file")"
	{
		printf '%s\n' 'diff --git a/Makefile b/Makefile' '--- a/Makefile' '+++ b/Makefile'
		printf '@@ -%s +%s,%s @@\n' "$line_count" "$line_count" "$((addition_count + 1))"
		printf ' %s\n' "$last_line"
		while IFS= read -r line; do
			printf '+%s\n' "$line"
		done < "$additions_file"
	} > "$patch_file"
	git apply --cached "$patch_file"
	git diff --cached --check
	;;
commit)
	git diff --cached --check
	git commit -m 'feat: add opt-in transparent SQL projections'
	;;
push)
	git push origin master
	;;
*)
	echo "usage: $0 {status|apply|commit|push}" >&2
	exit 2
	;;
esac
