#!/bin/sh
set -eu

mode="${1:-status}"
case "$mode" in
status)
	git status --short
	git diff --cached --check
	;;
apply)
	git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md SUBSCRIPTION_FRONTIERS.md \
		hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/subscription.go \
		hat/hatSql/subscription_frontier_test.go scripts/benchmark-sql-subscription-frontier.sh \
		scripts/format-sql-subscription-frontier.sh scripts/test-sql-subscription-frontier.sh \
		scripts/deliver-subscription-frontiers.sh
	patch_file="$(mktemp /tmp/hatrie-subscription-frontiers.XXXXXX)"
	additions_file="$(mktemp /tmp/hatrie-subscription-frontiers-additions.XXXXXX)"
	trap 'rm -f "$patch_file" "$additions_file"' EXIT
	printf '%s\n' \
		'' \
		'test-sql-subscription-frontier:' \
		'\tsh ./scripts/test-sql-subscription-frontier.sh' \
		'' \
		'format-sql-subscription-frontier:' \
		'\tsh ./scripts/format-sql-subscription-frontier.sh' \
		'' \
		'benchmark-sql-subscription-frontier:' \
		'\tsh ./scripts/benchmark-sql-subscription-frontier.sh' \
		'' \
		'deliver-subscription-frontiers:' \
		'\tsh ./scripts/deliver-subscription-frontiers.sh apply' \
		'' \
		'commit-subscription-frontiers:' \
		'\tsh ./scripts/deliver-subscription-frontiers.sh commit' \
		'' \
		'push-subscription-frontiers:' \
		'\tsh ./scripts/deliver-subscription-frontiers.sh push' \
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
	git commit -m 'feat: add SQL subscription frontiers'
	;;
push)
	git push origin master
	;;
*)
	echo "usage: $0 {status|apply|commit|push}" >&2
	exit 2
	;;
esac
