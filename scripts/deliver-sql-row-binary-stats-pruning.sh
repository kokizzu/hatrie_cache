#!/bin/sh
set -eu

mode=${1:-preview}
case "$mode" in
preview|commit|push)
	;;
*)
	printf '%s\n' "usage: $0 [preview|commit|push]" >&2
	exit 2
	;;
esac

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-stats-pruning-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-stats-pruning-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-stats-pruning-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	ROW_BINARY_STATS_PRUNING.md \
	hat/hatSql/row_binary_stats_pruning.go \
	hat/hatSql/row_binary_stats_pruning_test.go \
	scripts/benchmark-sql-row-binary-stats-pruning.sh \
	scripts/deliver-sql-row-binary-stats-pruning.sh \
	scripts/format-sql-row-binary-stats-pruning.sh \
	scripts/test-race-sql-row-binary-stats-pruning.sh \
	scripts/test-sql-row-binary-stats-pruning.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: test-sql-row-binary-stats-pruning\ntest-sql-row-binary-stats-pruning:\n\tsh ./scripts/test-sql-row-binary-stats-pruning.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: format-sql-row-binary-stats-pruning\nformat-sql-row-binary-stats-pruning:\n\tsh ./scripts/format-sql-row-binary-stats-pruning.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-row-binary-stats-pruning\ntest-race-sql-row-binary-stats-pruning:\n\tsh ./scripts/test-race-sql-row-binary-stats-pruning.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-row-binary-stats-pruning\nbenchmark-sql-row-binary-stats-pruning:\n\tsh ./scripts/benchmark-sql-row-binary-stats-pruning.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-row-binary-stats-pruning\ndeliver-sql-row-binary-stats-pruning:\n\tsh ./scripts/deliver-sql-row-binary-stats-pruning.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: commit-sql-row-binary-stats-pruning\ncommit-sql-row-binary-stats-pruning:\n\tsh ./scripts/deliver-sql-row-binary-stats-pruning.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-row-binary-stats-pruning:' "$makefile"; then
	printf '\n.PHONY: push-sql-row-binary-stats-pruning\npush-sql-row-binary-stats-pruning:\n\tsh ./scripts/deliver-sql-row-binary-stats-pruning.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'C047a' "$inspiration"; then
	sed -i '/^- \[ \] C047 /a - [x] C047a Conservative block min/max predicate pruning.' "$inspiration"
fi
inspiration_blob=$(git hash-object -w "$inspiration")
git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

printf '%s\n' 'Isolated delivery diff:'
git diff --cached --name-status
git diff --cached --stat
if [ "$mode" = preview ]; then
	exit 0
fi
if [ "$mode" = commit ]; then
	git commit -m 'feat(sql): add RowBinary statistics pruning'
	exit 0
fi
git push
