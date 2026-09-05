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

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-read-stats-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-read-stats-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-read-stats-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	ROW_BINARY_READ_STATS.md \
	hat/hatSql/row_binary_read_stats.go \
	hat/hatSql/row_binary_read_stats_test.go \
	scripts/benchmark-sql-row-binary-read-stats.sh \
	scripts/deliver-sql-row-binary-read-stats.sh \
	scripts/format-sql-row-binary-read-stats.sh \
	scripts/test-race-sql-row-binary-read-stats.sh \
	scripts/test-sql-row-binary-read-stats.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: test-sql-row-binary-read-stats\ntest-sql-row-binary-read-stats:\n\tsh ./scripts/test-sql-row-binary-read-stats.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: format-sql-row-binary-read-stats\nformat-sql-row-binary-read-stats:\n\tsh ./scripts/format-sql-row-binary-read-stats.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-row-binary-read-stats\ntest-race-sql-row-binary-read-stats:\n\tsh ./scripts/test-race-sql-row-binary-read-stats.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-row-binary-read-stats\nbenchmark-sql-row-binary-read-stats:\n\tsh ./scripts/benchmark-sql-row-binary-read-stats.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-row-binary-read-stats\ndeliver-sql-row-binary-read-stats:\n\tsh ./scripts/deliver-sql-row-binary-read-stats.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: commit-sql-row-binary-read-stats\ncommit-sql-row-binary-read-stats:\n\tsh ./scripts/deliver-sql-row-binary-read-stats.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-row-binary-read-stats:' "$makefile"; then
	printf '\n.PHONY: push-sql-row-binary-read-stats\npush-sql-row-binary-read-stats:\n\tsh ./scripts/deliver-sql-row-binary-read-stats.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'C046a' "$inspiration"; then
	sed -i '/^- \[ \] C046 /a - [x] C046a Statistics-only per-column RowBinary read accounting.' "$inspiration"
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
	git commit -m 'feat(sql): add RowBinary read amplification stats'
	exit 0
fi
git push
