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

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-bitmap-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-bitmap-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-bitmap-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	ROW_BINARY_NULLABLE_BITMAP.md \
	hat/hatSql/row_binary_nullable_bitmap.go \
	hat/hatSql/row_binary_nullable_bitmap_test.go \
	scripts/benchmark-sql-row-binary-bitmap.sh \
	scripts/deliver-sql-row-binary-bitmap.sh \
	scripts/format-sql-row-binary-bitmap.sh \
	scripts/test-race-sql-row-binary-bitmap.sh \
	scripts/test-sql-row-binary-bitmap.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: test-sql-row-binary-bitmap\ntest-sql-row-binary-bitmap:\n\tsh ./scripts/test-sql-row-binary-bitmap.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: format-sql-row-binary-bitmap\nformat-sql-row-binary-bitmap:\n\tsh ./scripts/format-sql-row-binary-bitmap.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-row-binary-bitmap\ntest-race-sql-row-binary-bitmap:\n\tsh ./scripts/test-race-sql-row-binary-bitmap.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-row-binary-bitmap\nbenchmark-sql-row-binary-bitmap:\n\tsh ./scripts/benchmark-sql-row-binary-bitmap.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-row-binary-bitmap\ndeliver-sql-row-binary-bitmap:\n\tsh ./scripts/deliver-sql-row-binary-bitmap.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: commit-sql-row-binary-bitmap\ncommit-sql-row-binary-bitmap:\n\tsh ./scripts/deliver-sql-row-binary-bitmap.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-row-binary-bitmap:' "$makefile"; then
	printf '\n.PHONY: push-sql-row-binary-bitmap\npush-sql-row-binary-bitmap:\n\tsh ./scripts/deliver-sql-row-binary-bitmap.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'C053a' "$inspiration"; then
	sed -i '/^- \[ \] C053 /a - [x] C053a Nullable-column bitmap RowBinary format.' "$inspiration"
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
	git commit -m 'feat(sql): add nullable bitmap RowBinary format'
	exit 0
fi
git push
