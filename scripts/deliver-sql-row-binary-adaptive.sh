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

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-adaptive-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-adaptive-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-adaptive-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	ROW_BINARY_ADAPTIVE.md \
	hat/hatSql/row_binary_adaptive_codec.go \
	hat/hatSql/row_binary_adaptive_codec_test.go \
	scripts/benchmark-sql-row-binary-adaptive.sh \
	scripts/deliver-sql-row-binary-adaptive.sh \
	scripts/format-sql-row-binary-adaptive.sh \
	scripts/test-race-sql-row-binary-adaptive.sh \
	scripts/test-sql-row-binary-adaptive.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: test-sql-row-binary-adaptive\ntest-sql-row-binary-adaptive:\n\tsh ./scripts/test-sql-row-binary-adaptive.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: format-sql-row-binary-adaptive\nformat-sql-row-binary-adaptive:\n\tsh ./scripts/format-sql-row-binary-adaptive.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-row-binary-adaptive\ntest-race-sql-row-binary-adaptive:\n\tsh ./scripts/test-race-sql-row-binary-adaptive.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-row-binary-adaptive\nbenchmark-sql-row-binary-adaptive:\n\tsh ./scripts/benchmark-sql-row-binary-adaptive.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-row-binary-adaptive\ndeliver-sql-row-binary-adaptive:\n\tsh ./scripts/deliver-sql-row-binary-adaptive.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: commit-sql-row-binary-adaptive\ncommit-sql-row-binary-adaptive:\n\tsh ./scripts/deliver-sql-row-binary-adaptive.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-row-binary-adaptive:' "$makefile"; then
	printf '\n.PHONY: push-sql-row-binary-adaptive\npush-sql-row-binary-adaptive:\n\tsh ./scripts/deliver-sql-row-binary-adaptive.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'C048a' "$inspiration"; then
	sed -i '/^- \[ \] C048 /a - [x] C048a Exact full-batch codec selection by encoded size.' "$inspiration"
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
	git commit -m 'feat(sql): add adaptive RowBinary codec selection'
	exit 0
fi
git push
