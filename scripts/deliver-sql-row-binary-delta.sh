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

repo=$(pwd)
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-delta-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-delta-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-delta-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	ROW_BINARY_DELTA.md \
	hat/hatSql/row_binary_delta_codec.go \
	hat/hatSql/row_binary_delta_codec_test.go \
	scripts/benchmark-sql-row-binary-delta.sh \
	scripts/format-sql-row-binary-delta.sh \
	scripts/test-race-sql-row-binary-delta.sh \
	scripts/test-sql-row-binary-delta.sh \
	scripts/deliver-sql-row-binary-delta.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: test-sql-row-binary-delta\ntest-sql-row-binary-delta:\n\tsh ./scripts/test-sql-row-binary-delta.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: format-sql-row-binary-delta\nformat-sql-row-binary-delta:\n\tsh ./scripts/format-sql-row-binary-delta.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-row-binary-delta\nbenchmark-sql-row-binary-delta:\n\tsh ./scripts/benchmark-sql-row-binary-delta.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-row-binary-delta\ntest-race-sql-row-binary-delta:\n\tsh ./scripts/test-race-sql-row-binary-delta.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-row-binary-delta\ndeliver-sql-row-binary-delta:\n\tsh ./scripts/deliver-sql-row-binary-delta.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: commit-sql-row-binary-delta\ncommit-sql-row-binary-delta:\n\tsh ./scripts/deliver-sql-row-binary-delta.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-row-binary-delta:' "$makefile"; then
	printf '\n.PHONY: push-sql-row-binary-delta\npush-sql-row-binary-delta:\n\tsh ./scripts/deliver-sql-row-binary-delta.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
sed -i \
	-e 's/^- \[ \] C055 /- [x] C055 /' \
	-e 's/^- \[ \] C056 /- [x] C056 /' \
	"$inspiration"
inspiration_blob=$(git hash-object -w "$inspiration")
git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

printf '%s\n' 'Isolated delivery diff:'
git diff --cached --name-status
git diff --cached --stat
if [ "$mode" = preview ]; then
	exit 0
fi
if [ "$mode" = commit ]; then
	git commit -m 'feat(sql): add opt-in delta RowBinary codecs'
	exit 0
fi
git push
