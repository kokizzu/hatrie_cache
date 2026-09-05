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

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-differential-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-differential-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-differential-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	DIFFERENTIAL_ROWS.md \
	hat/hatSql/differential_rows.go \
	hat/hatSql/differential_rows_test.go \
	scripts/benchmark-sql-differential-rows.sh \
	scripts/deliver-sql-differential-rows.sh \
	scripts/format-sql-differential-rows.sh \
	scripts/test-race-sql-differential-rows.sh \
	scripts/test-sql-differential-rows.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: test-sql-differential-rows\ntest-sql-differential-rows:\n\tsh ./scripts/test-sql-differential-rows.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: format-sql-differential-rows\nformat-sql-differential-rows:\n\tsh ./scripts/format-sql-differential-rows.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-differential-rows\ntest-race-sql-differential-rows:\n\tsh ./scripts/test-race-sql-differential-rows.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-differential-rows\nbenchmark-sql-differential-rows:\n\tsh ./scripts/benchmark-sql-differential-rows.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-differential-rows\ndeliver-sql-differential-rows:\n\tsh ./scripts/deliver-sql-differential-rows.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: commit-sql-differential-rows\ncommit-sql-differential-rows:\n\tsh ./scripts/deliver-sql-differential-rows.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-differential-rows:' "$makefile"; then
	printf '\n.PHONY: push-sql-differential-rows\npush-sql-differential-rows:\n\tsh ./scripts/deliver-sql-differential-rows.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'M002a' "$inspiration"; then
	sed -i '/^- \[ \] M002 /a - [x] M002a Exported differential row batch representation.' "$inspiration"
fi
if ! grep -q 'M009a' "$inspiration"; then
	sed -i '/^- \[ \] M009 /a - [x] M009a Overflow-safe batch consolidation by key and time.' "$inspiration"
fi
if ! grep -q 'M037a' "$inspiration"; then
	sed -i '/^- \[ \] M037 /a - [x] M037a Signed negative diffs in the reusable batch primitive.' "$inspiration"
fi
if ! grep -q 'M038a' "$inspiration"; then
	sed -i '/^- \[ \] M038 /a - [x] M038a Duplicate multiplicity retained as signed diff weights.' "$inspiration"
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
	git commit -m 'feat(sql): add differential row batch primitive'
	exit 0
fi
git push
