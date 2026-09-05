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

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-string-dictionary-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-string-dictionary-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-string-dictionary-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	STRING_DICTIONARY.md \
	hat/hatSql/string_dictionary.go \
	hat/hatSql/string_dictionary_test.go \
	scripts/benchmark-sql-string-dictionary.sh \
	scripts/deliver-sql-string-dictionary.sh \
	scripts/format-sql-string-dictionary.sh \
	scripts/test-race-sql-string-dictionary.sh \
	scripts/test-sql-string-dictionary.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: test-sql-string-dictionary\ntest-sql-string-dictionary:\n\tsh ./scripts/test-sql-string-dictionary.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: format-sql-string-dictionary\nformat-sql-string-dictionary:\n\tsh ./scripts/format-sql-string-dictionary.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-string-dictionary\ntest-race-sql-string-dictionary:\n\tsh ./scripts/test-race-sql-string-dictionary.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-string-dictionary\nbenchmark-sql-string-dictionary:\n\tsh ./scripts/benchmark-sql-string-dictionary.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-string-dictionary\ndeliver-sql-string-dictionary:\n\tsh ./scripts/deliver-sql-string-dictionary.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: commit-sql-string-dictionary\ncommit-sql-string-dictionary:\n\tsh ./scripts/deliver-sql-string-dictionary.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-string-dictionary:' "$makefile"; then
	printf '\n.PHONY: push-sql-string-dictionary\npush-sql-string-dictionary:\n\tsh ./scripts/deliver-sql-string-dictionary.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'M096a' "$inspiration"; then
	sed -i '/^- \[ \] M096 /a - [x] M096a Deterministic low-cardinality string dictionary codec.' "$inspiration"
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
	git commit -m 'feat(sql): add string dictionary codec'
	exit 0
fi
git push
