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

index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-subscription-index.XXXXXX")
makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-subscription-makefile.XXXXXX")
inspiration=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-subscription-inspiration.XXXXXX")
trap 'rm -f "$index" "$makefile" "$inspiration"' EXIT
rm -f "$index"
export GIT_INDEX_FILE="$index"

git read-tree HEAD
git add -- \
	SUBSCRIPTION_STATUS.md \
	hat/hatSql/subscription_status.go \
	hat/hatSql/subscription_status_test.go \
	scripts/benchmark-sql-subscription-status.sh \
	scripts/deliver-sql-subscription-status.sh \
	scripts/format-sql-subscription-status.sh \
	scripts/test-race-sql-subscription-status.sh \
	scripts/test-sql-subscription-status.sh

git show HEAD:Makefile > "$makefile"
if ! grep -q '^test-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: test-sql-subscription-status\ntest-sql-subscription-status:\n\tsh ./scripts/test-sql-subscription-status.sh\n' >> "$makefile"
fi
if ! grep -q '^format-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: format-sql-subscription-status\nformat-sql-subscription-status:\n\tsh ./scripts/format-sql-subscription-status.sh\n' >> "$makefile"
fi
if ! grep -q '^test-race-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: test-race-sql-subscription-status\ntest-race-sql-subscription-status:\n\tsh ./scripts/test-race-sql-subscription-status.sh\n' >> "$makefile"
fi
if ! grep -q '^benchmark-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: benchmark-sql-subscription-status\nbenchmark-sql-subscription-status:\n\tsh ./scripts/benchmark-sql-subscription-status.sh\n' >> "$makefile"
fi
if ! grep -q '^deliver-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: deliver-sql-subscription-status\ndeliver-sql-subscription-status:\n\tsh ./scripts/deliver-sql-subscription-status.sh preview\n' >> "$makefile"
fi
if ! grep -q '^commit-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: commit-sql-subscription-status\ncommit-sql-subscription-status:\n\tsh ./scripts/deliver-sql-subscription-status.sh commit\n' >> "$makefile"
fi
if ! grep -q '^push-sql-subscription-status:' "$makefile"; then
	printf '\n.PHONY: push-sql-subscription-status\npush-sql-subscription-status:\n\tsh ./scripts/deliver-sql-subscription-status.sh push\n' >> "$makefile"
fi
makefile_blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$inspiration"
if ! grep -q 'M083a' "$inspiration"; then
	sed -i '/^- \[ \] M083 /a - [x] M083a On-demand per-subscription frontier and lag status.' "$inspiration"
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
	git commit -m 'feat(sql): expose subscription frontier status'
	exit 0
fi
git push
