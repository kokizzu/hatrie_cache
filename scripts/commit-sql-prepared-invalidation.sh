#!/bin/sh
set -eu

root=$(git rev-parse --show-toplevel)
cd "$root"

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
	exit 1
fi

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

git show HEAD:Makefile | awk '
/^audit-extensibility-goal:/ && !inserted {
	print "commit-sql-prepared-invalidation:"
	print "\tsh ./scripts/commit-sql-prepared-invalidation.sh"
	print ""
	inserted = 1
}
{ print }
END {
	if (!inserted) {
		print "could not find Makefile insertion point" > "/dev/stderr"
		exit 1
	}
}' > "$tmp"

blob=$(git hash-object -w "$tmp")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git add -- \
	BENCHMARK.md \
	INSPIRATION.md \
	hat/hatSql/prepared_cache_key.go \
	hat/hatSql/prepared_cache_normalized_test.go \
	scripts/commit-sql-prepared-invalidation.sh

expected='BENCHMARK.md
INSPIRATION.md
Makefile
hat/hatSql/prepared_cache_key.go
hat/hatSql/prepared_cache_normalized_test.go
scripts/commit-sql-prepared-invalidation.sh'
actual=$(git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'refusing to commit: staged paths are not limited to C103' >&2
	printf '%s\n' "$actual" >&2
	exit 1
fi

git diff --cached --check
git commit -m 'feat: add SQL prepared-plan invalidation hooks'
git push origin master
