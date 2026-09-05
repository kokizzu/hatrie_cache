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
	print "test-sql-result-cache:"
	print "\tsh ./scripts/test-sql-result-cache.sh"
	print ""
	print "format-sql-result-cache:"
	print "\tsh ./scripts/format-sql-result-cache.sh"
	print ""
	print "benchmark-sql-result-cache:"
	print "\tsh ./scripts/benchmark-sql-result-cache.sh"
	print ""
	print "commit-sql-result-cache:"
	print "\tsh ./scripts/commit-sql-result-cache.sh"
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
	hat/hatCache/sql_result_cache_test.go \
	scripts/benchmark-sql-result-cache.sh \
	scripts/commit-sql-result-cache.sh \
	scripts/format-sql-result-cache.sh \
	scripts/test-sql-result-cache.sh

expected='BENCHMARK.md
INSPIRATION.md
Makefile
hat/hatCache/sql_result_cache_test.go
scripts/benchmark-sql-result-cache.sh
scripts/commit-sql-result-cache.sh
scripts/format-sql-result-cache.sh
scripts/test-sql-result-cache.sh'
actual=$(git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'refusing to commit: staged paths are not limited to SQL result-cache coverage' >&2
	printf '%s\n' "$actual" >&2
	exit 1
fi

git diff --cached --check
git commit -m 'test: cover parameterized SQL result cache reuse'
git push origin master
