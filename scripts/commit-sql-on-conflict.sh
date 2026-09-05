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
	print "test-sql-mutation:"
	print "\tsh ./scripts/test-sql-mutation.sh"
	print ""
	print "format-sql-mutation:"
	print "\tsh ./scripts/format-sql-mutation.sh"
	print ""
	print "benchmark-sql-mutation:"
	print "\tsh ./scripts/benchmark-sql-mutation.sh"
	print ""
	print "commit-sql-on-conflict:"
	print "\tsh ./scripts/commit-sql-on-conflict.sh"
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
	README.md \
	hat/hatCache/sql.go \
	hat/hatCache/sql_test.go \
	hat/hatCache/sql_mutation_benchmark_test.go \
	scripts/benchmark-sql-mutation.sh \
	scripts/format-sql-mutation.sh \
	scripts/test-sql-mutation.sh \
	scripts/commit-sql-on-conflict.sh

expected='BENCHMARK.md
INSPIRATION.md
Makefile
README.md
hat/hatCache/sql.go
hat/hatCache/sql_mutation_benchmark_test.go
hat/hatCache/sql_test.go
scripts/benchmark-sql-mutation.sh
scripts/commit-sql-on-conflict.sh
scripts/format-sql-mutation.sh
scripts/test-sql-mutation.sh'
actual=$(git diff --cached --name-only)
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'refusing to commit: staged paths are not limited to ON CONFLICT' >&2
	printf '%s\n' "$actual" >&2
	exit 1
fi

git diff --cached --check
git commit -m 'feat: add SQL ON CONFLICT mutations'
git push origin master
